// Package reconcile compares what the Run machine believes about an effect
// with what the execution store actually holds, and turns the difference into
// Run commands or a second Dispatch (RUN-CMT-7, RUN-EXE-15). It is the
// process manager between the two authorities and the one place that reads
// three sources: the machine says which effects are outstanding (an
// Executing model step or tool call and the EffectID it requested), the
// executor says whether it still holds an attempt for that effect, and the
// dispatch ledger (agentcore/process) says how many times this effect was
// handed to the executor again. Per effect the Reconciler decides whether
// the Run keeps waiting for the attempt's Outcome, hands the effect to the
// executor again, or disposes it. Neither the Loop nor the store adapter
// interprets executor observations, and the Run never learns which attempt
// the executor made.
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/process"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/plan"
	"github.com/felinics/twilight/agentcore/run/runtime"
	"github.com/felinics/twilight/agentcore/run/schema"
)

// Verdict is the reconciler's decision for one Executing target.
type Verdict string

const (
	// Keep: the executor still holds an attempt for the effect (active, or
	// terminal with an Outcome to read), so the target stays Executing and
	// its Outcome settles under the effect's settlement identity.
	Keep Verdict = "keep"
	// Defer: the executor holds a durable record nobody is running (orphaned).
	// The target stays Executing and its Outcome is still awaited: nothing
	// proves the effect was absent. The Reconciler asks a port that can
	// (effect.Recoverer) to take the record back once; only an explicit
	// disposal ends the wait otherwise.
	Defer Verdict = "defer"
	// Dispose: the executor holds no attempt for the effect and none will be
	// made, so the Run recovers the target itself: an Executing model step
	// is withdrawn to Open, an Executing tool call settles as Unknown.
	Dispose Verdict = "dispose"
	// Redispatch: the executor held no attempt for the effect and was handed
	// the Assignment again within the budget (RUN-EXE-15). The target stays
	// Executing and its Outcome is awaited like a kept one.
	Redispatch Verdict = "redispatch"
)

// Decision is one target's verdict and, for Dispose, the recovery command.
type Decision struct {
	Target   plan.RecoveryTarget
	Observed effect.AttachmentState
	Verdict  Verdict
	Recovery *plan.RecoveryDisposition
}

// ErrNoExecutionPort reports a Plan over Executing targets with no executor
// to ask and no Abandon: an executor that cannot be reached proves nothing
// about the executions it may hold, so the targets cannot be disposed.
var ErrNoExecutionPort = errors.New("reconcile: executing targets but no execution port to ask; set Abandon to dispose without proof")

// ErrTargetWithoutEffect reports an Executing target whose start fact
// recorded no EffectID: the executor cannot be asked about it and the
// machine state is inconsistent (RUN-WIR-1).
var ErrTargetWithoutEffect = errors.New("reconcile: executing target records no effect")

// Reconciler is the recovery decision of one owner over a Scope.
type Reconciler struct {
	// Executions is the execution store's port, asked once per Executing
	// target. Nil with Abandon unset is an error whenever a target exists:
	// "no executor to ask" is not "no execution" (RUN-CMT-7). A port that
	// also implements effect.Recoverer is asked to take an orphaned record
	// back; one that does not leaves orphaned records to an external
	// controller.
	Executions effect.ExecutionPort
	// Abandon disposes every Executing target without asking an executor. It
	// is the caller's explicit statement that no executor holds anything for
	// this Scope (the executions died with the process that ran them, or the
	// deployment has decided to give them up); it is never inferred from an
	// unreachable or absent Executions. Abandon takes precedence over
	// Executions.
	Abandon bool
	// Deliver receives the Outcome of every kept target once it can be read;
	// the caller settles it through the Loop. Nil means kept targets stay
	// Executing until something else delivers their Outcome.
	Deliver func(effect.Outcome)
	// Fail receives a kept target whose Outcome can no longer be read: the
	// executor holds no record for it (effect.ErrExecutionNotFound), or reads
	// kept failing past ReadRetries. The target stays Executing; the next
	// RecoverInterrupted plans it again, and a record that is gone by then is
	// disposed. Nil discards the report.
	Fail func(effect.AssignmentKey, error)
	// ReadRetries bounds consecutive failed Outcome reads that are neither
	// ErrOutcomeNotReady (the execution is still running: waited for without
	// limit) nor definitive (stopped at once). Zero selects DefaultReadRetries.
	ReadRetries int
	// Lifetime bounds the background Outcome reads of kept targets.
	Lifetime context.Context

	// Redispatch hands the Assignment of an Executing effect the executor
	// holds nothing for to the executor again (loop.Redispatch on the
	// owner's Writer). Nil keeps RUN-CMT-7's plain disposal of missing
	// effects. With it, Attempts is required.
	Redispatch func(ctx context.Context, key effect.AssignmentKey) error
	// Attempts is the dispatch ledger: how many times each effect was
	// redispatched and whether the reconciler gave up (RUN-EXE-15).
	Attempts process.Store
	// Epoch fences the dispatch ledger: the Session owner's.
	Epoch ledger.Epoch
	// MaxRedispatches bounds redispatches per effect; zero selects
	// DefaultMaxRedispatches.
	MaxRedispatches int
	// Now stamps the dispatch ledger; nil selects time.Now.
	Now func() time.Time
}

// DefaultMaxRedispatches is the redispatch budget of one effect.
const DefaultMaxRedispatches = 3

// DefaultReadRetries is about a minute of failed reads at the 1s backoff cap.
const DefaultReadRetries = 60

// readVerdict classifies one failed Outcome read.
type readVerdict uint8

const (
	readWait       readVerdict = iota // the execution is still running
	readRetry                         // a read failed; the execution may still finish
	readDefinitive                    // nothing will ever be read for this key
)

// classifyRead is the error taxonomy of Outcome reads: what the executor
// says will never answer is definitive; a not-ready answer is the normal
// wait; anything else is a read failure to retry within the budget.
func classifyRead(err error) readVerdict {
	switch {
	case errors.Is(err, effect.ErrOutcomeNotReady):
		return readWait
	case errors.Is(err, effect.ErrExecutionNotFound), errors.Is(err, effect.ErrOutcomeUnavailable):
		return readDefinitive
	default:
		return readRetry
	}
}

// AssignmentFromTarget rebuilds the Assignment of an Executing target from
// the machine state, so the executor can be asked whether it still holds an
// attempt for the effect. Only the key and the digest-level description are
// known here; the inline request body never travels this way (RUN-EXE-7).
func AssignmentFromTarget(scope run.Scope, t plan.RecoveryTarget) effect.Assignment {
	a := effect.Assignment{Session: scope, RunID: t.RunID, StepID: t.StepID, CallID: t.CallID, Effect: t.Effect}
	switch {
	case t.Call != nil:
		a.Body = effect.ToolAssignment{ToolRef: t.Call.ToolRef, DefinitionDigest: t.Call.DefinitionDigest, Arguments: t.Call.Arguments, Policy: t.Call.Policy}
	case t.Model != nil:
		a.Body = effect.ModelAssignment{Model: t.Model.Model, RequestDigest: t.Model.RequestDigest}
	}
	return a
}

// verdictOf maps an executor observation onto the reconciler's vocabulary.
func verdictOf(state effect.AttachmentState) (Verdict, error) {
	switch state {
	case effect.AttachmentActive, effect.AttachmentTerminal:
		return Keep, nil
	case effect.AttachmentOrphaned:
		return Defer, nil
	case effect.AttachmentMissing:
		return Dispose, nil
	default:
		return Dispose, fmt.Errorf("reconcile: unknown attachment state %q", state)
	}
}

// Plan decides every Executing target of one Run. It asks the executor once
// per effect and starts the Outcome read of every effect it does not dispose;
// it writes nothing. The recovery command of a disposed effect is identified
// by the effect (RUN-WIR-1), so any owner that plans the same state issues
// the same command.
func (r *Reconciler) Plan(ctx context.Context, scope run.Scope, snapshot *runtime.Snapshot) ([]Decision, error) {
	targets := plan.RecoveryTargets(&snapshot.State)
	if len(targets) == 0 {
		return nil, nil
	}
	out := make([]Decision, 0, len(targets))
	for _, t := range targets {
		if t.Effect == "" {
			return nil, fmt.Errorf("%w: run %s step %s call %q", ErrTargetWithoutEffect, t.RunID, t.StepID, t.CallID)
		}
		d := Decision{Target: t, Observed: effect.AttachmentMissing, Verdict: Dispose}
		if !r.Abandon {
			if r.Executions == nil {
				return nil, ErrNoExecutionPort
			}
			assignment := AssignmentFromTarget(scope, t)
			attachment, err := r.Executions.Attach(ctx, assignment.Key())
			if err != nil {
				return nil, err
			}
			if !attachment.State.Valid() {
				return nil, fmt.Errorf("reconcile: executor returned attachment state %q", attachment.State)
			}
			d.Observed = attachment.State
			if d.Verdict, err = verdictOf(attachment.State); err != nil {
				return nil, err
			}
			if d.Verdict == Defer {
				r.recoverOrphan(ctx, assignment.Key())
			}
			if d.Verdict == Dispose {
				if d.Verdict, err = r.missing(ctx, assignment.Key()); err != nil {
					return nil, err
				}
			}
			if d.Verdict != Dispose {
				r.awaitOutcome(assignment.Key())
			}
		}
		if d.Verdict == Dispose {
			rec := plan.RecoveryCommand(schema.Identity, t)
			d.Recovery = &rec
		}
		out = append(out, d)
	}
	return out, nil
}

// missing decides an effect the executor holds nothing for: within the
// redispatch budget it is handed over again and stays Executing, otherwise
// it is disposed (RUN-EXE-15). Each redispatch is written to the dispatch
// ledger before it is made, so a crash in between costs one attempt and
// never an unrecorded Dispatch. A refusal the executor may lift later
// (ErrDispatchRetryable) or a lost response (ErrDispatchUnknown) leaves the
// target Executing for the next reconciliation; any other rejection ends
// the attempts.
func (r *Reconciler) missing(ctx context.Context, key effect.AssignmentKey) (Verdict, error) {
	if r.Redispatch == nil {
		return Dispose, nil
	}
	if r.Attempts == nil {
		return Dispose, errors.New("reconcile: Redispatch requires the dispatch ledger (Attempts)")
	}
	state, _, _, err := r.Attempts.Load(ctx, key)
	if err != nil {
		return Dispose, err
	}
	if state.GivenUp {
		return Dispose, nil
	}
	budget := r.MaxRedispatches
	if budget <= 0 {
		budget = DefaultMaxRedispatches
	}
	if state.Attempts >= budget {
		if err := process.GiveUp(ctx, r.Attempts, r.Epoch, key, fmt.Sprintf("redispatch budget of %d exhausted", budget), r.now()); err != nil {
			return Dispose, err
		}
		return Dispose, nil
	}
	if _, err := process.Attempt(ctx, r.Attempts, r.Epoch, key, r.now()); err != nil {
		return Dispose, err
	}
	err = r.Redispatch(ctx, key)
	switch {
	case err == nil:
		return Redispatch, nil
	case errors.Is(err, effect.ErrDispatchRetryable), errors.Is(err, effect.ErrDispatchUnknown):
		return Defer, nil
	default:
		if gerr := process.GiveUp(ctx, r.Attempts, r.Epoch, key, err.Error(), r.now()); gerr != nil {
			return Dispose, gerr
		}
		return Dispose, nil
	}
}

func (r *Reconciler) now() int64 {
	if r.Now != nil {
		return r.Now().UnixMilli()
	}
	return time.Now().UnixMilli()
}

// awaitOutcome reads the Outcome of a kept effect in the background and
// hands it to Deliver. A not-ready answer is waited for as long as Lifetime
// lasts; a read failure is retried with backoff up to ReadRetries times; a
// definitive answer (the executor holds nothing for the key) or an exhausted
// budget is reported through Fail and the watcher stops, so a target the
// executor will never answer for does not poll forever behind a stuck Turn.
func (r *Reconciler) awaitOutcome(key effect.AssignmentKey) {
	if r.Deliver == nil || r.Lifetime == nil {
		return
	}
	budget := r.ReadRetries
	if budget <= 0 {
		budget = DefaultReadRetries
	}
	go func() {
		delay := 10 * time.Millisecond
		failures := 0
		asked := false
		for {
			out, err := r.Executions.GetOutcome(r.Lifetime, key)
			if err == nil {
				if r.Lifetime.Err() == nil {
					r.Deliver(out)
				}
				return
			}
			if r.Lifetime.Err() != nil {
				return
			}
			switch classifyRead(err) {
			case readWait:
				failures = 0
				// An execution that stays not-ready may have lost its Worker
				// meanwhile. Once the backoff has settled, look at the record
				// and ask for recovery once per orphaned episode.
				if delay >= time.Second {
					r.probeOrphan(key, &asked)
				}
			case readDefinitive:
				r.fail(key, err)
				return
			case readRetry:
				failures++
				if failures >= budget {
					r.fail(key, fmt.Errorf("reconcile: outcome read gave up after %d failures: %w", failures, err))
					return
				}
			}
			timer := time.NewTimer(delay)
			select {
			case <-r.Lifetime.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if delay < time.Second {
				delay = min(delay*2, time.Second)
			}
		}
	}()
}

func (r *Reconciler) fail(key effect.AssignmentKey, err error) {
	if r.Fail != nil {
		r.Fail(key, err)
	}
}

// recoverOrphan asks the executor to take an orphaned record back, when the
// port can (effect.Recoverer). It is the Owner acting on its own Run's
// effect: the record was just observed orphaned, so this is the moment to
// ask. A failed or impossible recovery leaves the target deferred; giving
// the execution up is a separate decision, made through Dispose.
func (r *Reconciler) recoverOrphan(ctx context.Context, key effect.AssignmentKey) {
	if rec, ok := r.Executions.(effect.Recoverer); ok {
		_ = rec.RecoverExecution(ctx, key)
	}
}

// probeOrphan re-reads the record of a kept target that keeps answering
// not-ready and asks for recovery when it has become orphaned. asked keeps
// the request to once per orphaned episode; a record under a live lease
// again resets it.
func (r *Reconciler) probeOrphan(key effect.AssignmentKey, asked *bool) {
	if _, ok := r.Executions.(effect.Recoverer); !ok {
		return
	}
	attachment, err := r.Executions.Attach(r.Lifetime, key)
	if err != nil {
		return
	}
	switch attachment.State {
	case effect.AttachmentOrphaned:
		if !*asked {
			*asked = true
			r.recoverOrphan(r.Lifetime, key)
		}
	case effect.AttachmentActive:
		*asked = false
	}
}

// Apply commits the Dispose decisions through the bound store and returns
// how many were accepted. A decision another actor has already overtaken
// (stale, terminal, conflict) is skipped.
func Apply(ctx context.Context, store runtime.RunStore, decisions []Decision) (int, error) {
	n := 0
	for i := range decisions {
		d := &decisions[i]
		if d.Recovery == nil {
			continue
		}
		env, err := schema.Wire.Envelope(d.Target.RunID, d.Recovery.ID, d.Recovery.Command)
		if err != nil {
			return n, err
		}
		res, err := store.Commit(ctx, runtime.CommitRequest{Command: env})
		if err != nil {
			if errors.Is(err, run.ErrStaleRuntime) || errors.Is(err, run.ErrRunTerminal) || errors.Is(err, run.ErrCommandConflict) {
				continue
			}
			return n, err
		}
		if res.Status == runtime.CommitAccepted {
			n++
		}
	}
	return n, nil
}

// Reconcile is Plan then Apply for one Run: the takeover disposition of its
// Executing targets (RUN-CMT-7). It returns the number of accepted recovery
// commands.
func (r *Reconciler) Reconcile(ctx context.Context, store runtime.RunStore, snapshot *runtime.Snapshot) (int, error) {
	if r.Lifetime != nil {
		if err := r.Lifetime.Err(); err != nil {
			return 0, err
		}
	}
	decisions, err := r.Plan(ctx, store.Scope(), snapshot)
	if err != nil {
		return 0, err
	}
	return Apply(ctx, store, decisions)
}
