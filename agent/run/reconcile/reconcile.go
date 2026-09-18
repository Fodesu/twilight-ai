// Package reconcile compares what the Run machine believes about an execution
// with what the execution store actually holds, and turns the difference into
// Run commands (RUN-CMT-7). It is the one place that reads both sides: the
// machine says which targets are Executing and under which Claim, the
// executor says whether that attempt still exists, and the Reconciler decides
// per target whether the Run keeps waiting for the attempt's Outcome or
// disposes it. Neither the Loop nor the store adapter interprets executor
// observations.
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/effect"
	"github.com/felinics/twilight/agent/run/plan"
	"github.com/felinics/twilight/agent/run/runtime"
	"github.com/felinics/twilight/agent/run/schema"
)

// Verdict is the reconciler's decision for one Executing target.
type Verdict string

const (
	// Keep: the executor still holds the attempt (active, or terminal with an
	// Outcome to read), so the target stays Executing and its Outcome settles
	// under the original Claim.
	Keep Verdict = "keep"
	// Defer: the executor holds a durable record it cannot reach (orphaned).
	// The target stays Executing and its Outcome is still awaited: the
	// control plane may take the record over and finish it, and nothing
	// proves the effect was absent. Only an explicit disposal ends it.
	Defer Verdict = "defer"
	// Dispose: the executor knows nothing of the attempt, so the Run recovers
	// the target itself: an Executing model step is withdrawn to Open, an
	// Executing tool call settles as Unknown.
	Dispose Verdict = "dispose"
)

// Decision is one target's verdict and, for Dispose, the recovery command.
type Decision struct {
	Target   plan.RecoveryTarget
	Observed effect.AttachmentState
	Verdict  Verdict
	Recovery *plan.RecoveryDisposition
}

// Reconciler is the recovery control plane of one owner over a Scope.
type Reconciler struct {
	// Executions is the execution store's port. Nil means no executor is
	// reachable: every target is missing and disposed.
	Executions effect.ExecutionPort
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
}

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
// the machine state, so the executor can be asked whether that attempt still
// runs. Only the key and the digest-level description are known here; the
// inline request body never travels this way (RUN-EXE-7).
func AssignmentFromTarget(scope run.Scope, t plan.RecoveryTarget) effect.Assignment {
	a := effect.Assignment{Session: scope, RunID: t.RunID, StepID: t.StepID, CallID: t.CallID, Claim: t.Claim, Schema: t.Schema}
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

// Plan decides every Executing target of one Run under the owner's takeover
// claim. It asks the executor once per target and starts the Outcome read of
// every target it does not dispose; it writes nothing.
func (r *Reconciler) Plan(ctx context.Context, scope run.Scope, snapshot *runtime.Snapshot, claim run.ExecutionClaim) ([]Decision, error) {
	targets := plan.RecoveryTargets(&snapshot.State)
	if len(targets) == 0 {
		return nil, nil
	}
	sch, err := snapshot.Schema()
	if err != nil {
		return nil, err
	}
	out := make([]Decision, 0, len(targets))
	for _, t := range targets {
		t.Schema = snapshot.SchemaVersion
		d := Decision{Target: t, Observed: effect.AttachmentMissing, Verdict: Dispose}
		if r.Executions != nil && t.Claim != "" {
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
			if d.Verdict != Dispose {
				r.awaitOutcome(assignment.Key())
			}
		}
		if d.Verdict == Dispose {
			rec := plan.RecoveryCommand(sch.Identity, t, claim)
			d.Recovery = &rec
		}
		out = append(out, d)
	}
	return out, nil
}

// awaitOutcome reads the Outcome of a kept attempt in the background and
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

// Apply commits the Dispose decisions through the bound store and returns
// how many were accepted. A decision another actor has already overtaken
// (stale, terminal, conflict) is skipped.
func Apply(ctx context.Context, store runtime.RunStore, sch schema.Schema, decisions []Decision) (int, error) {
	n := 0
	for i := range decisions {
		d := &decisions[i]
		if d.Recovery == nil {
			continue
		}
		env, err := sch.Wire.Envelope(d.Target.RunID, d.Recovery.ID, d.Recovery.Command)
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
func (r *Reconciler) Reconcile(ctx context.Context, store runtime.RunStore, snapshot *runtime.Snapshot, claim run.ExecutionClaim) (int, error) {
	if r.Lifetime != nil {
		if err := r.Lifetime.Err(); err != nil {
			return 0, err
		}
	}
	decisions, err := r.Plan(ctx, store.Scope(), snapshot, claim)
	if err != nil {
		return 0, err
	}
	sch, err := snapshot.Schema()
	if err != nil {
		return 0, err
	}
	return Apply(ctx, store, sch, decisions)
}
