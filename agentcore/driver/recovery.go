package driver

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/reconcile"
	"github.com/felinics/twilight/agentcore/run/redispatch"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
)

// Recovery is the takeover supervisor of the Sessions this process owns.
// Open installs a Session's recovery lifetime and runs the takeover
// disposition: the reconciler compares every Executing effect with the
// execution store, keeps waiting for attempts that survived, hands missing
// effects to the Executor again within the redispatch budget or disposes
// them. An Outcome of a kept attempt settles through the Loop of the Turn
// that owns its Run and drives the Run on. Stop ends a Session's listeners;
// Close ends every Session's.
type Recovery struct {
	// Runs is the Run module's Session adapter; recovery binds it to the
	// Writer the Session was opened with.
	Runs        *sessionstore.SessionRunStore
	Executor    effect.ExecutionPort
	Loops       *Loops
	Compactions *Compactions
	// Driver continues reattached Runs through Session maintenance and waits.
	Driver *Driver
	// Watcher is where every Reconciler waits for Outcomes: the settlement
	// subscription shared with the Loops; required.
	Watcher *effect.Watcher
	// Fail receives failures of work done outside any caller's call, such as
	// settling a reattached Outcome; nil discards them.
	Fail func(session.SessionID, error)
	// MissingEffects is the takeover policy for an Executing effect the
	// Executor holds nothing for: the zero value disposes,
	// reconcile.RedispatchMissing redispatches within the budget and
	// requires Redispatches.
	MissingEffects reconcile.MissingPolicy
	// Redispatches is the dispatch ledger the reconciler writes before and
	// after it hands an effect to the Executor again; required under
	// RedispatchMissing, unused otherwise.
	Redispatches redispatch.Store
	// MaxRedispatches bounds redispatches per effect; zero selects the
	// reconciler's default.
	MaxRedispatches int
	// RedispatchRetry is the interval between reconciliations after the
	// executor refused a redispatch before the effect boundary. Unknown
	// boundaries are never automatically redispatched. Zero selects
	// DefaultRedispatchRetry.
	RedispatchRetry time.Duration
	// OrphanProbe is how often an effect still waiting is attached and, when
	// orphaned, handed to RecoverExecution by the Reconciler of a takeover;
	// zero selects its default.
	OrphanProbe time.Duration
	// Sink receives the provisional observations of a Run driven on after a
	// reattached Outcome; nil discards them.
	Sink loop.EventSink

	mu        sync.Mutex
	lifetimes map[session.SessionID]*lifetime
}

// lifetime is the detached context a Session's recovery goroutines --
// reattached outcome reads, resumed answers -- live under, and the cancel
// that stops them. w is the Writer the Session was opened with; everything
// the lifetime settles commits through it.
type lifetime struct {
	ctx         context.Context
	cancel      context.CancelFunc
	w           writer.Writer
	maintenance sync.WaitGroup

	retryMu sync.Mutex
	retries map[effect.AssignmentKey]struct{}
}

// DefaultRedispatchRetry bounds how long a live owner leaves an effect at a
// temporary redispatch boundary before reconciling it again.
const DefaultRedispatchRetry = time.Second

func (r *Recovery) fail(sid session.SessionID, err error) {
	if r.Fail != nil {
		// A callback may close the Session. Never run it on a Watcher or
		// continuation that Stop must join before releasing that Session.
		go r.Fail(sid, err)
	}
}

// installLocked replaces the Session's lifetime with one derived from
// parent, stopping the previous listeners. Callers hold r.mu.
func (r *Recovery) installLocked(w writer.Writer, parent context.Context) *lifetime {
	sid := w.SessionID()
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent)) //nolint:gosec // G118: the lifetime owns cancel; Stop and Close call it
	if previous := r.lifetimes[sid]; previous != nil {
		previous.cancel()
	}
	if r.lifetimes == nil {
		r.lifetimes = make(map[session.SessionID]*lifetime)
	}
	lt := &lifetime{ctx: ctx, cancel: cancel, w: w}
	r.lifetimes[sid] = lt
	return lt
}

// lifetimeOf returns the Session's lifetime, installing a detached one when
// absent. Open replaces it instead: a takeover supersedes the previous
// owner's listeners.
func (r *Recovery) lifetimeOf(w writer.Writer) *lifetime {
	r.mu.Lock()
	defer r.mu.Unlock()
	if lt, ok := r.lifetimes[w.SessionID()]; ok {
		return lt
	}
	return r.installLocked(w, context.Background())
}

// Open installs the Session's recovery lifetime under w. Accepted compaction
// work is settled before Run recovery can attach outcomes and resume building
// requests. Success is the handoff to normal driving: callers must await Open
// before driving this Writer. The count covers Run recovery commands only.
// Opening is bounded by both ctx and the Session lifetime (Stop/Close).
func (r *Recovery) Open(ctx context.Context, w writer.Writer) (n int, err error) {
	r.mu.Lock()
	lt := r.installLocked(w, ctx)
	lt.maintenance.Add(1)
	r.mu.Unlock()

	opening, cancel := context.WithCancel(ctx)
	stopCancel := context.AfterFunc(lt.ctx, cancel)
	defer func() {
		stopCancel()
		cancel()
		// Stop joins owned work, including this opening. Release our own
		// registration before cleaning up a failed opening's listeners.
		lt.maintenance.Done()
		if err != nil {
			r.Stop(w.SessionID())
		}
	}()

	var hadCompactionWork bool
	if r.Compactions != nil {
		hadCompactionWork, err = r.Compactions.NeedsRecovery(opening, w)
		if err != nil {
			return 0, err
		}
		if hadCompactionWork {
			if err := r.Compactions.Recover(opening, w); err != nil {
				return 0, err
			}
		}
	}
	// Use this opening's lifetime, rather than discovering/installing another
	// one if Stop has cancelled it. No normal outcome callback is registered
	// until the summary has been adopted (or durably disposed as stale).
	n, err = r.Runs.RecoverInterrupted(opening, w, r.reconciler(lt))
	if err != nil {
		return n, err
	}
	if err := opening.Err(); err != nil {
		return n, err
	}
	if hadCompactionWork {
		r.resumeAfterCompaction(lt)
	}
	return n, nil
}

// resumeAfterCompaction preserves automatic continuation of an active Turn,
// but never runs its summary in parallel with candidate construction. Idle
// Sessions need no continuation; their accepted work was settled by Open.
func (r *Recovery) resumeAfterCompaction(lt *lifetime) {
	r.runOwned(lt, func() error {
		surface, err := turn.ReadSurface(lt.ctx, lt.w.Projections(), lt.w.SessionID())
		if err != nil {
			return err
		}
		if active, ok := surface.Active(); ok {
			return r.drive(lt.ctx, lt.w, active.TurnID)
		}
		return nil
	})
}

// runOwned keeps Outcome callbacks short and joins their continuations before
// closing the Watcher. In particular a reattached tool outcome may initiate a
// summary, which must not wait on the Watcher's own read goroutine.
func (r *Recovery) runOwned(lt *lifetime, work func() error) {
	r.mu.Lock()
	if r.lifetimes[lt.w.SessionID()] != lt || lt.ctx.Err() != nil {
		r.mu.Unlock()
		return
	}
	lt.maintenance.Add(1)
	r.mu.Unlock()
	go func() {
		err := work()
		lt.maintenance.Done()
		if err != nil && lt.ctx.Err() == nil && !errors.Is(err, loop.ErrRunAlreadyRunning) {
			r.fail(lt.w.SessionID(), err)
		}
	}()
}

func (r *Recovery) drive(ctx context.Context, w writer.Writer, id turn.TurnID) error {
	drive := r.Driver
	if drive == nil {
		drive = &Driver{Runs: r.Runs, Loops: r.Loops, Recovery: r, Compactions: r.Compactions, Sink: r.Sink}
	}
	_, err := drive.Drive(ctx, w, id)
	return err
}

// Recover runs the takeover disposition for the Session: the reconciler
// compares every Executing target with the execution store and disposes
// only what no record answers for. It is the explicit recovery behind an
// unknown dispatch boundary -- a drive kept the call Executing, so the
// durable record, not a duplicate dispatch, decides the settlement.
func (r *Recovery) Recover(ctx context.Context, w writer.Writer) (int, error) {
	lt := r.lifetimeOf(w)
	return r.Runs.RecoverInterrupted(ctx, w, r.reconciler(lt))
}

// reconciler builds one pass over lt. A transient redispatch asks the
// lifetime's deduplicated interval worker to revisit only that effect.
func (r *Recovery) reconciler(lt *lifetime) *reconcile.Reconciler {
	sid := lt.w.SessionID()
	rec := &reconcile.Reconciler{Executions: r.Executor, Lifetime: lt.ctx, Watcher: r.Watcher, Deliver: r.reattachDeliver(lt),
		Fail: func(key effect.AssignmentKey, err error) {
			r.fail(sid, fmt.Errorf("driver: outcome of run %s effect %s cannot be read; the target stays executing until reconciliation: %w", key.RunID, key.Effect, err))
		}}
	rec.Missing = r.MissingEffects
	rec.OrphanProbe = r.OrphanProbe
	if r.MissingEffects == reconcile.RedispatchMissing {
		// Missing effects are handed to the Executor again within the
		// budget; the dispatch ledger remembers the attempts.
		rec.Attempts, rec.Epoch, rec.MaxRedispatches = r.Redispatches, lt.w.Epoch(), r.MaxRedispatches
		rec.Redispatch = func(ctx context.Context, key effect.AssignmentKey) error { return r.redispatch(ctx, lt.w, key) }
		rec.Requeue = func(key effect.AssignmentKey) { r.requeue(lt, key) }
	}
	return rec
}

// requeue starts at most one cancellable reconciliation worker for a safely
// retryable refusal in this Session lifetime. Every pass reloads the durable
// Run snapshot and redispatch ledger. It stops when the effect is gone, is
// disposed, reaches an accepted execution, or the Session lifetime closes.
func (r *Recovery) requeue(lt *lifetime, key effect.AssignmentKey) {
	lt.retryMu.Lock()
	if lt.ctx.Err() != nil {
		lt.retryMu.Unlock()
		return
	}
	if lt.retries == nil {
		lt.retries = make(map[effect.AssignmentKey]struct{})
	}
	if _, exists := lt.retries[key]; exists {
		lt.retryMu.Unlock()
		return
	}
	lt.retries[key] = struct{}{}
	lt.retryMu.Unlock()

	go func() {
		defer func() {
			lt.retryMu.Lock()
			delete(lt.retries, key)
			lt.retryMu.Unlock()
		}()
		interval := r.RedispatchRetry
		if interval <= 0 {
			interval = DefaultRedispatchRetry
		}
		timer := time.NewTimer(interval)
		defer timer.Stop()
		for {
			select {
			case <-lt.ctx.Done():
				return
			case <-timer.C:
			}
			st := r.Runs.Bind(lt.w)
			snapshot, err := st.Load(lt.ctx, key.RunID)
			if err != nil {
				if lt.ctx.Err() != nil {
					return
				}
				r.fail(lt.w.SessionID(), fmt.Errorf("driver: reload run %s for redispatch reconciliation: %w", key.RunID, err))
				timer.Reset(interval)
				continue
			}
			decision, ok, err := r.reconciler(lt).ReconcileEffect(lt.ctx, st, &snapshot, key)
			if err != nil {
				if lt.ctx.Err() != nil {
					return
				}
				r.fail(lt.w.SessionID(), fmt.Errorf("driver: reconcile redispatch of run %s effect %s: %w", key.RunID, key.Effect, err))
				timer.Reset(interval)
				continue
			}
			if !ok || !decision.Retry {
				return
			}
			timer.Reset(interval)
		}
	}()
}

// redispatch is the reconciler's Redispatch port: the Assignment of an
// Executing effect is rebuilt on the Session's Writer and handed to the
// Executor again.
func (r *Recovery) redispatch(ctx context.Context, w writer.Writer, key effect.AssignmentKey) error {
	l, _, err := r.Loops.ForRun(ctx, w, key.RunID)
	if err != nil {
		return fmt.Errorf("driver: redispatch: %w", err)
	}
	return l.Redispatch(ctx, r.Runs.Bind(w), key)
}

// reattachDeliver is what the reconciler hands a kept attempt's Outcome to:
// an Outcome of an attempt that survived the previous owner is settled
// through the Loop of the Turn that owns its Run, and the Run is driven on
// from there.
func (r *Recovery) reattachDeliver(lt *lifetime) func(effect.Outcome) {
	ctx, w := lt.ctx, lt.w
	return func(out effect.Outcome) {
		if ctx.Err() != nil {
			return
		}
		r.runOwned(lt, func() error {
			lp, turnID, err := r.Loops.ForRun(ctx, w, out.Key.RunID)
			if err != nil {
				return fmt.Errorf("driver: reattached outcome for run %s: %w", out.Key.RunID, err)
			}
			res, err := lp.Deliver(ctx, r.Runs.Bind(w), out, r.Sink)
			if err != nil {
				return fmt.Errorf("driver: settling reattached outcome for run %s: %w", out.Key.RunID, err)
			}
			if res.Disposition != loop.LoopDelivered {
				return nil
			}
			return r.drive(ctx, w, turnID)
		})
	}
}

// Stop cancels the Session's recovery listeners.
func (r *Recovery) Stop(sid session.SessionID) {
	r.mu.Lock()
	lt := r.lifetimes[sid]
	if lt != nil {
		lt.cancel()
		delete(r.lifetimes, sid)
	}
	r.mu.Unlock()
	if lt != nil {
		lt.maintenance.Wait()
	}
}

// Close cancels every Session's recovery listeners.
func (r *Recovery) Close() {
	r.mu.Lock()
	lifetimes := r.lifetimes
	r.lifetimes = nil
	for _, lt := range lifetimes {
		lt.cancel()
	}
	r.mu.Unlock()
	for _, lt := range lifetimes {
		lt.maintenance.Wait()
	}
}
