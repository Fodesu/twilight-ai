// Package executor makes provider effects durable: a Worker owns the
// execution record of every accepted Assignment -- its payload, its
// ExecutionRef, its lifecycle state and its Outcome -- and hands the effect
// itself to one Backend chosen once, at Dispatch (RUN-EXE-3, RUN-EXE-10).
// Agent Core sees the Worker as an effect.ExecutionPort addressed by AssignmentKey;
// Backends see only the Ref the record holds for them.
package executor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/felinics/twilight/agentcore/executor/protocol"
	executionstore "github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/schema"
)

// WorkerOptions configure one Worker incarnation.
type WorkerOptions struct {
	// ID identifies this process incarnation. It must not be reused by a
	// restarted process while an older incarnation could still be alive.
	ID            string
	LeaseDuration time.Duration
	// Clock reads the lease clock. It must agree with the Store's clock; the
	// Store remains the fencing authority. Defaults to time.Now.
	Clock func() time.Time
	// Retry is the deployment's budget for re-dispatching an effect after a
	// Known failure that declares itself retryable (RUN-EXE-11): at most
	// MaxAttempts executions in total, Backoff multiplied by the attempts so
	// far between them. The zero value disables retries; whether a given
	// failure is worth retrying is the failure's own disposition.
	Retry RetryBudget
	// Progress is the hub the Worker's backends publish progress into and
	// the Worker serves through Progress (RUN-EXE-12); nil builds one with
	// the default window. The composer hands the same hub to its backends.
	Progress *ProgressHub
}

// RetryBudget bounds the Worker's retries of one effect.
type RetryBudget struct {
	MaxAttempts int
	Backoff     time.Duration
}

const defaultLeaseDuration = 30 * time.Second

// Worker owns execution leases, not Session ownership. It selects a Backend
// for an Assignment once, persists the resulting ExecutionRef, and from then
// on resolves record -> provider -> Backend for every lifecycle operation.
//
// The Worker knows how to recover one execution and nothing about when:
// RecoverExecution (effect.Recoverer) takes an expired record back under a
// live lease and continues it from the persisted payload and Ref, and
// Dispose settles a record its caller has given up on. Which records to
// recover, when to ask and when to give up are decisions of whoever observes
// the record as orphaned — the Owner reconciling its own Run (RUN-CMT-7) or
// an external controller — and the Worker runs no loop of its own
// (RUN-EXE-6). Neither operation is part of effect.ExecutionPort, which
// stays the per-assignment data plane.
type Worker struct {
	store     executionstore.Store
	routes    []Route
	backends  map[string]ExecutionBackend
	id        string
	lease     time.Duration
	now       func() time.Time
	lifecycle context.Context
	stop      context.CancelFunc
	// wg counts the goroutines the Worker started: each record's heartbeat
	// and watcher. Close cancels lifecycle and waits.
	wg sync.WaitGroup

	retry    RetryBudget
	progress *ProgressHub

	mu     sync.Mutex
	notify map[effect.AssignmentKey]chan struct{}
}

// NewWorker builds a Worker over records with the given routes; the last
// route is normally Default. A Worker with no route serves nothing.
func NewWorker(ctx context.Context, records executionstore.Store, routes []Route, options ...WorkerOptions) (*Worker, error) {
	if records == nil {
		return nil, errors.New("executor: nil worker store")
	}
	if len(routes) == 0 {
		return nil, errors.New("executor: worker requires at least one route")
	}
	backends := make(map[string]ExecutionBackend, len(routes))
	for _, r := range routes {
		if r.Provider == "" || r.Backend == nil {
			return nil, errors.New("executor: route requires a provider and a backend")
		}
		if _, dup := backends[r.Provider]; dup {
			return nil, fmt.Errorf("executor: duplicate provider %q", r.Provider)
		}
		backends[r.Provider] = r.Backend
	}
	var opts WorkerOptions
	if len(options) > 0 {
		opts = options[0]
	}
	if opts.ID == "" {
		var raw [16]byte
		if _, err := rand.Read(raw[:]); err != nil {
			return nil, fmt.Errorf("executor: generate worker id: %w", err)
		}
		opts.ID = "worker-" + hex.EncodeToString(raw[:])
	}
	if opts.LeaseDuration <= 0 {
		opts.LeaseDuration = defaultLeaseDuration
	}
	now := opts.Clock
	if now == nil {
		now = time.Now
	}
	lifecycle, stop := context.WithCancel(context.WithoutCancel(ctx))
	progress := opts.Progress
	if progress == nil {
		progress = NewProgressHub(0)
	}
	w := &Worker{store: records, routes: routes, backends: backends, id: opts.ID, lease: opts.LeaseDuration, now: now,
		retry: opts.Retry, progress: progress,
		lifecycle: lifecycle, stop: stop, notify: make(map[effect.AssignmentKey]chan struct{})}
	if err := w.recover(ctx); err != nil {
		stop()
		return nil, err
	}
	return w, nil
}

// spawn runs fn as a Worker goroutine counted by Close.
func (w *Worker) spawn(fn func()) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		fn()
	}()
}

// Close stops every heartbeat and every watcher, and
// waits for them. Records keep their leases until they expire: another
// incarnation adopts them through RecoverExecution (RUN-EXE-6). Close
// does not cancel backend executions.
func (w *Worker) Close() {
	w.stop()
	w.wg.Wait()
}

// route selects the Backend for an Assignment: the first Route whose Match
// accepts it (RUN-EXE-10).
func (w *Worker) route(a effect.Assignment) (Route, error) {
	for _, r := range w.routes {
		if r.Match == nil || r.Match(a) {
			return r, nil
		}
	}
	return Route{}, fmt.Errorf("%w: no route accepts the assignment", ErrUnknownProvider)
}

// backend resolves the Backend a record's ExecutionRef names.
func (w *Worker) backend(ref ExecutionRef) (ExecutionBackend, error) {
	b, ok := w.backends[ref.Provider]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownProvider, ref.Provider)
	}
	return b, nil
}

func (w *Worker) Validate(ctx context.Context, a effect.Assignment) (*run.ToolFailure, error) {
	r, err := w.route(a)
	if err != nil {
		return nil, err
	}
	return r.Backend.Validate(ctx, a)
}

// --- ledger helpers ---

// commitFn decides the commit to append from the current fold of a ledger;
// a nil commit means there is nothing to do. The Seq is the caller's.
type commitFn func(state *executionstore.Execution, head executionstore.Head) (*executionstore.Commit, error)

// commit loads the key's fold, lets fn decide, and appends under lease (a
// zero lease is an unfenced append). A commit the ledger already holds is
// success; a Seq conflict reloads and decides again, so a Worker never
// writes from a fold another writer has moved past.
func (w *Worker) commit(ctx context.Context, lease executionstore.Lease, key effect.AssignmentKey, fn commitFn) error {
	for attempt := 0; ; attempt++ {
		state, head, ok, err := w.store.Load(ctx, key)
		if err != nil {
			return err
		}
		if !ok {
			return effect.ErrExecutionNotFound
		}
		c, err := fn(&state, head)
		if err != nil || c == nil {
			return err
		}
		c.Seq = head.Next
		err = w.store.Append(ctx, lease, key, *c)
		switch {
		case err == nil, errors.Is(err, executionstore.ErrAlreadyApplied):
			return nil
		case errors.Is(err, executionstore.ErrConflict) && attempt < 8:
			continue
		default:
			return err
		}
	}
}

func (w *Worker) event(typ executionstore.EventType, payload any) executionstore.Event {
	ev, err := executionstore.NewEvent(typ, w.now().UnixMilli(), payload)
	if err != nil {
		// The payloads are the store's own closed types; encoding cannot fail.
		panic(err)
	}
	return ev
}

// transition is the commitFn of one state-machine step under lease: it
// appends typ when the fold allows it and does nothing when the fold has
// moved on (another writer, or a replay).
func (w *Worker) transition(lease executionstore.Lease, typ executionstore.EventType, to effect.ExecutionStatus, command string) commitFn {
	return func(state *executionstore.Execution, _ executionstore.Head) (*executionstore.Commit, error) {
		if state.State == to || !executionstore.LegalTransition(state.State, to) {
			return nil, nil
		}
		return &executionstore.Commit{CommitID: executionstore.DeriveCommitID(lease.Key, command, fmt.Sprintf("%d/%s", uint64(lease.Epoch), state.State)), Events: []executionstore.Event{w.event(typ, nil)}}, nil
	}
}

// holdsLease reports whether this Worker's lease on the fold is live.
func (w *Worker) holdsLease(s *executionstore.Execution) bool {
	return !s.Terminal() && s.Lease.Owner == w.id && s.Lease.Epoch > 0 && s.Lease.UntilUnixMilli > w.now().UnixMilli()
}

// Dispatch accepts an Assignment (RUN-EXE-3): it selects the Backend,
// prepares the Ref, opens the execution's ledger with the acceptance and
// the binding in one commit, then starts the execution. A replay of the
// same Assignment is answered from the ledger; recovery of an existing
// execution goes through RecoverExecution.
func (w *Worker) Dispatch(ctx context.Context, a effect.Assignment) error {
	if a.Body == nil {
		return errors.New("executor: assignment without body")
	}
	if model, ok := a.Model(); ok {
		if model.Request == nil {
			return errors.New("executor: model assignment requires an inline request payload")
		}
		requestDigest, err := schema.Canonical().DigestRequest(*model.Request)
		if err != nil {
			return err
		}
		if requestDigest != model.RequestDigest || model.Request.Model != string(model.Model) {
			return errors.New("executor: model request digest or model mismatch")
		}
	}
	digest, err := a.Digest()
	if err != nil {
		return err
	}
	route, err := w.route(a)
	if err != nil {
		return err
	}
	ref, err := route.Backend.Prepare(ctx, a)
	if err != nil {
		return err
	}
	if ref == "" {
		return errors.New("executor: backend prepared an empty execution ref")
	}
	key := a.Key()
	c := executionstore.Commit{Seq: 0, CommitID: executionstore.AcceptCommitID(key), Events: []executionstore.Event{
		w.event(executionstore.EventExecutionAccepted, executionstore.Accepted{Assignment: a}),
		w.event(executionstore.EventExecutionBound, executionstore.Bound{Ref: ExecutionRef{Provider: route.Provider, Ref: ref}}),
	}}
	err = w.store.Append(ctx, executionstore.Lease{}, key, c)
	switch {
	case err == nil:
	case errors.Is(err, executionstore.ErrAlreadyApplied), errors.Is(err, executionstore.ErrConflict):
		// The ledger was opened before (a replayed Dispatch, another writer,
		// a seed): the same Assignment acknowledges the acceptance, another
		// is a conflict. The ledger holds one acceptance per key; telling the
		// two apart is the Worker's reading, not the store's (RUN-EXE-14).
		state, _, ok, loadErr := w.store.Load(ctx, key)
		if loadErr != nil {
			return fmt.Errorf("%w: %w", effect.ErrDispatchRetryable, loadErr)
		}
		if ok {
			accepted, err := state.Assignment.Digest()
			if err != nil {
				return err
			}
			if accepted != digest {
				return executionstore.ErrAssignmentConflict
			}
		}
		return nil
	default:
		// The ledger store, not the Assignment, refused: nothing started,
		// and the same Dispatch may succeed later (RUN-EXE-3).
		return fmt.Errorf("%w: %w", effect.ErrDispatchRetryable, err)
	}
	if err := w.acquireAndStart(ctx, key); err != nil {
		if errors.Is(err, effect.ErrDispatchUnknown) {
			return err
		}
		// Between acceptance and Backend.Start only this Worker's own store
		// operations can fail; a Start failure settles the execution instead
		// of returning. The ledger is open and Accepted, so the caller may
		// dispatch again and the replay resumes it.
		return fmt.Errorf("%w: %w", effect.ErrDispatchRetryable, err)
	}
	return nil
}

// RecoverExecution is effect.Recoverer: it takes the execution of key back
// under this Worker's lease when the previous lease expired or was never
// held, and continues it from the persisted payload and Ref — attaching the
// previous backend execution, restarting it once the backend proves it
// missing, or settling it as Unknown when the tool's replay declaration
// forbids a restart (RUN-EXE-3, RUN-EXE-9). A settled execution, one already
// under this Worker's lease and one under another live lease are left as
// they are. The caller has observed the execution as orphaned; how often to
// ask again, and when to give up through Dispose, is the caller's.
func (w *Worker) RecoverExecution(ctx context.Context, key effect.AssignmentKey) error {
	state, _, ok, err := w.store.Load(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		return effect.ErrExecutionNotFound
	}
	if state.Terminal() {
		return nil
	}
	if state.ExecutionRef.Provider != "" {
		if _, err := w.backend(state.ExecutionRef); err != nil {
			return err
		}
	}
	if w.holdsLease(&state) {
		return nil
	}
	return w.acquireAndStart(ctx, key)
}

// Acknowledge is effect.Acknowledger (RUN-EXE-13): the Owner reports that
// the Outcome of key is settled as a Session fact. The ledger records
// outcome_acknowledged and its fold collects the execution: the payload,
// Outcome and Superseded refs leave the fold, while the key, its digest,
// state and ExecutionRef remain, so the execution still answers Attach with
// terminal, refuses a Dispatch that would execute the key again, and answers
// GetOutcome with ErrOutcomeCollected. The execution must be settled;
// acknowledging one still in flight is a caller error (ErrStateConflict),
// and a key without a ledger is ErrExecutionNotFound. Repeating it changes
// nothing.
func (w *Worker) Acknowledge(ctx context.Context, key effect.AssignmentKey) error {
	return w.commit(ctx, executionstore.Lease{}, key, func(state *executionstore.Execution, _ executionstore.Head) (*executionstore.Commit, error) {
		if !state.Terminal() {
			return nil, fmt.Errorf("%w: acknowledging an execution in state %s", executionstore.ErrStateConflict, state.State)
		}
		if state.Acknowledged {
			return nil, nil
		}
		return &executionstore.Commit{CommitID: executionstore.AcknowledgeCommitID(key),
			Events: []executionstore.Event{w.event(executionstore.EventOutcomeAcknowledged, nil)}}, nil
	})
}

// Dispose settles an unsettled execution as Unknown without re-dispatching
// it. The caller has given the execution up: it will not be recovered, and
// the Owner disposes the Run target on its next read (RUN-CMT-7). Unlike
// RecoverExecution, Dispose is unconditional — it also applies to executions
// whose lease holder is dead or absent — and unlike Cancel it does not
// require backend reachability: the backend is cancelled best-effort after
// the settle.
func (w *Worker) Dispose(ctx context.Context, key effect.AssignmentKey) error {
	var ref ExecutionRef
	err := w.commit(ctx, executionstore.Lease{}, key, func(state *executionstore.Execution, _ executionstore.Head) (*executionstore.Commit, error) {
		ref = state.ExecutionRef
		if state.Terminal() {
			return nil, nil
		}
		env := protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: key, Unknown: true,
			Error: &protocol.WireError{Code: "disposed", Message: "execution disposed by its controller"}}
		return w.settlement(key, &env, effect.ExecutionUnknown), nil
	})
	if err != nil {
		return err
	}
	if b, err := w.backend(ref); err == nil {
		_ = b.Cancel(context.WithoutCancel(ctx), ref.Ref)
	}
	w.wake(key)
	return nil
}

// settlement is the commit that ends an execution: execution_settled under
// the settle CommitID, so the watcher's settle and a controller's Dispose
// race for one identity and the loser reads the winner's Outcome.
func (w *Worker) settlement(key effect.AssignmentKey, outcome *protocol.OutcomeEnvelope, state effect.ExecutionStatus) *executionstore.Commit {
	return &executionstore.Commit{CommitID: executionstore.SettleCommitID(key),
		Events: []executionstore.Event{w.event(executionstore.EventExecutionSettled, executionstore.Settled{State: state, Outcome: *outcome})}}
}

// wake notifies one blocked GetOutcome waiter, if any.
func (w *Worker) wake(key effect.AssignmentKey) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if ch := w.notify[key]; ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// acquireAndStart takes the key's lease and continues the execution from its
// fold (RUN-EXE-3, RUN-EXE-9): an execution with an attachable backend
// execution is observed, one whose execution the backend proves missing is
// re-started (a model, or a tool whose Replay policy allows it) or settled
// Unknown (any other tool, TRN-DUR-4), one whose execution the backend
// cannot confirm (orphaned) is held under the lease and asked about again
// until the backend can, and one never started is started.
func (w *Worker) acquireAndStart(ctx context.Context, key effect.AssignmentKey) error {
	lease, acquired, err := w.store.Acquire(ctx, key, w.id, w.lease)
	if err != nil {
		return err
	}
	if !acquired {
		return nil
	}
	claimed, _, ok, err := w.store.Load(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		return effect.ErrExecutionNotFound
	}
	// A ledger opened without its binding (an older store, a test fixture)
	// is routed and prepared here, before any backend call, and the Ref is
	// committed first (RUN-EXE-9).
	if claimed.ExecutionRef.Provider == "" && claimed.State != effect.ExecutionCancelRequested {
		route, err := w.route(claimed.Assignment)
		if err != nil {
			return err
		}
		ref, err := route.Backend.Prepare(ctx, claimed.Assignment)
		if err != nil {
			return err
		}
		bound := ExecutionRef{Provider: route.Provider, Ref: ref}
		err = w.commit(ctx, lease, key, func(state *executionstore.Execution, _ executionstore.Head) (*executionstore.Commit, error) {
			if state.ExecutionRef.Provider != "" {
				return nil, nil
			}
			return &executionstore.Commit{CommitID: executionstore.DeriveCommitID(key, "bind", fmt.Sprint(uint64(lease.Epoch))),
				Events: []executionstore.Event{w.event(executionstore.EventExecutionBound, executionstore.Bound{Ref: bound})}}, nil
		})
		if err != nil {
			return err
		}
		claimed.ExecutionRef = bound
	}
	backend, err := w.backend(claimed.ExecutionRef)
	if err != nil {
		return err
	}
	ref := claimed.ExecutionRef.Ref
	if claimed.State == effect.ExecutionCancelRequested {
		leaseDone := make(chan struct{})
		w.spawn(func() { w.heartbeat(lease, leaseDone) })
		attachment, attachErr := backend.Attach(ctx, ref)
		if attachErr != nil {
			close(leaseDone)
			return attachErr
		}
		if attachment.State != effect.AttachmentActive && attachment.State != effect.AttachmentTerminal {
			env := protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: key, Unknown: true,
				Error: &protocol.WireError{Code: "cancel_reconciliation_unknown", Message: "cancelled execution is no longer attached"}}
			_ = w.finishOwned(ctx, lease, &env, effect.ExecutionUnknown, nil)
			close(leaseDone)
			return nil
		}
		cancelErr := backend.Cancel(ctx, ref)
		w.spawn(func() { w.watch(lease, backend, ref, leaseDone) })
		return cancelErr
	}
	leaseDone := make(chan struct{})
	w.spawn(func() { w.heartbeat(lease, leaseDone) })
	if claimed.State == effect.ExecutionRunning || claimed.State == effect.ExecutionDispatching {
		// Prefer adoption over retry. Recovery is allowed to retry only after
		// the backend proves that the old execution is missing; an answer it
		// cannot give yet (orphaned) keeps the lease and the question open.
		attachment, attachErr := backend.Attach(ctx, ref)
		if attachErr != nil {
			close(leaseDone)
			return attachErr
		}
		switch attachment.State {
		case effect.AttachmentActive, effect.AttachmentTerminal:
			// Keep Dispatching as a conservative pre-outcome state. The watcher
			// will terminalize it after the adopted backend produces an outcome.
			w.spawn(func() { w.watch(lease, backend, ref, leaseDone) })
			return nil
		case effect.AttachmentOrphaned:
			held := claimed
			w.spawn(func() { w.awaitBackend(lease, &held, backend, ref, leaseDone) })
			return nil
		}
		return w.replay(ctx, lease, &claimed, backend, ref, leaseDone)
	}
	return w.start(ctx, lease, &claimed, backend, ref, leaseDone)
}

// awaitBackend holds a taken-over execution whose backend could not confirm
// it (orphaned): it asks the backend again with backoff, under the
// heartbeat leaseDone ends, until the backend observes the execution (then
// it is watched), proves it missing (then it is replayed or settled as a
// missing one would be), or this incarnation loses the lease. Nothing is
// re-dispatched while the backend is undecided (RUN-EXE-3, TRN-DUR-4).
func (w *Worker) awaitBackend(lease executionstore.Lease, claimed *executionstore.Execution, backend ExecutionBackend, ref string, leaseDone chan struct{}) {
	delay := 10 * time.Millisecond
	for {
		if !w.waitOwned(lease, delay) {
			close(leaseDone)
			return
		}
		delay = min(delay*2, time.Second)
		attachCtx, cancel := context.WithTimeout(w.lifecycle, w.lease)
		attachment, err := backend.Attach(attachCtx, ref)
		cancel()
		if err != nil {
			continue
		}
		switch attachment.State {
		case effect.AttachmentActive, effect.AttachmentTerminal:
			w.watch(lease, backend, ref, leaseDone)
			return
		case effect.AttachmentMissing:
			// A failed replay has released the lease (replay closes leaseDone
			// on every error path), so the execution reads as orphaned again
			// and the Owner's next recovery request retries it.
			_ = w.replay(w.lifecycle, lease, claimed, backend, ref, leaseDone)
			return
		}
	}
}

// replay is what a takeover does with an execution the backend proved
// missing. A model is always replayed; a tool only when the Replay policy
// its Assignment carries allows it, because its lost execution may have
// crossed the effect boundary before its worker died (TRN-DUR-4). The policy
// travels with the Assignment, so a local and a remote Worker decide alike
// from the ledger; a forbidden or unjudged tool settles Unknown, the message
// naming the declaration. Restart replays the same Assignment as a new
// generation: it allocates the Ref of the new physical execution and the old
// Ref, just proved missing, moves to the audit trail as execution_restarted
// (RUN-EXE-9). leaseDone ends the running heartbeat when nothing is left to
// watch.
func (w *Worker) replay(ctx context.Context, lease executionstore.Lease, claimed *executionstore.Execution, backend ExecutionBackend, ref string, leaseDone chan struct{}) error {
	if tool, ok := claimed.Assignment.Tool(); ok && tool.Replay != run.ReplayAllowed {
		env := protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: lease.Key, Unknown: true,
			Error: &protocol.WireError{Code: "adopted_without_replay", Message: fmt.Sprintf("tool %q declares replay %s; the lost execution is not re-dispatched", tool.ToolRef, tool.Replay)}}
		err := w.finishOwned(ctx, lease, &env, effect.ExecutionUnknown, nil)
		close(leaseDone)
		return err
	}
	fresh, err := backend.Restart(ctx, ref, claimed.Assignment)
	if err != nil {
		close(leaseDone)
		return err
	}
	if fresh == "" {
		close(leaseDone)
		return errors.New("executor: backend restarted with an empty execution ref")
	}
	if fresh != ref {
		if err := w.restarted(ctx, lease, claimed.ExecutionRef, fresh); err != nil {
			close(leaseDone)
			return err
		}
		claimed.Superseded = append(claimed.Superseded, claimed.ExecutionRef)
		claimed.ExecutionRef.Ref = fresh
		ref = fresh
		w.progress.Reset(lease.Key)
	}
	return w.start(ctx, lease, claimed, backend, ref, leaseDone)
}

// restarted commits execution_restarted: from leaves, fresh continues.
func (w *Worker) restarted(ctx context.Context, lease executionstore.Lease, from ExecutionRef, fresh string) error {
	return w.commit(ctx, lease, lease.Key, func(state *executionstore.Execution, _ executionstore.Head) (*executionstore.Commit, error) {
		if state.ExecutionRef.Ref == fresh {
			return nil, nil
		}
		if state.ExecutionRef != from {
			return nil, fmt.Errorf("%w: restart of %+v, ledger holds %+v", executionstore.ErrStateConflict, from, state.ExecutionRef)
		}
		return &executionstore.Commit{CommitID: executionstore.DeriveCommitID(lease.Key, "restart", from.Ref),
			Events: []executionstore.Event{w.event(executionstore.EventExecutionRestarted, executionstore.Restarted{Superseded: from, Ref: fresh})}}, nil
	})
}

// start moves the execution through Dispatching to Running around
// Backend.Start and hands it to a watcher; the heartbeat leaseDone ends is
// already running.
func (w *Worker) start(ctx context.Context, lease executionstore.Lease, claimed *executionstore.Execution, backend ExecutionBackend, ref string, leaseDone chan struct{}) error {
	key := lease.Key
	if claimed.State != effect.ExecutionDispatching {
		if err := w.commit(ctx, lease, key, w.transition(lease, executionstore.EventExecutionStarted, effect.ExecutionDispatching, "start")); err != nil {
			close(leaseDone)
			if errors.Is(err, executionstore.ErrLeaseLost) || errors.Is(err, executionstore.ErrStateConflict) {
				return nil
			}
			return err
		}
	}
	if err := backend.Start(context.WithoutCancel(ctx), ref, claimed.Assignment); err != nil {
		if errors.Is(err, effect.ErrDispatchUnknown) {
			w.spawn(func() { w.watch(lease, backend, ref, leaseDone) })
			return err
		}
		settleErr := w.finishOwned(ctx, lease, &protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: key,
			Error: &protocol.WireError{Code: "dispatch_failed", Message: err.Error()}}, effect.ExecutionFailed, err)
		close(leaseDone)
		return settleErr
	}
	if err := w.commit(ctx, lease, key, w.transition(lease, executionstore.EventExecutionRunning, effect.ExecutionRunning, "run")); err != nil {
		// The backend call may already have crossed its external boundary.
		// Keep the watcher alive and report Dispatch as accepted; recovery
		// must reconcile the Dispatching/Running execution rather than replan.
		w.spawn(func() { w.watch(lease, backend, ref, leaseDone) })
		return nil
	}
	w.spawn(func() { w.watch(lease, backend, ref, leaseDone) })
	return nil
}

// watch reads the backend's Outcome for ref and settles the execution under
// this incarnation's lease.
func (w *Worker) watch(lease executionstore.Lease, backend ExecutionBackend, ref string, done chan struct{}) {
	defer close(done)
	for {
		if !w.observe(lease, backend, &ref) {
			return
		}
	}
}

// observe reads the Outcome of ref and settles it, or restarts the effect
// after a retryable failure and reports true with ref moved to the new
// execution; false ends the watch.
func (w *Worker) observe(lease executionstore.Lease, backend ExecutionBackend, ref *string) bool {
	delay := 10 * time.Millisecond
	var out effect.Outcome
	for {
		// Bound each read by the execution lease so a blocked backend read
		// eventually yields to the ownership check before the next poll.
		readCtx, cancelRead := context.WithTimeout(w.lifecycle, w.lease)
		var err error
		out, err = backend.Outcome(readCtx, *ref)
		cancelRead()
		if err == nil {
			break
		}
		if !w.waitOwned(lease, delay) {
			return false
		}
		delay = min(delay*2, time.Second)
	}
	// A Known transient failure of an effect whose policy allows it is
	// re-dispatched under the same ledger within the budget (RUN-EXE-11):
	// the old Ref joins the audit trail and the watch continues on the new one.
	if next, ok := w.retryAfter(lease, backend, *ref, out); ok {
		*ref = next
		return true
	}
	// The backend knows the Ref, not the attempt: the ledger's key is the
	// Outcome's key (RUN-EXE-9).
	out.Key = lease.Key
	env := protocol.EncodeOutcome(out)
	state := protocol.StatusForOutcome(out)
	for {
		if err := w.finishOwned(w.lifecycle, lease, &env, state, nil); err == nil {
			return false
		}
		if !w.waitOwned(lease, delay) {
			return false
		}
		delay = min(delay*2, time.Second)
	}
}

// retryAfter decides whether out, the Outcome of ref, is a Known failure
// that declares itself retryable and the Worker's budget still allows,
// and if so restarts the effect: Backoff scaled by the attempts so far, then
// Backend.Restart and Start under the same lease, the old Ref recorded as
// superseded. It returns the new Ref. A backend whose Restart returns the
// same Ref cannot re-execute (a Port-shaped adapter), so nothing is retried.
func (w *Worker) retryAfter(lease executionstore.Lease, backend ExecutionBackend, ref string, out effect.Outcome) (string, bool) {
	if w.retry.MaxAttempts <= 0 || !retryableFailure(out.Result) {
		return "", false
	}
	ctx := w.lifecycle
	state, _, ok, err := w.store.Load(ctx, lease.Key)
	if err != nil || !ok || state.Lease.Owner != w.id || state.Lease.Epoch != lease.Epoch {
		return "", false
	}
	attempts := len(state.Superseded) + 1
	if attempts >= w.retry.MaxAttempts {
		return "", false
	}
	if delay := w.retry.Backoff * time.Duration(attempts); delay > 0 && !w.waitOwned(lease, delay) {
		return "", false
	}
	fresh, err := backend.Restart(ctx, ref, state.Assignment)
	if err != nil || fresh == "" || fresh == ref {
		return "", false
	}
	if err := w.restarted(ctx, lease, state.ExecutionRef, fresh); err != nil {
		return "", false
	}
	// The next generation of frames starts here; what the receiver saw of
	// the failed attempt is void (RUN-EXE-12).
	w.progress.Reset(lease.Key)
	if err := backend.Start(context.WithoutCancel(ctx), fresh, state.Assignment); err != nil && !errors.Is(err, effect.ErrDispatchUnknown) {
		// The retry itself was refused before starting: settle the original
		// failure rather than loop on the refusal.
		return "", false
	}
	return fresh, true
}

// retryableFailure reports whether a Known outcome declares itself worth a
// second execution (RUN-EXE-11): the disposition the effect layer derived
// for a model failure, or the one the tool gave its failure.
func retryableFailure(result effect.OutcomeResult) bool {
	switch r := result.(type) {
	case effect.ModelFailed:
		return r.Retry() == run.RetryAllowed
	case effect.ToolExecutionFailed:
		return r.Retry == run.RetryAllowed
	default:
		return false
	}
}

func (w *Worker) waitOwned(lease executionstore.Lease, delay time.Duration) bool {
	if !w.ownershipIntact(lease) {
		return false
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-w.lifecycle.Done():
		return false
	case <-timer.C:
		return w.ownershipIntact(lease)
	}
}

// ownershipIntact reports whether this Worker incarnation still owns the
// execution: same owner, same Epoch, unsettled. A lapsed lease does not end
// ownership — Renew re-establishes it once transient store errors stop — so
// watchers keep polling through outages shorter than adoption. A missing
// ledger, a settled execution, or a re-acquired one (a higher Epoch) end
// ownership; the heartbeat then exits too, because Renew reports
// ErrLeaseLost for them.
func (w *Worker) ownershipIntact(lease executionstore.Lease) bool {
	state, _, ok, err := w.store.Load(w.lifecycle, lease.Key)
	if err != nil {
		// A transient read error must not stop the watcher; the caller's
		// backoff retries the ownership check.
		return true
	}
	if !ok {
		return false
	}
	return !state.Terminal() && state.Lease.Owner == w.id && state.Lease.Epoch == lease.Epoch
}

func (w *Worker) heartbeat(lease executionstore.Lease, done <-chan struct{}) {
	interval := w.lease / 3
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-w.lifecycle.Done():
			return
		case <-ticker.C:
			if err := w.store.Renew(w.lifecycle, lease, w.lease); err != nil {
				if errors.Is(err, executionstore.ErrLeaseLost) || errors.Is(err, effect.ErrExecutionNotFound) {
					return
				}
				// Transient store errors must not silently stop lease
				// maintenance; the next tick retries. If the lease nonetheless
				// expires, the next RecoverExecution re-adopts the execution.
			}
		}
	}
}

// finishOwned settles the execution under lease. A settlement another writer
// made first (a Dispose, or a watcher of a later Epoch) is the execution's;
// this one is dropped and dispatchErr, the error the caller was going to
// report, is returned as it was.
func (w *Worker) finishOwned(ctx context.Context, lease executionstore.Lease, outcome *protocol.OutcomeEnvelope, state effect.ExecutionStatus, dispatchErr error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	err := w.commit(ctx, lease, lease.Key, func(current *executionstore.Execution, _ executionstore.Head) (*executionstore.Commit, error) {
		if current.Terminal() {
			return nil, nil
		}
		return w.settlement(lease.Key, outcome, state), nil
	})
	switch {
	case err == nil:
	case errors.Is(err, executionstore.ErrLeaseLost):
		return dispatchErr
	default:
		return err
	}
	w.progress.End(lease.Key)
	if ch := w.notify[lease.Key]; ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	return dispatchErr
}

// Progress is effect.ProgressPort (RUN-EXE-12): the frames of an execution
// this Worker's backends published, from the hub. An execution this Worker
// holds but whose Backend is itself a port (a remote Worker behind
// PortBackend) is served from that port, so a chain of Workers relays the
// frames of the one that runs the effect. A key without a ledger is
// ErrExecutionNotFound; a settled execution the hub no longer holds ends the
// stream at once.
func (w *Worker) Progress(ctx context.Context, key effect.AssignmentKey, after uint64, fn func(effect.ProgressFrame) bool) error {
	if w.progress.Known(key) {
		return w.progress.Progress(ctx, key, after, fn)
	}
	state, _, ok, err := w.store.Load(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		return effect.ErrExecutionNotFound
	}
	if backend, err := w.backend(state.ExecutionRef); err == nil {
		if relay, ok := backend.(effect.ProgressPort); ok {
			return relay.Progress(ctx, key, after, fn)
		}
	}
	if state.Terminal() {
		return nil
	}
	return w.progress.Progress(ctx, key, after, fn)
}

// Attach reports the execution's observation state (RUN-EXE-3). The ledger
// is the authority: missing without a ledger, terminal once settled,
// orphaned when no incarnation holds a live lease on it, active while one
// does. Which Worker answers does not matter: a live lease held by another
// incarnation is proof of its heartbeat, and its watcher settles the Outcome
// into the same store GetOutcome reads. Only for its own live lease does
// this Worker also ask the Backend, and a Backend that no longer finds the
// Ref makes the execution orphaned until RecoverExecution restarts it or
// Dispose settles it.
func (w *Worker) Attach(ctx context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	state, _, ok, err := w.store.Load(ctx, key)
	if err != nil {
		return effect.Attachment{}, err
	}
	if !ok {
		// Dispatch opens the ledger before any Start and the store is
		// durable, so a key without a ledger never started: missing is a
		// proof (RUN-EXE-3).
		return effect.Attachment{State: effect.AttachmentMissing, Execution: effect.ExecutionNotFound}, nil
	}
	attachment := effect.Attachment{Execution: state.State, Owner: state.Lease.Owner, FencingEpoch: uint64(state.Lease.Epoch), LeaseUntilUnixMilli: state.Lease.UntilUnixMilli}
	if state.Terminal() {
		attachment.State = effect.AttachmentTerminal
		attachment.BackendAttached = false
		return attachment, nil
	}
	if state.Lease.Epoch == 0 || state.Lease.UntilUnixMilli <= w.now().UnixMilli() {
		attachment.State = effect.AttachmentOrphaned
		return attachment, nil
	}
	if state.Lease.Owner != w.id {
		attachment.State = effect.AttachmentActive
		return attachment, nil
	}
	backend, err := w.backend(state.ExecutionRef)
	if err != nil {
		return effect.Attachment{}, err
	}
	backendAttachment, err := backend.Attach(ctx, state.ExecutionRef.Ref)
	if err != nil {
		return effect.Attachment{}, err
	}
	attachment.BackendAttached = backendAttachment.State == effect.AttachmentActive || backendAttachment.State == effect.AttachmentTerminal
	if attachment.BackendAttached {
		attachment.State = effect.AttachmentActive
	} else {
		attachment.State = effect.AttachmentOrphaned
	}
	return attachment, nil
}

// GetStatus is the execution's state; while it is with the backend
// (Dispatching, Running, CancelRequested) it is the backend's view of the Ref.
func (w *Worker) GetStatus(ctx context.Context, key effect.AssignmentKey) (effect.ExecutionStatus, error) {
	state, _, ok, err := w.store.Load(ctx, key)
	if err != nil {
		return effect.ExecutionNotFound, err
	}
	if !ok {
		return effect.ExecutionNotFound, effect.ErrExecutionNotFound
	}
	switch state.State {
	case effect.ExecutionDispatching, effect.ExecutionRunning, effect.ExecutionCancelRequested:
		backend, err := w.backend(state.ExecutionRef)
		if err != nil {
			return state.State, err
		}
		return backend.Status(ctx, state.ExecutionRef.Ref)
	}
	return state.State, nil
}

func (w *Worker) GetOutcome(ctx context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
	for {
		state, _, ok, err := w.store.Load(ctx, key)
		if err != nil {
			return effect.Outcome{}, err
		}
		if !ok {
			return effect.Outcome{}, effect.ErrExecutionNotFound
		}
		if state.Acknowledged {
			return effect.Outcome{}, effect.ErrOutcomeCollected
		}
		if state.Outcome != nil {
			w.mu.Lock()
			delete(w.notify, key)
			w.mu.Unlock()
			return protocol.DecodeOutcome(state.Outcome), nil
		}
		ch := w.signal(key)
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ch:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return effect.Outcome{}, ctx.Err()
		}
	}
}

// GetOutcomeEnvelope returns the persisted wire outcome without losing the
// stable error/status representation used by the HTTP binding.
func (w *Worker) GetOutcomeEnvelope(ctx context.Context, key effect.AssignmentKey) (protocol.OutcomeEnvelope, error) {
	if _, err := w.GetOutcome(ctx, key); err != nil {
		return protocol.OutcomeEnvelope{}, err
	}
	state, _, ok, err := w.store.Load(ctx, key)
	if err != nil {
		return protocol.OutcomeEnvelope{}, err
	}
	if !ok || state.Outcome == nil {
		return protocol.OutcomeEnvelope{}, effect.ErrOutcomeNotReady
	}
	return *state.Outcome, nil
}

func (w *Worker) signal(key effect.AssignmentKey) chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	if ch := w.notify[key]; ch != nil {
		return ch
	}
	ch := make(chan struct{}, 1)
	w.notify[key] = ch
	return ch
}

func (w *Worker) Cancel(ctx context.Context, key effect.AssignmentKey) error {
	state, _, ok, err := w.store.Load(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		return effect.ErrExecutionNotFound
	}
	if state.Terminal() {
		return nil
	}
	backend, err := w.backend(state.ExecutionRef)
	if err != nil {
		return err
	}
	lease := state.Lease
	if err := w.requestCancelOwned(ctx, lease); err != nil {
		return err
	}
	return backend.Cancel(ctx, state.ExecutionRef.Ref)
}

// requestCancelOwned commits cancel_requested under the current lease; an
// execution already cancelling or settled is left as it is.
func (w *Worker) requestCancelOwned(ctx context.Context, lease executionstore.Lease) error {
	return w.commit(ctx, lease, lease.Key, w.transition(lease, executionstore.EventCancelRequested, effect.ExecutionCancelRequested, "cancel"))
}

// recover resumes observation of the executions this incarnation still
// holds after a restart with the same ID. It is the lease holder's own duty
// and reads only its own leases; it adopts nothing.
func (w *Worker) recover(ctx context.Context) error {
	keys, err := w.store.ListOwned(ctx, w.id)
	if err != nil {
		return err
	}
	for _, key := range keys {
		state, _, ok, err := w.store.Load(ctx, key)
		if err != nil {
			return err
		}
		if !ok || !w.holdsLease(&state) {
			continue
		}
		backend, err := w.backend(state.ExecutionRef)
		if err != nil {
			continue
		}
		attachment, attachErr := backend.Attach(ctx, state.ExecutionRef.Ref)
		if attachErr != nil {
			// One broken backend read must not block recovery of the other
			// executions; a later RecoverExecution retries this one.
			continue
		}
		if attachment.State == effect.AttachmentActive || attachment.State == effect.AttachmentTerminal {
			lease := state.Lease
			done := make(chan struct{})
			w.spawn(func() { w.heartbeat(lease, done) })
			w.spawn(func() { w.watch(lease, backend, state.ExecutionRef.Ref, done) })
		}
	}
	return nil
}

var (
	_ effect.ExecutionPort = (*Worker)(nil)
	_ effect.Acknowledger  = (*Worker)(nil)
	_ effect.Recoverer     = (*Worker)(nil)
)
