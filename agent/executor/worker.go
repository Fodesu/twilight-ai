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

	executionstore "github.com/felinics/twilight/agent/executor/store"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/effect"
	"github.com/felinics/twilight/agent/run/protocol"
)

// WorkerOptions configure one Worker incarnation.
type WorkerOptions struct {
	// ID identifies this process incarnation. It must not be reused by a
	// restarted process while an older incarnation could still be alive.
	ID            string
	LeaseDuration time.Duration
	// ReconcileInterval starts a background control-plane loop that adopts
	// execution records whose lease expired — orphaned assignments left by a
	// dead incarnation, including ones this Worker's own heartbeat lost. Zero
	// disables the loop; deployments with an external control plane call
	// Takeover explicitly instead.
	ReconcileInterval time.Duration
	// Clock reads the lease clock. It must agree with the Store's clock; the
	// Store remains the fencing authority. Defaults to time.Now.
	Clock func() time.Time
}

const defaultLeaseDuration = 30 * time.Second

// Worker owns execution leases, not Session ownership. It selects a Backend
// for an Assignment once, persists the resulting ExecutionRef, and from then
// on resolves record -> provider -> Backend for every lifecycle operation. A
// Worker can acquire an expired record from a shared Store and continue it
// from the persisted payload and Ref. Takeover is explicit: failure detection
// and the decision to retry an effect belong to the control plane; Reconcile
// is the built-in loop form of that decision, while deployments with an
// external control plane drive Takeover directly. Dispose settles a record
// the control plane has given up on. None of the three is part of
// effect.ExecutionPort, which stays the per-assignment data plane.
type Worker struct {
	store     executionstore.Store
	routes    []Route
	backends  map[string]ExecutionBackend
	id        string
	lease     time.Duration
	now       func() time.Time
	lifecycle context.Context
	stop      context.CancelFunc
	// wg counts the goroutines the Worker started: the reconcile loop and
	// each record's heartbeat and watcher. Close cancels lifecycle and waits.
	wg sync.WaitGroup

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
	w := &Worker{store: records, routes: routes, backends: backends, id: opts.ID, lease: opts.LeaseDuration, now: now,
		lifecycle: lifecycle, stop: stop, notify: make(map[effect.AssignmentKey]chan struct{})}
	if err := w.recover(ctx); err != nil {
		stop()
		return nil, err
	}
	if opts.ReconcileInterval > 0 {
		w.spawn(func() { w.reconcileLoop(opts.ReconcileInterval) })
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

// Close stops the reconcile loop, every heartbeat and every watcher, and
// waits for them. Records keep their leases until they expire: another
// incarnation adopts them through Reconcile or Takeover (RUN-EXE-6). Close
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

// Dispatch accepts an Assignment (RUN-EXE-3): it selects the Backend,
// prepares the Ref, persists the record with its ExecutionRef, then starts
// the execution. A replay of the same Assignment acknowledges the persisted
// acceptance; recovery of an existing record goes through Takeover.
func (w *Worker) Dispatch(ctx context.Context, a effect.Assignment) error {
	if a.Body == nil {
		return errors.New("executor: assignment without body")
	}
	if model, ok := a.Model(); ok {
		if model.Request == nil {
			return errors.New("executor: model assignment requires an inline request payload")
		}
		schema, err := run.SchemaFor(a.Schema)
		if err != nil {
			return err
		}
		requestDigest, err := schema.Canonical.DigestRequest(*model.Request)
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
	record := executionstore.Record{Assignment: a, AssignmentDigest: digest, State: effect.ExecutionAccepted,
		ExecutionRef: ExecutionRef{Provider: route.Provider, Ref: ref}}
	old, created, err := w.store.Create(ctx, record)
	if err != nil {
		return err
	}
	if !created && old.AssignmentDigest != digest {
		return executionstore.ErrAssignmentConflict
	}
	if !created {
		return nil
	}
	return w.acquireAndStart(ctx, key)
}

// Takeover explicitly asks this Worker to acquire an expired Assignment. The
// caller is the control plane: it must have decided that retrying this effect
// is safe or that provider reconciliation has already been attempted.
func (w *Worker) Takeover(ctx context.Context, key effect.AssignmentKey) error {
	r, ok, err := w.store.Get(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		return effect.ErrExecutionNotFound
	}
	if protocol.StatusTerminal(r.State) {
		return nil
	}
	if r.ExecutionRef.Provider != "" {
		if _, err := w.backend(r.ExecutionRef); err != nil {
			return err
		}
	}
	owned, err := w.store.LeaseOwned(ctx, key, w.id, r.FencingEpoch)
	if err != nil {
		return err
	}
	if owned {
		return nil
	}
	return w.acquireAndStart(ctx, key)
}

// Reconcile offers every non-terminal execution record whose lease expired —
// or that was never acquired — to Takeover. It is the control-plane step for
// orphaned executions: a restarted Worker resumes them from the persisted
// payload and Ref, first trying to attach the previous backend execution and
// only re-dispatching after the backend reports it unattachable. Records
// under a live lease, this Worker's or another's, are skipped; the store
// remains the fencing authority. One record's failure does not stop the
// others. It returns the number of records handed to Takeover.
func (w *Worker) Reconcile(ctx context.Context) (int, error) {
	records, err := w.store.List(ctx)
	if err != nil {
		return 0, err
	}
	now := w.now().UnixMilli()
	var firstErr error
	n := 0
	for _, r := range records {
		if protocol.StatusTerminal(r.State) {
			continue
		}
		if r.FencingEpoch != 0 && r.LeaseUntilUnixMilli > now {
			continue
		}
		if err := w.Takeover(ctx, r.Assignment.Key()); err != nil && firstErr == nil {
			firstErr = err
		}
		n++
	}
	return n, firstErr
}

func (w *Worker) reconcileLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-w.lifecycle.Done():
			return
		case <-ticker.C:
			_, _ = w.Reconcile(w.lifecycle)
		}
	}
}

// Dispose settles a non-terminal record as Unknown without re-dispatching it.
// The caller is the control plane: it has decided that the execution cannot be
// recovered and that the authority should dispose the Run target (RUN-CMT-7).
// Unlike Takeover, Dispose is unconditional — it also applies to records whose
// owner is dead or absent — and unlike Cancel it does not require backend
// reachability: the backend is cancelled best-effort after the settle.
func (w *Worker) Dispose(ctx context.Context, key effect.AssignmentKey) error {
	r, ok, err := w.store.Get(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		return effect.ErrExecutionNotFound
	}
	if protocol.StatusTerminal(r.State) {
		return nil
	}
	env := protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: key, AssignmentDigest: r.AssignmentDigest, Unknown: true,
		Error: &protocol.WireError{Code: "disposed", Message: "execution record disposed by the control plane"}}
	r.Outcome = &env
	r.State = effect.ExecutionUnknown
	if err := w.store.Put(ctx, r); err != nil {
		return err
	}
	if b, err := w.backend(r.ExecutionRef); err == nil {
		_ = b.Cancel(context.WithoutCancel(ctx), r.ExecutionRef.Ref)
	}
	w.wake(key)
	return nil
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

// acquireAndStart takes the record's lease and brings its execution to
// Running: a record with an attachable execution is observed, one whose
// execution the backend no longer finds is re-started (model) or settled
// Unknown (tool, TRN-DUR-4), one never started is started.
func (w *Worker) acquireAndStart(ctx context.Context, key effect.AssignmentKey) error {
	claimed, acquired, err := w.store.Acquire(ctx, key, w.id, w.lease)
	if err != nil {
		return err
	}
	if !acquired {
		return nil
	}
	digest := claimed.AssignmentDigest
	// A record written outside Dispatch (an older store, a test fixture) may
	// lack its ExecutionRef; it is routed and prepared here, before any
	// backend call, and the Ref is persisted first (RUN-EXE-9).
	if claimed.ExecutionRef.Provider == "" && claimed.State != effect.ExecutionCancelRequested {
		route, err := w.route(claimed.Assignment)
		if err != nil {
			return err
		}
		ref, err := route.Backend.Prepare(ctx, claimed.Assignment)
		if err != nil {
			return err
		}
		claimed.ExecutionRef = ExecutionRef{Provider: route.Provider, Ref: ref}
		if err := w.store.PutOwned(ctx, claimed, w.id, claimed.FencingEpoch); err != nil {
			return err
		}
	}
	backend, err := w.backend(claimed.ExecutionRef)
	if err != nil {
		return err
	}
	ref := claimed.ExecutionRef.Ref
	if claimed.State == effect.ExecutionCancelRequested {
		leaseDone := make(chan struct{})
		w.spawn(func() { w.heartbeat(key, claimed.FencingEpoch, leaseDone) })
		attachment, attachErr := backend.Attach(ctx, ref)
		if attachErr != nil {
			close(leaseDone)
			return attachErr
		}
		if attachment.State != effect.AttachmentActive && attachment.State != effect.AttachmentTerminal {
			env := protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: key, AssignmentDigest: digest, Unknown: true,
				Error: &protocol.WireError{Code: "cancel_reconciliation_unknown", Message: "cancelled execution is no longer attached"}}
			_ = w.finishOwned(ctx, key, claimed.FencingEpoch, env, effect.ExecutionUnknown, nil)
			close(leaseDone)
			return nil
		}
		cancelErr := backend.Cancel(ctx, ref)
		w.spawn(func() { w.watch(key, digest, claimed.FencingEpoch, backend, ref, leaseDone) })
		return cancelErr
	}
	if claimed.State == effect.ExecutionRunning || claimed.State == effect.ExecutionDispatching {
		// Prefer adoption over retry. Takeover is allowed to retry only after
		// the backend says that the old execution is not attachable.
		attachment, attachErr := backend.Attach(ctx, ref)
		if attachErr != nil {
			return attachErr
		}
		if attachment.State == effect.AttachmentActive || attachment.State == effect.AttachmentTerminal {
			leaseDone := make(chan struct{})
			w.spawn(func() { w.heartbeat(key, claimed.FencingEpoch, leaseDone) })
			// Keep Dispatching as a conservative pre-outcome state. The watcher
			// will terminalize it after the adopted backend produces an outcome.
			w.spawn(func() { w.watch(key, digest, claimed.FencingEpoch, backend, ref, leaseDone) })
			return nil
		}
		if claimed.Assignment.Kind() == effect.AssignmentTool {
			// A tool execution the backend no longer finds may have crossed
			// the effect boundary before its worker died. Re-dispatch may
			// repeat side effects, so adoption settles Unknown (TRN-DUR-4)
			// instead of retrying. A replayable tool declares that on its
			// definition; until the declaration exists, no tool is
			// re-dispatched by adoption.
			env := protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: key, AssignmentDigest: digest, Unknown: true,
				Error: &protocol.WireError{Code: "adopted_without_replay", Message: "tool execution adopted without a replay declaration"}}
			return w.finishOwned(ctx, key, claimed.FencingEpoch, env, effect.ExecutionUnknown, nil)
		}
		// A model execution replays the same frozen request as a new
		// generation: Restart allocates the Ref of the new physical execution
		// and the old Ref, just confirmed missing, moves to the audit trail
		// (RUN-EXE-9).
		fresh, err := backend.Restart(ctx, ref, claimed.Assignment)
		if err != nil {
			return err
		}
		if fresh == "" {
			return errors.New("executor: backend restarted with an empty execution ref")
		}
		if fresh != ref {
			claimed.Superseded = append(claimed.Superseded, claimed.ExecutionRef)
			claimed.ExecutionRef.Ref = fresh
			if err := w.store.PutOwned(ctx, claimed, w.id, claimed.FencingEpoch); err != nil {
				return err
			}
			ref = fresh
		}
	}
	if claimed.State != effect.ExecutionDispatching {
		if err := w.store.TransitionOwned(ctx, key, w.id, claimed.FencingEpoch, claimed.State, effect.ExecutionDispatching); err != nil {
			if errors.Is(err, executionstore.ErrLeaseLost) || errors.Is(err, executionstore.ErrStateConflict) {
				return nil
			}
			return err
		}
	}
	leaseDone := make(chan struct{})
	w.spawn(func() { w.heartbeat(key, claimed.FencingEpoch, leaseDone) })
	if err := backend.Start(context.WithoutCancel(ctx), ref, claimed.Assignment); err != nil {
		if errors.Is(err, effect.ErrDispatchUnknown) {
			w.spawn(func() { w.watch(key, digest, claimed.FencingEpoch, backend, ref, leaseDone) })
			return err
		}
		settleErr := w.finishOwned(ctx, key, claimed.FencingEpoch, protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: key, AssignmentDigest: digest,
			Error: &protocol.WireError{Code: "dispatch_failed", Message: err.Error()}}, effect.ExecutionFailed, err)
		close(leaseDone)
		return settleErr
	}
	if err := w.store.TransitionOwned(ctx, key, w.id, claimed.FencingEpoch, effect.ExecutionDispatching, effect.ExecutionRunning); err != nil {
		// The backend call may already have crossed its external boundary.
		// Keep the watcher alive and report Dispatch as accepted; recovery
		// must reconcile the Dispatching/Running record rather than replan.
		w.spawn(func() { w.watch(key, digest, claimed.FencingEpoch, backend, ref, leaseDone) })
		return nil
	}
	w.spawn(func() { w.watch(key, digest, claimed.FencingEpoch, backend, ref, leaseDone) })
	return nil
}

// watch reads the backend's Outcome for ref and settles the record under
// this incarnation's lease.
func (w *Worker) watch(key effect.AssignmentKey, digest run.Digest, epoch uint64, backend ExecutionBackend, ref string, done chan struct{}) {
	defer close(done)
	delay := 10 * time.Millisecond
	var out effect.Outcome
	for {
		// Bound each read by the execution lease so a blocked backend read
		// eventually yields to the ownership check before the next poll.
		readCtx, cancelRead := context.WithTimeout(w.lifecycle, w.lease)
		var err error
		out, err = backend.Outcome(readCtx, ref)
		cancelRead()
		if err == nil {
			break
		}
		if !w.waitOwned(key, epoch, delay) {
			return
		}
		delay = min(delay*2, time.Second)
	}
	// The backend knows the Ref, not the attempt: the record's key is the
	// Outcome's key (RUN-EXE-9).
	out.Key = key
	env := protocol.EncodeOutcome(out, digest)
	state := protocol.StatusForOutcome(out)
	for {
		if err := w.finishOwned(w.lifecycle, key, epoch, env, state, nil); err == nil {
			return
		}
		if !w.waitOwned(key, epoch, delay) {
			return
		}
		delay = min(delay*2, time.Second)
	}
}

func (w *Worker) waitOwned(key effect.AssignmentKey, epoch uint64, delay time.Duration) bool {
	if !w.ownershipIntact(key, epoch) {
		return false
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-w.lifecycle.Done():
		return false
	case <-timer.C:
		return w.ownershipIntact(key, epoch)
	}
}

// ownershipIntact reports whether this Worker incarnation still owns the
// record: same owner, same fencing epoch, non-terminal. A lapsed lease does
// not end ownership — Renew re-establishes it once transient store errors
// stop — so watchers keep polling through outages shorter than adoption.
// Missing, terminal, or re-acquired records (a higher epoch) end ownership;
// the heartbeat then exits too, because Renew reports ErrLeaseLost for them.
func (w *Worker) ownershipIntact(key effect.AssignmentKey, epoch uint64) bool {
	r, ok, err := w.store.Get(w.lifecycle, key)
	if err != nil {
		// A transient read error must not stop the watcher; the caller's
		// backoff retries the ownership check.
		return true
	}
	if !ok {
		return false
	}
	return !protocol.StatusTerminal(r.State) && r.Owner == w.id && r.FencingEpoch == epoch
}

func (w *Worker) heartbeat(key effect.AssignmentKey, epoch uint64, done <-chan struct{}) {
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
			if err := w.store.Renew(w.lifecycle, key, w.id, epoch, w.lease); err != nil {
				if errors.Is(err, executionstore.ErrLeaseLost) || errors.Is(err, effect.ErrExecutionNotFound) {
					return
				}
				// Transient store errors must not silently stop lease
				// maintenance; the next tick retries. If the lease nonetheless
				// expires, Reconcile re-adopts the record.
			}
		}
	}
}

func (w *Worker) finishOwned(ctx context.Context, key effect.AssignmentKey, epoch uint64, outcome protocol.OutcomeEnvelope, state effect.ExecutionStatus, dispatchErr error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	r, ok, err := w.store.Get(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		return effect.ErrExecutionNotFound
	}
	if protocol.StatusTerminal(r.State) {
		return dispatchErr
	}
	r.Outcome = &outcome
	r.State = state
	if err := w.store.PutOwned(ctx, r, w.id, epoch); err != nil {
		if errors.Is(err, executionstore.ErrLeaseLost) {
			return dispatchErr
		}
		return err
	}
	if ch := w.notify[key]; ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	return dispatchErr
}

// Attach reports the record's observation state (RUN-EXE-3): missing without
// a record, terminal once settled, orphaned when no live incarnation owns it,
// and otherwise what this incarnation's Backend finds for the Ref.
func (w *Worker) Attach(ctx context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	r, ok, err := w.store.Get(ctx, key)
	if err != nil {
		return effect.Attachment{}, err
	}
	if !ok {
		return effect.Attachment{State: effect.AttachmentMissing, Execution: effect.ExecutionNotFound}, nil
	}
	attachment := effect.Attachment{Execution: r.State, Owner: r.Owner, FencingEpoch: r.FencingEpoch, LeaseUntilUnixMilli: r.LeaseUntilUnixMilli}
	if protocol.StatusTerminal(r.State) {
		attachment.State = effect.AttachmentTerminal
		attachment.BackendAttached = false
		return attachment, nil
	}
	if r.Owner != w.id || r.FencingEpoch == 0 {
		attachment.State = effect.AttachmentOrphaned
		return attachment, nil
	}
	owned, err := w.store.LeaseOwned(ctx, key, w.id, r.FencingEpoch)
	if err != nil {
		return effect.Attachment{}, err
	}
	if !owned {
		attachment.State = effect.AttachmentOrphaned
		return attachment, nil
	}
	backend, err := w.backend(r.ExecutionRef)
	if err != nil {
		return effect.Attachment{}, err
	}
	backendAttachment, err := backend.Attach(ctx, r.ExecutionRef.Ref)
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

// GetStatus is the record's state; while the execution is with the backend
// (Dispatching, Running, CancelRequested) it is the backend's view of the Ref.
func (w *Worker) GetStatus(ctx context.Context, key effect.AssignmentKey) (effect.ExecutionStatus, error) {
	r, ok, err := w.store.Get(ctx, key)
	if err != nil {
		return effect.ExecutionNotFound, err
	}
	if !ok {
		return effect.ExecutionNotFound, effect.ErrExecutionNotFound
	}
	switch r.State {
	case effect.ExecutionDispatching, effect.ExecutionRunning, effect.ExecutionCancelRequested:
		backend, err := w.backend(r.ExecutionRef)
		if err != nil {
			return r.State, err
		}
		return backend.Status(ctx, r.ExecutionRef.Ref)
	}
	return r.State, nil
}

func (w *Worker) GetOutcome(ctx context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
	for {
		r, ok, err := w.store.Get(ctx, key)
		if err != nil {
			return effect.Outcome{}, err
		}
		if !ok {
			return effect.Outcome{}, effect.ErrExecutionNotFound
		}
		if r.Outcome != nil {
			w.mu.Lock()
			delete(w.notify, key)
			w.mu.Unlock()
			return protocol.DecodeOutcome(*r.Outcome), nil
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
	r, ok, err := w.store.Get(ctx, key)
	if err != nil {
		return protocol.OutcomeEnvelope{}, err
	}
	if !ok || r.Outcome == nil {
		return protocol.OutcomeEnvelope{}, effect.ErrOutcomeNotReady
	}
	return *r.Outcome, nil
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
	r, ok, err := w.store.Get(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		return effect.ErrExecutionNotFound
	}
	if protocol.StatusTerminal(r.State) {
		return nil
	}
	backend, err := w.backend(r.ExecutionRef)
	if err != nil {
		return err
	}
	if err := w.requestCancelOwned(ctx, key, r.FencingEpoch); err != nil {
		return err
	}
	return backend.Cancel(ctx, r.ExecutionRef.Ref)
}

func (w *Worker) requestCancelOwned(ctx context.Context, key effect.AssignmentKey, epoch uint64) error {
	for {
		r, ok, err := w.store.Get(ctx, key)
		if err != nil {
			return err
		}
		if !ok {
			return effect.ErrExecutionNotFound
		}
		if protocol.StatusTerminal(r.State) || r.State == effect.ExecutionCancelRequested {
			return nil
		}
		err = w.store.TransitionOwned(ctx, key, w.id, epoch, r.State, effect.ExecutionCancelRequested)
		if errors.Is(err, executionstore.ErrStateConflict) {
			continue
		}
		return err
	}
}

// recover resumes observation of the records this incarnation still owns
// after a restart with the same ID.
func (w *Worker) recover(ctx context.Context) error {
	records, err := w.store.List(ctx)
	if err != nil {
		return err
	}
	for _, r := range records {
		if protocol.StatusTerminal(r.State) || r.Owner != w.id || r.FencingEpoch == 0 {
			continue
		}
		owned, err := w.store.LeaseOwned(ctx, r.Assignment.Key(), w.id, r.FencingEpoch)
		if err != nil {
			return err
		}
		if !owned {
			continue
		}
		backend, err := w.backend(r.ExecutionRef)
		if err != nil {
			continue
		}
		attachment, attachErr := backend.Attach(ctx, r.ExecutionRef.Ref)
		if attachErr != nil {
			// One broken backend read must not block recovery of the other
			// records; a later Reconcile or explicit Takeover retries this one.
			continue
		}
		if attachment.State == effect.AttachmentActive || attachment.State == effect.AttachmentTerminal {
			done := make(chan struct{})
			w.spawn(func() { w.heartbeat(r.Assignment.Key(), r.FencingEpoch, done) })
			w.spawn(func() {
				w.watch(r.Assignment.Key(), r.AssignmentDigest, r.FencingEpoch, backend, r.ExecutionRef.Ref, done)
			})
		}
	}
	return nil
}

var _ effect.ExecutionPort = (*Worker)(nil)
