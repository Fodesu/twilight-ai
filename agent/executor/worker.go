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

	"github.com/felinics/twilight/agent/executor/protocol"
	executionstore "github.com/felinics/twilight/agent/executor/store"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/effect"
	"github.com/felinics/twilight/agent/run/schema"
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
	// DisposeAfter bounds how long an orphaned record waits for a Worker
	// able to adopt it: once this Worker's takeovers of a record have been
	// failing for that long, Reconcile disposes the record (RUN-EXE-6) and
	// reports it through Warn. Zero leaves deferred unbounded, the protocol
	// default. The clock is this incarnation's: a restarted Worker starts
	// counting again.
	DisposeAfter time.Duration
	// Warn receives control-plane events no caller waits for, such as a
	// record disposed after DisposeAfter (ErrOrphanDisposed). nil discards.
	Warn func(error)
	// CollectAfter is the time-based fallback of record collection
	// (RUN-EXE-13): a terminal record the Owner never acknowledged is
	// collected once its settlement is this old. Zero collects only
	// acknowledged records; the protocol default keeps every unacknowledged
	// Outcome readable.
	CollectAfter time.Duration
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

	disposeAfter time.Duration
	collectAfter time.Duration
	warn         func(error)
	retry        RetryBudget
	progress     *ProgressHub
	// provesAbsence is whether a key without a record proves that no
	// execution exists for it (RUN-EXE-3): the store is durable, so a record
	// is on disk before any Start, or every Backend is Colocated, so an
	// execution cannot outlive the records kept in this process.
	provesAbsence bool

	mu     sync.Mutex
	notify map[effect.AssignmentKey]chan struct{}
	// orphans records when this incarnation first failed to take over each
	// expired record; DisposeAfter is measured from there.
	orphans map[effect.AssignmentKey]time.Time
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
	warn := opts.Warn
	if warn == nil {
		warn = func(error) {}
	}
	progress := opts.Progress
	if progress == nil {
		progress = NewProgressHub(0)
	}
	w := &Worker{store: records, routes: routes, backends: backends, id: opts.ID, lease: opts.LeaseDuration, now: now,
		disposeAfter: opts.DisposeAfter, collectAfter: opts.CollectAfter, warn: warn, retry: opts.Retry, progress: progress,
		provesAbsence: provesAbsence(records, routes),
		lifecycle:     lifecycle, stop: stop, notify: make(map[effect.AssignmentKey]chan struct{}),
		orphans: make(map[effect.AssignmentKey]time.Time)}
	if err := w.recover(ctx); err != nil {
		stop()
		return nil, err
	}
	if opts.ReconcileInterval > 0 {
		w.spawn(func() { w.reconcileLoop(opts.ReconcileInterval) })
	}
	return w, nil
}

// provesAbsence decides whether a missing record proves a missing execution
// (RUN-EXE-3). Dispatch persists the record before Start, so under a durable
// store a key without a record never started; under a memory store the
// record may have died with a previous process while its execution lives
// on, unless every Backend is Colocated and died with it too.
func provesAbsence(records executionstore.Store, routes []Route) bool {
	if d, ok := records.(interface{ Durable() bool }); ok && d.Durable() {
		return true
	}
	for _, r := range routes {
		c, ok := r.Backend.(Colocated)
		if !ok || !c.Colocated() {
			return false
		}
	}
	return true
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
		sch, err := schema.For(a.Schema)
		if err != nil {
			return err
		}
		requestDigest, err := sch.Canonical.DigestRequest(*model.Request)
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
		// The record store, not the Assignment, refused: nothing started,
		// and the same Dispatch may succeed later (RUN-EXE-3).
		return fmt.Errorf("%w: %w", effect.ErrDispatchRetryable, err)
	}
	if !created && old.AssignmentDigest != digest {
		return executionstore.ErrAssignmentConflict
	}
	if !created {
		return nil
	}
	if err := w.acquireAndStart(ctx, key); err != nil {
		if errors.Is(err, effect.ErrDispatchUnknown) {
			return err
		}
		// Between the record and Backend.Start only this Worker's own store
		// operations can fail; a Start failure settles the record instead of
		// returning. The record exists and is Accepted, so the caller may
		// dispatch again and the replay resumes it.
		return fmt.Errorf("%w: %w", effect.ErrDispatchRetryable, err)
	}
	return nil
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
// others. A record this Worker has been failing to take over for
// DisposeAfter is disposed instead (RUN-EXE-6). It returns the number of
// records handed to Takeover.
func (w *Worker) Reconcile(ctx context.Context) (int, error) {
	records, err := w.store.List(ctx)
	if err != nil {
		return 0, err
	}
	now := w.now()
	var firstErr error
	n := 0
	for i := range records {
		r := &records[i]
		key := r.Assignment.Key()
		if protocol.StatusTerminal(r.State) {
			w.forgetOrphan(key)
			continue
		}
		if r.FencingEpoch != 0 && r.LeaseUntilUnixMilli > now.UnixMilli() {
			continue
		}
		n++
		err := w.Takeover(ctx, key)
		if err == nil {
			w.forgetOrphan(key)
			continue
		}
		if w.expireOrphan(ctx, key, now, err) {
			continue
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	return n, firstErr
}

// expireOrphan disposes a record this Worker has failed to take over for
// DisposeAfter, measured from the first failure this incarnation saw. It
// re-reads the record first: an execution the failed takeover nonetheless
// started under this Worker's live lease is adopted, not disposed.
func (w *Worker) expireOrphan(ctx context.Context, key effect.AssignmentKey, now time.Time, cause error) bool {
	if w.disposeAfter <= 0 {
		return false
	}
	w.mu.Lock()
	since, seen := w.orphans[key]
	if !seen {
		since = now
		w.orphans[key] = since
	}
	w.mu.Unlock()
	age := now.Sub(since)
	if age < w.disposeAfter {
		return false
	}
	r, ok, err := w.store.Get(ctx, key)
	if err != nil || !ok || protocol.StatusTerminal(r.State) {
		return false
	}
	if r.FencingEpoch != 0 {
		owned, err := w.store.LeaseOwned(ctx, key, w.id, r.FencingEpoch)
		if err != nil {
			return false
		}
		if owned {
			w.forgetOrphan(key)
			return false
		}
	}
	if err := w.Dispose(ctx, key); err != nil {
		return false
	}
	w.forgetOrphan(key)
	w.warn(fmt.Errorf("%w: run %s effect %s, unadoptable for %s: %w", ErrOrphanDisposed, key.RunID, key.Effect, age.Round(time.Millisecond), cause))
	return true
}

func (w *Worker) forgetOrphan(key effect.AssignmentKey) {
	w.mu.Lock()
	delete(w.orphans, key)
	w.mu.Unlock()
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
			_, _ = w.Collect(w.lifecycle)
		}
	}
}

// Acknowledge is effect.Acknowledger (RUN-EXE-13): the Owner reports that
// the Outcome of key is settled as a Session fact. The record must be
// terminal; acknowledging an execution still in flight is a caller error
// (ErrStateConflict), and a key without a record is ErrExecutionNotFound.
// Repeating it changes nothing.
func (w *Worker) Acknowledge(ctx context.Context, key effect.AssignmentKey) error {
	r, ok, err := w.store.Get(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		return effect.ErrExecutionNotFound
	}
	if !protocol.StatusTerminal(r.State) {
		return fmt.Errorf("%w: acknowledging an execution in state %s", executionstore.ErrStateConflict, r.State)
	}
	if r.AcknowledgedAtUnixMilli != 0 {
		return nil
	}
	r.AcknowledgedAtUnixMilli = w.now().UnixMilli()
	return w.store.Put(ctx, r)
}

// Collect strips the payload and Outcome of every terminal record whose
// settlement the Owner acknowledged, or that has been terminal for
// CollectAfter when that is set (RUN-EXE-13). What remains is the key, its
// digest, state and ExecutionRef: the record still answers Attach with
// terminal and refuses a Dispatch that would execute the key again, while
// GetOutcome answers ErrOutcomeCollected. Nothing in flight is touched. It
// returns the number of records collected; one record's failure does not
// stop the others.
func (w *Worker) Collect(ctx context.Context) (int, error) {
	records, err := w.store.List(ctx)
	if err != nil {
		return 0, err
	}
	now := w.now().UnixMilli()
	n := 0
	var firstErr error
	for i := range records {
		r := &records[i]
		if !protocol.StatusTerminal(r.State) || r.Collected {
			continue
		}
		expired := w.collectAfter > 0 && r.SettledAtUnixMilli != 0 && now-r.SettledAtUnixMilli >= w.collectAfter.Milliseconds()
		if r.AcknowledgedAtUnixMilli == 0 && !expired {
			continue
		}
		r.Assignment.Body, r.Assignment.Target, r.Outcome, r.Superseded, r.Collected = nil, nil, nil, nil, true
		if err := w.store.Put(ctx, *r); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		n++
	}
	return n, firstErr
}

// Dispose settles a non-terminal record as Unknown without re-dispatching it.
// The caller is the control plane: it has decided that the execution cannot be
// recovered and that the Owner should dispose the Run target (RUN-CMT-7).
// Unlike Takeover, Dispose is unconditional — it also applies to records whose
// owner is dead or absent — and unlike Cancel it does not require backend
// reachability: the backend is cancelled best-effort after the settle.
//
// A key with no record is disposed too, by writing a terminal Unknown record
// for it: the store did not prove the execution absent (Attach answered
// orphaned, RUN-EXE-3), so only the control plane can end the wait, and the
// record it leaves makes the disposal readable and idempotent. A Dispatch of
// that key afterwards meets the record and starts nothing.
func (w *Worker) Dispose(ctx context.Context, key effect.AssignmentKey) error {
	r, ok, err := w.store.Get(ctx, key)
	if err != nil {
		return err
	}
	if ok && protocol.StatusTerminal(r.State) {
		return nil
	}
	if !ok {
		r = executionstore.Record{Assignment: effect.Assignment{Session: key.Session, RunID: key.RunID, Effect: key.Effect}}
	}
	env := protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: key, AssignmentDigest: r.AssignmentDigest, Unknown: true,
		Error: &protocol.WireError{Code: "disposed", Message: "execution record disposed by the control plane"}}
	r.Outcome = &env
	r.State = effect.ExecutionUnknown
	r.SettledAtUnixMilli = w.now().UnixMilli()
	if err := w.store.Put(ctx, r); err != nil {
		return err
	}
	if !ok {
		w.wake(key)
		return nil
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
// execution the backend proves missing is re-started (a model, or a tool
// whose Replay policy allows it) or settled Unknown (any other tool,
// TRN-DUR-4), one whose execution the backend cannot confirm (orphaned) is
// held under the lease and asked about again until the backend can, and one
// never started is started.
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
			_ = w.finishOwned(ctx, key, claimed.FencingEpoch, &env, effect.ExecutionUnknown, nil)
			close(leaseDone)
			return nil
		}
		cancelErr := backend.Cancel(ctx, ref)
		w.spawn(func() { w.watch(key, digest, claimed.FencingEpoch, backend, ref, leaseDone) })
		return cancelErr
	}
	leaseDone := make(chan struct{})
	w.spawn(func() { w.heartbeat(key, claimed.FencingEpoch, leaseDone) })
	if claimed.State == effect.ExecutionRunning || claimed.State == effect.ExecutionDispatching {
		// Prefer adoption over retry. Takeover is allowed to retry only after
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
			w.spawn(func() { w.watch(key, digest, claimed.FencingEpoch, backend, ref, leaseDone) })
			return nil
		case effect.AttachmentOrphaned:
			held := claimed
			w.spawn(func() { w.awaitBackend(key, &held, backend, ref, leaseDone) })
			return nil
		}
		return w.replay(ctx, key, &claimed, backend, ref, leaseDone)
	}
	return w.start(ctx, key, &claimed, backend, ref, leaseDone)
}

// awaitBackend holds a taken-over record whose backend could not confirm its
// execution (orphaned): it asks the backend again with backoff, under the
// heartbeat leaseDone ends, until the backend observes the execution (then
// it is watched), proves it missing (then it is replayed or settled as a
// missing one would be), or this incarnation loses the record. Nothing is
// re-dispatched while the backend is undecided (RUN-EXE-3, TRN-DUR-4).
func (w *Worker) awaitBackend(key effect.AssignmentKey, claimed *executionstore.Record, backend ExecutionBackend, ref string, leaseDone chan struct{}) {
	delay := 10 * time.Millisecond
	for {
		if !w.waitOwned(key, claimed.FencingEpoch, delay) {
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
			w.watch(key, claimed.AssignmentDigest, claimed.FencingEpoch, backend, ref, leaseDone)
			return
		case effect.AttachmentMissing:
			if err := w.replay(w.lifecycle, key, claimed, backend, ref, leaseDone); err != nil {
				w.warn(fmt.Errorf("executor: replaying run %s effect %s after its backend proved the execution missing: %w", key.RunID, key.Effect, err))
			}
			return
		}
	}
}

// replay is what a takeover does with an execution the backend proved
// missing. A model is always replayed; a tool only when the Replay policy
// its Assignment carries allows it, because its lost execution may have
// crossed the effect boundary before its worker died (TRN-DUR-4). The policy
// travels with the Assignment, so a local and a remote Worker decide alike
// from the record; a forbidden or unjudged tool settles Unknown, the message
// naming the declaration. Restart replays the same Assignment as a new
// generation: it allocates the Ref of the new physical execution and the old
// Ref, just proved missing, moves to the audit trail (RUN-EXE-9). leaseDone
// ends the running heartbeat when nothing is left to watch.
func (w *Worker) replay(ctx context.Context, key effect.AssignmentKey, claimed *executionstore.Record, backend ExecutionBackend, ref string, leaseDone chan struct{}) error {
	if tool, ok := claimed.Assignment.Tool(); ok && tool.Replay != run.ReplayAllowed {
		env := protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: key, AssignmentDigest: claimed.AssignmentDigest, Unknown: true,
			Error: &protocol.WireError{Code: "adopted_without_replay", Message: fmt.Sprintf("tool %q declares replay %s; the lost execution is not re-dispatched", tool.ToolRef, tool.Replay)}}
		err := w.finishOwned(ctx, key, claimed.FencingEpoch, &env, effect.ExecutionUnknown, nil)
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
		claimed.Superseded = append(claimed.Superseded, claimed.ExecutionRef)
		claimed.ExecutionRef.Ref = fresh
		if err := w.store.PutOwned(ctx, *claimed, w.id, claimed.FencingEpoch); err != nil {
			close(leaseDone)
			return err
		}
		ref = fresh
		w.progress.Reset(key)
	}
	return w.start(ctx, key, claimed, backend, ref, leaseDone)
}

// start moves the claimed record through Dispatching to Running around
// Backend.Start and hands the execution to a watcher; the heartbeat
// leaseDone ends is already running.
func (w *Worker) start(ctx context.Context, key effect.AssignmentKey, claimed *executionstore.Record, backend ExecutionBackend, ref string, leaseDone chan struct{}) error {
	digest := claimed.AssignmentDigest
	if claimed.State != effect.ExecutionDispatching {
		if err := w.store.TransitionOwned(ctx, key, w.id, claimed.FencingEpoch, claimed.State, effect.ExecutionDispatching); err != nil {
			close(leaseDone)
			if errors.Is(err, executionstore.ErrLeaseLost) || errors.Is(err, executionstore.ErrStateConflict) {
				return nil
			}
			return err
		}
	}
	if err := backend.Start(context.WithoutCancel(ctx), ref, claimed.Assignment); err != nil {
		if errors.Is(err, effect.ErrDispatchUnknown) {
			w.spawn(func() { w.watch(key, digest, claimed.FencingEpoch, backend, ref, leaseDone) })
			return err
		}
		settleErr := w.finishOwned(ctx, key, claimed.FencingEpoch, &protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: key, AssignmentDigest: digest,
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
	for {
		if !w.observe(key, digest, epoch, backend, &ref) {
			return
		}
	}
}

// observe reads the Outcome of ref and settles it, or restarts the effect
// after a retryable failure and reports true with ref moved to the new
// execution; false ends the watch.
func (w *Worker) observe(key effect.AssignmentKey, digest run.Digest, epoch uint64, backend ExecutionBackend, ref *string) bool {
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
		if !w.waitOwned(key, epoch, delay) {
			return false
		}
		delay = min(delay*2, time.Second)
	}
	// A Known transient failure of an effect whose policy allows it is
	// re-dispatched under the same record within the budget (RUN-EXE-11):
	// the old Ref joins Superseded and the watch continues on the new one.
	if next, ok := w.retryAfter(key, epoch, backend, *ref, out); ok {
		*ref = next
		return true
	}
	// The backend knows the Ref, not the attempt: the record's key is the
	// Outcome's key (RUN-EXE-9).
	out.Key = key
	env := protocol.EncodeOutcome(out, digest)
	state := protocol.StatusForOutcome(out)
	for {
		if err := w.finishOwned(w.lifecycle, key, epoch, &env, state, nil); err == nil {
			return false
		}
		if !w.waitOwned(key, epoch, delay) {
			return false
		}
		delay = min(delay*2, time.Second)
	}
}

// retryAfter decides whether out, the Outcome of ref, is a Known failure
// that declares itself retryable and the Worker's budget still allows,
// and if so restarts the effect: Backoff scaled by the attempts so far, then
// Backend.Restart and Start under the same lease, the old Ref appended to
// Superseded. It returns the new Ref. A backend whose Restart returns the
// same Ref cannot re-execute (a Port-shaped adapter), so nothing is retried.
func (w *Worker) retryAfter(key effect.AssignmentKey, epoch uint64, backend ExecutionBackend, ref string, out effect.Outcome) (string, bool) {
	if w.retry.MaxAttempts <= 0 || !retryableFailure(out.Result) {
		return "", false
	}
	ctx := w.lifecycle
	r, ok, err := w.store.Get(ctx, key)
	if err != nil || !ok || r.Owner != w.id || r.FencingEpoch != epoch {
		return "", false
	}
	attempts := len(r.Superseded) + 1
	if attempts >= w.retry.MaxAttempts {
		return "", false
	}
	if delay := w.retry.Backoff * time.Duration(attempts); delay > 0 && !w.waitOwned(key, epoch, delay) {
		return "", false
	}
	fresh, err := backend.Restart(ctx, ref, r.Assignment)
	if err != nil || fresh == "" || fresh == ref {
		return "", false
	}
	r.Superseded = append(r.Superseded, r.ExecutionRef)
	r.ExecutionRef.Ref = fresh
	if err := w.store.PutOwned(ctx, r, w.id, epoch); err != nil {
		return "", false
	}
	// The next generation of frames starts here; what the receiver saw of
	// the failed attempt is void (RUN-EXE-12).
	w.progress.Reset(key)
	if err := backend.Start(context.WithoutCancel(ctx), fresh, r.Assignment); err != nil && !errors.Is(err, effect.ErrDispatchUnknown) {
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

func (w *Worker) finishOwned(ctx context.Context, key effect.AssignmentKey, epoch uint64, outcome *protocol.OutcomeEnvelope, state effect.ExecutionStatus, dispatchErr error) error {
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
	r.Outcome = outcome
	r.State = state
	r.SettledAtUnixMilli = w.now().UnixMilli()
	if err := w.store.PutOwned(ctx, r, w.id, epoch); err != nil {
		if errors.Is(err, executionstore.ErrLeaseLost) {
			return dispatchErr
		}
		return err
	}
	w.progress.End(key)
	if ch := w.notify[key]; ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	return dispatchErr
}

// Progress is effect.ProgressPort (RUN-EXE-12): the frames of an execution
// this Worker's backends published, from the hub. A record this Worker
// holds but whose Backend is itself a port (a remote Worker behind
// PortBackend) is served from that port, so a chain of Workers relays the
// frames of the one that runs the effect. A key without a record is
// ErrExecutionNotFound; a terminal record the hub no longer holds ends the
// stream at once.
func (w *Worker) Progress(ctx context.Context, key effect.AssignmentKey, after uint64, fn func(effect.ProgressFrame) bool) error {
	if w.progress.Known(key) {
		return w.progress.Progress(ctx, key, after, fn)
	}
	r, ok, err := w.store.Get(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		return effect.ErrExecutionNotFound
	}
	if backend, err := w.backend(r.ExecutionRef); err == nil {
		if relay, ok := backend.(effect.ProgressPort); ok {
			return relay.Progress(ctx, key, after, fn)
		}
	}
	if protocol.StatusTerminal(r.State) {
		return nil
	}
	return w.progress.Progress(ctx, key, after, fn)
}

// Attach reports the record's observation state (RUN-EXE-3). The record in
// the shared store is the authority: missing without a record when its
// absence proves the execution absent (provesAbsence), orphaned without a
// record otherwise, terminal once settled, orphaned when no incarnation
// holds a live lease on it, active while one does. Which Worker answers does not matter: a live lease held by
// another incarnation is proof of its heartbeat, and its watcher settles the
// Outcome into the same store GetOutcome reads. Only for its own live lease
// does this Worker also ask the Backend, and a Backend that no longer finds
// the Ref makes the record orphaned until Reconcile restarts or disposes it.
func (w *Worker) Attach(ctx context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	r, ok, err := w.store.Get(ctx, key)
	if err != nil {
		return effect.Attachment{}, err
	}
	if !ok {
		// No record proves no execution only when the store could not have
		// lost one (RUN-EXE-3); otherwise the answer is orphaned and the
		// control plane disposes the key explicitly.
		if w.provesAbsence {
			return effect.Attachment{State: effect.AttachmentMissing, Execution: effect.ExecutionNotFound}, nil
		}
		return effect.Attachment{State: effect.AttachmentOrphaned, Execution: effect.ExecutionNotFound}, nil
	}
	attachment := effect.Attachment{Execution: r.State, Owner: r.Owner, FencingEpoch: r.FencingEpoch, LeaseUntilUnixMilli: r.LeaseUntilUnixMilli}
	if protocol.StatusTerminal(r.State) {
		attachment.State = effect.AttachmentTerminal
		attachment.BackendAttached = false
		return attachment, nil
	}
	if r.FencingEpoch == 0 || r.LeaseUntilUnixMilli <= w.now().UnixMilli() {
		attachment.State = effect.AttachmentOrphaned
		return attachment, nil
	}
	if r.Owner != w.id {
		attachment.State = effect.AttachmentActive
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
		if r.Collected {
			return effect.Outcome{}, effect.ErrOutcomeCollected
		}
		if r.Outcome != nil {
			w.mu.Lock()
			delete(w.notify, key)
			w.mu.Unlock()
			return protocol.DecodeOutcome(r.Outcome), nil
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
	for i := range records {
		r := &records[i]
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
