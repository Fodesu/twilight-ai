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

// Worker makes any local effect.Port available behind a durable execution
// record. The backend performs the actual Model/Tool effect; Worker owns the
// accepted-assignment and outcome lifecycle.
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

// Worker owns execution leases, not Session ownership. A Worker can acquire
// an expired Assignment from a shared Store and resume it using the payload
// persisted in the execution record. Takeover is explicit: failure detection
// and the decision to retry an effect belong to the control plane; Reconcile
// is the built-in loop form of that decision, while deployments with an
// external control plane drive Takeover directly.
type Worker struct {
	store     executionstore.Store
	backend   effect.Port
	id        string
	lease     time.Duration
	now       func() time.Time
	lifecycle context.Context

	mu     sync.Mutex
	notify map[effect.AssignmentKey]chan struct{}
}

func NewWorker(ctx context.Context, records executionstore.Store, backend effect.Port, options ...WorkerOptions) (*Worker, error) {
	if records == nil {
		return nil, errors.New("executor: nil worker store")
	}
	if backend == nil {
		return nil, errors.New("executor: nil worker backend")
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
	w := &Worker{store: records, backend: backend, id: opts.ID, lease: opts.LeaseDuration, now: now,
		lifecycle: context.WithoutCancel(ctx), notify: make(map[effect.AssignmentKey]chan struct{})}
	if err := w.recover(ctx); err != nil {
		return nil, err
	}
	if opts.ReconcileInterval > 0 {
		go w.reconcileLoop(opts.ReconcileInterval)
	}
	return w, nil
}

func (w *Worker) Validate(ctx context.Context, a effect.Assignment) (*run.ToolFailure, error) {
	return w.backend.Validate(ctx, a)
}

func (w *Worker) Dispatch(ctx context.Context, a effect.Assignment) error {
	if a.Kind == effect.AssignmentModel {
		if a.Model == nil || a.Model.Request == nil {
			return errors.New("executor: model assignment requires an inline request payload")
		}
		proto, err := run.ProtocolFor(a.Schema)
		if err != nil {
			return err
		}
		requestDigest, err := proto.DigestRequest(*a.Model.Request)
		if err != nil {
			return err
		}
		if requestDigest != a.Model.RequestDigest || a.Model.Request.Model != string(a.Model.Model) {
			return errors.New("executor: model request digest or model mismatch")
		}
	}
	digest, err := a.Digest()
	if err != nil {
		return err
	}
	key := a.Key()
	record := executionstore.Record{Assignment: a, AssignmentDigest: digest, State: effect.ExecutionAccepted}
	old, created, err := w.store.Create(ctx, record)
	if err != nil {
		return err
	}
	if !created && old.AssignmentDigest != digest {
		return executionstore.ErrAssignmentConflict
	}
	if !created {
		// A replay acknowledges the persisted acceptance. Recovery of an
		// existing record is authorized separately through Takeover.
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
	if err := w.checkBinding(r.ExecutionBinding); err != nil {
		return err
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
// payload, first trying to attach the previous backend execution and only
// re-dispatching after the backend reports it unattachable. Records under a
// live lease, this Worker's or another's, are skipped; the store remains the
// fencing authority. One record's failure does not stop the others. It
// returns the number of records handed to Takeover.
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

func (w *Worker) acquireAndStart(ctx context.Context, key effect.AssignmentKey) error {
	claimed, acquired, err := w.store.Acquire(ctx, key, w.id, w.lease)
	if err != nil {
		return err
	}
	if !acquired {
		return nil
	}
	if err := w.checkBinding(claimed.ExecutionBinding); err != nil {
		return err
	}
	digest := claimed.AssignmentDigest
	var binding *effect.ExecutionBinding
	if claimed.ExecutionBinding != nil {
		b := *claimed.ExecutionBinding
		binding = &b
	}
	if claimed.State != effect.ExecutionCancelRequested {
		if provider, ok := w.backend.(effect.BindingPort); ok && binding == nil {
			b, err := provider.PrepareBinding(ctx, claimed.Assignment)
			if err != nil {
				return err
			}
			if b.ExecutionRef == "" {
				return errors.New("executor: backend returned an empty execution binding")
			}
			binding = &b
			claimed.ExecutionBinding = binding
			if err := w.store.PutOwned(ctx, claimed, w.id, claimed.FencingEpoch); err != nil {
				return err
			}
		}
	}
	if claimed.State == effect.ExecutionCancelRequested {
		leaseDone := make(chan struct{})
		go w.heartbeat(key, claimed.FencingEpoch, leaseDone)
		attachment, attachErr := w.attachBackend(ctx, key, binding)
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
		cancelErr := w.cancelBackend(ctx, key, binding)
		go w.watch(key, digest, claimed.FencingEpoch, binding, leaseDone)
		return cancelErr
	}
	if claimed.State == effect.ExecutionRunning || claimed.State == effect.ExecutionDispatching {
		// Prefer adoption over retry. Takeover is allowed to retry only after
		// the backend says that the old execution is not attachable.
		attachment, attachErr := w.attachBackend(ctx, key, binding)
		if attachErr != nil {
			return attachErr
		}
		if attachment.State == effect.AttachmentActive || attachment.State == effect.AttachmentTerminal {
			leaseDone := make(chan struct{})
			go w.heartbeat(key, claimed.FencingEpoch, leaseDone)
			// Keep Dispatching as a conservative pre-outcome state. The watcher
			// will terminalize it after the adopted backend produces an outcome.
			go w.watch(key, digest, claimed.FencingEpoch, binding, leaseDone)
			return nil
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
	go w.heartbeat(key, claimed.FencingEpoch, leaseDone)
	if err := w.dispatchBackend(context.WithoutCancel(ctx), claimed.Assignment, binding); err != nil {
		if errors.Is(err, effect.ErrDispatchUnknown) {
			go w.watch(key, digest, claimed.FencingEpoch, binding, leaseDone)
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
		go w.watch(key, digest, claimed.FencingEpoch, binding, leaseDone)
		return nil
	}
	go w.watch(key, digest, claimed.FencingEpoch, binding, leaseDone)
	return nil
}

func (w *Worker) dispatchBackend(ctx context.Context, assignment effect.Assignment, binding *effect.ExecutionBinding) error {
	if err := w.checkBinding(binding); err != nil {
		return err
	}
	if binding != nil {
		provider := w.backend.(effect.BindingPort)
		return provider.DispatchBound(ctx, assignment, *binding)
	}
	return w.backend.Dispatch(ctx, assignment)
}

func (w *Worker) attachBackend(ctx context.Context, key effect.AssignmentKey, binding *effect.ExecutionBinding) (effect.Attachment, error) {
	if err := w.checkBinding(binding); err != nil {
		return effect.Attachment{}, err
	}
	if binding != nil {
		provider := w.backend.(effect.BindingPort)
		return provider.AttachBound(ctx, key, *binding)
	}
	return w.backend.Attach(ctx, key)
}

func (w *Worker) getOutcomeBackend(ctx context.Context, key effect.AssignmentKey, binding *effect.ExecutionBinding) (effect.Outcome, error) {
	if err := w.checkBinding(binding); err != nil {
		return effect.Outcome{}, err
	}
	if binding != nil {
		provider := w.backend.(effect.BindingPort)
		return provider.GetOutcomeBound(ctx, key, *binding)
	}
	return w.backend.GetOutcome(ctx, key)
}

func (w *Worker) cancelBackend(ctx context.Context, key effect.AssignmentKey, binding *effect.ExecutionBinding) error {
	if err := w.checkBinding(binding); err != nil {
		return err
	}
	if binding != nil {
		provider := w.backend.(effect.BindingPort)
		return provider.CancelBound(ctx, key, *binding)
	}
	return w.backend.Cancel(ctx, key)
}

func (w *Worker) checkBinding(binding *effect.ExecutionBinding) error {
	if binding != nil {
		if _, ok := w.backend.(effect.BindingPort); !ok {
			return effect.ErrBindingUnsupported
		}
	}
	return nil
}

func (w *Worker) watch(key effect.AssignmentKey, digest run.Digest, epoch uint64, binding *effect.ExecutionBinding, done chan struct{}) {
	defer close(done)
	delay := 10 * time.Millisecond
	var out effect.Outcome
	for {
		// Bound each read by the execution lease so a blocked backend read
		// eventually yields to the ownership check before the next poll.
		readCtx, cancelRead := context.WithTimeout(w.lifecycle, w.lease)
		var err error
		out, err = w.getOutcomeBackend(readCtx, key, binding)
		cancelRead()
		if err == nil {
			break
		}
		if !w.waitOwned(key, epoch, delay) {
			return
		}
		delay = min(delay*2, time.Second)
	}
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
	owned, err := w.store.LeaseOwned(w.lifecycle, key, w.id, epoch)
	if (err == nil && !owned) || errors.Is(err, effect.ErrExecutionNotFound) {
		return false
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-w.lifecycle.Done():
		return false
	case <-timer.C:
		return true
	}
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
		case <-ticker.C:
			if err := w.store.Renew(w.lifecycle, key, w.id, epoch, w.lease); err != nil {
				return
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
	backendAttachment, err := w.attachBackend(ctx, key, r.ExecutionBinding)
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

func (w *Worker) GetStatus(ctx context.Context, key effect.AssignmentKey) (effect.ExecutionStatus, error) {
	r, ok, err := w.store.Get(ctx, key)
	if err != nil {
		return effect.ExecutionNotFound, err
	}
	if !ok {
		return effect.ExecutionNotFound, effect.ErrExecutionNotFound
	}
	if r.ExecutionBinding != nil && !protocol.StatusTerminal(r.State) {
		if err := w.checkBinding(r.ExecutionBinding); err != nil {
			return r.State, err
		}
		provider := w.backend.(effect.BindingPort)
		return provider.GetStatusBound(ctx, key, *r.ExecutionBinding)
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
	if err := w.checkBinding(r.ExecutionBinding); err != nil {
		return err
	}
	if err := w.requestCancelOwned(ctx, key, r.FencingEpoch); err != nil {
		return err
	}
	return w.cancelBackend(ctx, key, r.ExecutionBinding)
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
		attachment, attachErr := w.attachBackend(ctx, r.Assignment.Key(), r.ExecutionBinding)
		if attachErr != nil {
			// One broken backend read must not block recovery of the other
			// records; a later Reconcile or explicit Takeover retries this one.
			continue
		}
		if attachment.State == effect.AttachmentActive || attachment.State == effect.AttachmentTerminal {
			done := make(chan struct{})
			go w.heartbeat(r.Assignment.Key(), r.FencingEpoch, done)
			go w.watch(r.Assignment.Key(), r.AssignmentDigest, r.FencingEpoch, r.ExecutionBinding, done)
		}
	}
	return nil
}

var _ effect.Port = (*Worker)(nil)
