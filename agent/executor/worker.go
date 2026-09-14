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
}

const defaultLeaseDuration = 30 * time.Second

// Worker owns execution leases, not Session ownership. A Worker can acquire
// an expired Assignment from a shared Store and resume it using the payload
// persisted in the execution record. Takeover is explicit: failure detection
// and the decision to retry an effect belong to the control plane.
type Worker struct {
	store     executionstore.Store
	backend   effect.Port
	id        string
	lease     time.Duration
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
	w := &Worker{store: records, backend: backend, id: opts.ID, lease: opts.LeaseDuration,
		lifecycle: context.WithoutCancel(ctx), notify: make(map[effect.AssignmentKey]chan struct{})}
	if err := w.recover(ctx); err != nil {
		return nil, err
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
	if !created && protocol.StatusTerminal(old.State) {
		return nil
	}
	if !created && old.Owner == w.id && old.FencingEpoch != 0 {
		owned, err := w.store.LeaseOwned(ctx, key, w.id, old.FencingEpoch)
		if err != nil {
			return err
		}
		if owned {
			return nil
		}
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
	owned, err := w.store.LeaseOwned(ctx, key, w.id, r.FencingEpoch)
	if err != nil {
		return err
	}
	if owned {
		return nil
	}
	return w.acquireAndStart(ctx, key)
}

func (w *Worker) acquireAndStart(ctx context.Context, key effect.AssignmentKey) error {
	claimed, acquired, err := w.store.Acquire(ctx, key, w.id, w.lease)
	if err != nil {
		return err
	}
	if !acquired {
		return nil
	}
	digest := claimed.AssignmentDigest
	var binding *effect.BackendBinding
	if claimed.BackendBinding != nil {
		b := *claimed.BackendBinding
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
			claimed.BackendBinding = binding
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
		cancelErr := w.backend.Cancel(ctx, key)
		go w.watch(key, digest, claimed.FencingEpoch, leaseDone)
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
			go w.watch(key, digest, claimed.FencingEpoch, leaseDone)
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
		settleErr := w.finishOwned(ctx, key, claimed.FencingEpoch, protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: key, AssignmentDigest: digest,
			Error: &protocol.WireError{Code: "dispatch_failed", Message: err.Error()}}, effect.ExecutionFailed, err)
		close(leaseDone)
		return settleErr
	}
	if err := w.store.TransitionOwned(ctx, key, w.id, claimed.FencingEpoch, effect.ExecutionDispatching, effect.ExecutionRunning); err != nil {
		// The backend call may already have crossed its external boundary.
		// Keep the watcher alive and report Dispatch as accepted; recovery
		// must reconcile the Dispatching/Running record rather than replan.
		go w.watch(key, digest, claimed.FencingEpoch, leaseDone)
		return nil
	}
	go w.watch(key, digest, claimed.FencingEpoch, leaseDone)
	return nil
}

func (w *Worker) dispatchBackend(ctx context.Context, assignment effect.Assignment, binding *effect.BackendBinding) error {
	if provider, ok := w.backend.(effect.BindingPort); ok && binding != nil {
		return provider.DispatchBound(ctx, assignment, *binding)
	}
	return w.backend.Dispatch(ctx, assignment)
}

func (w *Worker) attachBackend(ctx context.Context, key effect.AssignmentKey, binding *effect.BackendBinding) (effect.Attachment, error) {
	if provider, ok := w.backend.(effect.BindingPort); ok && binding != nil {
		return provider.AttachBound(ctx, key, *binding)
	}
	return w.backend.Attach(ctx, key)
}

func (w *Worker) watch(key effect.AssignmentKey, digest run.Digest, epoch uint64, done chan struct{}) {
	out, err := w.backend.GetOutcome(w.lifecycle, key)
	if err != nil {
		// GetOutcome failure is a lifecycle/transport failure after dispatch,
		// not evidence that the provider rejected the request. Persist Unknown
		// so the authority can reconcile or explicitly dispose the attempt.
		out = effect.Outcome{Key: key, Err: err, Unknown: true}
	}
	env := protocol.EncodeOutcome(out, digest)
	state := protocol.StatusForOutcome(out)
	_ = w.finishOwned(w.lifecycle, key, epoch, env, state, nil)
	// Keep the lease until the terminal outcome has been durably accepted;
	// otherwise a takeover can start a duplicate while this worker settles.
	close(done)
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
	backendAttachment, err := w.attachBackend(ctx, key, r.BackendBinding)
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
	if err := w.requestCancelOwned(ctx, key, r.FencingEpoch); err != nil {
		return err
	}
	return w.backend.Cancel(ctx, key)
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
		attachment, attachErr := w.attachBackend(ctx, r.Assignment.Key(), r.BackendBinding)
		if attachErr != nil {
			return attachErr
		}
		if attachment.State == effect.AttachmentActive || attachment.State == effect.AttachmentTerminal {
			done := make(chan struct{})
			go w.heartbeat(r.Assignment.Key(), r.FencingEpoch, done)
			go w.watch(r.Assignment.Key(), r.AssignmentDigest, r.FencingEpoch, done)
		}
	}
	return nil
}

var _ effect.Port = (*Worker)(nil)
