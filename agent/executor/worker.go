package executor

import (
	"context"
	"errors"
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
type Worker struct {
	store   executionstore.Store
	backend effect.Port

	mu     sync.Mutex
	notify map[effect.AssignmentKey]chan struct{}
}

func NewWorker(ctx context.Context, records executionstore.Store, backend effect.Port) (*Worker, error) {
	if records == nil {
		return nil, errors.New("executor: nil worker store")
	}
	if backend == nil {
		return nil, errors.New("executor: nil worker backend")
	}
	w := &Worker{store: records, backend: backend, notify: make(map[effect.AssignmentKey]chan struct{})}
	if err := w.recover(ctx); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *Worker) Validate(ctx context.Context, a effect.Assignment) (*run.ToolFailure, error) {
	return w.backend.Validate(ctx, a)
}

func (w *Worker) Dispatch(ctx context.Context, a effect.Assignment) error {
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
	if !created {
		if old.AssignmentDigest != digest {
			return executionstore.ErrAssignmentConflict
		}
		return nil
	}
	if err := w.store.Put(ctx, record); err != nil {
		return err
	}
	if err := w.backend.Dispatch(context.WithoutCancel(ctx), a); err != nil {
		return w.finish(ctx, key, protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: key, AssignmentDigest: digest,
			Error: &protocol.WireError{Code: "dispatch_failed", Message: err.Error()}}, effect.ExecutionFailed, err)
	}
	if err := w.setState(ctx, key, effect.ExecutionRunning); err != nil {
		return err
	}
	go w.watch(key, digest)
	return nil
}

func (w *Worker) watch(key effect.AssignmentKey, digest run.Digest) {
	out, err := w.backend.GetOutcome(context.Background(), key)
	if err != nil {
		out = effect.Outcome{Key: key, Err: err}
	}
	env := protocol.EncodeOutcome(out, digest)
	state := protocol.StatusForOutcome(out)
	_ = w.finish(context.Background(), key, env, state, nil)
}

func (w *Worker) finish(ctx context.Context, key effect.AssignmentKey, outcome protocol.OutcomeEnvelope, state effect.ExecutionStatus, dispatchErr error) error {
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
	if err := w.store.Put(ctx, r); err != nil {
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

func (w *Worker) setState(ctx context.Context, key effect.AssignmentKey, state effect.ExecutionStatus) error {
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
		return nil
	}
	r.State = state
	return w.store.Put(ctx, r)
}

func (w *Worker) Attach(ctx context.Context, key effect.AssignmentKey) (bool, error) {
	r, ok, err := w.store.Get(ctx, key)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	// A model whose worker record became Unknown must be disposed by the
	// authority's model recovery path rather than reported as a provider error.
	if r.State == effect.ExecutionUnknown && r.Assignment.Kind == effect.AssignmentModel {
		return false, nil
	}
	return true, nil
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
	if err := w.setState(ctx, key, effect.ExecutionCancelRequested); err != nil {
		return err
	}
	return w.backend.Cancel(ctx, key)
}

func (w *Worker) recover(ctx context.Context) error {
	records, err := w.store.List(ctx)
	if err != nil {
		return err
	}
	for _, r := range records {
		if protocol.StatusTerminal(r.State) {
			continue
		}
		attached, attachErr := w.backend.Attach(ctx, r.Assignment.Key())
		if attachErr != nil {
			return attachErr
		}
		if attached {
			go w.watch(r.Assignment.Key(), r.AssignmentDigest)
			continue
		}
		// We cannot prove whether an interrupted arbitrary effect happened.
		// Preserve that uncertainty durably; the authority decides how to settle it.
		env := protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: r.Assignment.Key(), AssignmentDigest: r.AssignmentDigest,
			Error: &protocol.WireError{Code: "effect_unknown", Message: "worker restarted while effect was in progress"}}
		if err := w.finish(ctx, r.Assignment.Key(), env, effect.ExecutionUnknown, nil); err != nil {
			return err
		}
	}
	return nil
}

var _ effect.Port = (*Worker)(nil)
