package executor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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
	ID            string
	LeaseDuration time.Duration
	Now           func() time.Time
}

const defaultLeaseDuration = 30 * time.Second

var workerSequence uint64

// Worker owns execution leases, not Session ownership. A Worker can acquire
// an expired Assignment from a shared Store and resume it using the payload
// persisted in the execution record. Takeover is explicit: failure detection
// and the decision to retry an effect belong to the control plane.
type Worker struct {
	store   executionstore.Store
	backend effect.Port
	id      string
	lease   time.Duration
	now     func() time.Time

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
		opts.ID = fmt.Sprintf("worker-%d", atomic.AddUint64(&workerSequence, 1))
	}
	if opts.LeaseDuration <= 0 {
		opts.LeaseDuration = defaultLeaseDuration
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	w := &Worker{store: records, backend: backend, id: opts.ID, lease: opts.LeaseDuration, now: opts.Now, notify: make(map[effect.AssignmentKey]chan struct{})}
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
		return nil
	}
	return w.acquireAndStart(ctx, key)
}

// Takeover explicitly asks this Worker to acquire an expired Assignment. The
// caller is the control plane: it must have decided that retrying this effect
// is safe or that provider reconciliation has already been attempted.
func (w *Worker) Takeover(ctx context.Context, key effect.AssignmentKey) error {
	return w.acquireAndStart(ctx, key)
}

func (w *Worker) acquireAndStart(ctx context.Context, key effect.AssignmentKey) error {
	claimed, acquired, err := w.store.Acquire(ctx, key, w.id, w.now(), w.lease)
	if err != nil {
		return err
	}
	if !acquired {
		return nil
	}
	digest := claimed.AssignmentDigest
	leaseDone := make(chan struct{})
	go w.heartbeat(key, claimed.FencingEpoch, leaseDone)
	if err := w.backend.Dispatch(context.WithoutCancel(ctx), claimed.Assignment); err != nil {
		close(leaseDone)
		return w.finishOwned(ctx, key, claimed.FencingEpoch, protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: key, AssignmentDigest: digest,
			Error: &protocol.WireError{Code: "dispatch_failed", Message: err.Error()}}, effect.ExecutionFailed, err)
	}
	if err := w.setStateOwned(ctx, key, claimed.FencingEpoch, effect.ExecutionRunning); err != nil {
		close(leaseDone)
		return err
	}
	go w.watch(key, digest, claimed.FencingEpoch, leaseDone)
	return nil
}

func (w *Worker) watch(key effect.AssignmentKey, digest run.Digest, epoch uint64, done chan struct{}) {
	out, err := w.backend.GetOutcome(context.Background(), key)
	if err != nil {
		out = effect.Outcome{Key: key, Err: err}
	}
	close(done)
	env := protocol.EncodeOutcome(out, digest)
	state := protocol.StatusForOutcome(out)
	_ = w.finishOwned(context.Background(), key, epoch, env, state, nil)
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
		case now := <-ticker.C:
			if err := w.store.Renew(context.Background(), key, w.id, epoch, now, w.lease); err != nil {
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

func (w *Worker) setStateOwned(ctx context.Context, key effect.AssignmentKey, epoch uint64, state effect.ExecutionStatus) error {
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
	return w.store.PutOwned(ctx, r, w.id, epoch)
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
	if err := w.setStateOwned(ctx, key, r.FencingEpoch, effect.ExecutionCancelRequested); err != nil {
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
		if protocol.StatusTerminal(r.State) || r.Owner != w.id || r.FencingEpoch == 0 {
			continue
		}
		attached, attachErr := w.backend.Attach(ctx, r.Assignment.Key())
		if attachErr != nil {
			return attachErr
		}
		if attached {
			done := make(chan struct{})
			go w.heartbeat(r.Assignment.Key(), r.FencingEpoch, done)
			go w.watch(r.Assignment.Key(), r.AssignmentDigest, r.FencingEpoch, done)
		}
	}
	return nil
}

var _ effect.Port = (*Worker)(nil)
