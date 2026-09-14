package cloudtest

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
)

// errExecutorUnreachable is what the authority observes for every in-flight
// assignment once the executor stops answering. The Loop treats this as an
// unknown effect boundary rather than a provider rejection.
var errExecutorUnreachable = errors.New("executor unreachable")

// remoteExecutor is the authority-side loop.Executor over HTTP. The callback
// endpoint only completes a local outcome record; Loop retrieves the result by
// key through GetOutcome. This keeps callback an internal transport optimization
// rather than part of the Executor interface.
type remoteExecutor struct {
	base     string
	callback string
	client   *http.Client

	mu      sync.Mutex
	pending map[loop.AssignmentKey]chan loop.Outcome
}

func newRemoteExecutor(base, callback string) *remoteExecutor {
	return &remoteExecutor{base: base, callback: callback,
		client:  &http.Client{Timeout: 5 * time.Second},
		pending: make(map[loop.AssignmentKey]chan loop.Outcome)}
}

func (r *remoteExecutor) register(key loop.AssignmentKey) chan loop.Outcome {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ch := r.pending[key]; ch != nil {
		return ch
	}
	ch := make(chan loop.Outcome, 1)
	r.pending[key] = ch
	return ch
}

func (r *remoteExecutor) complete(key loop.AssignmentKey, out loop.Outcome) {
	ch := r.register(key)
	out.Key = key
	select {
	case ch <- out:
	default: // duplicate callback; GetOutcome remains idempotent
	}
}

func (r *remoteExecutor) Validate(_ context.Context, a loop.Assignment) (*run.ToolFailure, error) {
	var resp struct {
		Failure *run.ToolFailure `json:"failure"`
	}
	if err := postJSON(r.client, r.base+"/validate", a, &resp); err != nil {
		return nil, err
	}
	return resp.Failure, nil
}

func (r *remoteExecutor) Dispatch(_ context.Context, a loop.Assignment) error {
	r.register(a.Key())
	return postJSON(r.client, r.base+"/dispatch", dispatchRequest{Assignment: a, Callback: r.callback}, nil)
}

func (r *remoteExecutor) Attach(_ context.Context, key loop.AssignmentKey) (loop.Attachment, error) {
	r.register(key)
	var attachment loop.Attachment
	if err := postJSON(r.client, r.base+"/attach", attachRequest{Key: key, Callback: r.callback}, &attachment); err != nil {
		return loop.Attachment{}, err
	}
	if attachment.State != loop.AttachmentActive && attachment.State != loop.AttachmentTerminal {
		r.mu.Lock()
		delete(r.pending, key)
		r.mu.Unlock()
	}
	return attachment, nil
}

func (r *remoteExecutor) GetStatus(_ context.Context, key loop.AssignmentKey) (loop.ExecutionStatus, error) {
	return loop.ExecutionRunning, nil
}

func (r *remoteExecutor) GetOutcome(ctx context.Context, key loop.AssignmentKey) (loop.Outcome, error) {
	r.mu.Lock()
	ch := r.pending[key]
	r.mu.Unlock()
	if ch == nil {
		return loop.Outcome{}, loop.ErrExecutionNotFound
	}
	select {
	case out := <-ch:
		return out, nil
	case <-ctx.Done():
		return loop.Outcome{}, ctx.Err()
	}
}

func (r *remoteExecutor) Cancel(_ context.Context, key loop.AssignmentKey) error {
	return postJSON(r.client, r.base+"/cancel", struct {
		Key loop.AssignmentKey `json:"key"`
	}{key}, nil)
}

// handleOutcome is the callback endpoint the executor posts Outcomes to.
func (r *remoteExecutor) handleOutcome(w http.ResponseWriter, req *http.Request) {
	var wire wireOutcome
	if err := readJSON(req, &wire); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.complete(wire.Key, decodeOutcome(wire))
	w.WriteHeader(http.StatusOK)
}

// monitor polls the executor while assignments are pending; after two missed
// health checks every pending assignment reports errExecutorUnreachable.
func (r *remoteExecutor) monitor(ctx context.Context) {
	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()
	misses := 0
	probe := &http.Client{Timeout: 500 * time.Millisecond}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		r.mu.Lock()
		n := len(r.pending)
		r.mu.Unlock()
		if n == 0 {
			misses = 0
			continue
		}
		if err := getJSON(probe, r.base+"/health", nil); err == nil {
			misses = 0
			continue
		}
		misses++
		if misses < 2 {
			continue
		}
		r.mu.Lock()
		stale := make(map[loop.AssignmentKey]chan loop.Outcome, len(r.pending))
		for key, ch := range r.pending {
			stale[key] = ch
		}
		r.mu.Unlock()
		for key, ch := range stale {
			select {
			case ch <- loop.Outcome{Key: key, Err: errExecutorUnreachable}:
			default:
			}
		}
		misses = 0
	}
}
