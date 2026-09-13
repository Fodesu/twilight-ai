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

// errExecutorUnreachable is what the authority delivers for every in-flight
// assignment once the executor stops answering: the Loop settles a tool as
// Unknown (the effect may have happened) and a model call as a provider
// failure, which is the classification RUN-EXE-2 asks for.
var errExecutorUnreachable = errors.New("executor unreachable")

// remoteExecutor is the authority-side loop.Executor over HTTP. Dispatch and
// Attach register the Loop's deliver under the assignment key and forward the
// call; the executor posts Outcomes back to the callback endpoint, which pops
// the deliver so each accepted assignment reports exactly once. A liveness
// monitor turns an executor that stopped answering into errors for whatever
// is still pending, so the authority never waits forever for a dead worker.
type remoteExecutor struct {
	base     string
	callback string
	client   *http.Client

	mu      sync.Mutex
	pending map[loop.AssignmentKey]loop.Deliver
}

func newRemoteExecutor(base, callback string) *remoteExecutor {
	return &remoteExecutor{base: base, callback: callback,
		client:  &http.Client{Timeout: 5 * time.Second},
		pending: make(map[loop.AssignmentKey]loop.Deliver)}
}

func (r *remoteExecutor) register(key loop.AssignmentKey, d loop.Deliver) {
	r.mu.Lock()
	r.pending[key] = d
	r.mu.Unlock()
}

func (r *remoteExecutor) pop(key loop.AssignmentKey) (loop.Deliver, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.pending[key]
	delete(r.pending, key)
	return d, ok
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

func (r *remoteExecutor) Dispatch(_ context.Context, a loop.Assignment, deliver loop.Deliver) error {
	key := a.Key()
	r.register(key, deliver)
	if err := postJSON(r.client, r.base+"/dispatch", dispatchRequest{Assignment: a, Callback: r.callback}, nil); err != nil {
		r.pop(key)
		return err
	}
	return nil
}

func (r *remoteExecutor) Attach(_ context.Context, a loop.Assignment, deliver loop.Deliver) (bool, error) {
	key := a.Key()
	r.register(key, deliver)
	var resp struct {
		Attached bool `json:"attached"`
	}
	if err := postJSON(r.client, r.base+"/attach", dispatchRequest{Assignment: a, Callback: r.callback}, &resp); err != nil {
		r.pop(key)
		return false, err
	}
	if !resp.Attached {
		r.pop(key)
	}
	return resp.Attached, nil
}

func (r *remoteExecutor) Cancel(_ context.Context, runID run.RunID) error {
	return postJSON(r.client, r.base+"/cancel", struct {
		RunID run.RunID `json:"runId"`
	}{runID}, nil)
}

// handleOutcome is the callback endpoint the executor posts Outcomes to.
func (r *remoteExecutor) handleOutcome(w http.ResponseWriter, req *http.Request) {
	var wire wireOutcome
	if err := readJSON(req, &wire); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	d, ok := r.pop(wire.Key)
	if !ok {
		// Not ours (already delivered, or an attempt this process never
		// dispatched): the transport is at-least-once, the Loop is the judge.
		w.WriteHeader(http.StatusOK)
		return
	}
	out := decodeOutcome(wire)
	go d(out)
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
		stale := r.pending
		r.pending = make(map[loop.AssignmentKey]loop.Deliver)
		r.mu.Unlock()
		for key, d := range stale {
			go d(loop.Outcome{Key: key, Err: errExecutorUnreachable})
		}
		misses = 0
	}
}
