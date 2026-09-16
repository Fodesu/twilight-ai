package cloudtest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
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
	// waiters counts blocked GetOutcome calls per key. The monitor's
	// errExecutorUnreachable push only reaches assignments a drive is
	// actually waiting for: a settled replacement's durable outcome must
	// not lose to a synthetic error left over from an unawaited dispatch.
	waiters map[loop.AssignmentKey]int
}

func newRemoteExecutor(base, callback string) *remoteExecutor {
	return &remoteExecutor{base: base, callback: callback,
		client:  &http.Client{Timeout: 5 * time.Second},
		pending: make(map[loop.AssignmentKey]chan loop.Outcome),
		waiters: make(map[loop.AssignmentKey]int)}
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
	err := postJSON(r.client, r.base+"/dispatch", dispatchRequest{Assignment: a, Callback: r.callback}, nil)
	if err == nil {
		return nil
	}
	// A transport failure means the request may have reached the executor and
	// crossed its acceptance boundary before the process died — the response
	// was lost, not the request rejected. Report the unknown boundary instead
	// of a definite error (RUN-EXE-3); the durable record remains the single
	// source of the outcome.
	var transport *url.Error
	if errors.As(err, &transport) {
		return fmt.Errorf("%w: %v", loop.ErrDispatchUnknown, err)
	}
	return err
}

func (r *remoteExecutor) Attach(_ context.Context, key loop.AssignmentKey) (loop.Attachment, error) {
	r.register(key)
	var attachment loop.Attachment
	if err := postJSON(r.client, r.base+"/attach", attachRequest{Key: key, Callback: r.callback}, &attachment); err != nil {
		return loop.Attachment{}, err
	}
	// The pending entry stays: an orphaned record (not yet adopted by a
	// replacement worker) still settles, and GetOutcome's long-poll is how
	// this authority reads that settlement. Only a missing record avoids a
	// read, and RecoverInterrupted disposes that target instead.
	return attachment, nil
}

func (r *remoteExecutor) GetStatus(_ context.Context, key loop.AssignmentKey) (loop.ExecutionStatus, error) {
	return loop.ExecutionRunning, nil
}

func (r *remoteExecutor) GetOutcome(ctx context.Context, key loop.AssignmentKey) (loop.Outcome, error) {
	r.mu.Lock()
	ch := r.pending[key]
	if ch == nil {
		r.mu.Unlock()
		return loop.Outcome{}, loop.ErrExecutionNotFound
	}
	r.waiters[key]++
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.waiters[key]--
		r.mu.Unlock()
	}()
	for {
		select {
		case out := <-ch:
			return out, nil
		default:
		}
		// The callback only wakes a waiter; the durable read is a long-poll
		// of the executor's GetOutcome (RUN-EXE-8 pull channel). A dead
		// executor fails the poll, and the health misses above report
		// errExecutorUnreachable through the channel.
		var wire wireOutcome
		err := postJSON(r.client, r.base+"/outcome", struct {
			Key loop.AssignmentKey `json:"key"`
		}{key}, &wire)
		if err == nil {
			return decodeOutcome(wire), nil
		}
		select {
		case out := <-ch:
			return out, nil
		case <-time.After(200 * time.Millisecond):
		case <-ctx.Done():
			return loop.Outcome{}, ctx.Err()
		}
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
// health checks every pending assignment with a blocked reader reports
// errExecutorUnreachable. Assignments no drive waits on are left to their
// long-poll: a replacement executor's durable record is authoritative, and a
// stale synthetic error must not preempt it.
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
			if r.waiters[key] == 0 {
				continue
			}
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
