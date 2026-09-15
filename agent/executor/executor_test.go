package executor_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/executor"
	executorhttp "github.com/felinics/twilight/agent/executor/http"
	"github.com/felinics/twilight/agent/executor/store"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/effect"
	"github.com/felinics/twilight/sdk"
)

type testBackend struct {
	mu       sync.Mutex
	calls    int
	last     effect.Assignment
	outcomes map[effect.AssignmentKey]chan effect.Outcome
}

func newTestBackend() *testBackend {
	return &testBackend{outcomes: make(map[effect.AssignmentKey]chan effect.Outcome)}
}

type bindingBackend struct {
	*testBackend
	mu       sync.Mutex
	prepared int
	bound    int
}

func (b *bindingBackend) PrepareBinding(context.Context, effect.Assignment) (effect.ExecutionBinding, error) {
	b.mu.Lock()
	b.prepared++
	b.mu.Unlock()
	return effect.ExecutionBinding{Provider: "test", ExecutionRef: "execution-1"}, nil
}

func (b *bindingBackend) DispatchBound(ctx context.Context, a effect.Assignment, binding effect.ExecutionBinding) error {
	if binding.ExecutionRef == "" {
		return errors.New("missing execution binding")
	}
	b.mu.Lock()
	b.bound++
	b.mu.Unlock()
	return b.testBackend.Dispatch(ctx, a)
}

func (b *bindingBackend) AttachBound(ctx context.Context, key effect.AssignmentKey, binding effect.ExecutionBinding) (effect.Attachment, error) {
	if binding.ExecutionRef == "" {
		return effect.Attachment{}, errors.New("missing execution binding")
	}
	return b.testBackend.Attach(ctx, key)
}

func (b *bindingBackend) GetStatusBound(ctx context.Context, key effect.AssignmentKey, binding effect.ExecutionBinding) (effect.ExecutionStatus, error) {
	if binding.ExecutionRef == "" {
		return effect.ExecutionNotFound, errors.New("missing execution binding")
	}
	return b.testBackend.GetStatus(ctx, key)
}

func (b *bindingBackend) GetOutcomeBound(ctx context.Context, key effect.AssignmentKey, binding effect.ExecutionBinding) (effect.Outcome, error) {
	if binding.ExecutionRef == "" {
		return effect.Outcome{}, errors.New("missing execution binding")
	}
	return b.testBackend.GetOutcome(ctx, key)
}

func (b *bindingBackend) CancelBound(ctx context.Context, key effect.AssignmentKey, binding effect.ExecutionBinding) error {
	if binding.ExecutionRef == "" {
		return errors.New("missing execution binding")
	}
	return b.testBackend.Cancel(ctx, key)
}

func (b *testBackend) Validate(context.Context, effect.Assignment) (*run.ToolFailure, error) {
	return nil, nil
}
func (b *testBackend) Dispatch(_ context.Context, a effect.Assignment) error {
	b.mu.Lock()
	b.calls++
	b.last = a
	if b.outcomes[a.Key()] == nil {
		b.outcomes[a.Key()] = make(chan effect.Outcome, 1)
	}
	ch := b.outcomes[a.Key()]
	b.mu.Unlock()
	go func() { ch <- effect.Outcome{Key: a.Key(), Model: &sdk.ModelResult{Text: "ok"}} }()
	return nil
}
func (b *testBackend) Attach(_ context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.outcomes[key] == nil {
		return effect.Attachment{State: effect.AttachmentMissing, Execution: effect.ExecutionNotFound}, nil
	}
	return effect.Attachment{State: effect.AttachmentActive, Execution: effect.ExecutionRunning, BackendAttached: true}, nil
}
func (b *testBackend) GetStatus(_ context.Context, key effect.AssignmentKey) (effect.ExecutionStatus, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.outcomes[key] == nil {
		return effect.ExecutionNotFound, effect.ErrExecutionNotFound
	}
	return effect.ExecutionRunning, nil
}
func (b *testBackend) GetOutcome(ctx context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
	b.mu.Lock()
	ch := b.outcomes[key]
	b.mu.Unlock()
	if ch == nil {
		return effect.Outcome{}, effect.ErrExecutionNotFound
	}
	select {
	case out := <-ch:
		return out, nil
	case <-ctx.Done():
		return effect.Outcome{}, ctx.Err()
	}
}
func (b *testBackend) Cancel(context.Context, effect.AssignmentKey) error { return nil }

func testAssignment() effect.Assignment {
	request := run.ModelRequest{Model: "m"}
	digest, err := run.ProtocolV1().DigestRequest(request)
	if err != nil {
		panic(err)
	}
	return effect.Assignment{Session: "s", RunID: "r", StepID: "step", Claim: "claim", Schema: 1,
		Kind: effect.AssignmentModel, Model: &effect.ModelAssignment{Model: "m", Request: &request, RequestDigest: digest}}
}

func TestWorkerIdempotentAndOutcome(t *testing.T) {
	ctx := context.Background()
	backend := newTestBackend()
	worker, err := executor.NewWorker(ctx, store.NewMemoryStore(), backend)
	if err != nil {
		t.Fatal(err)
	}
	a := testAssignment()
	if err := worker.Dispatch(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := worker.Dispatch(ctx, a); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	calls := backend.calls
	backend.mu.Unlock()
	if calls != 1 {
		t.Fatalf("backend calls = %d, want 1", calls)
	}
	out, err := worker.GetOutcome(ctx, a.Key())
	if err != nil || out.Model == nil || out.Model.Text != "ok" {
		t.Fatalf("outcome = %+v, %v", out, err)
	}
	out2, err := worker.GetOutcome(ctx, a.Key())
	if err != nil || out2.Model == nil || out2.Model.Text != "ok" {
		t.Fatalf("replayed outcome = %+v, %v", out2, err)
	}
}

func TestWorkerDispatchReplayPreservesExistingExecution(t *testing.T) {
	for _, state := range []effect.ExecutionStatus{effect.ExecutionAccepted, effect.ExecutionDispatching, effect.ExecutionRunning, effect.ExecutionCancelRequested} {
		t.Run(string(state), func(t *testing.T) {
			ctx := context.Background()
			records := store.NewMemoryStore()
			a := testAssignment()
			digest, err := a.Digest()
			if err != nil {
				t.Fatal(err)
			}
			r := store.Record{Assignment: a, AssignmentDigest: digest, State: state,
				Owner: "expired-worker", FencingEpoch: 4, LeaseUntilUnixMilli: 1}
			if err := records.Put(ctx, r); err != nil {
				t.Fatal(err)
			}
			backend := newTestBackend()
			worker, err := executor.NewWorker(ctx, records, backend, executor.WorkerOptions{ID: "new-worker"})
			if err != nil {
				t.Fatal(err)
			}
			if err := worker.Dispatch(ctx, a); err != nil {
				t.Fatal(err)
			}
			got, _, err := records.Get(ctx, a.Key())
			if err != nil || got.State != r.State || got.Owner != r.Owner || got.FencingEpoch != r.FencingEpoch {
				t.Fatalf("replay changed execution: %+v, %v", got, err)
			}
			backend.mu.Lock()
			calls := backend.calls
			backend.mu.Unlock()
			if calls != 0 {
				t.Fatalf("replay dispatched %d backend calls", calls)
			}
		})
	}
}

type uncertainDispatchBackend struct {
	*testBackend
	ready chan struct{}
}

func (b *uncertainDispatchBackend) Dispatch(_ context.Context, a effect.Assignment) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	b.last = a
	return effect.ErrDispatchUnknown
}

func (b *uncertainDispatchBackend) GetOutcome(ctx context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
	select {
	case <-b.ready:
		return effect.Outcome{Key: key, Model: &sdk.ModelResult{Text: "accepted before response was lost"}}, nil
	case <-ctx.Done():
		return effect.Outcome{}, ctx.Err()
	}
}

type handlerTransport struct{ handler http.Handler }

func (t handlerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response := httptest.NewRecorder()
	t.handler.ServeHTTP(response, request)
	return response.Result(), nil
}

func TestWorkerUncertainDispatchPreservesExecution(t *testing.T) {
	for _, overHTTP := range []bool{false, true} {
		t.Run(fmt.Sprint("http=", overHTTP), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			records := store.NewMemoryStore()
			backend := &uncertainDispatchBackend{testBackend: newTestBackend(), ready: make(chan struct{})}
			defer close(backend.ready)
			worker, err := executor.NewWorker(ctx, records, backend)
			if err != nil {
				t.Fatal(err)
			}
			var port effect.Port = worker
			if overHTTP {
				port = &executorhttp.Client{BaseURL: "http://executor.invalid",
					HTTP: &http.Client{Transport: handlerTransport{handler: (&executorhttp.Server{Worker: worker}).Handler()}}}
			}
			a := testAssignment()
			err = port.Dispatch(ctx, a)
			if overHTTP && err != nil || !overHTTP && !errors.Is(err, effect.ErrDispatchUnknown) {
				t.Fatalf("dispatch error = %v", err)
			}
			r, _, err := records.Get(ctx, a.Key())
			if err != nil || r.State != effect.ExecutionDispatching || r.Outcome != nil {
				t.Fatalf("uncertain dispatch changed execution: %+v, %v", r, err)
			}
			if err := port.Dispatch(ctx, a); err != nil {
				t.Fatalf("acceptance replay = %v", err)
			}
			backend.mu.Lock()
			calls := backend.calls
			backend.mu.Unlock()
			if calls != 1 {
				t.Fatalf("backend calls = %d, want 1", calls)
			}
			select {
			case backend.ready <- struct{}{}:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			out, err := port.GetOutcome(ctx, a.Key())
			if err != nil || out.Unknown || out.Err != nil || out.Model == nil || out.Model.Text != "accepted before response was lost" {
				t.Fatalf("eventual outcome = %+v, %v", out, err)
			}
		})
	}
}

func TestHTTPDispatchAssignmentConflictIsDefinite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	worker, err := executor.NewWorker(ctx, store.NewMemoryStore(), newTestBackend())
	if err != nil {
		t.Fatal(err)
	}
	client := &executorhttp.Client{BaseURL: "http://executor.invalid",
		HTTP: &http.Client{Transport: handlerTransport{handler: (&executorhttp.Server{Worker: worker}).Handler()}}}
	a := testAssignment()
	if err := client.Dispatch(ctx, a); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetOutcome(ctx, a.Key()); err != nil {
		t.Fatal(err)
	}
	a.Target = &run.TargetRef{Kind: "workspace", ID: "conflicting-target"}
	if err := client.Dispatch(ctx, a); err == nil || errors.Is(err, effect.ErrDispatchUnknown) {
		t.Fatalf("assignment conflict = %v, want definite rejection", err)
	}
}

type temporarilyUnreadableBackend struct {
	*testBackend
	failed  chan struct{}
	ready   chan struct{}
	once    sync.Once
	unknown bool
}

func (b *temporarilyUnreadableBackend) GetOutcome(ctx context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
	select {
	case <-b.ready:
		if b.unknown {
			return effect.Outcome{Key: key, Unknown: true, Err: errors.New("execution explicitly abandoned")}, nil
		}
		return b.testBackend.GetOutcome(ctx, key)
	default:
		b.once.Do(func() { close(b.failed) })
		return effect.Outcome{}, errors.New("temporary outcome transport failure")
	}
}

func TestWorkerOutcomeReadFailurePreservesExecution(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprint("explicit_unknown=", unknown), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			records := store.NewMemoryStore()
			backend := &temporarilyUnreadableBackend{testBackend: newTestBackend(), failed: make(chan struct{}), ready: make(chan struct{}), unknown: unknown}
			worker, err := executor.NewWorker(ctx, records, backend)
			if err != nil {
				t.Fatal(err)
			}
			a := testAssignment()
			if err := worker.Dispatch(ctx, a); err != nil {
				t.Fatal(err)
			}
			select {
			case <-backend.failed:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			r, _, err := records.Get(ctx, a.Key())
			if err != nil || r.State != effect.ExecutionRunning || r.Outcome != nil {
				t.Fatalf("read error changed execution: %+v, %v", r, err)
			}
			close(backend.ready)
			out, err := worker.GetOutcome(ctx, a.Key())
			if err != nil || out.Unknown != unknown || (!unknown && (out.Model == nil || out.Model.Text != "ok")) {
				t.Fatalf("eventual outcome = %+v, %v", out, err)
			}
		})
	}
}

func TestWorkerTakeoverRequiresPersistedBindingCapability(t *testing.T) {
	ctx := context.Background()
	records := store.NewMemoryStore()
	a := testAssignment()
	digest, err := a.Digest()
	if err != nil {
		t.Fatal(err)
	}
	r := store.Record{Assignment: a, AssignmentDigest: digest, State: effect.ExecutionRunning,
		ExecutionBinding: &effect.ExecutionBinding{Provider: "provider", ExecutionRef: "existing-job"},
		Owner:            "expired-worker", FencingEpoch: 3, LeaseUntilUnixMilli: 1}
	if err := records.Put(ctx, r); err != nil {
		t.Fatal(err)
	}
	backend := newTestBackend()
	worker, err := executor.NewWorker(ctx, records, backend, executor.WorkerOptions{ID: "new-worker"})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Takeover(ctx, a.Key()); !errors.Is(err, effect.ErrBindingUnsupported) {
		t.Fatalf("takeover = %v, want ErrBindingUnsupported", err)
	}
	if _, err := worker.GetStatus(ctx, a.Key()); !errors.Is(err, effect.ErrBindingUnsupported) {
		t.Fatalf("status = %v, want ErrBindingUnsupported", err)
	}
	got, _, err := records.Get(ctx, a.Key())
	if err != nil || got.Owner != r.Owner || got.FencingEpoch != r.FencingEpoch || got.ExecutionBinding.ExecutionRef != "existing-job" {
		t.Fatalf("unsupported takeover changed execution: %+v, %v", got, err)
	}
	backend.mu.Lock()
	calls := backend.calls
	backend.mu.Unlock()
	if calls != 0 {
		t.Fatalf("unsupported takeover dispatched %d calls", calls)
	}
}

func TestRecordReadsLegacyBackendBinding(t *testing.T) {
	var record store.Record
	if err := json.Unmarshal([]byte(`{"backendBinding":{"provider":"local","workspace":"ws-1","executionRef":"exec-1"}}`), &record); err != nil {
		t.Fatal(err)
	}
	if record.ExecutionBinding == nil || record.ExecutionBinding.Provider != "local" || record.ExecutionBinding.ExecutionRef != "exec-1" {
		t.Fatalf("execution binding = %+v", record.ExecutionBinding)
	}
}

func TestWorkerPersistsExecutionBindingBeforeDispatch(t *testing.T) {
	ctx := context.Background()
	records := store.NewMemoryStore()
	backend := &bindingBackend{testBackend: newTestBackend()}
	worker, err := executor.NewWorker(ctx, records, backend, executor.WorkerOptions{ID: "worker-a", LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	a := testAssignment()
	if err := worker.Dispatch(ctx, a); err != nil {
		t.Fatal(err)
	}
	record, ok, err := records.Get(ctx, a.Key())
	if err != nil || !ok {
		t.Fatalf("record = %+v, ok=%v, err=%v", record, ok, err)
	}
	if record.ExecutionBinding == nil || record.ExecutionBinding.ExecutionRef != "execution-1" {
		t.Fatalf("execution binding = %+v", record.ExecutionBinding)
	}
	backend.mu.Lock()
	prepared, bound := backend.prepared, backend.bound
	backend.mu.Unlock()
	if prepared != 1 || bound != 1 {
		t.Fatalf("binding calls = prepared:%d bound:%d, want 1/1", prepared, bound)
	}
}

func TestExecutionStoreFencesTakeover(t *testing.T) {
	ctx := context.Background()
	base := time.Unix(100, 0)
	now := base
	records := store.NewMemoryStore(store.MemoryStoreOptions{Now: func() time.Time { return now }})
	a := testAssignment()
	digest, err := a.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if _, created, err := records.Create(ctx, store.Record{Assignment: a, AssignmentDigest: digest, State: effect.ExecutionAccepted}); err != nil || !created {
		t.Fatalf("create = %v, created=%v", err, created)
	}
	first, acquired, err := records.Acquire(ctx, a.Key(), "worker-a", time.Second)
	if err != nil || !acquired || first.FencingEpoch != 1 {
		t.Fatalf("first acquire = %+v, acquired=%v, err=%v", first, acquired, err)
	}
	now = base.Add(500 * time.Millisecond)
	if _, acquired, err := records.Acquire(ctx, a.Key(), "worker-b", time.Second); err != nil || acquired {
		t.Fatalf("live lease acquire = acquired=%v, err=%v; want rejected", acquired, err)
	}
	now = base.Add(2 * time.Second)
	second, acquired, err := records.Acquire(ctx, a.Key(), "worker-b", time.Second)
	if err != nil || !acquired || second.FencingEpoch != 2 {
		t.Fatalf("takeover = %+v, acquired=%v, err=%v", second, acquired, err)
	}
	if err := records.PutOwned(ctx, first, "worker-a", first.FencingEpoch); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("stale put = %v, want ErrLeaseLost", err)
	}
}

func TestExecutionStoreRequiresDispatchingBarrier(t *testing.T) {
	ctx := context.Background()
	base := time.Unix(100, 0)
	now := base
	records := store.NewMemoryStore(store.MemoryStoreOptions{Now: func() time.Time { return now }})
	a := testAssignment()
	digest, err := a.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := records.Create(ctx, store.Record{Assignment: a, AssignmentDigest: digest, State: effect.ExecutionAccepted}); err != nil {
		t.Fatal(err)
	}
	claimed, acquired, err := records.Acquire(ctx, a.Key(), "worker-a", time.Second)
	if err != nil || !acquired {
		t.Fatalf("acquire = %+v, acquired=%v, err=%v", claimed, acquired, err)
	}
	if err := records.TransitionOwned(ctx, a.Key(), "worker-a", claimed.FencingEpoch, effect.ExecutionAccepted, effect.ExecutionRunning); !errors.Is(err, store.ErrStateConflict) {
		t.Fatalf("accepted to running = %v, want ErrStateConflict", err)
	}
	if err := records.TransitionOwned(ctx, a.Key(), "worker-a", claimed.FencingEpoch, effect.ExecutionAccepted, effect.ExecutionDispatching); err != nil {
		t.Fatalf("accepted to dispatching = %v", err)
	}
	now = base.Add(2 * time.Second)
	if err := records.TransitionOwned(ctx, a.Key(), "worker-a", claimed.FencingEpoch, effect.ExecutionDispatching, effect.ExecutionRunning); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("expired transition = %v, want ErrLeaseLost", err)
	}
}

func TestWorkerReclaimsExpiredAssignment(t *testing.T) {
	ctx := context.Background()
	base := time.Unix(100, 0)
	now := base
	records := store.NewMemoryStore(store.MemoryStoreOptions{Now: func() time.Time { return now }})
	a := testAssignment()
	digest, err := a.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if _, created, err := records.Create(ctx, store.Record{Assignment: a, AssignmentDigest: digest, State: effect.ExecutionAccepted}); err != nil || !created {
		t.Fatalf("create = %v, created=%v", err, created)
	}
	claimed, acquired, err := records.Acquire(ctx, a.Key(), "worker-a", time.Second)
	if err != nil || !acquired {
		t.Fatalf("initial acquire = %+v, acquired=%v", claimed, acquired)
	}
	if err := records.TransitionOwned(ctx, a.Key(), "worker-a", claimed.FencingEpoch, effect.ExecutionAccepted, effect.ExecutionDispatching); err != nil {
		t.Fatalf("mark dispatching = %v", err)
	}
	backend := newTestBackend()
	worker, err := executor.NewWorker(ctx, records, backend, executor.WorkerOptions{
		ID: "worker-b", LeaseDuration: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	now = base.Add(2 * time.Second)
	if err := worker.Takeover(ctx, a.Key()); err != nil {
		t.Fatal(err)
	}
	readCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	out, err := worker.GetOutcome(readCtx, a.Key())
	if err != nil || out.Model == nil || out.Model.Text != "ok" {
		t.Fatalf("recovered outcome = %+v, %v", out, err)
	}
	backend.mu.Lock()
	calls := backend.calls
	var request *run.ModelRequest
	if backend.last.Model != nil {
		request = backend.last.Model.Request
	}
	backend.mu.Unlock()
	if calls != 1 || request == nil {
		t.Fatalf("recovery dispatch calls=%d request=%v, want one inline request", calls, request)
	}
}

func TestWorkerReconcileAdoptsExpiredLease(t *testing.T) {
	ctx := context.Background()
	base := time.Unix(100, 0)
	now := base.Add(2 * time.Second)
	records := store.NewMemoryStore(store.MemoryStoreOptions{Now: func() time.Time { return now }})
	a := testAssignment()
	digest, err := a.Digest()
	if err != nil {
		t.Fatal(err)
	}
	r := store.Record{Assignment: a, AssignmentDigest: digest, State: effect.ExecutionRunning,
		Owner: "dead-worker", FencingEpoch: 4, LeaseUntilUnixMilli: base.Add(time.Second).UnixMilli()}
	if err := records.Put(ctx, r); err != nil {
		t.Fatal(err)
	}
	backend := newTestBackend()
	worker, err := executor.NewWorker(ctx, records, backend, executor.WorkerOptions{
		ID: "worker-b", LeaseDuration: time.Second, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	n, err := worker.Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("reconcile offered = %d, want 1", n)
	}
	readCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	out, err := worker.GetOutcome(readCtx, a.Key())
	if err != nil || out.Model == nil || out.Model.Text != "ok" {
		t.Fatalf("adopted outcome = %+v, %v", out, err)
	}
	got, _, err := records.Get(ctx, a.Key())
	if err != nil || got.Owner != "worker-b" {
		t.Fatalf("adopted record = %+v, %v", got, err)
	}
	backend.mu.Lock()
	calls := backend.calls
	backend.mu.Unlock()
	if calls != 1 {
		t.Fatalf("backend calls = %d, want 1", calls)
	}
}

func TestWorkerReconcileLeavesLiveLease(t *testing.T) {
	ctx := context.Background()
	base := time.Unix(100, 0)
	now := base
	records := store.NewMemoryStore(store.MemoryStoreOptions{Now: func() time.Time { return now }})
	a := testAssignment()
	digest, err := a.Digest()
	if err != nil {
		t.Fatal(err)
	}
	r := store.Record{Assignment: a, AssignmentDigest: digest, State: effect.ExecutionRunning,
		Owner: "worker-a", FencingEpoch: 4, LeaseUntilUnixMilli: base.Add(time.Second).UnixMilli()}
	if err := records.Put(ctx, r); err != nil {
		t.Fatal(err)
	}
	backend := newTestBackend()
	worker, err := executor.NewWorker(ctx, records, backend, executor.WorkerOptions{
		ID: "worker-b", LeaseDuration: time.Second, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if n, err := worker.Reconcile(ctx); err != nil || n != 0 {
		t.Fatalf("reconcile = %d, %v; want no candidates", n, err)
	}
	got, _, err := records.Get(ctx, a.Key())
	if err != nil || got.Owner != "worker-a" || got.FencingEpoch != 4 {
		t.Fatalf("live record changed: %+v, %v", got, err)
	}
	backend.mu.Lock()
	calls := backend.calls
	backend.mu.Unlock()
	if calls != 0 {
		t.Fatalf("reconcile dispatched %d backend calls", calls)
	}
}

func TestWorkerReconcileLoopAdoptsOrphanedRecord(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	records := store.NewMemoryStore()
	a := testAssignment()
	digest, err := a.Digest()
	if err != nil {
		t.Fatal(err)
	}
	r := store.Record{Assignment: a, AssignmentDigest: digest, State: effect.ExecutionDispatching,
		Owner: "dead-worker", FencingEpoch: 2, LeaseUntilUnixMilli: time.Now().Add(-time.Second).UnixMilli()}
	if err := records.Put(ctx, r); err != nil {
		t.Fatal(err)
	}
	backend := newTestBackend()
	worker, err := executor.NewWorker(ctx, records, backend, executor.WorkerOptions{
		ID: "worker-b", LeaseDuration: time.Second, ReconcileInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	out, err := worker.GetOutcome(ctx, a.Key())
	if err != nil || out.Model == nil || out.Model.Text != "ok" {
		t.Fatalf("reconciled outcome = %+v, %v", out, err)
	}
	backend.mu.Lock()
	calls := backend.calls
	backend.mu.Unlock()
	if calls != 1 {
		t.Fatalf("backend calls = %d, want 1", calls)
	}
}

func TestFileStoreSurvivesWorkerRecreation(t *testing.T) {
	ctx := context.Background()
	fs, err := store.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	a := testAssignment()
	first, err := executor.NewWorker(ctx, fs, newTestBackend())
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Dispatch(ctx, a); err != nil {
		t.Fatal(err)
	}
	if _, err := first.GetOutcome(ctx, a.Key()); err != nil {
		t.Fatal(err)
	}
	second, err := executor.NewWorker(ctx, fs, newTestBackend())
	if err != nil {
		t.Fatal(err)
	}
	out, err := second.GetOutcome(ctx, a.Key())
	if err != nil || out.Model == nil {
		t.Fatalf("recreated worker outcome = %+v, %v", out, err)
	}
}

func TestHTTPClientAndServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	worker, err := executor.NewWorker(ctx, store.NewMemoryStore(), newTestBackend())
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((&executorhttp.Server{Worker: worker}).Handler())
	defer server.Close()
	client := &executorhttp.Client{BaseURL: server.URL}
	a := testAssignment()
	if err := client.Dispatch(ctx, a); err != nil {
		t.Fatal(err)
	}
	status, err := client.GetStatus(ctx, a.Key())
	if err != nil || status == effect.ExecutionNotFound {
		t.Fatalf("status = %s, %v", status, err)
	}
	out, err := client.GetOutcome(ctx, a.Key())
	if err != nil || out.Model == nil || out.Model.Text != "ok" {
		t.Fatalf("HTTP outcome = %+v, %v", out, err)
	}
}
