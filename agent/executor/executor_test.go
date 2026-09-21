package executor_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/executor"
	executorhttp "github.com/felinics/twilight/agent/executor/http"
	"github.com/felinics/twilight/agent/executor/protocol"
	"github.com/felinics/twilight/agent/executor/store"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/effect"
	"github.com/felinics/twilight/agent/run/model"
	"github.com/felinics/twilight/agent/run/schema"
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
	go func() {
		ch <- effect.Outcome{Key: a.Key(), Result: effect.ModelSucceeded{Result: sdk.ModelResult{Text: "ok"}}}
	}()
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

// routes serves every Assignment from one Port-shaped fake under the "test"
// provider.
func routes(p effect.ExecutionPort) []executor.Route {
	return []executor.Route{executor.Default("test", executor.PortBackend(p))}
}

// refBackend implements the Backend contract directly and counts its
// lifecycle calls; the Ref it prepares is fixed.
type refBackend struct {
	*testBackend
	mu       sync.Mutex
	prepared int
	started  int
	restarts int
	startRef string
}

func (b *refBackend) Prepare(context.Context, effect.Assignment) (string, error) {
	b.mu.Lock()
	b.prepared++
	b.mu.Unlock()
	return "execution-1", nil
}

func (b *refBackend) Start(ctx context.Context, ref string, a effect.Assignment) error {
	b.mu.Lock()
	b.started++
	b.startRef = ref
	b.mu.Unlock()
	return b.Dispatch(ctx, a)
}

func (b *refBackend) Restart(context.Context, string, effect.Assignment) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.restarts++
	return fmt.Sprintf("execution-%d", b.restarts+1), nil
}

func (b *refBackend) Attach(ctx context.Context, ref string) (effect.Attachment, error) {
	return b.testBackend.Attach(ctx, b.lastKey())
}

func (b *refBackend) Status(ctx context.Context, ref string) (effect.ExecutionStatus, error) {
	return b.GetStatus(ctx, b.lastKey())
}

func (b *refBackend) Outcome(ctx context.Context, ref string) (effect.Outcome, error) {
	return b.GetOutcome(ctx, b.lastKey())
}

func (b *refBackend) Cancel(ctx context.Context, ref string) error {
	return b.testBackend.Cancel(ctx, b.lastKey())
}

func (b *testBackend) lastKey() effect.AssignmentKey {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.last.Key()
}

func testAssignment() effect.Assignment {
	request := model.ModelRequest{Model: "m"}
	digest, err := schema.V1().Canonical.DigestRequest(request)
	if err != nil {
		panic(err)
	}
	return effect.Assignment{Session: "s", RunID: "r", StepID: "step", Effect: "effect", Schema: 1,
		Body: effect.ModelAssignment{Model: "m", Request: &request, RequestDigest: digest}}
}

func TestWorkerIdempotentAndOutcome(t *testing.T) {
	ctx := context.Background()
	backend := newTestBackend()
	worker, err := executor.NewWorker(ctx, store.NewMemoryStore(), routes(backend))
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
	if err != nil || modelText(out) != "ok" {
		t.Fatalf("outcome = %+v, %v", out, err)
	}
	out2, err := worker.GetOutcome(ctx, a.Key())
	if err != nil || modelText(out2) != "ok" {
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
			worker, err := executor.NewWorker(ctx, records, routes(backend), executor.WorkerOptions{ID: "new-worker"})
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
		return effect.Outcome{Key: key, Result: effect.ModelSucceeded{Result: sdk.ModelResult{Text: "accepted before response was lost"}}}, nil
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
			worker, err := executor.NewWorker(ctx, records, routes(backend))
			if err != nil {
				t.Fatal(err)
			}
			var port effect.ExecutionPort = worker
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
			if err != nil || modelText(out) != "accepted before response was lost" {
				t.Fatalf("eventual outcome = %+v, %v", out, err)
			}
		})
	}
}

func TestHTTPDispatchAssignmentConflictIsDefinite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	worker, err := executor.NewWorker(ctx, store.NewMemoryStore(), routes(newTestBackend()))
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
			return effect.Outcome{Key: key, Result: effect.Unknown{Message: "execution explicitly abandoned"}}, nil
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
			worker, err := executor.NewWorker(ctx, records, routes(backend))
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
			if _, isUnknown := out.Result.(effect.Unknown); err != nil || isUnknown != unknown || (!unknown && modelText(out) != "ok") {
				t.Fatalf("eventual outcome = %+v, %v", out, err)
			}
		})
	}
}

// holdBackend accepts a Dispatch and never produces its Outcome.
type holdBackend struct{ *testBackend }

func (b *holdBackend) Dispatch(_ context.Context, a effect.Assignment) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.outcomes[a.Key()] == nil {
		b.outcomes[a.Key()] = make(chan effect.Outcome, 1)
	}
	return nil
}

// Close stops the reconcile loop and every watcher and heartbeat, and
// returns while a backend execution is still running; the record keeps its
// lease for another incarnation.
func TestWorkerCloseStopsGoroutines(t *testing.T) {
	ctx := context.Background()
	records := store.NewMemoryStore()
	backend := &holdBackend{newTestBackend()}
	worker, err := executor.NewWorker(ctx, records, []executor.Route{executor.Default("test", executor.PortBackend(backend))},
		executor.WorkerOptions{ID: "worker-c", LeaseDuration: time.Second, ReconcileInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	a := testAssignment()
	if err := worker.Dispatch(ctx, a); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { worker.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return with a watcher in flight")
	}
	r, ok, err := records.Get(ctx, a.Key())
	if err != nil || !ok || r.State.Terminal() {
		t.Fatalf("record after close = %+v ok=%v %v, want a live non-terminal record", r, ok, err)
	}
}

// Adopting a model record whose execution the backend no longer finds
// restarts it as a new generation: the previous Ref moves to Superseded and
// Start receives the new one (RUN-EXE-9).
func TestWorkerRestartSupersedesRef(t *testing.T) {
	ctx := context.Background()
	records := store.NewMemoryStore()
	a := testAssignment()
	digest, err := a.Digest()
	if err != nil {
		t.Fatal(err)
	}
	r := store.Record{Assignment: a, AssignmentDigest: digest, State: effect.ExecutionRunning,
		ExecutionRef: store.ExecutionRef{Provider: "ref", Ref: "execution-1"},
		Owner:        "dead-worker", FencingEpoch: 2, LeaseUntilUnixMilli: 1}
	if err := records.Put(ctx, r); err != nil {
		t.Fatal(err)
	}
	backend := &refBackend{testBackend: newTestBackend()}
	worker, err := executor.NewWorker(ctx, records, []executor.Route{executor.Default("ref", backend)}, executor.WorkerOptions{ID: "worker-b", LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	if err := worker.Takeover(ctx, a.Key()); err != nil {
		t.Fatal(err)
	}
	got, _, err := records.Get(ctx, a.Key())
	if err != nil {
		t.Fatal(err)
	}
	if got.ExecutionRef.Ref != "execution-2" || len(got.Superseded) != 1 || got.Superseded[0].Ref != "execution-1" {
		t.Fatalf("restarted record = ref %q superseded %+v, want execution-2 over [execution-1]", got.ExecutionRef.Ref, got.Superseded)
	}
	backend.mu.Lock()
	prepared, restarts, startRef := backend.prepared, backend.restarts, backend.startRef
	backend.mu.Unlock()
	if prepared != 0 || restarts != 1 || startRef != "execution-2" {
		t.Fatalf("backend calls = prepared:%d restarts:%d start ref:%q, want 0/1/execution-2", prepared, restarts, startRef)
	}
}

// A record whose ExecutionRef names a provider this Worker has no Backend
// for is left untouched: Takeover and GetStatus report ErrUnknownProvider and
// no backend is called (RUN-EXE-10).
func TestWorkerTakeoverRefusesUnknownProvider(t *testing.T) {
	ctx := context.Background()
	records := store.NewMemoryStore()
	a := testAssignment()
	digest, err := a.Digest()
	if err != nil {
		t.Fatal(err)
	}
	r := store.Record{Assignment: a, AssignmentDigest: digest, State: effect.ExecutionRunning,
		ExecutionRef: store.ExecutionRef{Provider: "elsewhere", Ref: "existing-job"},
		Owner:        "expired-worker", FencingEpoch: 3, LeaseUntilUnixMilli: 1}
	if err := records.Put(ctx, r); err != nil {
		t.Fatal(err)
	}
	backend := newTestBackend()
	worker, err := executor.NewWorker(ctx, records, routes(backend), executor.WorkerOptions{ID: "new-worker"})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Takeover(ctx, a.Key()); !errors.Is(err, executor.ErrUnknownProvider) {
		t.Fatalf("takeover = %v, want ErrUnknownProvider", err)
	}
	if _, err := worker.GetStatus(ctx, a.Key()); !errors.Is(err, executor.ErrUnknownProvider) {
		t.Fatalf("status = %v, want ErrUnknownProvider", err)
	}
	got, _, err := records.Get(ctx, a.Key())
	if err != nil || got.Owner != r.Owner || got.FencingEpoch != r.FencingEpoch || got.ExecutionRef != r.ExecutionRef {
		t.Fatalf("unknown-provider takeover changed the record: %+v, %v", got, err)
	}
	backend.mu.Lock()
	calls := backend.calls
	backend.mu.Unlock()
	if calls != 0 {
		t.Fatalf("unknown-provider takeover dispatched %d calls", calls)
	}
}

// Dispatch selects the Backend once, prepares the Ref and persists it with the
// record before Start; Start receives the persisted Ref (RUN-EXE-9/10).
func TestWorkerPersistsExecutionRefBeforeStart(t *testing.T) {
	ctx := context.Background()
	records := store.NewMemoryStore()
	backend := &refBackend{testBackend: newTestBackend()}
	worker, err := executor.NewWorker(ctx, records, []executor.Route{executor.Default("ref", backend)},
		executor.WorkerOptions{ID: "worker-a", LeaseDuration: time.Second})
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
	if record.ExecutionRef != (store.ExecutionRef{Provider: "ref", Ref: "execution-1"}) {
		t.Fatalf("execution ref = %+v", record.ExecutionRef)
	}
	backend.mu.Lock()
	prepared, started, startRef := backend.prepared, backend.started, backend.startRef
	backend.mu.Unlock()
	if prepared != 1 || started != 1 || startRef != "execution-1" {
		t.Fatalf("backend calls = prepared:%d started:%d ref:%q, want 1/1/execution-1", prepared, started, startRef)
	}
	if err := worker.Dispatch(ctx, a); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	prepared, started = backend.prepared, backend.started
	backend.mu.Unlock()
	if prepared != 2 || started != 1 {
		t.Fatalf("replay prepared %d times and started %d times, want a second prepare and no second start", prepared, started)
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
	worker, err := executor.NewWorker(ctx, records, routes(backend), executor.WorkerOptions{
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
	if err != nil || modelText(out) != "ok" {
		t.Fatalf("recovered outcome = %+v, %v", out, err)
	}
	backend.mu.Lock()
	calls := backend.calls
	var request *model.ModelRequest
	if modelAssignment, ok := backend.last.Model(); ok {
		request = modelAssignment.Request
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
	worker, err := executor.NewWorker(ctx, records, routes(backend), executor.WorkerOptions{
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
	if err != nil || modelText(out) != "ok" {
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
	worker, err := executor.NewWorker(ctx, records, routes(backend), executor.WorkerOptions{
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
	worker, err := executor.NewWorker(ctx, records, routes(backend), executor.WorkerOptions{
		ID: "worker-b", LeaseDuration: time.Second, ReconcileInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	out, err := worker.GetOutcome(ctx, a.Key())
	if err != nil || modelText(out) != "ok" {
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
	first, err := executor.NewWorker(ctx, fs, routes(newTestBackend()))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Dispatch(ctx, a); err != nil {
		t.Fatal(err)
	}
	if _, err := first.GetOutcome(ctx, a.Key()); err != nil {
		t.Fatal(err)
	}
	second, err := executor.NewWorker(ctx, fs, routes(newTestBackend()))
	if err != nil {
		t.Fatal(err)
	}
	out, err := second.GetOutcome(ctx, a.Key())
	if _, ok := out.ModelResult(); err != nil || !ok {
		t.Fatalf("recreated worker outcome = %+v, %v", out, err)
	}
}

func TestHTTPClientAndServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	worker, err := executor.NewWorker(ctx, store.NewMemoryStore(), routes(newTestBackend()))
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
	if err != nil || modelText(out) != "ok" {
		t.Fatalf("HTTP outcome = %+v, %v", out, err)
	}
}

func testToolAssignment() effect.Assignment {
	return effect.Assignment{Session: "s", RunID: "r", StepID: "step", CallID: "call-1", Effect: "effect", Schema: 1,
		Body: effect.ToolAssignment{ToolRef: "gate", DefinitionDigest: "d", Arguments: run.MustParseCanonicalJSON(`{}`), Policy: run.DirectExecution}}
}

// Dispose is the control plane's give-up path: the record settles Unknown
// regardless of owner or lease, so the authority's next read disposes the Run
// target; terminal records and missing keys are not errors.
func TestWorkerDisposeSettlesUnknown(t *testing.T) {
	completedEnv := protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Unknown: false}
	rows := []struct {
		name    string
		record  *store.Record // nil means the key was never written
		wantErr error
	}{
		{"expired foreign owner", &store.Record{State: effect.ExecutionRunning, Owner: "dead-worker", FencingEpoch: 3, LeaseUntilUnixMilli: 1}, nil},
		{"live foreign owner", &store.Record{State: effect.ExecutionDispatching, Owner: "live-worker", FencingEpoch: 3,
			LeaseUntilUnixMilli: time.Now().Add(time.Hour).UnixMilli()}, nil},
		{"already terminal", &store.Record{State: effect.ExecutionCompleted, Owner: "dead-worker", FencingEpoch: 3, LeaseUntilUnixMilli: 1, Outcome: &completedEnv}, nil},
		{"missing record", nil, effect.ErrExecutionNotFound},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			ctx := context.Background()
			records := store.NewMemoryStore()
			a := testAssignment()
			key := a.Key()
			if row.record != nil {
				digest, err := a.Digest()
				if err != nil {
					t.Fatal(err)
				}
				r := *row.record
				r.Assignment, r.AssignmentDigest = a, digest
				if err := records.Put(ctx, r); err != nil {
					t.Fatal(err)
				}
			}
			worker, err := executor.NewWorker(ctx, records, routes(newTestBackend()), executor.WorkerOptions{ID: "worker-a"})
			if err != nil {
				t.Fatal(err)
			}
			err = worker.Dispose(ctx, key)
			if !errors.Is(err, row.wantErr) {
				t.Fatalf("dispose = %v, want %v", err, row.wantErr)
			}
			if row.record == nil {
				return
			}
			got, _, err := records.Get(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if row.record.State == effect.ExecutionCompleted {
				if got.State != effect.ExecutionCompleted || got.Outcome == nil || got.Outcome.Unknown {
					t.Fatalf("terminal record changed: %+v", got)
				}
				return
			}
			if got.State != effect.ExecutionUnknown || got.Outcome == nil || !got.Outcome.Unknown {
				t.Fatalf("record after dispose = %+v", got)
			}
		})
	}
}

// Adoption never re-dispatches an unbound tool whose prior execution may have
// crossed the effect boundary (TRN-DUR-4); it settles Unknown instead. A
// record that never dispatched (Accepted) still executes on adoption, and
// model assignments stay replayable (TestWorkerReconcileAdoptsExpiredLease).
func TestWorkerAdoptionOfUnattachableToolSettlesUnknown(t *testing.T) {
	rows := []struct {
		state       effect.ExecutionStatus
		wantCalls   int
		wantUnknown bool
	}{
		{effect.ExecutionRunning, 0, true},
		{effect.ExecutionDispatching, 0, true},
		{effect.ExecutionAccepted, 1, false},
	}
	base := time.Unix(100, 0)
	now := base.Add(2 * time.Second)
	for _, row := range rows {
		t.Run(string(row.state), func(t *testing.T) {
			ctx := context.Background()
			records := store.NewMemoryStore(store.MemoryStoreOptions{Now: func() time.Time { return now }})
			a := testToolAssignment()
			digest, err := a.Digest()
			if err != nil {
				t.Fatal(err)
			}
			r := store.Record{Assignment: a, AssignmentDigest: digest, State: row.state,
				Owner: "dead-worker", FencingEpoch: 2, LeaseUntilUnixMilli: base.Add(time.Second).UnixMilli()}
			if err := records.Put(ctx, r); err != nil {
				t.Fatal(err)
			}
			backend := newTestBackend()
			worker, err := executor.NewWorker(ctx, records, routes(backend), executor.WorkerOptions{
				ID: "worker-b", LeaseDuration: time.Second, Clock: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			if err := worker.Takeover(ctx, a.Key()); err != nil {
				t.Fatal(err)
			}
			got, _, err := records.Get(ctx, a.Key())
			if err != nil {
				t.Fatal(err)
			}
			if row.wantUnknown {
				if got.State != effect.ExecutionUnknown || got.Outcome == nil || !got.Outcome.Unknown {
					t.Fatalf("adopted record = %+v, want Unknown settle", got)
				}
			} else if got.Owner != "worker-b" {
				t.Fatalf("adopted record owner = %q, want worker-b", got.Owner)
			}
			backend.mu.Lock()
			calls := backend.calls
			backend.mu.Unlock()
			if calls != row.wantCalls {
				t.Fatalf("backend calls = %d, want %d", calls, row.wantCalls)
			}
		})
	}
}

type flakyRenewStore struct {
	store.Store
	mu       sync.Mutex
	deadline time.Time
	renewals int
}

func (s *flakyRenewStore) Renew(ctx context.Context, key effect.AssignmentKey, owner string, epoch uint64, ttl time.Duration) error {
	s.mu.Lock()
	s.renewals++
	failing := time.Now().Before(s.deadline)
	s.mu.Unlock()
	if failing {
		return errors.New("store temporarily unavailable")
	}
	return s.Store.Renew(ctx, key, owner, epoch, ttl)
}

// A transient Renew failure must not stop lease maintenance; the heartbeat
// retries and the record stays owned. Without the retry the heartbeat exited
// on the first error and the lease stayed lost once the failure outlasted it.
func TestWorkerHeartbeatRetriesTransientRenewErrors(t *testing.T) {
	records := &flakyRenewStore{Store: store.NewMemoryStore(), deadline: time.Now().Add(1100 * time.Millisecond)}
	backend := &uncertainDispatchBackend{testBackend: newTestBackend(), ready: make(chan struct{})}
	defer close(backend.ready)
	worker, err := executor.NewWorker(context.Background(), records, routes(backend), executor.WorkerOptions{ID: "worker-a", LeaseDuration: 800 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	a := testAssignment()
	if err := worker.Dispatch(context.Background(), a); err == nil || !errors.Is(err, effect.ErrDispatchUnknown) {
		t.Fatalf("dispatch = %v, want ErrDispatchUnknown", err)
	}
	// The injected failure outlasts one 800ms lease; only a retrying
	// heartbeat re-establishes the lease after the store recovers.
	time.Sleep(time.Until(records.deadline) + 400*time.Millisecond)
	deadline := time.Now().Add(3 * time.Second)
	for {
		r, ok, err := records.Get(context.Background(), a.Key())
		if err != nil || !ok {
			t.Fatalf("record = %+v ok=%v err=%v", r, ok, err)
		}
		owned, err := records.LeaseOwned(context.Background(), a.Key(), "worker-a", r.FencingEpoch)
		if err == nil && owned {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lease lost after transient Renew failures; heartbeat did not retry")
		}
		time.Sleep(50 * time.Millisecond)
	}
	records.mu.Lock()
	renewals := records.renewals
	records.mu.Unlock()
	if renewals < 4 {
		t.Fatalf("renewals = %d, want at least 4 (failures retried)", renewals)
	}
}

func TestHTTPControlEndpoints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// The backend never answers, so Dispose races no watcher settlement.
	backend := &uncertainDispatchBackend{testBackend: newTestBackend(), ready: make(chan struct{})}
	defer close(backend.ready)
	worker, err := executor.NewWorker(ctx, store.NewMemoryStore(), routes(backend))
	if err != nil {
		t.Fatal(err)
	}
	client := &executorhttp.Client{BaseURL: "http://executor.invalid",
		HTTP: &http.Client{Transport: handlerTransport{handler: (&executorhttp.Server{Worker: worker}).Handler()}}}
	a := testAssignment()
	if err := client.Dispatch(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := client.Takeover(ctx, a.Key()); err != nil {
		t.Fatalf("takeover = %v", err)
	}
	if n, err := client.Reconcile(ctx); err != nil || n != 0 {
		t.Fatalf("reconcile = %d, %v; want no candidates", n, err)
	}
	b := testAssignment()
	b.Effect = "effect-2"
	if err := client.Dispatch(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := client.Dispose(ctx, b.Key()); err != nil {
		t.Fatalf("dispose = %v", err)
	}
	out, err := client.GetOutcome(ctx, b.Key())
	if _, isUnknown := out.Result.(effect.Unknown); err != nil || !isUnknown {
		t.Fatalf("disposed outcome = %+v, %v", out, err)
	}
}

func TestFileStoreListSkipsCorruptRecords(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fs, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	a := testAssignment()
	if err := fs.Put(ctx, store.Record{Assignment: a, AssignmentDigest: mustDigest(a), State: effect.ExecutionRunning}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "corrupt.json"), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	records, err := fs.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("list = %d records, want 1 (corrupt skipped)", len(records))
	}
}

func mustDigest(a effect.Assignment) run.Digest {
	d, err := a.Digest()
	if err != nil {
		panic(err)
	}
	return d
}

// modelText is the text of a ModelSucceeded outcome, "" for anything else.
func modelText(out effect.Outcome) string {
	r, ok := out.ModelResult()
	if !ok {
		return ""
	}
	return r.Text
}

// A lost tool execution is re-dispatched by adoption only when the Replay
// policy its Assignment carries is allowed; a forbidden or unjudged tool is
// settled Unknown, the message naming the declaration, and never started
// again (RUN-EXE-9, TRN-DUR-4).
func TestWorkerAdoptsToolByReplayDeclaration(t *testing.T) {
	cases := []struct {
		name     string
		policy   run.ReplayPolicy
		replayed bool
	}{
		{"allowed", run.ReplayAllowed, true},
		{"forbidden", run.ReplayForbidden, false},
		{"unknown", run.ReplayUnknown, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			records := store.NewMemoryStore()
			a := testToolAssignment()
			body, _ := a.Tool()
			body.Replay = tc.policy
			a.Body = body
			digest, err := a.Digest()
			if err != nil {
				t.Fatal(err)
			}
			r := store.Record{Assignment: a, AssignmentDigest: digest, State: effect.ExecutionRunning,
				ExecutionRef: store.ExecutionRef{Provider: "ref", Ref: "execution-1"},
				Owner:        "dead-worker", FencingEpoch: 2, LeaseUntilUnixMilli: 1}
			if err := records.Put(ctx, r); err != nil {
				t.Fatal(err)
			}
			backend := &refBackend{testBackend: newTestBackend()}
			worker, err := executor.NewWorker(ctx, records, []executor.Route{executor.Default("ref", backend)}, executor.WorkerOptions{ID: "worker-b", LeaseDuration: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer worker.Close()
			if err := worker.Takeover(ctx, a.Key()); err != nil {
				t.Fatal(err)
			}
			got, _, err := records.Get(ctx, a.Key())
			if err != nil {
				t.Fatal(err)
			}
			backend.mu.Lock()
			started, startRef := backend.started, backend.startRef
			backend.mu.Unlock()
			if tc.replayed {
				if got.ExecutionRef.Ref != "execution-2" || len(got.Superseded) != 1 || started != 1 || startRef != "execution-2" {
					t.Fatalf("replayed tool: record ref %q superseded %d started %d at %q", got.ExecutionRef.Ref, len(got.Superseded), started, startRef)
				}
				return
			}
			if got.State != effect.ExecutionUnknown || got.Outcome == nil || !got.Outcome.Unknown || got.Outcome.Error == nil ||
				got.Outcome.Error.Code != "adopted_without_replay" || !strings.Contains(got.Outcome.Error.Message, "declares replay "+tc.policy.String()) {
				t.Fatalf("unreplayable tool: state %s outcome %+v", got.State, got.Outcome)
			}
			if started != 0 || got.ExecutionRef.Ref != "execution-1" || len(got.Superseded) != 0 {
				t.Fatalf("unreplayable tool was re-dispatched: started %d ref %q superseded %d", started, got.ExecutionRef.Ref, len(got.Superseded))
			}
		})
	}
}

// Reconcile bounds deferred: a record this Worker keeps failing to take over
// is disposed once DisposeAfter has elapsed since the first failure, and the
// disposal is reported through Warn; before that the takeover error is
// returned and the record is untouched (RUN-EXE-6).
func TestWorkerReconcileDisposesUnadoptableOrphans(t *testing.T) {
	ctx := context.Background()
	records := store.NewMemoryStore()
	a := testAssignment()
	digest, err := a.Digest()
	if err != nil {
		t.Fatal(err)
	}
	r := store.Record{Assignment: a, AssignmentDigest: digest, State: effect.ExecutionRunning,
		ExecutionRef: store.ExecutionRef{Provider: "elsewhere", Ref: "existing-job"},
		Owner:        "expired-worker", FencingEpoch: 3, LeaseUntilUnixMilli: 1}
	if err := records.Put(ctx, r); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	now := time.Unix(1_000_000, 0)
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	var warned []error
	warn := func(err error) { mu.Lock(); defer mu.Unlock(); warned = append(warned, err) }
	worker, err := executor.NewWorker(ctx, records, routes(newTestBackend()),
		executor.WorkerOptions{ID: "new-worker", Clock: clock, DisposeAfter: 10 * time.Second, Warn: warn})
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	for _, step := range []struct {
		advance  time.Duration
		disposed bool
	}{{0, false}, {5 * time.Second, false}, {5 * time.Second, true}} {
		mu.Lock()
		now = now.Add(step.advance)
		mu.Unlock()
		_, err := worker.Reconcile(ctx)
		got, _, gerr := records.Get(ctx, a.Key())
		if gerr != nil {
			t.Fatal(gerr)
		}
		if !step.disposed {
			if !errors.Is(err, executor.ErrUnknownProvider) || got.State != effect.ExecutionRunning || got.Owner != r.Owner {
				t.Fatalf("before the bound: reconcile = %v, record %s/%s", err, got.State, got.Owner)
			}
			continue
		}
		if err != nil || got.State != effect.ExecutionUnknown || got.Outcome == nil || got.Outcome.Error == nil || got.Outcome.Error.Code != "disposed" {
			t.Fatalf("at the bound: reconcile = %v, record %s outcome %+v", err, got.State, got.Outcome)
		}
		mu.Lock()
		n := len(warned)
		var last error
		if n > 0 {
			last = warned[n-1]
		}
		mu.Unlock()
		if n != 1 || !errors.Is(last, executor.ErrOrphanDisposed) || !errors.Is(last, executor.ErrUnknownProvider) {
			t.Fatalf("warn = %v (%d), want one ErrOrphanDisposed wrapping the takeover error", last, n)
		}
	}
	if _, err := worker.Reconcile(ctx); err != nil {
		t.Fatalf("reconcile after disposal = %v", err)
	}
}

// Attach classifies by the record's lease in the shared store, not by which
// Worker answers: a live lease held by another incarnation is active, an
// expired one orphaned (RUN-EXE-3).
func TestWorkerAttachClassifiesByLease(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(2_000_000, 0)
	cases := []struct {
		name  string
		owner string
		epoch uint64
		lease int64
		want  effect.AttachmentState
	}{
		{"another incarnation, live lease", "other-worker", 3, now.Add(time.Minute).UnixMilli(), effect.AttachmentActive},
		{"another incarnation, expired lease", "other-worker", 3, now.Add(-time.Minute).UnixMilli(), effect.AttachmentOrphaned},
		{"never acquired", "", 0, 0, effect.AttachmentOrphaned},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			records := store.NewMemoryStore(store.MemoryStoreOptions{Now: func() time.Time { return now }})
			a := testAssignment()
			digest, err := a.Digest()
			if err != nil {
				t.Fatal(err)
			}
			r := store.Record{Assignment: a, AssignmentDigest: digest, State: effect.ExecutionRunning,
				ExecutionRef: store.ExecutionRef{Provider: "elsewhere", Ref: "job"}, Owner: tc.owner, FencingEpoch: tc.epoch, LeaseUntilUnixMilli: tc.lease}
			if err := records.Put(ctx, r); err != nil {
				t.Fatal(err)
			}
			worker, err := executor.NewWorker(ctx, records, routes(newTestBackend()), executor.WorkerOptions{ID: "this-worker", Clock: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			defer worker.Close()
			got, err := worker.Attach(ctx, a.Key())
			if err != nil || got.State != tc.want {
				t.Fatalf("attach = %+v, %v, want %s", got, err, tc.want)
			}
		})
	}
}

// transientBackend fails the first executions of an effect with a transient
// provider failure and succeeds afterwards; each Restart is a new Ref.
type transientBackend struct {
	*testBackend
	mu       sync.Mutex
	failures int
	starts   []string
	code     effect.FailureCode
}

func (b *transientBackend) Prepare(context.Context, effect.Assignment) (string, error) {
	return "exec-1", nil
}
func (b *transientBackend) Restart(_ context.Context, previous string, _ effect.Assignment) (string, error) {
	return previous + "'", nil
}
func (b *transientBackend) Start(_ context.Context, ref string, a effect.Assignment) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.last = a
	b.starts = append(b.starts, ref)
	var result effect.OutcomeResult = effect.ModelSucceeded{Result: sdk.ModelResult{Text: "ok"}}
	if len(b.starts) <= b.failures {
		result = effect.ModelFailed{Code: b.code, Message: "try again"}
	}
	if b.outcomes[a.Key()] == nil {
		b.outcomes[a.Key()] = make(chan effect.Outcome, 8)
	}
	b.outcomes[a.Key()] <- effect.Outcome{Key: a.Key(), Result: result}
	return nil
}
func (b *transientBackend) Attach(ctx context.Context, _ string) (effect.Attachment, error) {
	return b.testBackend.Attach(ctx, b.lastKey())
}
func (b *transientBackend) Status(ctx context.Context, _ string) (effect.ExecutionStatus, error) {
	return b.GetStatus(ctx, b.lastKey())
}
func (b *transientBackend) Outcome(ctx context.Context, _ string) (effect.Outcome, error) {
	return b.GetOutcome(ctx, b.lastKey())
}
func (b *transientBackend) Cancel(ctx context.Context, _ string) error {
	return b.testBackend.Cancel(ctx, b.lastKey())
}

// A Known transient failure of an effect whose policy allows it is
// re-dispatched through Restart within the Worker's budget, the earlier Refs
// kept in Superseded; a non-transient failure, an effect without the policy
// or an exhausted budget settle the failure (RUN-EXE-11).
func TestWorkerRetriesTransientFailures(t *testing.T) {
	toolWith := func(policy run.RetryPolicy) effect.Assignment {
		a := testToolAssignment()
		body, _ := a.Tool()
		body.Retry = policy
		a.Body = body
		return a
	}
	cases := []struct {
		name       string
		assignment effect.Assignment
		failures   int
		code       effect.FailureCode
		budget     int
		wantStarts int
		wantOK     bool
	}{
		{"model retried until success", testAssignment(), 2, effect.FailureRateLimited, 3, 3, true},
		{"model budget exhausted", testAssignment(), 5, effect.FailureProviderUnavailable, 2, 2, false},
		{"model non-transient failure", testAssignment(), 1, effect.FailureAuthentication, 3, 1, false},
		{"model retries disabled", testAssignment(), 1, effect.FailureConnection, 0, 1, false},
		{"tool without retry policy", toolWith(run.RetryUnknown), 1, effect.FailureRateLimited, 3, 1, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			records := store.NewMemoryStore()
			backend := &transientBackend{testBackend: newTestBackend(), failures: tc.failures, code: tc.code}
			worker, err := executor.NewWorker(ctx, records, []executor.Route{executor.Default("t", backend)},
				executor.WorkerOptions{ID: "w", Retry: executor.RetryBudget{MaxAttempts: tc.budget, Backoff: time.Millisecond}})
			if err != nil {
				t.Fatal(err)
			}
			defer worker.Close()
			if err := worker.Dispatch(ctx, tc.assignment); err != nil {
				t.Fatal(err)
			}
			waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			var out effect.Outcome
			for {
				out, err = worker.GetOutcome(waitCtx, tc.assignment.Key())
				if err == nil {
					break
				}
				if waitCtx.Err() != nil {
					t.Fatalf("outcome: %v", err)
				}
				time.Sleep(time.Millisecond)
			}
			_, ok := out.Result.(effect.ModelSucceeded)
			if ok != tc.wantOK {
				t.Fatalf("outcome = %#v, want success=%v", out.Result, tc.wantOK)
			}
			backend.mu.Lock()
			starts := len(backend.starts)
			backend.mu.Unlock()
			got, _, _ := records.Get(ctx, tc.assignment.Key())
			if starts != tc.wantStarts || len(got.Superseded) != tc.wantStarts-1 {
				t.Fatalf("starts = %d superseded = %d, want %d executions", starts, len(got.Superseded), tc.wantStarts)
			}
		})
	}
}

// failingCreateStore refuses every Create: the record store is unavailable.
type failingCreateStore struct{ store.Store }

func (failingCreateStore) Create(context.Context, store.Record) (store.Record, bool, error) {
	return store.Record{}, false, errors.New("store unavailable")
}

// A Dispatch the Worker cannot record is a retryable refusal and, over HTTP,
// a 503; a definite rejection of the Assignment is a plain error and a 400;
// neither is an unknown outcome (RUN-EXE-3).
func TestDispatchRefusalClassification(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name       string
		records    store.Store
		assignment effect.Assignment
		retryable  bool
	}{
		{"record store unavailable", failingCreateStore{store.NewMemoryStore()}, testAssignment(), true},
		{"assignment without body", store.NewMemoryStore(), effect.Assignment{Session: "s", RunID: "r", Effect: "e", Schema: 1}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			worker, err := executor.NewWorker(ctx, tc.records, routes(newTestBackend()))
			if err != nil {
				t.Fatal(err)
			}
			defer worker.Close()
			direct := worker.Dispatch(ctx, tc.assignment)
			client := &executorhttp.Client{BaseURL: "http://executor.invalid",
				HTTP: &http.Client{Transport: handlerTransport{handler: (&executorhttp.Server{Worker: worker}).Handler()}}}
			overHTTP := client.Dispatch(ctx, tc.assignment)
			for name, err := range map[string]error{"direct": direct, "http": overHTTP} {
				if err == nil || errors.Is(err, effect.ErrDispatchUnknown) || errors.Is(err, effect.ErrDispatchRetryable) != tc.retryable {
					t.Fatalf("%s dispatch = %v, want retryable=%v and not unknown", name, err, tc.retryable)
				}
			}
		})
	}
}
