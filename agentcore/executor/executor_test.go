package executor_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/executor"
	executorhttp "github.com/felinics/twilight/agentcore/executor/http"
	"github.com/felinics/twilight/agentcore/executor/protocol"
	"github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/agentcore/store/sqlite"
	"github.com/felinics/twilight/agentcore/store/sqlite/sqlitetest"
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
	// attach, when set, scripts Attach's answer instead of the Port's.
	attach effect.AttachmentState
}

func (b *refBackend) setAttach(state effect.AttachmentState) {
	b.mu.Lock()
	b.attach = state
	b.mu.Unlock()
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
	b.mu.Lock()
	scripted := b.attach
	b.mu.Unlock()
	if scripted != "" {
		return effect.Attachment{State: scripted}, nil
	}
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
	worker, err := executor.NewWorker(ctx, sqlitetest.Open(t).Executions(), routes(backend))
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
			records := sqlitetest.Open(t).Executions()
			a := testAssignment()
			digest, err := a.Digest()
			if err != nil {
				t.Fatal(err)
			}
			r := store.ExecutionState{Assignment: a, AssignmentDigest: digest, State: state,
				Owner: "expired-worker", FencingEpoch: 4, LeaseUntilUnixMilli: 1}
			if err := records.Seed(ctx, r); err != nil {
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
			got, _, _, err := records.Load(ctx, a.Key())
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
			records := sqlitetest.Open(t).Executions()
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
			r, _, _, err := records.Load(ctx, a.Key())
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
	worker, err := executor.NewWorker(ctx, sqlitetest.Open(t).Executions(), routes(newTestBackend()))
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
			records := sqlitetest.Open(t).Executions()
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
			r, _, _, err := records.Load(ctx, a.Key())
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

// Close stops every watcher and heartbeat, and returns while a backend
// execution is still running; the record keeps its lease for another
// incarnation.
func TestWorkerCloseStopsGoroutines(t *testing.T) {
	ctx := context.Background()
	records := sqlitetest.Open(t).Executions()
	backend := &holdBackend{newTestBackend()}
	worker, err := executor.NewWorker(ctx, records, []executor.Route{executor.Default("test", executor.PortBackend(backend))},
		executor.WorkerOptions{ID: "worker-c", LeaseDuration: time.Second})
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
	r, _, ok, err := records.Load(ctx, a.Key())
	if err != nil || !ok || r.State.Terminal() {
		t.Fatalf("record after close = %+v ok=%v %v, want a live non-terminal record", r, ok, err)
	}
}

// Adopting a model record whose execution the backend no longer finds
// restarts it as a new generation: the previous Ref moves to Superseded and
// Start receives the new one (RUN-EXE-9).
func TestWorkerRestartSupersedesRef(t *testing.T) {
	ctx := context.Background()
	records := sqlitetest.Open(t).Executions()
	a := testAssignment()
	digest, err := a.Digest()
	if err != nil {
		t.Fatal(err)
	}
	r := store.ExecutionState{Assignment: a, AssignmentDigest: digest, State: effect.ExecutionRunning,
		ExecutionRef: store.ExecutionRef{Provider: "ref", Ref: "execution-1"},
		Owner:        "dead-worker", FencingEpoch: 2, LeaseUntilUnixMilli: 1}
	if err := records.Seed(ctx, r); err != nil {
		t.Fatal(err)
	}
	backend := &refBackend{testBackend: newTestBackend()}
	worker, err := executor.NewWorker(ctx, records, []executor.Route{executor.Default("ref", backend)}, executor.WorkerOptions{ID: "worker-b", LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	if err := worker.RecoverExecution(ctx, a.Key()); err != nil {
		t.Fatal(err)
	}
	got, _, _, err := records.Load(ctx, a.Key())
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
// for is left untouched: RecoverExecution and GetStatus report ErrUnknownProvider and
// no backend is called (RUN-EXE-10).
func TestWorkerRecoverExecutionRefusesUnknownProvider(t *testing.T) {
	ctx := context.Background()
	records := sqlitetest.Open(t).Executions()
	a := testAssignment()
	digest, err := a.Digest()
	if err != nil {
		t.Fatal(err)
	}
	r := store.ExecutionState{Assignment: a, AssignmentDigest: digest, State: effect.ExecutionRunning,
		ExecutionRef: store.ExecutionRef{Provider: "elsewhere", Ref: "existing-job"},
		Owner:        "expired-worker", FencingEpoch: 3, LeaseUntilUnixMilli: 1}
	if err := records.Seed(ctx, r); err != nil {
		t.Fatal(err)
	}
	backend := newTestBackend()
	worker, err := executor.NewWorker(ctx, records, routes(backend), executor.WorkerOptions{ID: "new-worker"})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RecoverExecution(ctx, a.Key()); !errors.Is(err, executor.ErrUnknownProvider) {
		t.Fatalf("takeover = %v, want ErrUnknownProvider", err)
	}
	if _, err := worker.GetStatus(ctx, a.Key()); !errors.Is(err, executor.ErrUnknownProvider) {
		t.Fatalf("status = %v, want ErrUnknownProvider", err)
	}
	got, _, _, err := records.Load(ctx, a.Key())
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
	records := sqlitetest.Open(t).Executions()
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
	record, _, ok, err := records.Load(ctx, a.Key())
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

func TestExecutionStoreFencesRecoverExecution(t *testing.T) {
	ctx := context.Background()
	base := time.Unix(100, 0)
	now := base
	records := sqlitetest.Open(t, sqlite.Options{Now: func() time.Time { return now }}).Executions()
	a := testAssignment()
	digest, err := a.Digest()
	if err != nil {
		t.Fatal(err)
	}
	openLedger(t, records, a, digest)
	first, acquired, err := records.Acquire(ctx, a.Key(), "worker-a", time.Second)
	if err != nil || !acquired || first.Epoch != 1 {
		t.Fatalf("first acquire = %+v, acquired=%v, err=%v", first, acquired, err)
	}
	now = base.Add(500 * time.Millisecond)
	if _, acquired, err := records.Acquire(ctx, a.Key(), "worker-b", time.Second); err != nil || acquired {
		t.Fatalf("live lease acquire = acquired=%v, err=%v; want rejected", acquired, err)
	}
	now = base.Add(2 * time.Second)
	second, acquired, err := records.Acquire(ctx, a.Key(), "worker-b", time.Second)
	if err != nil || !acquired || second.Epoch != 2 {
		t.Fatalf("takeover = %+v, acquired=%v, err=%v", second, acquired, err)
	}
	if err := appendStep(records, first, 3, store.EventExecutionStarted); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("stale commit = %v, want ErrLeaseLost", err)
	}
	// The ledger recorded both claims, in order, under their epochs.
	commits, head, err := records.Read(ctx, a.Key(), 0)
	if err != nil || head.Next != 3 || len(commits) != 3 {
		t.Fatalf("ledger = %d commits head %+v %v, want accept + two claims", len(commits), head, err)
	}
	for i, want := range []store.EventType{store.EventExecutionAccepted, store.EventExecutionClaimed, store.EventExecutionClaimed} {
		if commits[i].Events[0].Type != want {
			t.Fatalf("commit %d = %s, want %s", i, commits[i].Events[0].Type, want)
		}
	}
}

// openLedger writes the acceptance commit a Dispatch would.
func openLedger(t *testing.T, records store.Store, a effect.Assignment, digest run.Digest) {
	t.Helper()
	ev, err := store.NewEvent(store.EventExecutionAccepted, 0, store.Accepted{Assignment: a, AssignmentDigest: digest})
	if err != nil {
		t.Fatal(err)
	}
	if err := records.Append(context.Background(), store.Lease{}, a.Key(), store.Commit{CommitID: store.AcceptCommitID(a.Key()), Intent: digest, Events: []store.Event{ev}}); err != nil {
		t.Fatal(err)
	}
}

// appendStep commits one state-machine event at seq under lease.
func appendStep(records store.Store, lease store.Lease, seq store.CommitSeq, typ store.EventType) error {
	ev, err := store.NewEvent(typ, 0, nil)
	if err != nil {
		return err
	}
	return records.Append(context.Background(), lease, lease.Key, store.Commit{Seq: seq, CommitID: store.DeriveCommitID(lease.Key, "test", fmt.Sprintf("%s/%d", typ, seq)), Events: []store.Event{ev}})
}

func TestExecutionStoreRequiresDispatchingBarrier(t *testing.T) {
	ctx := context.Background()
	base := time.Unix(100, 0)
	now := base
	records := sqlitetest.Open(t, sqlite.Options{Now: func() time.Time { return now }}).Executions()
	a := testAssignment()
	digest, err := a.Digest()
	if err != nil {
		t.Fatal(err)
	}
	openLedger(t, records, a, digest)
	claimed, acquired, err := records.Acquire(ctx, a.Key(), "worker-a", time.Second)
	if err != nil || !acquired {
		t.Fatalf("acquire = %+v, acquired=%v, err=%v", claimed, acquired, err)
	}
	if err := appendStep(records, claimed, 2, store.EventExecutionRunning); !errors.Is(err, store.ErrStateConflict) {
		t.Fatalf("accepted to running = %v, want ErrStateConflict", err)
	}
	if err := appendStep(records, claimed, 2, store.EventExecutionStarted); err != nil {
		t.Fatalf("accepted to dispatching = %v", err)
	}
	if err := appendStep(records, claimed, 2, store.EventExecutionStarted); !errors.Is(err, store.ErrAlreadyApplied) {
		t.Fatalf("replayed commit = %v, want ErrAlreadyApplied", err)
	}
	if err := appendStep(records, claimed, 2, store.EventCancelRequested); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("another commit at a taken seq = %v, want ErrConflict", err)
	}
	now = base.Add(2 * time.Second)
	if err := appendStep(records, claimed, 3, store.EventExecutionRunning); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("expired transition = %v, want ErrLeaseLost", err)
	}
	// An unfenced append may not carry a fenced event.
	if err := appendStep(records, store.Lease{Key: a.Key()}, 3, store.EventExecutionRunning); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("unfenced fenced event = %v, want ErrLeaseLost", err)
	}
}

func TestWorkerReclaimsExpiredAssignment(t *testing.T) {
	ctx := context.Background()
	base := time.Unix(100, 0)
	now := base
	records := sqlitetest.Open(t, sqlite.Options{Now: func() time.Time { return now }}).Executions()
	a := testAssignment()
	digest, err := a.Digest()
	if err != nil {
		t.Fatal(err)
	}
	openLedger(t, records, a, digest)
	claimed, acquired, err := records.Acquire(ctx, a.Key(), "worker-a", time.Second)
	if err != nil || !acquired {
		t.Fatalf("initial acquire = %+v, acquired=%v", claimed, acquired)
	}
	if err := appendStep(records, claimed, 2, store.EventExecutionStarted); err != nil {
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
	if err := worker.RecoverExecution(ctx, a.Key()); err != nil {
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

func TestWorkerRecoverExecutionAdoptsExpiredLease(t *testing.T) {
	ctx := context.Background()
	base := time.Unix(100, 0)
	now := base.Add(2 * time.Second)
	records := sqlitetest.Open(t, sqlite.Options{Now: func() time.Time { return now }}).Executions()
	a := testAssignment()
	digest, err := a.Digest()
	if err != nil {
		t.Fatal(err)
	}
	r := store.ExecutionState{Assignment: a, AssignmentDigest: digest, State: effect.ExecutionRunning,
		Owner: "dead-worker", FencingEpoch: 4, LeaseUntilUnixMilli: base.Add(time.Second).UnixMilli()}
	if err := records.Seed(ctx, r); err != nil {
		t.Fatal(err)
	}
	backend := newTestBackend()
	worker, err := executor.NewWorker(ctx, records, routes(backend), executor.WorkerOptions{
		ID: "worker-b", LeaseDuration: time.Second, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RecoverExecution(ctx, a.Key()); err != nil {
		t.Fatal(err)
	}
	readCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	out, err := worker.GetOutcome(readCtx, a.Key())
	if err != nil || modelText(out) != "ok" {
		t.Fatalf("adopted outcome = %+v, %v", out, err)
	}
	got, _, _, err := records.Load(ctx, a.Key())
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

// A record under another Worker's live lease is not this Worker's to
// recover: RecoverExecution leaves it untouched and dispatches nothing.
func TestWorkerRecoverExecutionLeavesLiveLease(t *testing.T) {
	ctx := context.Background()
	base := time.Unix(100, 0)
	now := base
	records := sqlitetest.Open(t, sqlite.Options{Now: func() time.Time { return now }}).Executions()
	a := testAssignment()
	digest, err := a.Digest()
	if err != nil {
		t.Fatal(err)
	}
	r := store.ExecutionState{Assignment: a, AssignmentDigest: digest, State: effect.ExecutionRunning,
		Owner: "worker-a", FencingEpoch: 4, LeaseUntilUnixMilli: base.Add(time.Second).UnixMilli()}
	if err := records.Seed(ctx, r); err != nil {
		t.Fatal(err)
	}
	backend := newTestBackend()
	worker, err := executor.NewWorker(ctx, records, routes(backend), executor.WorkerOptions{
		ID: "worker-b", LeaseDuration: time.Second, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.RecoverExecution(ctx, a.Key()); err != nil {
		t.Fatalf("recover of a live lease = %v, want a no-op", err)
	}
	got, _, _, err := records.Load(ctx, a.Key())
	if err != nil || got.Owner != "worker-a" || got.FencingEpoch != 4 {
		t.Fatalf("live record changed: %+v, %v", got, err)
	}
	backend.mu.Lock()
	calls := backend.calls
	backend.mu.Unlock()
	if calls != 0 {
		t.Fatalf("recovery dispatched %d backend calls", calls)
	}
}

func TestRecordStoreSurvivesWorkerRecreation(t *testing.T) {
	ctx := context.Background()
	fs := sqlitetest.Open(t).Executions()
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
	worker, err := executor.NewWorker(ctx, sqlitetest.Open(t).Executions(), routes(newTestBackend()))
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
// regardless of owner or lease, so the Owner's next read disposes the Run
// target; a terminal record is left alone and a key with no record is not
// found.
func TestWorkerDisposeSettlesUnknown(t *testing.T) {
	completedEnv := protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Unknown: false}
	rows := []struct {
		name    string
		record  *store.ExecutionState // nil means the key was never written
		wantErr error
	}{
		{"expired foreign owner", &store.ExecutionState{State: effect.ExecutionRunning, Owner: "dead-worker", FencingEpoch: 3, LeaseUntilUnixMilli: 1}, nil},
		{"live foreign owner", &store.ExecutionState{State: effect.ExecutionDispatching, Owner: "live-worker", FencingEpoch: 3,
			LeaseUntilUnixMilli: time.Now().Add(time.Hour).UnixMilli()}, nil},
		{"already terminal", &store.ExecutionState{State: effect.ExecutionCompleted, Owner: "dead-worker", FencingEpoch: 3, LeaseUntilUnixMilli: 1, Outcome: &completedEnv}, nil},
		{"missing record", nil, effect.ErrExecutionNotFound},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			ctx := context.Background()
			records := sqlitetest.Open(t).Executions()
			a := testAssignment()
			key := a.Key()
			if row.record != nil {
				digest, err := a.Digest()
				if err != nil {
					t.Fatal(err)
				}
				r := *row.record
				r.Assignment, r.AssignmentDigest = a, digest
				if err := records.Seed(ctx, r); err != nil {
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
			got, _, _, err := records.Load(ctx, key)
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
			records := sqlitetest.Open(t, sqlite.Options{Now: func() time.Time { return now }}).Executions()
			a := testToolAssignment()
			digest, err := a.Digest()
			if err != nil {
				t.Fatal(err)
			}
			r := store.ExecutionState{Assignment: a, AssignmentDigest: digest, State: row.state,
				Owner: "dead-worker", FencingEpoch: 2, LeaseUntilUnixMilli: base.Add(time.Second).UnixMilli()}
			if err := records.Seed(ctx, r); err != nil {
				t.Fatal(err)
			}
			backend := newTestBackend()
			worker, err := executor.NewWorker(ctx, records, routes(backend), executor.WorkerOptions{
				ID: "worker-b", LeaseDuration: time.Second, Clock: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			// The accepted row dispatches to the backend on a goroutine; the
			// worker is closed before the database and its directory are.
			defer worker.Close()
			if err := worker.RecoverExecution(ctx, a.Key()); err != nil {
				t.Fatal(err)
			}
			got, _, _, err := records.Load(ctx, a.Key())
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

func (s *flakyRenewStore) Renew(ctx context.Context, lease store.Lease, ttl time.Duration) error {
	s.mu.Lock()
	s.renewals++
	failing := time.Now().Before(s.deadline)
	s.mu.Unlock()
	if failing {
		return errors.New("store temporarily unavailable")
	}
	return s.Store.Renew(ctx, lease, ttl)
}

// A transient Renew failure must not stop lease maintenance; the heartbeat
// retries and the record stays owned. Without the retry the heartbeat exited
// on the first error and the lease stayed lost once the failure outlasted it.
func TestWorkerHeartbeatRetriesTransientRenewErrors(t *testing.T) {
	records := &flakyRenewStore{Store: sqlitetest.Open(t).Executions(), deadline: time.Now().Add(1100 * time.Millisecond)}
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
		r, _, ok, err := records.Load(context.Background(), a.Key())
		if err != nil || !ok {
			t.Fatalf("record = %+v ok=%v err=%v", r, ok, err)
		}
		if r.Owner == "worker-a" && r.LeaseUntilUnixMilli > time.Now().UnixMilli() {
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
	worker, err := executor.NewWorker(ctx, sqlitetest.Open(t).Executions(), routes(backend))
	if err != nil {
		t.Fatal(err)
	}
	client := &executorhttp.Client{BaseURL: "http://executor.invalid",
		HTTP: &http.Client{Transport: handlerTransport{handler: (&executorhttp.Server{Worker: worker}).Handler()}}}
	a := testAssignment()
	if err := client.Dispatch(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := client.RecoverExecution(ctx, a.Key()); err != nil {
		t.Fatalf("recover = %v", err)
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
	// The settled record is acknowledged and collected over the wire; the
	// collected Outcome reads as a definitive, classifiable error.
	if err := client.Acknowledge(ctx, a.Key()); err == nil || !strings.Contains(err.Error(), "409") {
		t.Fatalf("acknowledge of an executing record = %v, want 409", err)
	}
	if err := client.Acknowledge(ctx, b.Key()); err != nil {
		t.Fatalf("acknowledge = %v", err)
	}
	if _, err := client.GetOutcome(ctx, b.Key()); !errors.Is(err, effect.ErrOutcomeUnavailable) {
		t.Fatalf("collected outcome over http = %v, want ErrOutcomeUnavailable", err)
	}
	c := testAssignment()
	c.Effect = "effect-3"
	if _, err := client.GetOutcome(ctx, c.Key()); !errors.Is(err, effect.ErrExecutionNotFound) {
		t.Fatalf("unknown key over http = %v, want ErrExecutionNotFound", err)
	}
}

// The Port adapter proves nothing about a Ref it cannot read as a key: that
// is an error, not a missing execution.
func TestPortBackendAttachRejectsForeignRef(t *testing.T) {
	_, err := executor.PortBackend(newTestBackend()).Attach(context.Background(), "not-a-key")
	if err == nil {
		t.Fatal("foreign ref answered instead of failing")
	}
}

// A takeover whose backend cannot confirm the execution (orphaned) holds the
// lease and asks again instead of restarting: nothing is re-dispatched until
// the backend proves the execution missing, and a backend that then observes
// it hands the Worker the original Outcome (RUN-EXE-3, TRN-DUR-4).
func TestWorkerRecoverExecutionWaitsForUnconfirmedBackend(t *testing.T) {
	rows := []struct {
		name         string
		assignment   effect.Assignment
		resolve      effect.AttachmentState
		wantRestarts int
		wantState    effect.ExecutionStatus
	}{
		{"model, backend later proves missing", testAssignment(), effect.AttachmentMissing, 1, effect.ExecutionCompleted},
		{"tool, backend later proves missing", testToolAssignment(), effect.AttachmentMissing, 0, effect.ExecutionUnknown},
		{"model, backend later observes it", testAssignment(), effect.AttachmentActive, 0, effect.ExecutionRunning},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			ctx := context.Background()
			records := sqlitetest.Open(t).Executions()
			a := row.assignment
			digest, err := a.Digest()
			if err != nil {
				t.Fatal(err)
			}
			r := store.ExecutionState{Assignment: a, AssignmentDigest: digest, State: effect.ExecutionRunning,
				ExecutionRef: store.ExecutionRef{Provider: "ref", Ref: "execution-1"},
				Owner:        "dead-worker", FencingEpoch: 2, LeaseUntilUnixMilli: 1}
			if err := records.Seed(ctx, r); err != nil {
				t.Fatal(err)
			}
			backend := &refBackend{testBackend: newTestBackend(), attach: effect.AttachmentOrphaned}
			worker, err := executor.NewWorker(ctx, records, []executor.Route{executor.Default("ref", backend)}, executor.WorkerOptions{ID: "worker-b", LeaseDuration: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer worker.Close()
			if err := worker.RecoverExecution(ctx, a.Key()); err != nil {
				t.Fatal(err)
			}
			// Undecided: the record is held under this Worker's lease, still
			// Running, and the backend has been neither restarted nor started.
			held, _, _, err := records.Load(ctx, a.Key())
			if err != nil {
				t.Fatal(err)
			}
			backend.mu.Lock()
			restarts, started := backend.restarts, backend.started
			backend.mu.Unlock()
			if held.Owner != "worker-b" || held.State != effect.ExecutionRunning || restarts != 0 || started != 0 {
				t.Fatalf("held record = %+v, backend restarts=%d started=%d; want the lease held and nothing dispatched", held, restarts, started)
			}
			// The holder itself reports what its backend can confirm: nothing
			// yet, so orphaned; the Reconciler defers and awaits the Outcome.
			if att, err := worker.Attach(ctx, a.Key()); err != nil || att.State != effect.AttachmentOrphaned || att.Owner != "worker-b" {
				t.Fatalf("attach while undecided = %+v %v, want orphaned under this Worker's lease", att, err)
			}
			backend.setAttach(row.resolve)
			deadline := time.Now().Add(5 * time.Second)
			for {
				got, _, _, err := records.Load(ctx, a.Key())
				if err != nil {
					t.Fatal(err)
				}
				backend.mu.Lock()
				restarts = backend.restarts
				backend.mu.Unlock()
				if got.State == row.wantState && restarts == row.wantRestarts && (row.resolve != effect.AttachmentActive || got.Owner == "worker-b") {
					if row.wantRestarts == 1 && (got.ExecutionRef.Ref != "execution-2" || len(got.Superseded) != 1) {
						t.Fatalf("restarted record = %+v", got)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("record after the backend answered %s = %+v, restarts=%d; want %s/%d", row.resolve, got, restarts, row.wantState, row.wantRestarts)
				}
				time.Sleep(5 * time.Millisecond)
			}
		})
	}
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
			records := sqlitetest.Open(t).Executions()
			a := testToolAssignment()
			body, _ := a.Tool()
			body.Replay = tc.policy
			a.Body = body
			digest, err := a.Digest()
			if err != nil {
				t.Fatal(err)
			}
			r := store.ExecutionState{Assignment: a, AssignmentDigest: digest, State: effect.ExecutionRunning,
				ExecutionRef: store.ExecutionRef{Provider: "ref", Ref: "execution-1"},
				Owner:        "dead-worker", FencingEpoch: 2, LeaseUntilUnixMilli: 1}
			if err := records.Seed(ctx, r); err != nil {
				t.Fatal(err)
			}
			backend := &refBackend{testBackend: newTestBackend()}
			worker, err := executor.NewWorker(ctx, records, []executor.Route{executor.Default("ref", backend)}, executor.WorkerOptions{ID: "worker-b", LeaseDuration: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer worker.Close()
			if err := worker.RecoverExecution(ctx, a.Key()); err != nil {
				t.Fatal(err)
			}
			got, _, _, err := records.Load(ctx, a.Key())
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

// Attach classifies by the record's lease in the shared store, not by which
// Worker answers: a live lease held by another incarnation is active, an
// expired one orphaned (RUN-EXE-3).
func TestWorkerAttachClassifiesByLease(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(2_000_000, 0)
	cases := []struct {
		name  string
		owner string
		epoch store.Epoch
		lease int64
		want  effect.AttachmentState
	}{
		{"another incarnation, live lease", "other-worker", 3, now.Add(time.Minute).UnixMilli(), effect.AttachmentActive},
		{"another incarnation, expired lease", "other-worker", 3, now.Add(-time.Minute).UnixMilli(), effect.AttachmentOrphaned},
		{"never acquired", "", 0, 0, effect.AttachmentOrphaned},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			records := sqlitetest.Open(t, sqlite.Options{Now: func() time.Time { return now }}).Executions()
			a := testAssignment()
			digest, err := a.Digest()
			if err != nil {
				t.Fatal(err)
			}
			r := store.ExecutionState{Assignment: a, AssignmentDigest: digest, State: effect.ExecutionRunning,
				ExecutionRef: store.ExecutionRef{Provider: "elsewhere", Ref: "job"}, Owner: tc.owner, FencingEpoch: tc.epoch, LeaseUntilUnixMilli: tc.lease}
			if err := records.Seed(ctx, r); err != nil {
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
	mu        sync.Mutex
	failures  int
	starts    []string
	code      effect.FailureCode
	toolRetry run.RetryDisposition
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
	if a.Kind() == effect.AssignmentTool {
		result = effect.ToolExecutionSucceeded{}
	}
	if len(b.starts) <= b.failures {
		if a.Kind() == effect.AssignmentTool {
			result = effect.ToolExecutionFailed{Failure: run.ToolFailure{Class: run.FailureUnavailable, Message: "try again"}, Retry: b.toolRetry}
		} else {
			result = effect.ModelFailed{Code: b.code, Message: "try again"}
		}
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

// A Known failure that declares itself retryable is re-dispatched through
// Restart within the Worker's budget, the earlier Refs kept in Superseded:
// a model failure by the disposition the effect layer derived from its
// code, a tool failure by the disposition the tool gave it. A failure that
// does not, or an exhausted budget, settles the failure (RUN-EXE-11).
func TestWorkerRetriesRetryableFailures(t *testing.T) {
	cases := []struct {
		name       string
		assignment effect.Assignment
		failures   int
		code       effect.FailureCode
		toolRetry  run.RetryDisposition
		budget     int
		wantStarts int
		wantOK     bool
	}{
		{"model retried until success", testAssignment(), 2, effect.FailureRateLimited, 0, 3, 3, true},
		{"model budget exhausted", testAssignment(), 5, effect.FailureProviderUnavailable, 0, 2, 2, false},
		{"model definite failure", testAssignment(), 1, effect.FailureAuthentication, 0, 3, 1, false},
		{"model retries disabled", testAssignment(), 1, effect.FailureConnection, 0, 0, 1, false},
		{"tool failure declared retryable", testToolAssignment(), 1, "", run.RetryAllowed, 3, 2, true},
		{"tool failure declared never", testToolAssignment(), 1, "", run.RetryNever, 3, 1, false},
		{"tool failure unjudged", testToolAssignment(), 1, "", run.RetryUnknown, 3, 1, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			records := sqlitetest.Open(t).Executions()
			backend := &transientBackend{testBackend: newTestBackend(), failures: tc.failures, code: tc.code, toolRetry: tc.toolRetry}
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
			var ok bool
			switch out.Result.(type) {
			case effect.ModelSucceeded, effect.ToolExecutionSucceeded:
				ok = true
			}
			if ok != tc.wantOK {
				t.Fatalf("outcome = %#v, want success=%v", out.Result, tc.wantOK)
			}
			backend.mu.Lock()
			starts := len(backend.starts)
			backend.mu.Unlock()
			got, _, _, _ := records.Load(ctx, tc.assignment.Key())
			if starts != tc.wantStarts || len(got.Superseded) != tc.wantStarts-1 {
				t.Fatalf("starts = %d superseded = %d, want %d executions", starts, len(got.Superseded), tc.wantStarts)
			}
		})
	}
}

// failingCreateStore refuses every Append: the ledger store is unavailable.
type failingCreateStore struct{ store.Store }

func (failingCreateStore) Append(context.Context, store.Lease, effect.AssignmentKey, store.Commit) error {
	return errors.New("store unavailable")
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
		{"record store unavailable", failingCreateStore{sqlitetest.Open(t).Executions()}, testAssignment(), true},
		{"assignment without body", sqlitetest.Open(t).Executions(), effect.Assignment{Session: "s", RunID: "r", Effect: "e", Schema: 1}, false},
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

// Acknowledge marks a settled record and collects it at once: the payload
// and Outcome go, the key, digest, state and ExecutionRef stay, so the record
// still answers Attach with terminal, GetOutcome with collected, and a
// Dispatch of the same key starts nothing. An unacknowledged record keeps
// its Outcome readable, and an execution in flight cannot be acknowledged
// (RUN-EXE-13).
func TestWorkerAcknowledgeCollects(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(3_000_000, 0)
	rows := []struct {
		name          string
		state         effect.ExecutionStatus
		acknowledge   bool
		wantAckErr    error
		wantCollected bool
	}{
		{"acknowledged", effect.ExecutionCompleted, true, nil, true},
		{"unacknowledged", effect.ExecutionCompleted, false, nil, false},
		{"executing", effect.ExecutionRunning, true, store.ErrStateConflict, false},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			records := sqlitetest.Open(t, sqlite.Options{Now: func() time.Time { return now }}).Executions()
			a := testAssignment()
			digest, err := a.Digest()
			if err != nil {
				t.Fatal(err)
			}
			r := store.ExecutionState{Assignment: a, AssignmentDigest: digest, State: row.state, ExecutionRef: store.ExecutionRef{Provider: "test", Ref: "job"},
				Owner: "worker-a", FencingEpoch: 1, LeaseUntilUnixMilli: now.Add(time.Hour).UnixMilli(), SettledAtUnixMilli: now.Add(-time.Minute).UnixMilli()}
			if row.state.Terminal() {
				env := protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: a.Key(), AssignmentDigest: digest}
				r.Outcome = &env
			}
			if err := records.Seed(ctx, r); err != nil {
				t.Fatal(err)
			}
			backend := newTestBackend()
			worker, err := executor.NewWorker(ctx, records, routes(backend), executor.WorkerOptions{ID: "worker-a", Clock: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			defer worker.Close()
			if row.acknowledge {
				err := worker.Acknowledge(ctx, a.Key())
				if !errors.Is(err, row.wantAckErr) {
					t.Fatalf("acknowledge = %v, want %v", err, row.wantAckErr)
				}
				if err == nil {
					if err := worker.Acknowledge(ctx, a.Key()); err != nil {
						t.Fatalf("repeated acknowledge = %v", err)
					}
				}
			}
			got, _, _, err := records.Load(ctx, a.Key())
			if err != nil {
				t.Fatal(err)
			}
			if !row.wantCollected {
				if got.Collected || got.Assignment.Body == nil || (row.state.Terminal() && got.Outcome == nil) {
					t.Fatalf("record was collected: %+v", got)
				}
				return
			}
			if !got.Collected || got.Assignment.Body != nil || got.Outcome != nil || got.Assignment.Key() != a.Key() || got.AssignmentDigest != digest || got.State != row.state {
				t.Fatalf("collected record = %+v", got)
			}
			if att, err := worker.Attach(ctx, a.Key()); err != nil || att.State != effect.AttachmentTerminal {
				t.Fatalf("attach of collected = %+v %v", att, err)
			}
			if _, err := worker.GetOutcome(ctx, a.Key()); !errors.Is(err, effect.ErrOutcomeCollected) || !errors.Is(err, effect.ErrOutcomeUnavailable) {
				t.Fatalf("outcome of collected = %v", err)
			}
			if err := worker.Dispatch(ctx, a); err != nil {
				t.Fatalf("dispatch replay of a collected key = %v", err)
			}
			backend.mu.Lock()
			calls := backend.calls
			backend.mu.Unlock()
			if calls != 0 {
				t.Fatalf("dispatch replay of a collected key executed %d times", calls)
			}
		})
	}
}
