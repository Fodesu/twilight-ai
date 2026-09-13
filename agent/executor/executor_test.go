package executor_test

import (
	"context"
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
	if b.outcomes[a.Key()] == nil {
		b.outcomes[a.Key()] = make(chan effect.Outcome, 1)
	}
	ch := b.outcomes[a.Key()]
	b.mu.Unlock()
	go func() { ch <- effect.Outcome{Key: a.Key(), Model: &sdk.ModelResult{Text: "ok"}} }()
	return nil
}
func (b *testBackend) Attach(_ context.Context, key effect.AssignmentKey) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.outcomes[key] != nil, nil
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
	return effect.Assignment{Session: "s", RunID: "r", StepID: "step", Claim: "claim", Schema: 1,
		Kind: effect.AssignmentModel, Model: &effect.ModelAssignment{Model: "m", RequestDigest: "sha256:req"}}
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
