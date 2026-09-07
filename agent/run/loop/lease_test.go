package loop

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/session"
	"github.com/memohai/twilight/sdk"
)

type renewCountingRuntime struct {
	Runtime
	renewals atomic.Int32
}

func (r *renewCountingRuntime) RenewLease(ctx context.Context, sid session.SessionID, runID RunID, stepID StepID, callID CallID, grant ExecutionGrant) error {
	r.renewals.Add(1)
	return r.Runtime.RenewLease(ctx, sid, runID, stepID, callID, grant)
}

// A tool that runs longer than the lease TTL keeps its lease alive through
// the Loop heartbeat, so the scanner does not settle it as Unknown and the
// worker's own result is accepted.
func TestLoopRenewsLeaseDuringLongTool(t *testing.T) {
	var mu sync.Mutex
	clock := time.Unix(1000, 0)
	now := func() time.Time { mu.Lock(); defer mu.Unlock(); return clock }
	advance := func(d time.Duration) { mu.Lock(); defer mu.Unlock(); clock = clock.Add(d) }

	stack := newTestStack(t, 200*time.Millisecond, now)
	stack.createRun(t, "run-1", AgentInput{ID: "seed", Payload: cj(`{}`)})
	rt := &renewCountingRuntime{Runtime: stack.runtime}

	spec := toolSpec(t, "slow", DirectExecution)
	slow := &fakeTool{ref: "slow", def: toolDef(spec.Name), policy: DirectExecution,
		execute: func(ctx context.Context, req ToolExecutionRequest) ToolExecutionOutcome {
			// Simulate a tool that outlives the TTL: advance the clock past
			// several deadlines while the heartbeat keeps renewing.
			for i := 0; i < 4; i++ {
				time.Sleep(30 * time.Millisecond)
				advance(150 * time.Millisecond)
				if _, err := rt.RecoverExpired(context.Background()); err != nil {
					return ToolExecutionFailed{Failure: ToolFailure{Class: FailureExecution, Message: err.Error()}}
				}
			}
			return ToolExecutionSucceeded{Result: ToolExecutionResult{Output: req.Arguments}}
		}}
	slowCall := sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls, Usage: sdk.Usage{TotalTokens: 2},
		ToolCalls: []sdk.ToolCall{{ToolCallID: "c1", ToolName: "slow", Input: `{"x":1}`}}}
	invoker := &fakeInvoker{results: []sdk.ModelResult{slowCall, textResult("done")}}
	interpreter, err := New(fakeCatalog{invoker}, fakeToolCatalog{map[ToolRef]ExecutableTool{"slow": slow}},
		staticPlanner{specs: []ToolSpec{spec}}, ExecutionPolicy{LeaseRenewInterval: 10 * time.Millisecond}, false)
	if err != nil {
		t.Fatal(err)
	}
	res, err := interpreter.Run(context.Background(), rt, testSession, "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Disposition != LoopFinished || res.Result == nil || res.Result.Status != RunCompleted {
		t.Fatalf("res = %+v", res)
	}
	if rt.renewals.Load() == 0 {
		t.Fatal("lease was never renewed")
	}
	for _, fact := range recordFacts(t, rt, "run-1") {
		if f, ok := fact.(ToolCallFailed); ok && f.Outcome == ToolOutcomeUnknown {
			t.Fatalf("tool call settled Unknown despite heartbeat: %+v", f)
		}
	}
}

func TestNewRejectsNegativeLeaseRenewInterval(t *testing.T) {
	_, err := New(fakeCatalog{&fakeInvoker{}}, fakeToolCatalog{}, staticPlanner{}, ExecutionPolicy{LeaseRenewInterval: -1}, false)
	if err == nil {
		t.Fatal("negative LeaseRenewInterval accepted")
	}
}

// A replacement Loop that shares the dead Loop's ClaimStore replays the
// derived start under the same claim, gets the live grant back, executes the
// tool and settles, without waiting for the lease to expire.
func TestLoopReplacementFinishesInheritedClaim(t *testing.T) {
	stack := newTestStack(t, time.Hour, nil)
	stack.createRun(t, "run-1", AgentInput{ID: "seed", Payload: cj(`{}`)})
	rt := stack.runtime

	shared := newMemoryClaims()
	spec := toolSpec(t, "echo", DirectExecution)
	block := make(chan struct{})
	var executions atomic.Int32
	tool := &fakeTool{ref: "echo", def: toolDef(spec.Name), policy: DirectExecution,
		execute: func(ctx context.Context, req ToolExecutionRequest) ToolExecutionOutcome {
			if executions.Add(1) == 1 {
				<-block // first process "dies" here
				return ToolExecutionUnknown{Failure: ToolFailure{Class: FailureEffectUnknown}}
			}
			return ToolExecutionSucceeded{Result: ToolExecutionResult{Output: req.Arguments}}
		}}
	invoker := &fakeInvoker{results: []sdk.ModelResult{toolCallResult("c1"), textResult("done")}}
	catalog := fakeToolCatalog{map[ToolRef]ExecutableTool{"echo": tool}}

	first, err := New(fakeCatalog{invoker}, catalog, staticPlanner{specs: []ToolSpec{spec}}, ExecutionPolicy{Claims: shared}, false)
	if err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan struct{})
	go func() { defer close(firstDone); _, _ = first.Run(context.Background(), rt, testSession, "run-1", nil) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		snap, err := rt.Load(context.Background(), testSession, "run-1")
		if err != nil {
			t.Fatal(err)
		}
		if len(ExecutingCalls(snap.State)) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("tool never started")
		}
		time.Sleep(time.Millisecond)
	}

	// Lease is nowhere near expiry (1h). A second Loop with the same claims
	// takes over immediately.
	second, err := New(fakeCatalog{invoker}, catalog, staticPlanner{specs: []ToolSpec{spec}}, ExecutionPolicy{Claims: shared}, false)
	if err != nil {
		t.Fatal(err)
	}
	res, err := second.Run(context.Background(), rt, testSession, "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Disposition != LoopFinished || res.Result == nil || res.Result.Status != RunCompleted {
		t.Fatalf("second loop result = %+v", res)
	}
	starts := 0
	for _, fact := range recordFacts(t, rt, "run-1") {
		switch f := fact.(type) {
		case ToolCallStarted:
			starts++
		case ToolCallFailed:
			if f.Outcome == ToolOutcomeUnknown {
				t.Fatalf("call settled Unknown; replacement did not inherit the claim: %+v", f)
			}
		}
	}
	if starts != 1 {
		t.Fatalf("ToolCallStarted facts = %d, want 1 (one attempt, replayed)", starts)
	}
	// The first worker eventually returns; its settlement replays the same
	// derived id with a different outcome and is rejected as a conflict, which
	// the Loop treats as "already settled by someone else".
	close(block)
	<-firstDone
}
