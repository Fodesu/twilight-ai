package loop

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	. "github.com/memohai/twilight/agent/run"

	"github.com/memohai/twilight/sdk"
)

// --- fakes ---

type fakeInvoker struct {
	results []sdk.ModelResult
	errs    []error
	calls   atomic.Int32
}

func (f *fakeInvoker) Generate(ctx context.Context, req sdk.Request) (sdk.ModelResult, error) {
	if err := ctx.Err(); err != nil {
		return sdk.ModelResult{}, err
	}
	n := int(f.calls.Add(1)) - 1
	if n < len(f.errs) && f.errs[n] != nil {
		return sdk.ModelResult{}, f.errs[n]
	}
	if n >= len(f.results) {
		return sdk.ModelResult{}, errors.New("fake: no scripted result")
	}
	return f.results[n], nil
}

type blockingInvoker struct {
	started chan struct{}
	release chan struct{}
}

func (b *blockingInvoker) Generate(context.Context, sdk.Request) (sdk.ModelResult, error) {
	close(b.started)
	<-b.release
	return textResult("done"), nil
}

type fakeCatalog struct{ invoker ModelInvoker }

func (c fakeCatalog) Resolve(ModelRef) (ModelInvoker, error) { return c.invoker, nil }

type fakeTool struct {
	ref     ToolRef
	def     sdk.ToolDefinition
	policy  ResponsePolicy
	execute func(context.Context, ToolExecutionRequest) ToolExecutionOutcome
	valErr  error
}

func (f *fakeTool) Ref() ToolRef                          { return f.ref }
func (f *fakeTool) Definition() sdk.ToolDefinition        { return f.def }
func (f *fakeTool) ResponsePolicy() ResponsePolicy        { return f.policy }
func (f *fakeTool) ValidateArguments(CanonicalJSON) error { return f.valErr }
func (f *fakeTool) Execute(ctx context.Context, req ToolExecutionRequest) ToolExecutionOutcome {
	return f.execute(ctx, req)
}

type fakeToolCatalog struct{ tools map[ToolRef]ExecutableTool }

func (c fakeToolCatalog) Resolve(ref ToolRef) (ExecutableTool, error) {
	t, ok := c.tools[ref]
	if !ok {
		return nil, fmt.Errorf("unknown tool %q", ref)
	}
	return t, nil
}

// staticPlanner freezes one request per Plan call; tools mirror the catalog.
type staticPlanner struct {
	model ModelRef
	specs []ToolSpec
}

func (p staticPlanner) Plan(_ context.Context, hint PlanningHint) (RequestPlan, error) {
	model := p.model
	if model == "" {
		model = testModel
	}
	req := sdk.Request{Model: string(model), Messages: []sdk.Message{sdk.UserMessage("go")}}
	for _, s := range p.specs {
		req.Tools = append(req.Tools, s.Definition.SDK())
	}
	ids := make([]InputID, len(hint.Inputs))
	for i, in := range hint.Inputs {
		ids[i] = in.ID
	}
	return RequestPlan{Model: model, Request: req, InputIDs: ids, Tools: p.specs}, nil
}

type resultCheckingPlanner struct {
	specs []ToolSpec
	saw   atomic.Bool
}

func (p *resultCheckingPlanner) Plan(ctx context.Context, hint PlanningHint) (RequestPlan, error) {
	if hint.LastToolStep != nil {
		for _, call := range hint.LastToolStep.Calls {
			if call.CallID == "c1" && call.Status == ToolCompleted && call.Result != nil && call.Result.Output.String() == `{"x":1}` {
				p.saw.Store(true)
			}
		}
	}
	return staticPlanner{specs: p.specs}.Plan(ctx, hint)
}

func toolSpec(t *testing.T, name string, policy ResponsePolicy) ToolSpec {
	t.Helper()
	def := sdk.ToolDefinition{Name: name, Parameters: json.RawMessage(`{"type":"object"}`)}
	frozen, err := FreezeToolDefinition(def)
	if err != nil {
		t.Fatal(err)
	}
	d, err := DigestToolDefinition(frozen)
	if err != nil {
		t.Fatal(err)
	}
	return ToolSpec{Ref: ToolRef(name), Definition: frozen, DefinitionDigest: d, Policy: policy}
}

func loopRuntime(t *testing.T) *MemoryRuntime {
	t.Helper()
	return newTestRuntime(t)
}

func textResult(text string) sdk.ModelResult {
	return sdk.ModelResult{Text: text, FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}
}

func toolCallResult(ids ...string) sdk.ModelResult {
	r := sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls, Usage: sdk.Usage{TotalTokens: 2}}
	for _, id := range ids {
		r.ToolCalls = append(r.ToolCalls, sdk.ToolCall{ToolCallID: id, ToolName: "echo", Input: `{"x":1}`})
	}
	return r
}

// --- tests ---

func TestLoopSingleModelCallCompletes(t *testing.T) {
	rt := loopRuntime(t)
	loop, err := New(fakeCatalog{&fakeInvoker{results: []sdk.ModelResult{textResult("hello")}}},
		fakeToolCatalog{}, staticPlanner{}, ExecutionPolicy{}, false)
	if err != nil {
		t.Fatal(err)
	}
	res, err := loop.Run(context.Background(), rt, "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Disposition != LoopFinished || res.Result == nil || res.Result.Status != RunCompleted {
		t.Fatalf("res = %+v", res)
	}
	if res.Result.Model.Text != "hello" {
		t.Fatalf("text = %q", res.Result.Model.Text)
	}
}

func TestLoopRejectsConcurrentRunForSameID(t *testing.T) {
	rt := loopRuntime(t)
	invoker := &blockingInvoker{started: make(chan struct{}), release: make(chan struct{})}
	loop, err := New(fakeCatalog{invoker}, fakeToolCatalog{}, staticPlanner{}, ExecutionPolicy{}, false)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, runErr := loop.Run(context.Background(), rt, "run-1", nil)
		done <- runErr
	}()
	<-invoker.started
	if _, err := loop.Run(context.Background(), rt, "run-1", nil); !errors.Is(err, ErrRunAlreadyRunning) {
		t.Fatalf("concurrent Run error = %v, want ErrRunAlreadyRunning", err)
	}
	close(invoker.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestLoopToolRoundTrip(t *testing.T) {
	spec := toolSpec(t, "echo", DirectExecution)
	echo := &fakeTool{ref: "echo", def: spec.Definition.SDK(), policy: DirectExecution,
		execute: func(_ context.Context, req ToolExecutionRequest) ToolExecutionOutcome {
			return ToolExecutionSucceeded{Result: ToolExecutionResult{Output: req.Arguments}}
		}}
	invoker := &fakeInvoker{results: []sdk.ModelResult{toolCallResult("c1"), textResult("done")}}
	rt := loopRuntime(t)
	planner := &resultCheckingPlanner{specs: []ToolSpec{spec}}
	loop, _ := New(fakeCatalog{invoker}, fakeToolCatalog{map[ToolRef]ExecutableTool{"echo": echo}},
		planner, ExecutionPolicy{}, false)

	res, err := loop.Run(context.Background(), rt, "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Disposition != LoopFinished || res.Result.Status != RunCompleted || res.Result.Model.Text != "done" {
		t.Fatalf("res = %+v", res)
	}
	if invoker.calls.Load() != 2 {
		t.Fatalf("model calls = %d, want 2", invoker.calls.Load())
	}
	if !planner.saw.Load() {
		t.Fatal("second planning call did not receive the completed tool result")
	}
	// Usage accumulated across both model steps.
	if res.Result.Usage.TotalTokens != 3 {
		t.Fatalf("usage = %d, want 3", res.Result.Usage.TotalTokens)
	}
}

func TestLoopApprovalWaitsAndResumes(t *testing.T) {
	spec := toolSpec(t, "echo", ApprovalRequired)
	executed := atomic.Bool{}
	echo := &fakeTool{ref: "echo", def: spec.Definition.SDK(), policy: ApprovalRequired,
		execute: func(context.Context, ToolExecutionRequest) ToolExecutionOutcome {
			executed.Store(true)
			return ToolExecutionSucceeded{Result: ToolExecutionResult{Output: cj(`"ok"`)}}
		}}
	invoker := &fakeInvoker{results: []sdk.ModelResult{toolCallResult("c1"), textResult("after")}}
	rt := loopRuntime(t)
	loop, _ := New(fakeCatalog{invoker}, fakeToolCatalog{map[ToolRef]ExecutableTool{"echo": echo}},
		staticPlanner{specs: []ToolSpec{spec}}, ExecutionPolicy{}, false)

	// First run: no executable effect remains; approval lives on the snapshot.
	res, err := loop.Run(context.Background(), rt, "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Disposition != LoopWaiting || res.ExecutionRecovery {
		t.Fatalf("res = %+v", res)
	}
	if executed.Load() {
		t.Fatal("tool executed before approval")
	}
	wait := snapshotWaiting(t, rt, "run-1")[0]
	if wait.Kind != ResponseApproval {
		t.Fatalf("kind = %v", wait.Kind)
	}

	// Ingress approves via the derived response command ID (RUN-WIR-3).
	cmdID := DeriveResponseCommandID(wait.RunID, wait.StepID, wait.CallID, wait.ID)
	env, err := BuildEnvelope(wait.RunID, cmdID, ApproveToolCall{
		StepID: wait.StepID, CallID: wait.CallID, ResponseID: wait.ID,
		ResponseDigest: responseDecisionDigest(t, ResponseApproval, ResponseDecisionApproved, ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	snap, _ := rt.Load(context.Background(), "run-1")
	if _, err := rt.Commit(context.Background(), CommitRequest{BaseRevision: snap.Revision, Command: env}); err != nil {
		t.Fatal(err)
	}

	// Wake: a new Loop run executes the tool and finishes.
	res, err = loop.Run(context.Background(), rt, "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Disposition != LoopFinished || res.Result.Model.Text != "after" {
		t.Fatalf("res = %+v", res)
	}
	if !executed.Load() {
		t.Fatal("approved tool never executed")
	}
}

func TestLoopYieldsWaitingBeforeStartingPending(t *testing.T) {
	var workRan, askRan atomic.Bool
	askSpec := toolSpec(t, "ask", ApprovalRequired)
	workSpec := toolSpec(t, "work", DirectExecution)
	ask := &fakeTool{ref: "ask", def: askSpec.Definition.SDK(), policy: ApprovalRequired,
		execute: func(context.Context, ToolExecutionRequest) ToolExecutionOutcome {
			askRan.Store(true)
			return ToolExecutionSucceeded{Result: ToolExecutionResult{Output: cj(`"asked"`)}}
		}}
	work := &fakeTool{ref: "work", def: workSpec.Definition.SDK(), policy: DirectExecution,
		execute: func(context.Context, ToolExecutionRequest) ToolExecutionOutcome {
			workRan.Store(true)
			return ToolExecutionSucceeded{Result: ToolExecutionResult{Output: cj(`"worked"`)}}
		}}
	invoker := &fakeInvoker{results: []sdk.ModelResult{
		{
			FinishReason: sdk.FinishReasonToolCalls,
			Usage:        sdk.Usage{TotalTokens: 2},
			ToolCalls: []sdk.ToolCall{
				{ToolCallID: "cA", ToolName: "ask", Input: `{"x":1}`},
				{ToolCallID: "cB", ToolName: "work", Input: `{"x":1}`},
			},
		},
		textResult("after"),
	}}
	rt := loopRuntime(t)
	loop, err := New(fakeCatalog{invoker}, fakeToolCatalog{map[ToolRef]ExecutableTool{"ask": ask, "work": work}},
		staticPlanner{specs: []ToolSpec{askSpec, workSpec}}, ExecutionPolicy{}, false)
	if err != nil {
		t.Fatal(err)
	}

	res, err := loop.Run(context.Background(), rt, "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Disposition != LoopWaiting || res.ExecutionRecovery {
		t.Fatalf("first run = %+v", res)
	}
	if !workRan.Load() {
		t.Fatal("DirectExecution tools must start in the same Run that returns LoopWaiting")
	}
	if askRan.Load() {
		t.Fatal("approval tool executed before ApproveToolCall")
	}

	wait := snapshotWaiting(t, rt, "run-1")[0]
	if wait.CallID != "cA" {
		t.Fatalf("waiting call = %s, want cA", wait.CallID)
	}
	cmdID := DeriveResponseCommandID(wait.RunID, wait.StepID, wait.CallID, wait.ID)
	env, err := BuildEnvelope(wait.RunID, cmdID, ApproveToolCall{
		StepID: wait.StepID, CallID: wait.CallID, ResponseID: wait.ID,
		ResponseDigest: responseDecisionDigest(t, ResponseApproval, ResponseDecisionApproved, ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := rt.Load(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Commit(context.Background(), CommitRequest{BaseRevision: snap.Revision, Command: env}); err != nil {
		t.Fatal(err)
	}

	res, err = loop.Run(context.Background(), rt, "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Disposition != LoopFinished || res.Result.Model.Text != "after" {
		t.Fatalf("after approval = %+v", res)
	}
	if !askRan.Load() {
		t.Fatal("approved tool never executed")
	}
}

func TestLoopUnknownOutcomeFailsRun(t *testing.T) {
	spec := toolSpec(t, "echo", DirectExecution)
	echo := &fakeTool{ref: "echo", def: spec.Definition.SDK(), policy: DirectExecution,
		execute: func(context.Context, ToolExecutionRequest) ToolExecutionOutcome {
			return ToolExecutionUnknown{Failure: ToolFailure{Class: FailureEffectUnknown, Message: "lost"}}
		}}
	invoker := &fakeInvoker{results: []sdk.ModelResult{toolCallResult("c1")}}
	rt := loopRuntime(t)
	loop, _ := New(fakeCatalog{invoker}, fakeToolCatalog{map[ToolRef]ExecutableTool{"echo": echo}},
		staticPlanner{specs: []ToolSpec{spec}}, ExecutionPolicy{}, false)

	res, err := loop.Run(context.Background(), rt, "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Disposition != LoopFinished || res.Result.Status != RunFailed || res.Result.Reason != ReasonEffectUnknown {
		t.Fatalf("res = %+v", res)
	}
	if res.Result.Failure == nil || res.Result.Failure.CallID != "c1" {
		t.Fatalf("failure = %+v", res.Result.Failure)
	}
}

func TestLoopKnownToolFailureContinues(t *testing.T) {
	spec := toolSpec(t, "echo", DirectExecution)
	echo := &fakeTool{ref: "echo", def: spec.Definition.SDK(), policy: DirectExecution,
		execute: func(context.Context, ToolExecutionRequest) ToolExecutionOutcome {
			return ToolExecutionFailed{Failure: ToolFailure{Class: FailureExecution, Message: "boom"}}
		}}
	invoker := &fakeInvoker{results: []sdk.ModelResult{toolCallResult("c1"), textResult("recovered")}}
	rt := loopRuntime(t)
	loop, _ := New(fakeCatalog{invoker}, fakeToolCatalog{map[ToolRef]ExecutableTool{"echo": echo}},
		staticPlanner{specs: []ToolSpec{spec}}, ExecutionPolicy{}, false)

	res, err := loop.Run(context.Background(), rt, "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	// Known failure feeds the next model request; run completes.
	if res.Result.Status != RunCompleted || res.Result.Model.Text != "recovered" {
		t.Fatalf("res = %+v", res)
	}
}

func TestLoopUnknownToolRefClosesAsLookupFailure(t *testing.T) {
	// Model calls a tool that is not in the catalog (and not in specs).
	invoker := &fakeInvoker{results: []sdk.ModelResult{
		func() sdk.ModelResult {
			r := sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls}
			r.ToolCalls = []sdk.ToolCall{{ToolCallID: "c1", ToolName: "ghost", Input: `{}`}}
			return r
		}(),
		textResult("moved on"),
	}}
	rt := loopRuntime(t)
	loop, _ := New(fakeCatalog{invoker}, fakeToolCatalog{tools: map[ToolRef]ExecutableTool{}},
		staticPlanner{}, ExecutionPolicy{}, false)

	res, err := loop.Run(context.Background(), rt, "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Result.Status != RunCompleted || res.Result.Model.Text != "moved on" {
		t.Fatalf("res = %+v", res)
	}
	// The failed call must be recorded as tool_lookup_failed in the log.
	found := false
	for _, e := range recordEvents(t, rt, "run-1") {
		if f, ok := e.Fact.(ToolCallFailed); ok && f.Failure.Class == FailureToolLookup {
			found = true
		}
	}
	if !found {
		t.Fatal("no tool_lookup_failed fact in the event log")
	}
}

func TestLoopParallelBounded(t *testing.T) {
	spec := toolSpec(t, "echo", DirectExecution)
	var concurrent, peak atomic.Int32
	gate := make(chan struct{})
	started := make(chan struct{}, 3)
	echo := &fakeTool{ref: "echo", def: spec.Definition.SDK(), policy: DirectExecution,
		execute: func(context.Context, ToolExecutionRequest) ToolExecutionOutcome {
			cur := concurrent.Add(1)
			for {
				p := peak.Load()
				if cur <= p || peak.CompareAndSwap(p, cur) {
					break
				}
			}
			started <- struct{}{}
			<-gate // hold every worker until released so concurrency is real
			concurrent.Add(-1)
			return ToolExecutionSucceeded{Result: ToolExecutionResult{Output: cj(`"ok"`)}}
		}}
	invoker := &fakeInvoker{results: []sdk.ModelResult{toolCallResult("c1", "c2", "c3"), textResult("done")}}
	rt := loopRuntime(t)
	loop, _ := New(fakeCatalog{invoker}, fakeToolCatalog{map[ToolRef]ExecutableTool{"echo": echo}},
		staticPlanner{specs: []ToolSpec{spec}}, ExecutionPolicy{MaxParallel: 2}, false)

	done := make(chan struct{})
	var res LoopResult
	var runErr error
	go func() {
		res, runErr = loop.Run(context.Background(), rt, "run-1", nil)
		close(done)
	}()

	// Exactly MaxParallel workers must be running before the gate opens.
	<-started
	<-started
	if concurrent.Load() != 2 {
		t.Fatalf("concurrent = %d before gate, want 2", concurrent.Load())
	}
	close(gate)
	<-done
	if runErr != nil {
		t.Fatal(runErr)
	}
	if res.Result.Status != RunCompleted {
		t.Fatalf("res = %+v", res)
	}
	if peak.Load() != 2 {
		t.Fatalf("peak concurrency = %d, want exactly 2 (bounded and actually parallel)", peak.Load())
	}
}

type staleCommitRuntime struct{ Runtime }

func (staleCommitRuntime) Commit(context.Context, CommitRequest) (CommitResult, error) {
	return CommitResult{}, ErrStaleRuntime
}

func TestToolStartStaleDropsLocalClaim(t *testing.T) {
	spec := toolSpec(t, "echo", DirectExecution)
	args := cj(`{}`)
	bindingDigest, err := DigestToolCallBinding("c1", spec.DefinitionDigest, spec.Policy, args)
	if err != nil {
		t.Fatal(err)
	}
	echo := &fakeTool{ref: "echo", def: spec.Definition.SDK(), policy: DirectExecution,
		execute: func(context.Context, ToolExecutionRequest) ToolExecutionOutcome {
			return ToolExecutionSucceeded{Result: ToolExecutionResult{Output: args}}
		}}
	loop, err := New(fakeCatalog{&fakeInvoker{}}, fakeToolCatalog{map[ToolRef]ExecutableTool{"echo": echo}},
		staticPlanner{}, ExecutionPolicy{}, false)
	if err != nil {
		t.Fatal(err)
	}
	stepID := StepID("step-1")
	snapshot := &RuntimeSnapshot{State: MachineState{
		RunID: "run-1", Status: RunActive,
		Current: ToolStep{
			RefValue: StepRef{RunID: "run-1", ID: stepID, Digest: Digest("sha256:step")},
			Source:   "model-1",
			Calls: []ToolCallState{{
				CallID: "c1", ToolRef: spec.Ref, DefinitionDigest: spec.DefinitionDigest,
				BindingDigest: bindingDigest, Arguments: args, Policy: DirectExecution, Status: ToolPending,
			}},
		},
	}, Revision: 1}

	if err := loop.runToolCalls(context.Background(), staleCommitRuntime{}, nil, snapshot,
		StartToolCalls{StepID: stepID, CallIDs: []CallID{"c1"}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := loop.lookupStart(startKey{runID: "run-1", stepID: stepID, callID: "c1"}); ok {
		t.Fatal("stale tool start retained a local execution claim")
	}
}

type responseLossRuntime struct {
	*MemoryRuntime
	mu             sync.Mutex
	count          map[CommandID]int
	loseModelStart bool
}

func newResponseLossRuntime(t *testing.T) *responseLossRuntime {
	t.Helper()
	base := loopRuntime(t)
	return &responseLossRuntime{MemoryRuntime: base, count: make(map[CommandID]int)}
}

func (r *responseLossRuntime) Commit(ctx context.Context, req CommitRequest) (CommitResult, error) {
	result, err := r.MemoryRuntime.Commit(ctx, req)
	if err != nil {
		return result, err
	}
	r.mu.Lock()
	r.count[req.Command.ID]++
	count := r.count[req.Command.ID]
	_, lose := req.Command.Command.(StartModelExecution)
	lose = lose && r.loseModelStart
	if _, ok := req.Command.Command.(SubmitToolResult); ok {
		lose = true
	}
	r.mu.Unlock()
	if lose && count <= 2 {
		return CommitResult{}, errors.New("test: response lost")
	}
	return result, nil
}

func TestLoopReplaysStartAfterTwoLostResponses(t *testing.T) {
	rt := newResponseLossRuntime(t)
	rt.loseModelStart = true
	invoker := &fakeInvoker{results: []sdk.ModelResult{textResult("recovered")}}
	loop, err := New(fakeCatalog{invoker}, fakeToolCatalog{}, staticPlanner{}, ExecutionPolicy{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loop.Run(context.Background(), rt, "run-1", nil); err == nil {
		t.Fatal("first run unexpectedly completed after lost start responses")
	}
	snapshot, err := rt.Load(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	// The first Loop reaches the start barrier; both start responses are lost,
	// so the authority remains Executing while the local claim is retained.
	if current, ok := snapshot.State.Current.(ModelStep); !ok || current.Status != ModelExecuting {
		t.Fatalf("current = %#v, want Executing ModelStep", snapshot.State.Current)
	}
	if _, err := loop.Run(context.Background(), rt, "run-1", nil); err != nil {
		t.Fatal(err)
	}
	if invoker.calls.Load() != 1 {
		t.Fatalf("model calls = %d, want 1", invoker.calls.Load())
	}
}

func TestLoopReplaysSettlementWithoutRepeatingTool(t *testing.T) {
	rt := newResponseLossRuntime(t)
	spec := toolSpec(t, "echo", DirectExecution)
	var executions atomic.Int32
	echo := &fakeTool{ref: "echo", def: spec.Definition.SDK(), policy: DirectExecution,
		execute: func(_ context.Context, req ToolExecutionRequest) ToolExecutionOutcome {
			executions.Add(1)
			return ToolExecutionSucceeded{Result: ToolExecutionResult{Output: req.Arguments}}
		}}
	invoker := &fakeInvoker{results: []sdk.ModelResult{toolCallResult("c1"), textResult("done")}}
	loop, err := New(fakeCatalog{invoker}, fakeToolCatalog{map[ToolRef]ExecutableTool{"echo": echo}},
		staticPlanner{specs: []ToolSpec{spec}}, ExecutionPolicy{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loop.Run(context.Background(), rt, "run-1", nil); err == nil {
		t.Fatal("first run unexpectedly completed after lost settlement responses")
	}
	if executions.Load() != 1 {
		t.Fatalf("tool executions = %d, want 1", executions.Load())
	}
	res, err := loop.Run(context.Background(), rt, "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Disposition != LoopFinished || res.Result == nil || res.Result.Model.Text != "done" {
		t.Fatalf("result = %+v", res)
	}
	if executions.Load() != 1 {
		t.Fatalf("tool executions after replay = %d, want 1", executions.Load())
	}
}

func TestLoopCtxCancelReturnsWithoutFailingRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	invoker := &fakeInvoker{errs: []error{context.Canceled}}
	rt := loopRuntime(t)
	loop, _ := New(fakeCatalog{invoker}, fakeToolCatalog{}, staticPlanner{}, ExecutionPolicy{}, false)

	cancel() // cancelled before the model call
	_, err := loop.Run(ctx, rt, "run-1", nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	// Run must still be active (recovery released it), never failed.
	snap, _ := rt.Load(context.Background(), "run-1")
	if snap.State.Status != RunActive {
		t.Fatalf("status = %v, want RunActive", snap.State.Status)
	}
	// A fresh Loop with a working invoker resumes the same frozen request.
	invoker2 := &fakeInvoker{results: []sdk.ModelResult{textResult("resumed")}}
	loop2, _ := New(fakeCatalog{invoker2}, fakeToolCatalog{}, staticPlanner{}, ExecutionPolicy{}, false)
	res, err := loop2.Run(context.Background(), rt, "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Result == nil || res.Result.Model.Text != "resumed" {
		t.Fatalf("res = %+v", res)
	}
}

func TestLoopMalformedModelResultDispositionFailsRun(t *testing.T) {
	rt := loopRuntime(t)
	bad := sdk.ModelResult{
		FinishReason: sdk.FinishReasonToolCalls,
		ToolCalls: []sdk.ToolCall{
			{ToolCallID: "dup", ToolName: "echo", Input: `{"x":1}`},
			{ToolCallID: "dup", ToolName: "echo", Input: `{"x":2}`},
		},
		Usage: sdk.Usage{TotalTokens: 1},
	}
	invoker := &fakeInvoker{results: []sdk.ModelResult{bad, bad, bad}}
	loop, err := New(fakeCatalog{invoker}, fakeToolCatalog{}, staticPlanner{}, ExecutionPolicy{
		OnMalformedModelResult: func(step ModelStep, _ StepFailure) ModelRejectDisposition {
			if step.Rejects < 2 {
				return ModelRejectRetry
			}
			return ModelRejectFailRun
		},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	res, err := loop.Run(context.Background(), rt, "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := invoker.calls.Load(); got != 3 {
		t.Fatalf("model calls = %d, want 3", got)
	}
	if res.Result == nil || res.Result.Status != RunFailed || res.Result.Reason != ReasonMalformedModel {
		t.Fatalf("loop result = %+v", res)
	}
}

type panicPlanner struct{}

func (panicPlanner) Plan(context.Context, PlanningHint) (RequestPlan, error) {
	panic("planner should not be called")
}

func TestLoopCancelRunViaCommand(t *testing.T) {
	// Host order: commit CancelRun first, then cancel ctx (RUN-LOP-5).
	rt := loopRuntime(t)
	snap, _ := rt.Load(context.Background(), "run-1")
	env, _ := BuildEnvelope("run-1", "cancel-1", CancelRun{})
	if _, err := rt.Commit(context.Background(), CommitRequest{BaseRevision: snap.Revision, Command: env}); err != nil {
		t.Fatal(err)
	}
	loop, _ := New(fakeCatalog{&fakeInvoker{}}, fakeToolCatalog{}, staticPlanner{}, ExecutionPolicy{}, false)
	res, err := loop.Run(context.Background(), rt, "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Result.Status != RunStopped || res.Result.Reason != ReasonCancelled {
		t.Fatalf("res = %+v", res)
	}
}

// cancellingInvoker cancels the outer ctx from inside Generate, simulating a
// shutdown arriving mid-execution.
type cancellingInvoker struct{ cancel context.CancelFunc }

func (c *cancellingInvoker) Generate(ctx context.Context, _ sdk.Request) (sdk.ModelResult, error) {
	c.cancel()
	<-ctx.Done()
	return sdk.ModelResult{}, ctx.Err()
}

func TestLoopMidExecutionCancelRecoversModelStep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	invoker := &cancellingInvoker{cancel: cancel}
	rt := loopRuntime(t)
	loop, _ := New(fakeCatalog{invoker}, fakeToolCatalog{}, staticPlanner{}, ExecutionPolicy{}, false)

	_, err := loop.Run(ctx, rt, "run-1", nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	// The model step must be back to Prepared via RecoverModelExecution:
	// same frozen request, run still active, ModelSteps not recounted.
	snap, _ := rt.Load(context.Background(), "run-1")
	ms, ok := snap.State.Current.(ModelStep)
	if !ok || ms.Status != ModelPrepared {
		t.Fatalf("current = %#v, want Prepared ModelStep", snap.State.Current)
	}
	if snap.State.ModelSteps != 1 {
		t.Fatalf("ModelSteps = %d", snap.State.ModelSteps)
	}
	recovered := false
	for _, e := range recordEvents(t, rt, "run-1") {
		if _, ok := e.Fact.(ModelStepRecovered); ok {
			recovered = true
		}
	}
	if !recovered {
		t.Fatal("no ModelStepRecovered fact committed")
	}

	// A fresh Loop resumes the SAME frozen step without a new Prepare.
	invoker2 := &fakeInvoker{results: []sdk.ModelResult{textResult("resumed")}}
	loop2, _ := New(fakeCatalog{invoker2}, fakeToolCatalog{}, staticPlanner{}, ExecutionPolicy{}, false)
	res, err := loop2.Run(context.Background(), rt, "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Result.Model.Text != "resumed" {
		t.Fatalf("res = %+v", res)
	}
	final, _ := rt.Load(context.Background(), "run-1")
	if final.State.ModelSteps != 1 {
		t.Fatalf("ModelSteps = %d after resume, want 1 (same frozen step)", final.State.ModelSteps)
	}
}
