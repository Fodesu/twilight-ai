package run

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

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
	return newTestRuntime(t, RunConfig{Model: "m-1"})
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
	loop, err := NewLoop(fakeCatalog{&fakeInvoker{results: []sdk.ModelResult{textResult("hello")}}},
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

func TestLoopToolRoundTrip(t *testing.T) {
	spec := toolSpec(t, "echo", DirectExecution)
	echo := &fakeTool{ref: "echo", def: spec.Definition.SDK(), policy: DirectExecution,
		execute: func(_ context.Context, req ToolExecutionRequest) ToolExecutionOutcome {
			return ToolExecutionSucceeded{Result: ToolExecutionResult{Output: req.Arguments}}
		}}
	invoker := &fakeInvoker{results: []sdk.ModelResult{toolCallResult("c1"), textResult("done")}}
	rt := loopRuntime(t)
	loop, _ := NewLoop(fakeCatalog{invoker}, fakeToolCatalog{map[ToolRef]ExecutableTool{"echo": echo}},
		staticPlanner{specs: []ToolSpec{spec}}, ExecutionPolicy{}, false)

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
	loop, _ := NewLoop(fakeCatalog{invoker}, fakeToolCatalog{map[ToolRef]ExecutableTool{"echo": echo}},
		staticPlanner{specs: []ToolSpec{spec}}, ExecutionPolicy{}, false)

	// First run: reaches Waiting(Approval) and returns.
	res, err := loop.Run(context.Background(), rt, "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Disposition != LoopWaiting || res.Reason != WaitingForResponse || len(res.Waiting) != 1 {
		t.Fatalf("res = %+v", res)
	}
	if executed.Load() {
		t.Fatal("tool executed before approval")
	}
	wait := res.Waiting[0]
	if wait.Kind != ResponseApproval {
		t.Fatalf("kind = %v", wait.Kind)
	}

	// Ingress approves via the derived response command id (spec §5.7).
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

func TestLoopUnknownOutcomeFailsRun(t *testing.T) {
	spec := toolSpec(t, "echo", DirectExecution)
	echo := &fakeTool{ref: "echo", def: spec.Definition.SDK(), policy: DirectExecution,
		execute: func(context.Context, ToolExecutionRequest) ToolExecutionOutcome {
			return ToolExecutionUnknown{Failure: ToolFailure{Class: FailureEffectUnknown, Message: "lost"}}
		}}
	invoker := &fakeInvoker{results: []sdk.ModelResult{toolCallResult("c1")}}
	rt := loopRuntime(t)
	loop, _ := NewLoop(fakeCatalog{invoker}, fakeToolCatalog{map[ToolRef]ExecutableTool{"echo": echo}},
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
	loop, _ := NewLoop(fakeCatalog{invoker}, fakeToolCatalog{map[ToolRef]ExecutableTool{"echo": echo}},
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
	loop, _ := NewLoop(fakeCatalog{invoker}, fakeToolCatalog{tools: map[ToolRef]ExecutableTool{}},
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
	loop, _ := NewLoop(fakeCatalog{invoker}, fakeToolCatalog{map[ToolRef]ExecutableTool{"echo": echo}},
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

func TestLoopCtxCancelReturnsWithoutFailingRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	invoker := &fakeInvoker{errs: []error{context.Canceled}}
	rt := loopRuntime(t)
	loop, _ := NewLoop(fakeCatalog{invoker}, fakeToolCatalog{}, staticPlanner{}, ExecutionPolicy{}, false)

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
	loop2, _ := NewLoop(fakeCatalog{invoker2}, fakeToolCatalog{}, staticPlanner{}, ExecutionPolicy{}, false)
	res, err := loop2.Run(context.Background(), rt, "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Result == nil || res.Result.Model.Text != "resumed" {
		t.Fatalf("res = %+v", res)
	}
}

func TestLoopModelStepLimitStopsBeforePlanning(t *testing.T) {
	// Corruption fixture: the limit is a snapshot-only condition that cannot
	// be reached without planning, so inject it locally after a real Revision-0
	// admission. Normal loop fixtures never mutate MemoryRuntime internals.
	rt := NewMemoryRuntime()
	newRun, err := BuildNewRun("run-1", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Create(context.Background(), newRun); err != nil {
		t.Fatal(err)
	}
	entry := memoryEntry(t, rt)
	entry.mu.Lock()
	entry.state.ModelSteps = 1
	entry.mu.Unlock()
	loop, err := NewLoop(fakeCatalog{}, fakeToolCatalog{}, panicPlanner{}, ExecutionPolicy{ModelStepLimit: 1}, false)
	if err != nil {
		t.Fatal(err)
	}
	res, err := loop.Run(context.Background(), rt, "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Disposition != LoopFinished || res.Result == nil || res.Result.Reason != ReasonStepLimit {
		t.Fatalf("loop result = %+v", res)
	}
}

func TestLoopMalformedModelResultLimitFailsRun(t *testing.T) {
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
	loop, err := NewLoop(fakeCatalog{invoker}, fakeToolCatalog{}, staticPlanner{}, ExecutionPolicy{MalformedModelResultLimit: 2}, false)
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
	// Host order: commit CancelRun first, then cancel ctx (spec §6.6).
	rt := loopRuntime(t)
	snap, _ := rt.Load(context.Background(), "run-1")
	env, _ := BuildEnvelope("run-1", "cancel-1", CancelRun{})
	if _, err := rt.Commit(context.Background(), CommitRequest{BaseRevision: snap.Revision, Command: env}); err != nil {
		t.Fatal(err)
	}
	loop, _ := NewLoop(fakeCatalog{&fakeInvoker{}}, fakeToolCatalog{}, staticPlanner{}, ExecutionPolicy{}, false)
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
	loop, _ := NewLoop(fakeCatalog{invoker}, fakeToolCatalog{}, staticPlanner{}, ExecutionPolicy{}, false)

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
	loop2, _ := NewLoop(fakeCatalog{invoker2}, fakeToolCatalog{}, staticPlanner{}, ExecutionPolicy{}, false)
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
