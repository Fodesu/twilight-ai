// Package runtest drives agent Run features for tests.
//
// A Feature owns one in-process Runtime and, when Run is called, one Loop.
// Tests name protocol features and speak in Tool/Model/Run/RunError/Approve/Require*.
// Digest, envelope, revision, and default claims stay inside the driver.
package runtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/run/loop"
	"github.com/memohai/twilight/sdk"
)

const (
	defaultRunID = "run-1"
	defaultModel = "m-1"
)

// Feature is one seeded Run plus the Loop/Runtime used to drive it.
type Feature struct {
	t      testing.TB
	ctx    context.Context
	runCtx context.Context
	runID  run.RunID
	rt     run.Runtime

	model   run.ModelRef
	results []sdk.ModelResult
	specs   []run.ToolSpec
	tools   map[run.ToolRef]*scriptTool
	invoker *scriptInvoker
	planner *scriptPlanner
	loop    *loop.Loop
	seq     int

	modelStepID run.StepID
	modelGrant  run.ExecutionGrant
	last        loop.LoopResult
	resolveErr  error
}

// New creates a Runtime, a Run, and the seed input. Configure tools and
// model results before Run or Executing*.
func New(t testing.TB) *Feature {
	t.Helper()
	rt := run.NewRuntime(run.NewMemoryStore())
	newRun, err := run.BuildNewRun(defaultRunID, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Create(context.Background(), newRun); err != nil {
		t.Fatal(err)
	}
	f := &Feature{
		t:      t,
		ctx:    context.Background(),
		runCtx: context.Background(),
		runID:  defaultRunID,
		rt:     rt,
		model:  defaultModel,
		tools:  make(map[run.ToolRef]*scriptTool),
	}
	f.commit(run.AcceptInput{Input: run.AgentInput{
		ID:      "seed",
		Payload: run.MustParseCanonicalJSON(`{"q":"hi"}`),
	}}, "")
	return f
}

// Tool registers a catalog tool. Default execution echoes the call arguments.
func (f *Feature) Tool(name string, policy run.ResponsePolicy) *Feature {
	f.t.Helper()
	f.guardConfig()
	spec := f.mustSpec(name, policy)
	f.specs = append(f.specs, spec)
	f.tools[spec.Ref] = &scriptTool{
		ref:    spec.Ref,
		def:    spec.Definition.SDK(),
		policy: policy,
	}
	return f
}

// Unknown registers a DirectExecution tool whose Execute returns Unknown.
func (f *Feature) Unknown(name string) *Feature {
	f.t.Helper()
	f.Tool(name, run.DirectExecution)
	f.tools[run.ToolRef(name)].unknown = true
	return f
}

// KnownFailure registers a DirectExecution tool whose Execute returns a known failure.
func (f *Feature) KnownFailure(name, class string) *Feature {
	f.t.Helper()
	f.Tool(name, run.DirectExecution)
	f.tools[run.ToolRef(name)].fail = class
	return f
}

// Model sets the scripted provider results, in Generate order.
func (f *Feature) Model(results ...sdk.ModelResult) *Feature {
	f.t.Helper()
	f.guardConfig()
	f.results = append(f.results, results...)
	return f
}

// ModelResolveError makes the Loop catalog Resolve return err.
func (f *Feature) ModelResolveError(err error) *Feature {
	f.t.Helper()
	f.guardConfig()
	f.resolveErr = err
	return f
}

// Context sets the context passed to the next Loop.Run. Load and Commit
// keep using the Feature's background context.
func (f *Feature) Context(ctx context.Context) *Feature {
	f.t.Helper()
	f.runCtx = ctx
	return f
}

// Run interprets executable effects through Loop until it yields or finishes.
// A Loop error fails the test; expected errors use RunError.
func (f *Feature) Run() *Feature {
	f.t.Helper()
	if err := f.drive(); err != nil {
		f.t.Fatalf("Run: %v", err)
	}
	return f
}

// RunError drives Loop and checks the error with errors.Is.
func (f *Feature) RunError(want error) *Feature {
	f.t.Helper()
	if want == nil {
		f.t.Fatal("RunError: nil want")
	}
	err := f.drive()
	if !errors.Is(err, want) {
		f.t.Fatalf("Run error = %v, want %v", err, want)
	}
	return f
}

func (f *Feature) drive() error {
	f.t.Helper()
	f.ensureLoop()
	res, err := f.loop.Run(f.runCtx, f.rt, f.runID, nil)
	f.last = res
	return err
}

// Waiting returns the current ResponseRequest. Tests that submit a
// malformed ingress command use this with TryCommit.
func (f *Feature) Waiting() run.ResponseRequest {
	f.t.Helper()
	return f.waiting()
}

// TryCommit submits cmd and returns the Runtime error. Feature tests use
// this for rejected ingress; happy-path commands go through Approve/Reject.
func (f *Feature) TryCommit(cmd run.AgentCommand) error {
	f.t.Helper()
	snap := f.load()
	proto, err := snap.Protocol()
	if err != nil {
		f.t.Fatal(err)
	}
	id := f.commandID(cmd, snap)
	cmd = withClaim(id, cmd)
	env, err := proto.BuildEnvelope(f.runID, id, cmd)
	if err != nil {
		return err
	}
	_, err = f.rt.Commit(f.ctx, run.CommitRequest{BaseRevision: snap.Revision, Command: env})
	return err
}

// Approve commits ApproveToolCall for the current waiting call.
func (f *Feature) Approve() *Feature {
	f.t.Helper()
	w := f.waiting()
	digest, err := run.DigestToolResponseDecision(w.Kind, run.ResponseDecisionApproved, "")
	if err != nil {
		f.t.Fatal(err)
	}
	f.commit(run.ApproveToolCall{
		StepID: w.StepID, CallID: w.CallID, ResponseID: w.ID, ResponseDigest: digest,
	}, "")
	return f
}

// Reject commits RejectToolCall for the current waiting call.
func (f *Feature) Reject(reason string) *Feature {
	f.t.Helper()
	w := f.waiting()
	digest, err := run.DigestToolResponseDecision(w.Kind, run.ResponseDecisionRejected, reason)
	if err != nil {
		f.t.Fatal(err)
	}
	f.commit(run.RejectToolCall{
		StepID: w.StepID, CallID: w.CallID, ResponseID: w.ID,
		ResponseDigest: digest, Reason: reason,
	}, "")
	return f
}

// Cancel commits CancelRun.
func (f *Feature) Cancel() *Feature {
	f.t.Helper()
	f.commit(run.CancelRun{}, "")
	return f
}

// ExecutingModel leaves the Run on an Executing ModelStep (no Loop).
func (f *Feature) ExecutingModel() *Feature {
	f.t.Helper()
	f.commitPrepare()
	res := f.commit(run.StartModelExecution{StepID: f.modelStepID}, "")
	f.modelGrant = res.Grant
	return f
}

// ExecutingTool leaves the named tool call Executing (no Loop).
func (f *Feature) ExecutingTool(name string, callID run.CallID) *Feature {
	f.t.Helper()
	var spec run.ToolSpec
	found := false
	for _, candidate := range f.specs {
		if candidate.Ref == run.ToolRef(name) {
			spec = candidate
			found = true
			break
		}
	}
	if !found {
		f.t.Fatalf("ExecutingTool: tool %q not registered", name)
	}
	f.ExecutingModel()
	args := run.MustParseCanonicalJSON(`{"x":1}`)
	binding, err := run.DigestToolCallBinding(callID, spec.DefinitionDigest, spec.Policy, args)
	if err != nil {
		f.t.Fatal(err)
	}
	frozen, err := run.FreezeModelResult(sdk.ModelResult{
		FinishReason: sdk.FinishReasonToolCalls,
		Usage:        sdk.Usage{TotalTokens: 2},
		ToolCalls: []sdk.ToolCall{{
			ToolCallID: string(callID), ToolName: string(spec.Ref), Input: `{"x":1}`,
		}},
	})
	if err != nil {
		f.t.Fatal(err)
	}
	res := f.commit(run.SubmitModelResult{
		StepID: f.modelStepID,
		Result: frozen,
		Calls: []run.ToolCallBinding{{
			CallID: callID, ToolRef: spec.Ref, DefinitionDigest: spec.DefinitionDigest,
			BindingDigest: binding, Arguments: args, Policy: spec.Policy,
		}},
	}, f.modelGrant)
	ts, ok := res.Snapshot.State.Current.(run.ToolStep)
	if !ok {
		f.t.Fatalf("after model result: %T", res.Snapshot.State.Current)
	}
	f.commit(run.StartToolCall{StepID: ts.Ref().ID, CallID: callID}, "")
	return f
}

func (f *Feature) guardConfig() {
	f.t.Helper()
	if f.loop != nil {
		f.t.Fatal("configure Tool/Model/ModelResolveError before Run")
	}
}

func (f *Feature) ensureLoop() {
	f.t.Helper()
	if f.loop != nil {
		return
	}
	f.invoker = &scriptInvoker{results: f.results}
	f.planner = &scriptPlanner{model: f.model, specs: f.specs}
	tools := make(map[run.ToolRef]loop.ExecutableTool, len(f.tools))
	for ref, tool := range f.tools {
		tools[ref] = tool
	}
	l, err := loop.New(scriptCatalog{invoker: f.invoker, err: f.resolveErr}, scriptToolCatalog{tools}, f.planner, loop.ExecutionPolicy{}, false)
	if err != nil {
		f.t.Fatal(err)
	}
	f.loop = l
}

func (f *Feature) load() run.RuntimeSnapshot {
	f.t.Helper()
	snap, err := f.rt.Load(f.ctx, f.runID)
	if err != nil {
		f.t.Fatal(err)
	}
	return snap
}

func (f *Feature) state() run.MachineState {
	f.t.Helper()
	return f.load().State
}

func (f *Feature) waiting() run.ResponseRequest {
	f.t.Helper()
	reqs := run.WaitingCalls(f.state())
	if len(reqs) == 0 {
		f.t.Fatal("no waiting call")
	}
	return reqs[0]
}

func (f *Feature) commit(cmd run.AgentCommand, grant run.ExecutionGrant) run.CommitResult {
	f.t.Helper()
	snap := f.load()
	proto, err := snap.Protocol()
	if err != nil {
		f.t.Fatal(err)
	}
	id := f.commandID(cmd, snap)
	cmd = withClaim(id, cmd)
	env, err := proto.BuildEnvelope(f.runID, id, cmd)
	if err != nil {
		f.t.Fatal(err)
	}
	res, err := f.rt.Commit(f.ctx, run.CommitRequest{
		BaseRevision: snap.Revision, Grant: grant, Command: env,
	})
	if err != nil {
		f.t.Fatalf("commit %T: %v", cmd, err)
	}
	return res
}

func (f *Feature) commandID(cmd run.AgentCommand, snap run.RuntimeSnapshot) run.CommandID {
	switch c := cmd.(type) {
	case run.AcceptInput:
		return run.DeriveInputCommandID(f.runID, c.Input.ID)
	case run.ApproveToolCall:
		return run.DeriveResponseCommandID(f.runID, c.StepID, c.CallID, c.ResponseID)
	case run.RejectToolCall:
		return run.DeriveResponseCommandID(f.runID, c.StepID, c.CallID, c.ResponseID)
	case run.SubmitToolResponse:
		return run.DeriveResponseCommandID(f.runID, c.StepID, c.CallID, c.ResponseID)
	case run.PrepareModelRequest:
		return run.DeriveModelRequestCommandID(f.runID, snap.Revision)
	case run.RecoverModelExecution:
		return run.DeriveModelRecoveryCommandID(f.runID, c.StepID, c.Claim)
	default:
		f.seq++
		return run.CommandID(fmt.Sprintf("cmd-%d", f.seq))
	}
}

func (f *Feature) commitPrepare() {
	f.t.Helper()
	snap := f.load()
	req := sdk.Request{Model: string(f.model), Messages: []sdk.Message{sdk.UserMessage("go")}}
	for _, spec := range f.specs {
		req.Tools = append(req.Tools, spec.Definition.SDK())
	}
	frozen, err := run.FreezeModelRequest(req)
	if err != nil {
		f.t.Fatal(err)
	}
	proto, err := snap.Protocol()
	if err != nil {
		f.t.Fatal(err)
	}
	reqDigest, err := proto.DigestRequest(frozen)
	if err != nil {
		f.t.Fatal(err)
	}
	toolsDigest, err := proto.DigestToolSpecs(f.specs)
	if err != nil {
		f.t.Fatal(err)
	}
	binding, err := proto.DigestModelStepBinding(f.model, reqDigest, toolsDigest)
	if err != nil {
		f.t.Fatal(err)
	}
	cmdID := run.DeriveModelRequestCommandID(f.runID, snap.Revision)
	stepID := run.DeriveModelStepID(f.runID, cmdID, binding)
	ids := make([]run.InputID, len(snap.State.PendingInputs))
	for i, in := range snap.State.PendingInputs {
		ids[i] = in.ID
	}
	f.modelStepID = stepID
	f.commit(run.PrepareModelRequest{
		StepID: stepID, Model: f.model, Request: frozen,
		RequestDigest: reqDigest, InputIDs: ids, Tools: f.specs, ToolsDigest: toolsDigest,
	}, "")
}

func (f *Feature) mustSpec(name string, policy run.ResponsePolicy) run.ToolSpec {
	f.t.Helper()
	def := sdk.ToolDefinition{Name: name, Parameters: json.RawMessage(`{"type":"object"}`)}
	frozen, err := run.FreezeToolDefinition(def)
	if err != nil {
		f.t.Fatal(err)
	}
	d, err := run.DigestToolDefinition(frozen)
	if err != nil {
		f.t.Fatal(err)
	}
	return run.ToolSpec{Ref: run.ToolRef(name), Definition: frozen, DefinitionDigest: d, Policy: policy}
}

func (f *Feature) facts() []run.Fact {
	f.t.Helper()
	record, err := f.rt.Record(f.ctx, f.runID)
	if err != nil {
		f.t.Fatal(err)
	}
	var out []run.Fact
	for _, tr := range record.Transitions {
		for _, e := range tr.Events {
			out = append(out, e.Fact)
		}
	}
	return out
}

func withClaim(id run.CommandID, cmd run.AgentCommand) run.AgentCommand {
	claim := run.ExecutionClaim("runtest/" + string(id))
	switch c := cmd.(type) {
	case run.StartModelExecution:
		if c.Claim == "" {
			c.Claim = claim
		}
		return c
	case run.StartToolCall:
		if c.Claim == "" {
			c.Claim = claim
		}
		return c
	default:
		return cmd
	}
}
