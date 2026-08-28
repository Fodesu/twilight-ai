// Package runtimetest contains the shared Runtime conformance suite. Durable
// Runtime implementations should run this suite in their own tests instead of
// copying MemoryRuntime-specific assertions.
package runtimetest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/memohai/twilight-ai/agent"
	"github.com/memohai/twilight-ai/sdk"
)

// Factory constructs a Runtime from an already-initialized Revision-0 state.
type Factory func(testing.TB, agent.MachineState) agent.Runtime

// Run executes the shared Runtime conformance suite.
func Run(t *testing.T, newRuntime Factory) {
	t.Helper()
	t.Run("IdempotentReplay", func(t *testing.T) { testIdempotentReplay(t, newRuntime) })
	t.Run("RevisionAndIndex", func(t *testing.T) { testRevisionAndIndex(t, newRuntime) })
	t.Run("StartGrantLifecycle", func(t *testing.T) { testStartGrantLifecycle(t, newRuntime) })
	t.Run("CallLocalRebase", func(t *testing.T) { testCallLocalRebase(t, newRuntime) })
	t.Run("PrepareDerivedIdentity", func(t *testing.T) { testPrepareDerivedIdentity(t, newRuntime) })
	t.Run("PrepareIsHardCAS", func(t *testing.T) { testPrepareIsHardCAS(t, newRuntime) })
	t.Run("CancelRebasesAndUnknownWins", func(t *testing.T) { testCancelRebasesAndUnknownWins(t, newRuntime) })
	t.Run("CancelOnStaleBase", func(t *testing.T) { testCancelOnStaleBase(t, newRuntime) })
	t.Run("ReplayFoldMatchesState", func(t *testing.T) { testReplayFoldMatchesState(t, newRuntime) })
	t.Run("AcceptInputByInputID", func(t *testing.T) { testAcceptInputByInputID(t, newRuntime) })
	t.Run("DerivedCommandIDEnforced", func(t *testing.T) { testDerivedCommandIDEnforced(t, newRuntime) })
}

type runtimeCase struct {
	t       testing.TB
	runID   agent.RunID
	initial agent.MachineState
	rt      agent.Runtime
	events  []agent.AgentEvent
}

func newCase(t testing.TB, newRuntime Factory, cfg agent.RunConfig) *runtimeCase {
	t.Helper()
	initial, err := agent.Initialize("run-1", cfg, agent.NextRun(agent.AgentInput{ID: "seed", Payload: cj(`{"q":"hi"}`)}))
	if err != nil {
		t.Fatal(err)
	}
	return &runtimeCase{t: t, runID: initial.RunID, initial: initial, rt: newRuntime(t, initial)}
}

func (c *runtimeCase) load() agent.RuntimeSnapshot {
	c.t.Helper()
	snap, err := c.rt.Load(context.Background())
	if err != nil {
		c.t.Fatal(err)
	}
	return snap
}

func (c *runtimeCase) commit(id agent.CommandID, base uint64, grant agent.ExecutionGrant, cmd agent.AgentCommand) (agent.CommitResult, error) {
	c.t.Helper()
	env, err := agent.BuildEnvelope(c.runID, id, cmd)
	if err != nil {
		c.t.Fatal(err)
	}
	res, err := c.rt.Commit(context.Background(), agent.CommitRequest{BaseRevision: base, Grant: grant, Command: env})
	if err == nil && res.Status == agent.CommitAccepted {
		c.events = append(c.events, res.Events...)
	}
	return res, err
}

func (c *runtimeCase) mustCommit(id agent.CommandID, base uint64, grant agent.ExecutionGrant, cmd agent.AgentCommand) agent.CommitResult {
	c.t.Helper()
	res, err := c.commit(id, base, grant, cmd)
	if err != nil {
		c.t.Fatalf("commit %T: %v", cmd, err)
	}
	return res
}

func preparedCase(t testing.TB, newRuntime Factory, tools []sdk.ToolDefinition, specs []agent.ToolSpec) (*runtimeCase, agent.StepID, agent.ExecutionGrant) {
	t.Helper()
	c := newCase(t, newRuntime, agent.RunConfig{Model: "m-1", ModelRejectLimit: 2})
	snap := c.load()
	prep, cmdID := buildPrepareFromSnap(t, snap, testRequest(tools...), specs)
	c.mustCommit(cmdID, snap.Revision, "", prep)
	start := c.mustCommit("start-1", 1, "", agent.StartModelExecution{StepID: prep.StepID})
	if start.Grant == "" {
		t.Fatal("accepted start returned no grant")
	}
	return c, prep.StepID, start.Grant
}

func buildPrepareFromSnap(t testing.TB, snap agent.RuntimeSnapshot, req sdk.Request, specs []agent.ToolSpec) (agent.PrepareModelRequest, agent.CommandID) {
	t.Helper()
	frozenReq, err := agent.FreezeModelRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	reqDigest, err := agent.DigestRequest(frozenReq)
	if err != nil {
		t.Fatal(err)
	}
	toolsDigest, err := agent.DigestToolSpecs(specs)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := agent.DigestModelStepBinding(snap.State.Config.Model, reqDigest, toolsDigest)
	if err != nil {
		t.Fatal(err)
	}
	cmdID := agent.DeriveModelRequestCommandID(snap.State.RunID, snap.Revision)
	stepID := agent.DeriveModelStepID(snap.State.RunID, cmdID, binding)
	ids := make([]agent.InputID, len(snap.State.PendingInputs))
	for i, in := range snap.State.PendingInputs {
		ids[i] = in.ID
	}
	return agent.PrepareModelRequest{
		StepID: stepID, Model: snap.State.Config.Model, Request: frozenReq,
		RequestDigest: reqDigest, InputIDs: ids, Tools: specs, ToolsDigest: toolsDigest,
	}, cmdID
}

func testRequest(tools ...sdk.ToolDefinition) sdk.Request {
	return sdk.Request{
		Model:    "m-1",
		Messages: []sdk.Message{sdk.UserMessage("hi")},
		Tools:    tools,
	}
}

func testToolDef(name string) sdk.ToolDefinition {
	return sdk.ToolDefinition{Name: name, Parameters: json.RawMessage(`{"type":"object"}`)}
}

func cj(raw string) agent.CanonicalJSON { return agent.MustParseCanonicalJSON(raw) }

func makeSpec(t testing.TB, def sdk.ToolDefinition, policy agent.ResponsePolicy) agent.ToolSpec {
	t.Helper()
	frozen, err := agent.FreezeToolDefinition(def)
	if err != nil {
		t.Fatal(err)
	}
	d, err := agent.DigestToolDefinition(frozen)
	if err != nil {
		t.Fatal(err)
	}
	return agent.ToolSpec{Ref: agent.ToolRef(def.Name), Definition: frozen, DefinitionDigest: d, Policy: policy}
}

func makeBinding(t testing.TB, callID string, spec agent.ToolSpec, args string) agent.ToolCallBinding {
	t.Helper()
	parsedArgs := cj(args)
	bd, err := agent.DigestToolCallBinding(agent.CallID(callID), spec.DefinitionDigest, spec.Policy, parsedArgs)
	if err != nil {
		t.Fatal(err)
	}
	return agent.ToolCallBinding{
		CallID:           agent.CallID(callID),
		ToolRef:          spec.Ref,
		DefinitionDigest: spec.DefinitionDigest,
		BindingDigest:    bd,
		Arguments:        parsedArgs,
		Policy:           spec.Policy,
	}
}

func modelResultWithCalls(callIDs ...string) agent.ModelResult {
	r := sdk.ModelResult{
		FinishReason: sdk.FinishReasonToolCalls,
		Usage:        sdk.Usage{InputTokens: 10, OutputTokens: 5, TotalTokens: 15},
	}
	for _, id := range callIDs {
		r.ToolCalls = append(r.ToolCalls, sdk.ToolCall{ToolCallID: id, ToolName: "t", Input: `{}`})
	}
	frozen, err := agent.FreezeModelResult(r)
	if err != nil {
		panic(err)
	}
	return frozen
}

func testIdempotentReplay(t *testing.T, newRuntime Factory) {
	c := newCase(t, newRuntime, agent.RunConfig{Model: "m-1"})
	res1 := c.mustCommit("cancel-1", 0, "", agent.CancelRun{})
	if res1.Status != agent.CommitAccepted || len(res1.Events) != 1 {
		t.Fatalf("res1 = %+v", res1)
	}
	res2 := c.mustCommit("cancel-1", 0, "", agent.CancelRun{})
	if res2.Status != agent.CommitAlreadyApplied {
		t.Fatalf("status = %v", res2.Status)
	}
	if len(res2.Events) != 1 || res2.Events[0].Digest != res1.Events[0].Digest ||
		res2.Events[0].Revision != res1.Events[0].Revision {
		t.Fatal("replay did not return the original event group")
	}
	if res2.Snapshot.Revision != res1.Snapshot.Revision {
		t.Fatal("replay advanced the revision")
	}
	_, err := c.commit("cancel-1", 0, "", agent.CancelRun{Reason: "other"})
	if err != agent.ErrCommandConflict {
		t.Fatalf("err = %v, want ErrCommandConflict", err)
	}
}

func testRevisionAndIndex(t *testing.T, newRuntime Factory) {
	def := testToolDef("t")
	spec := makeSpec(t, def, agent.DirectExecution)
	c, stepID, grant := preparedCase(t, newRuntime, []sdk.ToolDefinition{def}, []agent.ToolSpec{spec})

	b := makeBinding(t, "c1", spec, `{}`)
	res := c.mustCommit("complete-1", 2, grant,
		agent.SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []agent.ToolCallBinding{b}})
	if len(res.Events) != 2 {
		t.Fatalf("events = %d", len(res.Events))
	}
	for i, e := range res.Events {
		if e.Revision != res.Snapshot.Revision {
			t.Fatalf("event revision %d != snapshot %d", e.Revision, res.Snapshot.Revision)
		}
		if int(e.Index) != i {
			t.Fatalf("index[%d] = %d", i, e.Index)
		}
		if e.CommandID != "complete-1" {
			t.Fatal("command id not stamped")
		}
	}
}

func testStartGrantLifecycle(t *testing.T, newRuntime Factory) {
	c, stepID, grant := preparedCase(t, newRuntime, nil, nil)

	_, err := c.commit("done-x", 2, "", agent.SubmitModelResult{StepID: stepID, Result: agent.ModelResult{}})
	if !errors.Is(err, agent.ErrStaleRuntime) {
		t.Fatalf("grantless completion err = %v, want ErrStaleRuntime", err)
	}
	res := c.mustCommit("start-1", 1, "", agent.StartModelExecution{StepID: stepID})
	if res.Status != agent.CommitAlreadyApplied || res.Grant != "" {
		t.Fatalf("replayed start: %+v", res)
	}
	ok, err := agent.FreezeModelResult(sdk.ModelResult{Text: "ok"})
	if err != nil {
		t.Fatal(err)
	}
	res = c.mustCommit("done-1", 2, grant, agent.SubmitModelResult{StepID: stepID, Result: ok})
	if res.Status != agent.CommitAccepted || res.Snapshot.State.Status != agent.RunCompleted {
		t.Fatalf("completion: %+v", res.Snapshot.State.Status)
	}
}

func testCallLocalRebase(t *testing.T, newRuntime Factory) {
	defA, defB := testToolDef("a"), testToolDef("b")
	specA := makeSpec(t, defA, agent.DirectExecution)
	specB := makeSpec(t, defB, agent.DirectExecution)
	c, stepID, grant := preparedCase(t, newRuntime, []sdk.ToolDefinition{defA, defB}, []agent.ToolSpec{specA, specB})

	bA := makeBinding(t, "cA", specA, `{}`)
	bB := makeBinding(t, "cB", specB, `{}`)
	r, err := agent.FreezeModelResult(sdk.ModelResult{
		FinishReason: sdk.FinishReasonToolCalls,
		ToolCalls: []sdk.ToolCall{
			{ToolCallID: "cA", ToolName: "a", Input: `{}`},
			{ToolCallID: "cB", ToolName: "b", Input: `{}`},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	res := c.mustCommit("complete-1", 2, grant,
		agent.SubmitModelResult{StepID: stepID, Result: r, Calls: []agent.ToolCallBinding{bA, bB}})
	toolStep := res.Events[1].Fact.(agent.ToolStepOpened).StepID
	base := res.Snapshot.Revision

	startA := c.mustCommit("start-A", base, "", agent.StartToolCall{StepID: toolStep, CallID: "cA"})
	startB := c.mustCommit("start-B", base, "", agent.StartToolCall{StepID: toolStep, CallID: "cB"})
	if startB.Status != agent.CommitAccepted || startB.Grant == "" {
		t.Fatal("stale-base start of an untouched Pending call must rebase")
	}
	doneA := c.mustCommit("done-A", base, startA.Grant,
		agent.SubmitToolResult{StepID: toolStep, CallID: "cA", Result: agent.ToolExecutionResult{Output: cj(`1`)}})
	if doneA.Status != agent.CommitAccepted {
		t.Fatal("owner completion on stale base must rebase")
	}
	_, err = c.commit("start-A2", base, "", agent.StartToolCall{StepID: toolStep, CallID: "cA"})
	if !errors.Is(err, agent.ErrStaleRuntime) {
		t.Fatalf("restart of settled call err = %v, want ErrStaleRuntime", err)
	}
	_ = startB
}

func testPrepareDerivedIdentity(t *testing.T, newRuntime Factory) {
	c := newCase(t, newRuntime, agent.RunConfig{Model: "m-1"})
	snap := c.load()
	prep, cmdID := buildPrepareFromSnap(t, snap, testRequest(), nil)

	if _, err := c.commit("wrong-prepare-id", snap.Revision, "", prep); err == nil {
		t.Fatal("PrepareModelRequest accepted a non-derived CommandID")
	}

	bad := prep
	bad.StepID = "wrong-step"
	_, err := c.commit(cmdID, snap.Revision, "", bad)
	if !errors.Is(err, agent.ErrStaleRuntime) {
		t.Fatalf("bad prepare StepID err = %v, want ErrStaleRuntime", err)
	}
}

func testPrepareIsHardCAS(t *testing.T, newRuntime Factory) {
	c := newCase(t, newRuntime, agent.RunConfig{Model: "m-1"})
	snap := c.load()
	prep, cmdID := buildPrepareFromSnap(t, snap, testRequest(), nil)
	c.mustCommit(cmdID, snap.Revision, "", prep)

	otherReq := testRequest()
	otherReq.System = "different"
	prep2, cmdID2 := buildPrepareFromSnap(t, snap, otherReq, nil)
	if cmdID2 != cmdID {
		t.Fatal("same revision must derive the same command id")
	}
	_, err := c.commit(cmdID2, snap.Revision, "", prep2)
	if err != agent.ErrCommandConflict {
		t.Fatalf("err = %v, want ErrCommandConflict", err)
	}
	res := c.mustCommit(cmdID, snap.Revision, "", prep)
	if res.Status != agent.CommitAlreadyApplied {
		t.Fatalf("status = %v", res.Status)
	}
}

func testCancelRebasesAndUnknownWins(t *testing.T, newRuntime Factory) {
	def := testToolDef("t")
	spec := makeSpec(t, def, agent.DirectExecution)
	c, stepID, grant := preparedCase(t, newRuntime, []sdk.ToolDefinition{def}, []agent.ToolSpec{spec})
	b := makeBinding(t, "c1", spec, `{}`)
	res := c.mustCommit("complete-1", 2, grant,
		agent.SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []agent.ToolCallBinding{b}})
	toolStep := res.Events[1].Fact.(agent.ToolStepOpened).StepID
	startRes := c.mustCommit("start-c1", res.Snapshot.Revision, "", agent.StartToolCall{StepID: toolStep, CallID: "c1"})

	unknown := c.mustCommit("unk-1", startRes.Snapshot.Revision, startRes.Grant,
		agent.SubmitToolFailure{StepID: toolStep, CallID: "c1", Outcome: agent.ToolOutcomeUnknown})
	if unknown.Snapshot.State.Status != agent.RunFailed {
		t.Fatal("unknown did not fail the run")
	}
	_, err := c.commit("cancel-late", 0, "", agent.CancelRun{})
	if err != agent.ErrRunTerminal {
		t.Fatalf("late cancel err = %v, want ErrRunTerminal", err)
	}
}

func testCancelOnStaleBase(t *testing.T, newRuntime Factory) {
	c, _, _ := preparedCase(t, newRuntime, nil, nil)
	res := c.mustCommit("cancel-1", 0, "", agent.CancelRun{})
	if res.Status != agent.CommitAccepted || res.Snapshot.State.Status != agent.RunStopped {
		t.Fatalf("cancel: %+v", res.Snapshot.State.Status)
	}
}

func testReplayFoldMatchesState(t *testing.T, newRuntime Factory) {
	def := testToolDef("t")
	spec := makeSpec(t, def, agent.DirectExecution)
	c, stepID, grant := preparedCase(t, newRuntime, []sdk.ToolDefinition{def}, []agent.ToolSpec{spec})
	b := makeBinding(t, "c1", spec, `{}`)
	res := c.mustCommit("complete-1", 2, grant,
		agent.SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []agent.ToolCallBinding{b}})
	toolStep := res.Events[1].Fact.(agent.ToolStepOpened).StepID
	sRes := c.mustCommit("start-c1", res.Snapshot.Revision, "", agent.StartToolCall{StepID: toolStep, CallID: "c1"})
	c.mustCommit("done-c1", sRes.Snapshot.Revision, sRes.Grant,
		agent.SubmitToolResult{StepID: toolStep, CallID: "c1", Result: agent.ToolExecutionResult{Output: cj(`"ok"`)}})

	folded, lastRev, err := agent.FoldEvents(c.initial, c.events)
	if err != nil {
		t.Fatalf("FoldEvents: %v", err)
	}
	live := c.load()
	a, _ := json.Marshal(stateComparable(live.State))
	bts, _ := json.Marshal(stateComparable(folded))
	if string(a) != string(bts) {
		t.Fatalf("replay diverged:\n live   %s\n replay %s", a, bts)
	}
	if live.Revision != lastRev {
		t.Fatalf("snapshot revision %d != last event revision %d", live.Revision, lastRev)
	}
}

func testAcceptInputByInputID(t *testing.T, newRuntime Factory) {
	c := newCase(t, newRuntime, agent.RunConfig{Model: "m-1"})
	in := agent.AgentInput{ID: "in-9", Payload: cj(`{"t":"x"}`)}
	id := agent.DeriveInputCommandID("run-1", in.ID)
	res1 := c.mustCommit(id, 0, "", agent.NextStep(in))
	if res1.Status != agent.CommitAccepted {
		t.Fatal("first accept rejected")
	}
	res2 := c.mustCommit(id, 0, "", agent.NextStep(in))
	if res2.Status != agent.CommitAlreadyApplied {
		t.Fatalf("status = %v", res2.Status)
	}
	_, err := c.commit(id, 0, "", agent.NextStep(agent.AgentInput{ID: "in-9", Payload: cj(`{"t":"y"}`)}))
	if err != agent.ErrCommandConflict {
		t.Fatalf("err = %v, want ErrCommandConflict", err)
	}
}

func testDerivedCommandIDEnforced(t *testing.T, newRuntime Factory) {
	c := newCase(t, newRuntime, agent.RunConfig{Model: "m-1"})
	_, err := c.commit("random-id", 0, "", agent.NextStep(agent.AgentInput{ID: "in-1", Payload: cj(`1`)}))
	if err == nil {
		t.Fatal("AcceptInput with non-derived CommandID accepted")
	}
	_, err = c.commit("random-id-2", 0, "", agent.ApproveToolCall{StepID: "s", CallID: "c", ResponseID: "r"})
	if err == nil {
		t.Fatal("ApproveToolCall with non-derived CommandID accepted")
	}
}

func stateComparable(s agent.MachineState) map[string]any {
	m := map[string]any{
		"runId": s.RunID, "status": s.Status, "config": s.Config,
		"modelSteps": s.ModelSteps, "lastClosedStep": s.LastClosedStep,
		"usage": s.Usage, "pendingInputs": s.PendingInputs,
		"lastModelResult": s.LastModelResult, "result": s.Result,
	}
	switch cur := s.Current.(type) {
	case agent.ModelStep:
		m["modelStep"] = cur
	case agent.ToolStep:
		m["toolStep"] = cur
	}
	return m
}
