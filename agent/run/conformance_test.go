// Package runtimetest contains the shared Runtime conformance suite. Durable
// Runtime implementations should run this suite in their own tests instead of
// copying MemoryRuntime-specific assertions.
package run

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/memohai/twilight-ai/sdk"
)

// conformanceFactory constructs a Runtime from an already-initialized Revision-0 state.
type conformanceFactory func(testing.TB, MachineState) Runtime

// Run executes the shared Runtime conformance suite.
func runConformance(t *testing.T, newRuntime conformanceFactory) {
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

type conformanceCase struct {
	t       testing.TB
	runID   RunID
	initial MachineState
	rt      Runtime
	events  []AgentEvent
}

func newConformanceCase(t testing.TB, newRuntime conformanceFactory) *conformanceCase {
	t.Helper()
	initial, err := InitializeRun("run-1")
	if err != nil {
		t.Fatal(err)
	}
	initial.PendingInputs = []AgentInput{{ID: "seed", Payload: conformanceJSON(`{"q":"hi"}`)}}
	return &conformanceCase{t: t, runID: initial.RunID, initial: initial, rt: newRuntime(t, initial)}
}

func (c *conformanceCase) load() RuntimeSnapshot {
	c.t.Helper()
	snap, err := c.rt.Load(context.Background())
	if err != nil {
		c.t.Fatal(err)
	}
	return snap
}

func (c *conformanceCase) commit(id CommandID, base uint64, grant ExecutionGrant, cmd AgentCommand) (CommitResult, error) {
	c.t.Helper()
	env, err := BuildEnvelope(c.runID, id, cmd)
	if err != nil {
		c.t.Fatal(err)
	}
	res, err := c.rt.Commit(context.Background(), CommitRequest{BaseRevision: base, Grant: grant, Command: env})
	if err == nil && res.Status == CommitAccepted {
		c.events = append(c.events, res.Events...)
	}
	return res, err
}

func (c *conformanceCase) mustCommit(id CommandID, base uint64, grant ExecutionGrant, cmd AgentCommand) CommitResult {
	c.t.Helper()
	res, err := c.commit(id, base, grant, cmd)
	if err != nil {
		c.t.Fatalf("commit %T: %v", cmd, err)
	}
	return res
}

func preparedConformanceCase(t testing.TB, newRuntime conformanceFactory, tools []sdk.ToolDefinition, specs []ToolSpec) (*conformanceCase, StepID, ExecutionGrant) {
	t.Helper()
	c := newConformanceCase(t, newRuntime)
	snap := c.load()
	req := conformanceRequest(tools...)
	prep, cmdID := buildConformancePrepareFromSnap(t, &snap, &req, specs)
	c.mustCommit(cmdID, snap.Revision, "", prep)
	start := c.mustCommit("start-1", 1, "", StartModelExecution{StepID: prep.StepID})
	if start.Grant == "" {
		t.Fatal("accepted start returned no grant")
	}
	return c, prep.StepID, start.Grant
}

func buildConformancePrepareFromSnap(t testing.TB, snap *RuntimeSnapshot, req *sdk.Request, specs []ToolSpec) (PrepareModelRequest, CommandID) {
	t.Helper()
	frozenReq, err := FreezeModelRequest(*req)
	if err != nil {
		t.Fatal(err)
	}
	reqDigest, err := DigestRequest(frozenReq)
	if err != nil {
		t.Fatal(err)
	}
	toolsDigest, err := DigestToolSpecs(specs)
	if err != nil {
		t.Fatal(err)
	}
	model := ModelRef(frozenReq.Model)
	binding, err := DigestModelStepBinding(model, reqDigest, toolsDigest)
	if err != nil {
		t.Fatal(err)
	}
	cmdID := DeriveModelRequestCommandID(snap.State.RunID, snap.Revision)
	stepID := DeriveModelStepID(snap.State.RunID, cmdID, binding)
	ids := make([]InputID, len(snap.State.PendingInputs))
	for i, in := range snap.State.PendingInputs {
		ids[i] = in.ID
	}
	return PrepareModelRequest{
		StepID: stepID, Model: model, Request: frozenReq,
		RequestDigest: reqDigest, InputIDs: ids, Tools: specs, ToolsDigest: toolsDigest,
	}, cmdID
}

func conformanceRequest(tools ...sdk.ToolDefinition) sdk.Request {
	return sdk.Request{
		Model:    "m-1",
		Messages: []sdk.Message{sdk.UserMessage("hi")},
		Tools:    tools,
	}
}

func conformanceToolDef(name string) sdk.ToolDefinition {
	return sdk.ToolDefinition{Name: name, Parameters: json.RawMessage(`{"type":"object"}`)}
}

func conformanceJSON(raw string) CanonicalJSON { return MustParseCanonicalJSON(raw) }

func makeConformanceSpec(t testing.TB, def sdk.ToolDefinition) ToolSpec {
	t.Helper()
	frozen, err := FreezeToolDefinition(def)
	if err != nil {
		t.Fatal(err)
	}
	d, err := DigestToolDefinition(frozen)
	if err != nil {
		t.Fatal(err)
	}
	return ToolSpec{Ref: ToolRef(def.Name), Definition: frozen, DefinitionDigest: d, Policy: DirectExecution}
}

func makeConformanceBinding(t testing.TB, callID string, spec *ToolSpec) ToolCallBinding {
	t.Helper()
	parsedArgs := conformanceJSON(`{}`)
	bd, err := DigestToolCallBinding(CallID(callID), spec.DefinitionDigest, spec.Policy, parsedArgs)
	if err != nil {
		t.Fatal(err)
	}
	return ToolCallBinding{
		CallID:           CallID(callID),
		ToolRef:          spec.Ref,
		DefinitionDigest: spec.DefinitionDigest,
		BindingDigest:    bd,
		Arguments:        parsedArgs,
		Policy:           spec.Policy,
	}
}

func openedConformanceToolStepID(t testing.TB, res *CommitResult) StepID {
	t.Helper()
	if len(res.Events) < 2 {
		t.Fatalf("events = %d, want ToolStepOpened at index 1", len(res.Events))
	}
	opened, ok := res.Events[1].Fact.(ToolStepOpened)
	if !ok {
		t.Fatalf("event[1] fact = %T, want ToolStepOpened", res.Events[1].Fact)
	}
	return opened.StepID
}

func conformanceModelResultWithCalls(callIDs ...string) ModelResult {
	r := sdk.ModelResult{
		FinishReason: sdk.FinishReasonToolCalls,
		Usage:        sdk.Usage{InputTokens: 10, OutputTokens: 5, TotalTokens: 15},
	}
	for _, id := range callIDs {
		r.ToolCalls = append(r.ToolCalls, sdk.ToolCall{ToolCallID: id, ToolName: "t", Input: `{}`})
	}
	frozen, err := FreezeModelResult(r)
	if err != nil {
		panic(err)
	}
	return frozen
}

func testIdempotentReplay(t *testing.T, newRuntime conformanceFactory) {
	c := newConformanceCase(t, newRuntime)
	res1 := c.mustCommit("cancel-1", 0, "", CancelRun{})
	if res1.Status != CommitAccepted || len(res1.Events) != 1 {
		t.Fatalf("res1 = %+v", res1)
	}
	res2 := c.mustCommit("cancel-1", 0, "", CancelRun{})
	if res2.Status != CommitAlreadyApplied {
		t.Fatalf("status = %v", res2.Status)
	}
	if len(res2.Events) != 1 || res2.Events[0].Digest != res1.Events[0].Digest ||
		res2.Events[0].Revision != res1.Events[0].Revision {
		t.Fatal("replay did not return the original event group")
	}
	if res2.Snapshot.Revision != res1.Snapshot.Revision {
		t.Fatal("replay advanced the revision")
	}
	_, err := c.commit("cancel-1", 0, "", CancelRun{Reason: "other"})
	if !errors.Is(err, ErrCommandConflict) {
		t.Fatalf("err = %v, want ErrCommandConflict", err)
	}
}

func testRevisionAndIndex(t *testing.T, newRuntime conformanceFactory) {
	def := conformanceToolDef("t")
	spec := makeConformanceSpec(t, def)
	c, stepID, grant := preparedConformanceCase(t, newRuntime, []sdk.ToolDefinition{def}, []ToolSpec{spec})

	b := makeConformanceBinding(t, "c1", &spec)
	res := c.mustCommit("complete-1", 2, grant,
		SubmitModelResult{StepID: stepID, Result: conformanceModelResultWithCalls("c1"), Calls: []ToolCallBinding{b}})
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

func testStartGrantLifecycle(t *testing.T, newRuntime conformanceFactory) {
	c, stepID, grant := preparedConformanceCase(t, newRuntime, nil, nil)

	_, err := c.commit("done-x", 2, "", SubmitModelResult{StepID: stepID, Result: ModelResult{}})
	if !errors.Is(err, ErrStaleRuntime) {
		t.Fatalf("grantless completion err = %v, want ErrStaleRuntime", err)
	}
	res := c.mustCommit("start-1", 1, "", StartModelExecution{StepID: stepID})
	if res.Status != CommitAlreadyApplied || res.Grant != "" {
		t.Fatalf("replayed start: %+v", res)
	}
	ok, err := FreezeModelResult(sdk.ModelResult{Text: "ok"})
	if err != nil {
		t.Fatal(err)
	}
	res = c.mustCommit("done-1", 2, grant, SubmitModelResult{StepID: stepID, Result: ok})
	if res.Status != CommitAccepted || res.Snapshot.State.Status != RunCompleted {
		t.Fatalf("completion: %+v", res.Snapshot.State.Status)
	}
}

func testCallLocalRebase(t *testing.T, newRuntime conformanceFactory) {
	defA, defB := conformanceToolDef("a"), conformanceToolDef("b")
	specA := makeConformanceSpec(t, defA)
	specB := makeConformanceSpec(t, defB)
	c, stepID, grant := preparedConformanceCase(t, newRuntime, []sdk.ToolDefinition{defA, defB}, []ToolSpec{specA, specB})

	bA := makeConformanceBinding(t, "cA", &specA)
	bB := makeConformanceBinding(t, "cB", &specB)
	r, err := FreezeModelResult(sdk.ModelResult{
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
		SubmitModelResult{StepID: stepID, Result: r, Calls: []ToolCallBinding{bA, bB}})
	toolStep := openedConformanceToolStepID(t, &res)
	base := res.Snapshot.Revision

	startA := c.mustCommit("start-A", base, "", StartToolCall{StepID: toolStep, CallID: "cA"})
	startB := c.mustCommit("start-B", base, "", StartToolCall{StepID: toolStep, CallID: "cB"})
	if startB.Status != CommitAccepted || startB.Grant == "" {
		t.Fatal("stale-base start of an untouched Pending call must rebase")
	}
	doneA := c.mustCommit("done-A", base, startA.Grant,
		SubmitToolResult{StepID: toolStep, CallID: "cA", Result: ToolExecutionResult{Output: conformanceJSON(`1`)}})
	if doneA.Status != CommitAccepted {
		t.Fatal("owner completion on stale base must rebase")
	}
	_, err = c.commit("start-A2", base, "", StartToolCall{StepID: toolStep, CallID: "cA"})
	if !errors.Is(err, ErrStaleRuntime) {
		t.Fatalf("restart of settled call err = %v, want ErrStaleRuntime", err)
	}
	_ = startB
}

func testPrepareDerivedIdentity(t *testing.T, newRuntime conformanceFactory) {
	c := newConformanceCase(t, newRuntime)
	snap := c.load()
	req := conformanceRequest()
	prep, cmdID := buildConformancePrepareFromSnap(t, &snap, &req, nil)

	if _, err := c.commit("wrong-prepare-id", snap.Revision, "", prep); err == nil {
		t.Fatal("PrepareModelRequest accepted a non-derived CommandID")
	}

	bad := prep
	bad.StepID = "wrong-step"
	_, err := c.commit(cmdID, snap.Revision, "", bad)
	if !errors.Is(err, ErrStaleRuntime) {
		t.Fatalf("bad prepare StepID err = %v, want ErrStaleRuntime", err)
	}
}

func testPrepareIsHardCAS(t *testing.T, newRuntime conformanceFactory) {
	c := newConformanceCase(t, newRuntime)
	snap := c.load()
	req := conformanceRequest()
	prep, cmdID := buildConformancePrepareFromSnap(t, &snap, &req, nil)
	c.mustCommit(cmdID, snap.Revision, "", prep)

	otherReq := conformanceRequest()
	otherReq.System = "different"
	prep2, cmdID2 := buildConformancePrepareFromSnap(t, &snap, &otherReq, nil)
	if cmdID2 != cmdID {
		t.Fatal("same revision must derive the same command id")
	}
	_, err := c.commit(cmdID2, snap.Revision, "", prep2)
	if !errors.Is(err, ErrCommandConflict) {
		t.Fatalf("err = %v, want ErrCommandConflict", err)
	}
	res := c.mustCommit(cmdID, snap.Revision, "", prep)
	if res.Status != CommitAlreadyApplied {
		t.Fatalf("status = %v", res.Status)
	}
}

func testCancelRebasesAndUnknownWins(t *testing.T, newRuntime conformanceFactory) {
	def := conformanceToolDef("t")
	spec := makeConformanceSpec(t, def)
	c, stepID, grant := preparedConformanceCase(t, newRuntime, []sdk.ToolDefinition{def}, []ToolSpec{spec})
	b := makeConformanceBinding(t, "c1", &spec)
	res := c.mustCommit("complete-1", 2, grant,
		SubmitModelResult{StepID: stepID, Result: conformanceModelResultWithCalls("c1"), Calls: []ToolCallBinding{b}})
	toolStep := openedConformanceToolStepID(t, &res)
	startRes := c.mustCommit("start-c1", res.Snapshot.Revision, "", StartToolCall{StepID: toolStep, CallID: "c1"})

	unknown := c.mustCommit("unk-1", startRes.Snapshot.Revision, startRes.Grant,
		SubmitToolFailure{StepID: toolStep, CallID: "c1", Outcome: ToolOutcomeUnknown})
	if unknown.Snapshot.State.Status != RunFailed {
		t.Fatal("unknown did not fail the run")
	}
	_, err := c.commit("cancel-late", 0, "", CancelRun{})
	if !errors.Is(err, ErrRunTerminal) {
		t.Fatalf("late cancel err = %v, want ErrRunTerminal", err)
	}
}

func testCancelOnStaleBase(t *testing.T, newRuntime conformanceFactory) {
	c, _, _ := preparedConformanceCase(t, newRuntime, nil, nil)
	res := c.mustCommit("cancel-1", 0, "", CancelRun{})
	if res.Status != CommitAccepted || res.Snapshot.State.Status != RunStopped {
		t.Fatalf("cancel: %+v", res.Snapshot.State.Status)
	}
}

func testReplayFoldMatchesState(t *testing.T, newRuntime conformanceFactory) {
	def := conformanceToolDef("t")
	spec := makeConformanceSpec(t, def)
	c, stepID, grant := preparedConformanceCase(t, newRuntime, []sdk.ToolDefinition{def}, []ToolSpec{spec})
	b := makeConformanceBinding(t, "c1", &spec)
	res := c.mustCommit("complete-1", 2, grant,
		SubmitModelResult{StepID: stepID, Result: conformanceModelResultWithCalls("c1"), Calls: []ToolCallBinding{b}})
	toolStep := openedConformanceToolStepID(t, &res)
	sRes := c.mustCommit("start-c1", res.Snapshot.Revision, "", StartToolCall{StepID: toolStep, CallID: "c1"})
	c.mustCommit("done-c1", sRes.Snapshot.Revision, sRes.Grant,
		SubmitToolResult{StepID: toolStep, CallID: "c1", Result: ToolExecutionResult{Output: conformanceJSON(`"ok"`)}})

	folded, lastRev, err := FoldEvents(c.initial, c.events)
	if err != nil {
		t.Fatalf("FoldEvents: %v", err)
	}
	live := c.load()
	a, _ := json.Marshal(conformanceStateComparable(&live.State))
	bts, _ := json.Marshal(conformanceStateComparable(&folded))
	if !bytes.Equal(a, bts) {
		t.Fatalf("replay diverged:\n live   %s\n replay %s", a, bts)
	}
	if live.Revision != lastRev {
		t.Fatalf("snapshot revision %d != last event revision %d", live.Revision, lastRev)
	}
}

func testAcceptInputByInputID(t *testing.T, newRuntime conformanceFactory) {
	c := newConformanceCase(t, newRuntime)
	in := AgentInput{ID: "in-9", Payload: conformanceJSON(`{"t":"x"}`)}
	id := DeriveInputCommandID("run-1", in.ID)
	res1 := c.mustCommit(id, 0, "", NextStep(in))
	if res1.Status != CommitAccepted {
		t.Fatal("first accept rejected")
	}
	res2 := c.mustCommit(id, 0, "", NextStep(in))
	if res2.Status != CommitAlreadyApplied {
		t.Fatalf("status = %v", res2.Status)
	}
	_, err := c.commit(id, 0, "", NextStep(AgentInput{ID: "in-9", Payload: conformanceJSON(`{"t":"y"}`)}))
	if !errors.Is(err, ErrCommandConflict) {
		t.Fatalf("err = %v, want ErrCommandConflict", err)
	}
}

func testDerivedCommandIDEnforced(t *testing.T, newRuntime conformanceFactory) {
	c := newConformanceCase(t, newRuntime)
	_, err := c.commit("random-id", 0, "", NextStep(AgentInput{ID: "in-1", Payload: conformanceJSON(`1`)}))
	if err == nil {
		t.Fatal("AcceptInput with non-derived CommandID accepted")
	}
	_, err = c.commit("random-id-2", 0, "", ApproveToolCall{StepID: "s", CallID: "c", ResponseID: "r"})
	if err == nil {
		t.Fatal("ApproveToolCall with non-derived CommandID accepted")
	}
}

func conformanceStateComparable(s *MachineState) map[string]any {
	m := map[string]any{
		"runId": s.RunID, "status": s.Status,
		"modelSteps": s.ModelSteps, "lastClosedStep": s.LastClosedStep,
		"usage": s.Usage, "pendingInputs": s.PendingInputs,
		"lastModelResult": s.LastModelResult, "result": s.Result,
	}
	switch cur := s.Current.(type) {
	case ModelStep:
		m["modelStep"] = cur
	case ToolStep:
		m["toolStep"] = cur
	}
	return m
}

func TestMemoryRuntimeConformance(t *testing.T) {
	runConformance(t, func(t testing.TB, initial MachineState) Runtime {
		t.Helper()
		return NewMemoryRuntime(initial)
	})
}
