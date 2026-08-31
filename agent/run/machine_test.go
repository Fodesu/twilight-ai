package run

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/memohai/twilight/sdk"
)

// --- helpers ---

const testModel ModelRef = "m-1"

func testConfig() RunConfig {
	return RunConfig{Model: testModel}
}

func cj(raw string) CanonicalJSON { return MustParseCanonicalJSON(raw) }

func newRun(t *testing.T, _ RunConfig) MachineState {
	t.Helper()
	s, err := InitializeRun("run-1")
	if err != nil {
		t.Fatal(err)
	}
	return fold(t, s, mustDecide(t, s, AcceptInput{Input: AgentInput{ID: "seed", Payload: cj(`{"q":"hi"}`)}}))
}

func mustDecide(t *testing.T, s MachineState, c AgentCommand) []Fact {
	t.Helper()
	facts, err := Decide(s, c)
	if err != nil {
		t.Fatalf("Decide(%T): %v", c, err)
	}
	return facts
}

func fold(t *testing.T, s MachineState, facts []Fact) MachineState {
	t.Helper()
	for _, f := range facts {
		var err error
		s, err = Evolve(s, f)
		if err != nil {
			t.Fatalf("Evolve(%T): %v", f, err)
		}
	}
	return s
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

func buildPrepare(t *testing.T, s MachineState, req sdk.Request, specs []ToolSpec) (PrepareModelRequest, CommandID) {
	t.Helper()
	frozenReq, err := FreezeModelRequest(req)
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
	cmdID := DeriveModelRequestCommandID(s.RunID, 0)
	stepID := DeriveModelStepID(s.RunID, cmdID, binding)
	ids := make([]InputID, len(s.PendingInputs))
	for i, in := range s.PendingInputs {
		ids[i] = in.ID
	}
	return PrepareModelRequest{
		StepID:        stepID,
		Model:         model,
		Request:       frozenReq,
		RequestDigest: reqDigest,
		InputIDs:      ids,
		Tools:         specs,
		ToolsDigest:   toolsDigest,
	}, cmdID
}

func makeSpec(t *testing.T, def sdk.ToolDefinition, policy ResponsePolicy) ToolSpec {
	t.Helper()
	frozen, err := FreezeToolDefinition(def)
	if err != nil {
		t.Fatal(err)
	}
	d, err := DigestToolDefinition(frozen)
	if err != nil {
		t.Fatal(err)
	}
	return ToolSpec{Ref: ToolRef(def.Name), Definition: frozen, DefinitionDigest: d, Policy: policy}
}

func responseDecisionDigest(t *testing.T, kind ResponseKind, decision ResponseDecision, reason string) Digest {
	t.Helper()
	d, err := DigestToolResponseDecision(kind, decision, reason)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func responsePayloadDigest(t *testing.T, payload CanonicalJSON) Digest {
	t.Helper()
	d, err := DigestToolResponsePayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func makeBinding(t *testing.T, callID string, spec ToolSpec, args string) ToolCallBinding {
	t.Helper()
	parsedArgs := cj(args)
	bd, err := digestToolCallBinding(CallID(callID), spec.DefinitionDigest, spec.Policy, parsedArgs)
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

func modelResultWithCalls(callIDs ...string) ModelResult {
	return modelResultWithNamedCalls("t", `{}`, callIDs...)
}

// modelResultWithNamedCalls builds a result whose tool calls carry the given
// tool name and argument text — bindings must cross-check against these.
func modelResultWithNamedCalls(toolName, args string, callIDs ...string) ModelResult {
	r := sdk.ModelResult{
		Text:         "",
		FinishReason: sdk.FinishReasonToolCalls,
		Usage:        sdk.Usage{InputTokens: 10, OutputTokens: 5, TotalTokens: 15},
	}
	for _, id := range callIDs {
		r.ToolCalls = append(r.ToolCalls, sdk.ToolCall{ToolCallID: id, ToolName: toolName, Input: args})
	}
	frozen, err := FreezeModelResult(r)
	if err != nil {
		panic(err)
	}
	return frozen
}

// advance runs prepare+start and returns the state in Executing plus stepID.
func advanceToExecuting(t *testing.T, s MachineState, req sdk.Request, specs []ToolSpec) (MachineState, StepID) {
	t.Helper()
	prep, _ := buildPrepare(t, s, req, specs)
	s = fold(t, s, mustDecide(t, s, prep))
	s = fold(t, s, mustDecide(t, s, StartModelExecution{StepID: prep.StepID}))
	return s, prep.StepID
}

// --- tests ---

func TestInitializeRunIsMinimalAndLegacyConfigIsNotState(t *testing.T) {
	s, err := InitializeRun("r")
	if err != nil {
		t.Fatal(err)
	}
	if s.RunID != "r" || s.Status != RunActive || len(s.PendingInputs) != 0 {
		t.Fatalf("initial state = %+v", s)
	}
	legacy, err := Initialize("r", RunConfig{Model: "m"}, NextRun(AgentInput{ID: "i"}))
	if err != nil {
		t.Fatal(err)
	}
	if legacy.RunID != "r" || len(legacy.PendingInputs) != 1 {
		t.Fatalf("legacy initial state = %+v", legacy)
	}
	if _, err := Initialize("r", RunConfig{Model: "m", ModelStepLimit: -1}, NextRun(AgentInput{ID: "i"})); err == nil {
		t.Fatal("negative step limit accepted")
	}
}

func TestNextOnFreshRunNeedsModelRequest(t *testing.T) {
	s := newRun(t, testConfig())
	eff, err := Next(s)
	if err != nil {
		t.Fatal(err)
	}
	need, ok := eff.(NeedModelRequest)
	if !ok {
		t.Fatalf("effect = %T, want NeedModelRequest", eff)
	}
	if len(need.Hint.Inputs) != 1 || need.Hint.Inputs[0].ID != "seed" {
		t.Fatalf("hint inputs = %+v", need.Hint.Inputs)
	}
}

func TestPrepareConsumesInputsAndCounts(t *testing.T) {
	s := newRun(t, testConfig())
	prep, _ := buildPrepare(t, s, testRequest(), nil)
	facts := mustDecide(t, s, prep)
	if len(facts) != 1 {
		t.Fatalf("facts = %d, want 1", len(facts))
	}
	s = fold(t, s, facts)
	if len(s.PendingInputs) != 0 {
		t.Fatal("pending inputs not consumed")
	}
	if s.ModelSteps != 1 {
		t.Fatalf("ModelSteps = %d", s.ModelSteps)
	}
	if ms, ok := s.Current.(ModelStep); !ok || ms.Status != ModelPrepared {
		t.Fatalf("current = %#v", s.Current)
	}
}

func TestPrepareRejectsIncompleteInputIDs(t *testing.T) {
	s := newRun(t, testConfig())
	prep, _ := buildPrepare(t, s, testRequest(), nil)
	prep.InputIDs = nil
	if _, err := Decide(s, prep); err == nil {
		t.Fatal("prepare with missing InputIDs accepted")
	}
}

func TestModelCompleteNoToolsEndsRun(t *testing.T) {
	s := newRun(t, testConfig())
	s, stepID := advanceToExecuting(t, s, testRequest(), nil)
	result, err := FreezeModelResult(sdk.ModelResult{Text: "done", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 7}})
	if err != nil {
		t.Fatal(err)
	}
	facts := mustDecide(t, s, SubmitModelResult{StepID: stepID, Result: result})
	if len(facts) != 2 {
		t.Fatalf("facts = %d, want [completed, ended]", len(facts))
	}
	if _, ok := facts[1].(RunEnded); !ok {
		t.Fatalf("facts[1] = %T", facts[1])
	}
	s = fold(t, s, facts)
	if s.Status != RunCompleted {
		t.Fatalf("status = %v", s.Status)
	}
	if s.Result == nil || s.Result.Model == nil || s.Result.Model.Text != "done" {
		t.Fatalf("result = %+v", s.Result)
	}
	if s.Result.Usage.TotalTokens != 7 {
		t.Fatalf("usage not accumulated into result: %+v", s.Result.Usage)
	}
}

func TestModelCompleteWithToolsOpensToolStep(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, DirectExecution)
	s := newRun(t, testConfig())
	s, stepID := advanceToExecuting(t, s, testRequest(def), []ToolSpec{spec})

	b := makeBinding(t, "c1", spec, `{"x":1}`)
	facts := mustDecide(t, s, SubmitModelResult{StepID: stepID, Result: modelResultWithNamedCalls("t", `{"x":1}`, "c1"), Calls: []ToolCallBinding{b}})
	if len(facts) != 2 {
		t.Fatalf("facts = %d, want [completed, opened]", len(facts))
	}
	opened, ok := facts[1].(ToolStepOpened)
	if !ok {
		t.Fatalf("facts[1] = %T", facts[1])
	}
	if opened.Source != stepID {
		t.Fatal("tool step source mismatch")
	}
	s = fold(t, s, facts)
	ts, ok := s.Current.(ToolStep)
	if !ok {
		t.Fatalf("current = %T", s.Current)
	}
	if len(ts.Calls) != 1 || ts.Calls[0].Status != ToolPending {
		t.Fatalf("calls = %+v", ts.Calls)
	}
	if err := ValidateToolCallState(ts.Calls[0]); err != nil {
		t.Fatal(err)
	}
}

func TestApprovalCallOpensWaitingWithDerivedResponse(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, ApprovalRequired)
	s := newRun(t, testConfig())
	s, stepID := advanceToExecuting(t, s, testRequest(def), []ToolSpec{spec})

	b := makeBinding(t, "c1", spec, `{}`)
	facts := mustDecide(t, s, SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []ToolCallBinding{b}})
	opened := facts[1].(ToolStepOpened)
	if opened.Calls[0].Response == nil {
		t.Fatal("approval call has no derived ResponseRequest")
	}
	want := DeriveResponseID(s.RunID, opened.StepID, "c1", ResponseApproval)
	if opened.Calls[0].Response.ID != want {
		t.Fatal("ResponseID not derived per spec")
	}
	s = fold(t, s, facts)
	ts := s.Current.(ToolStep)
	if ts.Calls[0].Status != ToolWaiting {
		t.Fatalf("status = %v, want Waiting", ts.Calls[0].Status)
	}

	// Next must surface WaitForResponse with the routable request.
	eff, err := Next(s)
	if err != nil {
		t.Fatal(err)
	}
	wait, ok := eff.(WaitForResponse)
	if !ok || len(wait.Requests) != 1 || wait.Requests[0].ID != want {
		t.Fatalf("effect = %#v", eff)
	}

	if _, err := Decide(s, ApproveToolCall{StepID: opened.StepID, CallID: "c1", ResponseID: want, ResponseDigest: "sha256:bad"}); err == nil {
		t.Fatal("approval with bad response digest accepted")
	}

	// Approve -> Pending; then start -> execute path.
	s = fold(t, s, mustDecide(t, s, ApproveToolCall{StepID: opened.StepID, CallID: "c1", ResponseID: want,
		ResponseDigest: responseDecisionDigest(t, ResponseApproval, ResponseDecisionApproved, "")}))
	if s.Current.(ToolStep).Calls[0].Status != ToolPending {
		t.Fatal("approved call is not Pending")
	}
}

func TestRejectRecordsPermissionDenied(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, ApprovalRequired)
	s := newRun(t, testConfig())
	s, stepID := advanceToExecuting(t, s, testRequest(def), []ToolSpec{spec})
	b := makeBinding(t, "c1", spec, `{}`)
	facts := mustDecide(t, s, SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []ToolCallBinding{b}})
	opened := facts[1].(ToolStepOpened)
	s = fold(t, s, facts)
	respID := opened.Calls[0].Response.ID

	facts = mustDecide(t, s, RejectToolCall{StepID: opened.StepID, CallID: "c1", ResponseID: respID,
		ResponseDigest: responseDecisionDigest(t, ResponseApproval, ResponseDecisionRejected, "no"), Reason: "no"})
	// Single call: the Known failure implicitly closes the ToolStep.
	if len(facts) != 1 {
		t.Fatalf("facts = %d, want [failed]", len(facts))
	}
	failed := facts[0].(ToolCallFailed)
	if failed.Failure.Class != FailurePermissionDenied || failed.Outcome != ToolOutcomeKnown {
		t.Fatalf("failed = %+v", failed)
	}
	s = fold(t, s, facts)
	if s.Current != nil || s.Status != RunActive {
		t.Fatal("run should continue with no current step")
	}
}

func TestExternalResponseRequiresPayloadDigest(t *testing.T) {
	def := testToolDef("ask")
	spec := makeSpec(t, def, ExternalResponse)
	s := newRun(t, testConfig())
	s, stepID := advanceToExecuting(t, s, testRequest(def), []ToolSpec{spec})
	b := makeBinding(t, "c1", spec, `{}`)
	facts := mustDecide(t, s, SubmitModelResult{StepID: stepID, Result: modelResultWithNamedCalls("ask", `{}`, "c1"), Calls: []ToolCallBinding{b}})
	opened := facts[1].(ToolStepOpened)
	s = fold(t, s, facts)
	respID := opened.Calls[0].Response.ID
	payload := cj(`{"answer":"ok"}`)
	if _, err := Decide(s, SubmitToolResponse{StepID: opened.StepID, CallID: "c1", ResponseID: respID, ResponseDigest: "sha256:bad", Payload: payload}); err == nil {
		t.Fatal("external response with bad payload digest accepted")
	}
	facts = mustDecide(t, s, SubmitToolResponse{StepID: opened.StepID, CallID: "c1", ResponseID: respID,
		ResponseDigest: responsePayloadDigest(t, payload), Payload: payload})
	if len(facts) != 1 {
		t.Fatalf("facts = %d, want [answered]", len(facts))
	}
}

func TestUnknownFailureEndsRun(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, DirectExecution)
	s := newRun(t, testConfig())
	s, stepID := advanceToExecuting(t, s, testRequest(def), []ToolSpec{spec})
	b := makeBinding(t, "c1", spec, `{}`)
	facts := mustDecide(t, s, SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []ToolCallBinding{b}})
	opened := facts[1].(ToolStepOpened)
	s = fold(t, s, facts)
	s = fold(t, s, mustDecide(t, s, StartToolCall{StepID: opened.StepID, CallID: "c1"}))

	facts = mustDecide(t, s, SubmitToolFailure{StepID: opened.StepID, CallID: "c1", Outcome: ToolOutcomeUnknown})
	if len(facts) != 2 {
		t.Fatalf("facts = %d, want [failed, ended]", len(facts))
	}
	ended := facts[1].(RunEnded)
	failed, ok := ended.End.(RunFailedEnd)
	if !ok || failed.Reason != ReasonEffectUnknown || failed.Failure.CallID != "c1" {
		t.Fatalf("ended = %+v", ended.End)
	}
	s = fold(t, s, facts)
	if s.Status != RunFailed {
		t.Fatal("run not failed")
	}
	// Terminal absorbs: further commands rejected.
	if _, err := Decide(s, CancelRun{}); !errors.Is(err, ErrRunTerminal) {
		t.Fatalf("err = %v, want ErrRunTerminal", err)
	}
}

func TestParallelWaitingDoesNotBlockPending(t *testing.T) {
	defA, defB := testToolDef("a"), testToolDef("b")
	specA := makeSpec(t, defA, ApprovalRequired)
	specB := makeSpec(t, defB, DirectExecution)
	s := newRun(t, testConfig())
	s, stepID := advanceToExecuting(t, s, testRequest(defA, defB), []ToolSpec{specA, specB})

	bA := makeBinding(t, "cA", specA, `{}`)
	bB := makeBinding(t, "cB", specB, `{}`)
	r, err := FreezeModelResult(sdk.ModelResult{
		FinishReason: sdk.FinishReasonToolCalls,
		Usage:        sdk.Usage{TotalTokens: 15},
		ToolCalls: []sdk.ToolCall{
			{ToolCallID: "cA", ToolName: "a", Input: `{}`},
			{ToolCallID: "cB", ToolName: "b", Input: `{}`},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	facts := mustDecide(t, s, SubmitModelResult{StepID: stepID, Result: r, Calls: []ToolCallBinding{bA, bB}})
	opened := facts[1].(ToolStepOpened)
	s = fold(t, s, facts)

	eff, err := Next(s)
	if err != nil {
		t.Fatal(err)
	}
	start, ok := eff.(StartToolCalls)
	if !ok || len(start.CallIDs) != 1 || start.CallIDs[0] != "cB" {
		t.Fatalf("effect = %#v, want StartToolCalls[cB]", eff)
	}

	// Complete B; step must stay open because A is Waiting.
	s = fold(t, s, mustDecide(t, s, StartToolCall{StepID: opened.StepID, CallID: "cB"}))
	facts = mustDecide(t, s, SubmitToolResult{StepID: opened.StepID, CallID: "cB", Result: ToolExecutionResult{Output: cj(`"ok"`)}})
	if len(facts) != 1 {
		t.Fatalf("facts = %d, step must not close with A waiting", len(facts))
	}
	s = fold(t, s, facts)

	// Answer A via approval; approving moves to Pending, then completing it
	// implicitly closes the step.
	respID := opened.Calls[0].Response.ID
	s = fold(t, s, mustDecide(t, s, ApproveToolCall{StepID: opened.StepID, CallID: "cA", ResponseID: respID,
		ResponseDigest: responseDecisionDigest(t, ResponseApproval, ResponseDecisionApproved, "")}))
	s = fold(t, s, mustDecide(t, s, StartToolCall{StepID: opened.StepID, CallID: "cA"}))
	facts = mustDecide(t, s, SubmitToolResult{StepID: opened.StepID, CallID: "cA", Result: ToolExecutionResult{Output: cj(`"done"`)}})
	if len(facts) != 1 {
		t.Fatalf("facts = %d, want [completed]", len(facts))
	}
	s = fold(t, s, facts)
	if s.Current != nil {
		t.Fatal("tool step should be closed")
	}
}

func TestRejectModelResultDispositionRetriesThenFails(t *testing.T) {
	s := newRun(t, testConfig())
	s, stepID := advanceToExecuting(t, s, testRequest(), nil)

	usage := Usage{TotalTokens: 3}
	// Reject 1: back to Prepared.
	facts := mustDecide(t, s, RejectModelResult{StepID: stepID, Usage: usage, Failure: StepFailure{Class: FailureMalformedModel}})
	if len(facts) != 1 {
		t.Fatalf("facts = %d", len(facts))
	}
	s = fold(t, s, facts)
	if ms := s.Current.(ModelStep); ms.Status != ModelPrepared || ms.Rejects != 1 {
		t.Fatalf("model step = %+v", ms)
	}
	if s.Usage.TotalTokens != 3 {
		t.Fatal("usage not accumulated on reject")
	}

	// Start again, reject 2: host policy still chooses retry.
	s = fold(t, s, mustDecide(t, s, StartModelExecution{StepID: stepID}))
	s = fold(t, s, mustDecide(t, s, RejectModelResult{StepID: stepID, Usage: usage, Failure: StepFailure{Class: FailureMalformedModel}}))
	if ms := s.Current.(ModelStep); ms.Rejects != 2 {
		t.Fatalf("rejects = %d", ms.Rejects)
	}

	// Third reject: host policy chooses fail-run disposition.
	s = fold(t, s, mustDecide(t, s, StartModelExecution{StepID: stepID}))
	facts = mustDecide(t, s, RejectModelResult{StepID: stepID, Usage: usage, Failure: StepFailure{Class: FailureMalformedModel}, Disposition: ModelRejectFailRun})
	if len(facts) != 2 {
		t.Fatalf("facts = %d, want [rejected, ended]", len(facts))
	}
	s = fold(t, s, facts)
	if s.Status != RunFailed || s.Result.Reason != ReasonMalformedModel {
		t.Fatalf("result = %+v", s.Result)
	}
	if s.Usage.TotalTokens != 9 {
		t.Fatalf("usage = %d, want 9", s.Usage.TotalTokens)
	}
}

func TestModelRecoveryKeepsFrozenRequestAndCounts(t *testing.T) {
	s := newRun(t, testConfig())
	s, stepID := advanceToExecuting(t, s, testRequest(), nil)
	s = fold(t, s, mustDecide(t, s, RecoverModelExecution{StepID: stepID}))
	ms := s.Current.(ModelStep)
	if ms.Status != ModelPrepared {
		t.Fatal("recovered step not Prepared")
	}
	if s.ModelSteps != 1 {
		t.Fatal("recovery must not recount model steps")
	}
	if s.Usage.TotalTokens != 0 {
		t.Fatal("recovery must not change usage")
	}
}

func TestStopRunProducesStepLimit(t *testing.T) {
	s := newRun(t, testConfig())
	facts := mustDecide(t, s, StopRun{Reason: ReasonStepLimit})
	if len(facts) != 1 {
		t.Fatalf("facts = %d, want [ended]", len(facts))
	}
	ended := facts[0].(RunEnded)
	stopped, ok := ended.End.(RunStoppedEnd)
	if !ok || stopped.Reason != ReasonStepLimit {
		t.Fatalf("ended = %+v", ended.End)
	}
	if _, err := Decide(s, StopRun{Reason: ReasonCancelled}); err == nil {
		t.Fatal("StopRun accepted cancellation reason")
	}
}

func TestStopRunRequiresSafeBoundary(t *testing.T) {
	s := newRun(t, testConfig())
	s, stepID := advanceToExecuting(t, s, testRequest(), nil)
	if _, err := Decide(s, StopRun{Reason: ReasonStepLimit}); err == nil {
		t.Fatalf("StopRun accepted while ModelStep %q was executing", stepID)
	}
}

func TestCancelProducesRunStopped(t *testing.T) {
	s := newRun(t, testConfig())
	facts := mustDecide(t, s, CancelRun{})
	ended := facts[0].(RunEnded)
	stopped, ok := ended.End.(RunStoppedEnd)
	if !ok || stopped.Reason != ReasonCancelled {
		t.Fatalf("ended = %+v", ended.End)
	}
}

func TestAcceptInputIdempotentPerID(t *testing.T) {
	s := newRun(t, testConfig())
	facts := mustDecide(t, s, NextStep(AgentInput{ID: "in-2", Payload: cj(`1`)}))
	s = fold(t, s, facts)
	if len(s.PendingInputs) != 2 {
		t.Fatalf("pending = %d", len(s.PendingInputs))
	}
	// Same fact folded twice appends once.
	s = fold(t, s, facts)
	if len(s.PendingInputs) != 2 {
		t.Fatal("InputAccepted fold is not idempotent per InputID")
	}
	// AcceptInput rejected while a step is current.
	prep, _ := buildPrepare(t, s, testRequest(), nil)
	s = fold(t, s, mustDecide(t, s, prep))
	if _, err := Decide(s, NextStep(AgentInput{ID: "in-3"})); err == nil {
		t.Fatal("AcceptInput accepted with a current step")
	}
}

func TestAcceptInputRejectsSeedDuplicateID(t *testing.T) {
	s := newRun(t, testConfig())
	_, err := Decide(s, NextStep(AgentInput{ID: "seed", Payload: cj(`{"q":"other"}`)}))
	if !errors.Is(err, ErrCommandConflict) {
		t.Fatalf("duplicate seed input err = %v, want ErrCommandConflict", err)
	}
}

func TestSubmitModelResultRequiresCanonicalToolInput(t *testing.T) {
	if _, err := ParseCanonicalJSON([]byte(`{"path":`)); err == nil {
		t.Fatal("constructed non-canonical model tool input")
	}
}

func TestEvolvePreparedRequiresCompleteOrderedPendingInputs(t *testing.T) {
	minimal, err := InitializeRun("run-1")
	if err != nil {
		t.Fatal(err)
	}
	withInputs := func(ids ...InputID) MachineState {
		t.Helper()
		s := minimal
		for _, id := range ids {
			var foldErr error
			s, foldErr = Evolve(s, InputAccepted{Input: AgentInput{ID: id, Payload: cj(`null`)}})
			if foldErr != nil {
				t.Fatal(foldErr)
			}
		}
		return s
	}
	prepared := func(ids ...InputID) ModelStepPrepared {
		request := ModelRequest{Model: string(testModel)}
		requestDigest, err := DigestRequest(request)
		if err != nil {
			t.Fatal(err)
		}
		toolsDigest, err := DigestToolSpecs(nil)
		if err != nil {
			t.Fatal(err)
		}
		binding, err := DigestModelStepBinding(testModel, requestDigest, toolsDigest)
		if err != nil {
			t.Fatal(err)
		}
		return ModelStepPrepared{StepID: "step-1", Model: testModel, Request: request, RequestDigest: requestDigest, ToolsDigest: toolsDigest, BindingDigest: binding, InputIDs: ids}
	}

	t.Run("nonexistent input", func(t *testing.T) {
		s := withInputs("in-1")
		if _, err := Evolve(s, prepared("missing")); err == nil {
			t.Fatal("ModelStepPrepared consuming a nonexistent input folded")
		}
	})
	t.Run("length mismatch", func(t *testing.T) {
		s := withInputs("in-1", "in-2")
		if _, err := Evolve(s, prepared("in-1")); err == nil {
			t.Fatal("ModelStepPrepared consuming only a pending-input prefix folded")
		}
	})
	t.Run("order mismatch", func(t *testing.T) {
		s := withInputs("in-1", "in-2")
		if _, err := Evolve(s, prepared("in-2", "in-1")); err == nil {
			t.Fatal("ModelStepPrepared consuming pending inputs out of order folded")
		}
	})
	t.Run("complete ordered IDs", func(t *testing.T) {
		s := withInputs("in-1", "in-2")
		next, err := Evolve(s, prepared("in-1", "in-2"))
		if err != nil {
			t.Fatal(err)
		}
		if next.Current == nil || len(next.PendingInputs) != 0 {
			t.Fatalf("prepared state = %+v", next)
		}
	})
}

func TestEvolveRejectsModelPrepareOverCurrentStep(t *testing.T) {
	s := newRun(t, testConfig())
	s, _ = advanceToExecuting(t, s, testRequest(), nil)
	_, err := Evolve(s, ModelStepPrepared{
		StepID:        "other",
		Model:         testModel,
		Request:       ModelRequest{Model: string(testModel)},
		RequestDigest: "sha256:req",
		ToolsDigest:   "sha256:tools",
		BindingDigest: "sha256:binding",
	})
	if err == nil {
		t.Fatal("Evolve accepted ModelStepPrepared over an existing step")
	}
}

func TestReplayEquivalence(t *testing.T) {
	// state = fold(Evolve, initial, events): run a full happy path, capture
	// all facts, refold from initial, and compare canonical serializations.
	def := testToolDef("t")
	spec := makeSpec(t, def, DirectExecution)
	initial := newRun(t, testConfig())
	var log []Fact

	s := initial
	step := func(c AgentCommand) {
		facts := mustDecide(t, s, c)
		log = append(log, facts...)
		s = fold(t, s, facts)
	}
	prep, _ := buildPrepare(t, s, testRequest(def), []ToolSpec{spec})
	step(prep)
	step(StartModelExecution{StepID: prep.StepID})
	b := makeBinding(t, "c1", spec, `{}`)
	step(SubmitModelResult{StepID: prep.StepID, Result: modelResultWithCalls("c1"), Calls: []ToolCallBinding{b}})
	ts := s.Current.(ToolStep)
	step(StartToolCall{StepID: ts.RefValue.ID, CallID: "c1"})
	step(SubmitToolResult{StepID: ts.RefValue.ID, CallID: "c1", Result: ToolExecutionResult{Output: cj(`"ok"`)}})

	replayed := fold(t, initial, log)
	a, err := json.Marshal(stateSnapshotForTest(s))
	if err != nil {
		t.Fatal(err)
	}
	bts, err := json.Marshal(stateSnapshotForTest(replayed))
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != string(bts) {
		t.Fatalf("replay diverged:\n live   %s\n replay %s", a, bts)
	}
}

// stateSnapshotForTest flattens MachineState including the unexported-ish
// Current step for comparison.
func stateSnapshotForTest(s MachineState) map[string]any {
	m := map[string]any{
		"runId": s.RunID, "status": s.Status, "modelSteps": s.ModelSteps,
		"usage": s.Usage, "pending": s.PendingInputs, "result": s.Result,
	}
	switch cur := s.Current.(type) {
	case ModelStep:
		m["model"] = cur
	case ToolStep:
		m["tool"] = cur
	}
	return m
}
