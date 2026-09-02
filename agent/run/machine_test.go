package run

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/memohai/twilight/sdk"
)

// --- helpers ---

const testModel ModelRef = "m-1"

func cj(raw string) CanonicalJSON { return MustParseCanonicalJSON(raw) }

func newRun(t *testing.T) MachineState {
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

func TestInitializeRunIsMinimal(t *testing.T) {
	s, err := InitializeRun("r")
	if err != nil {
		t.Fatal(err)
	}
	if s.RunID != "r" || s.Status != RunActive || !atOpen(s.Current) || len(s.PendingInputs) != 0 {
		t.Fatalf("initial state = %+v", s)
	}
}

func TestNextOnFreshRunNeedsModelRequest(t *testing.T) {
	s := newRun(t)
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
	s := newRun(t)
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
	s := newRun(t)
	prep, _ := buildPrepare(t, s, testRequest(), nil)
	prep.InputIDs = nil
	if _, err := Decide(s, prep); err == nil {
		t.Fatal("prepare with missing InputIDs accepted")
	}
}

func TestModelCompleteWithToolsOpensToolStep(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, DirectExecution)
	s := newRun(t)
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

func TestExternalResponseRequiresPayloadDigest(t *testing.T) {
	def := testToolDef("ask")
	spec := makeSpec(t, def, ExternalResponse)
	s := newRun(t)
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

	s = newRun(t)
	s, stepID = advanceToExecuting(t, s, testRequest(def), []ToolSpec{spec})
	b = makeBinding(t, "c1", spec, `{}`)
	facts = mustDecide(t, s, SubmitModelResult{StepID: stepID, Result: modelResultWithNamedCalls("ask", `{}`, "c1"), Calls: []ToolCallBinding{b}})
	opened = facts[1].(ToolStepOpened)
	s = fold(t, s, facts)
	respID = opened.Calls[0].Response.ID
	facts, err := Decide(s, RejectToolCall{StepID: opened.StepID, CallID: "c1", ResponseID: respID,
		ResponseDigest: responseDecisionDigest(t, ResponseExternal, ResponseDecisionRejected, "user dismissed"), Reason: "user dismissed"})
	if err != nil {
		t.Fatal(err)
	}
	failed := facts[0].(ToolCallFailed)
	if failed.Failure.Class != FailureResponseRejected || failed.Outcome != ToolOutcomeKnown {
		t.Fatalf("failed = %+v", failed)
	}
	s = fold(t, s, facts)
	if s.Status != RunActive || !atOpen(s.Current) {
		t.Fatal("run should continue after rejecting the external response")
	}
}

func TestToolSchedulingFrozenOnToolStepOpened(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, DirectExecution)
	s := newRun(t)
	s, stepID := advanceToExecuting(t, s, testRequest(def), []ToolSpec{spec})
	b := makeBinding(t, "c1", spec, `{}`)
	facts := mustDecide(t, s, SubmitModelResult{
		StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []ToolCallBinding{b},
		Scheduling: ToolScheduling{Mode: ToolScheduleSequential, MaxParallel: 1},
	})
	s = fold(t, s, facts)
	ts := s.Current.(ToolStep)
	if ts.Scheduling.Mode != ToolScheduleSequential || ts.Scheduling.MaxParallel != 1 {
		t.Fatalf("scheduling = %+v", ts.Scheduling)
	}
}

func TestToolSchedulingRejectsUnknownMode(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, DirectExecution)
	s := newRun(t)
	s, stepID := advanceToExecuting(t, s, testRequest(def), []ToolSpec{spec})
	b := makeBinding(t, "c1", spec, `{}`)
	_, err := Decide(s, SubmitModelResult{
		StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []ToolCallBinding{b},
		Scheduling: ToolScheduling{Mode: "round-robin"},
	})
	if err == nil {
		t.Fatal("unknown scheduling mode accepted")
	}
}

func TestParallelWaitingDoesNotBlockPending(t *testing.T) {
	defA, defB := testToolDef("a"), testToolDef("b")
	specA := makeSpec(t, defA, ApprovalRequired)
	specB := makeSpec(t, defB, DirectExecution)
	s := newRun(t)
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

	eff, err = Next(s)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := eff.(Idle); !ok {
		t.Fatalf("effect after B completed = %#v, want Idle", eff)
	}
	if reqs := WaitingCalls(s); len(reqs) != 1 || reqs[0].CallID != "cA" {
		t.Fatalf("WaitingCalls = %#v", WaitingCalls(s))
	}

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
	if !atOpen(s.Current) {
		t.Fatal("tool step should be closed")
	}
}

func TestUnknownToolFailureSettlesOnlyThatCall(t *testing.T) {
	defA, defB := testToolDef("a"), testToolDef("b")
	specA := makeSpec(t, defA, DirectExecution)
	specB := makeSpec(t, defB, DirectExecution)
	s := newRun(t)
	s, stepID := advanceToExecuting(t, s, testRequest(defA, defB), []ToolSpec{specA, specB})

	bA := makeBinding(t, "cA", specA, `{}`)
	bB := makeBinding(t, "cB", specB, `{}`)
	r, err := FreezeModelResult(sdk.ModelResult{
		FinishReason: sdk.FinishReasonToolCalls,
		Usage:        sdk.Usage{TotalTokens: 2},
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
	s = fold(t, s, mustDecide(t, s, StartToolCall{StepID: opened.StepID, CallID: "cA"}))
	s = fold(t, s, mustDecide(t, s, StartToolCall{StepID: opened.StepID, CallID: "cB"}))

	facts = mustDecide(t, s, SubmitToolFailure{
		StepID:  opened.StepID,
		CallID:  "cA",
		Failure: ToolFailure{Class: FailureEffectUnknown, Message: "lost"},
		Outcome: ToolOutcomeUnknown,
	})
	if len(facts) != 1 {
		t.Fatalf("facts = %d, want [ToolCallFailed]", len(facts))
	}
	failed := facts[0].(ToolCallFailed)
	if failed.CallID != "cA" || failed.Outcome != ToolOutcomeUnknown {
		t.Fatalf("failed = %+v", failed)
	}
	s = fold(t, s, facts)
	if s.Status != RunActive {
		t.Fatalf("status = %v, want active", s.Status)
	}
	ts, ok := s.Current.(ToolStep)
	if !ok {
		t.Fatalf("current = %T, want ToolStep", s.Current)
	}
	if ts.Calls[0].Status != ToolFailed || ts.Calls[1].Status != ToolExecuting {
		t.Fatalf("calls = %+v", ts.Calls)
	}

	s = fold(t, s, mustDecide(t, s, SubmitToolResult{
		StepID: opened.StepID, CallID: "cB", Result: ToolExecutionResult{Output: cj(`"ok"`)},
	}))
	if s.Status != RunActive || !atOpen(s.Current) {
		t.Fatalf("after sibling complete: status=%v current=%T", s.Status, s.Current)
	}
	if s.LastToolStep == nil || s.LastToolStep.Calls[0].Status != ToolFailed || s.LastToolStep.Calls[1].Status != ToolCompleted {
		t.Fatalf("LastToolStep = %+v", s.LastToolStep)
	}
}

func TestRejectModelResultDispositionRetriesThenFails(t *testing.T) {
	s := newRun(t)
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

func TestAcceptInputIdempotentPerID(t *testing.T) {
	s := newRun(t)
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
	s := newRun(t)
	_, err := Decide(s, NextStep(AgentInput{ID: "seed", Payload: cj(`{"q":"other"}`)}))
	if !errors.Is(err, ErrCommandConflict) {
		t.Fatalf("duplicate seed input err = %v, want ErrCommandConflict", err)
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
		if _, ok := next.Current.(ModelStep); !ok || len(next.PendingInputs) != 0 {
			t.Fatalf("prepared state = %+v", next)
		}
	})
}

func TestEvolveRejectsModelPrepareOverCurrentStep(t *testing.T) {
	s := newRun(t)
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
