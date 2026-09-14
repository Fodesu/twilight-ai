package run

import (
	"testing"

	"github.com/felinics/twilight/sdk"
)

func TestRegressionZeroBindingsWithToolCallsRejected(t *testing.T) {
	s := newRun(t)
	s, stepID := advanceToExecuting(t, s, testRequest(), nil)
	result := modelResultWithCalls("c1")
	if _, err := ProtocolV1().Decide(s, SubmitModelResult{StepID: stepID, Result: result, Calls: nil}); err == nil {
		t.Fatal("result with tool calls and no bindings completed the run")
	}
}

func TestRegressionBindingMustMatchModelResult(t *testing.T) {
	safe := testToolDef("safe")
	danger := testToolDef("danger")
	specSafe := makeSpec(t, safe, DirectExecution)
	specDanger := makeSpec(t, danger, DirectExecution)
	s := newRun(t)
	s, stepID := advanceToExecuting(t, s, testRequest(safe, danger), []ToolSpec{specSafe, specDanger})

	evil := makeBinding(t, stepID, 0, "c1", specDanger, `{"rm":"-rf"}`)
	result := modelResultWithNamedCalls("safe", `{"a":1}`, "c1")
	if _, err := ProtocolV1().Decide(s, SubmitModelResult{StepID: stepID, Result: result, Calls: []ToolCallBinding{evil}}); err == nil {
		t.Fatal("binding for a tool the model never called was accepted")
	}

	tampered := makeBinding(t, stepID, 0, "c1", specSafe, `{"a":999}`)
	if _, err := ProtocolV1().Decide(s, SubmitModelResult{StepID: stepID, Result: result, Calls: []ToolCallBinding{tampered}}); err == nil {
		t.Fatal("binding with tampered arguments was accepted")
	}
}

func TestRegressionToolStepIDReproducible(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, ApprovalRequired)
	s := newRun(t)
	s, stepID := advanceToExecuting(t, s, testRequest(def), []ToolSpec{spec})
	b := makeBinding(t, stepID, 0, "c1", spec, `{}`)
	facts := mustDecide(t, s, SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []ToolCallBinding{b}})
	opened := facts[1].(ToolStepOpened)
	if DeriveToolStepID(opened.Source, opened.BindingSetDigest) != opened.StepID {
		t.Fatal("ToolStepOpened digest does not reproduce its StepID")
	}
	s = fold(t, s, facts)
	ts := s.Current.(ToolStep)
	if DeriveToolStepID(ts.Source, ts.RefValue.Digest) != ts.RefValue.ID {
		t.Fatal("persisted StepRef.Digest does not reproduce the step ID")
	}
}

func TestRegressionEvolveRejectsIllegalCallState(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, DirectExecution)
	s := newRun(t)
	s, stepID := advanceToExecuting(t, s, testRequest(def), []ToolSpec{spec})
	b := makeBinding(t, stepID, 0, "c1", spec, `{}`)
	facts := mustDecide(t, s, SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []ToolCallBinding{b}})
	opened := facts[1].(ToolStepOpened)
	s = fold(t, s, facts)
	s = fold(t, s, mustDecide(t, s, StartToolCall{StepID: opened.StepID, CallID: cid(stepID, 0), Claim: "attempt-1"}))

	_, err := ProtocolV1().Evolve(s, ToolCallFailed{
		StepID:  opened.StepID,
		CallID:  cid(stepID, 0),
		Failure: ToolFailure{Class: FailureExecution},
		Outcome: ToolOutcomeUnknown,
	})
	if err == nil {
		t.Fatal("Evolve accepted an illegal unknown-outcome class")
	}
}

func TestRegressionCancelReasonFixed(t *testing.T) {
	s := newRun(t)
	if _, err := ProtocolV1().Decide(s, CancelRun{Reason: RunReason("other")}); err == nil {
		t.Fatal("CancelRun accepted a non-cancellation reason")
	}
	facts := mustDecide(t, s, CancelRun{})
	end, ok := facts[0].(RunEnded).End.(RunStoppedEnd)
	if !ok || end.Reason != ReasonCancelled {
		t.Fatal("cancel reason not fixed to cancelled")
	}
}

func TestCancelSettlesEveryUnfinishedToolCall(t *testing.T) {
	for _, kind := range []ResponseKind{ResponseApproval, ResponseExternal} {
		t.Run(string(kind), func(t *testing.T) {
			policy := ApprovalRequired
			if kind == ResponseExternal {
				policy = ExternalResponse
			}
			def, waitDef := testToolDef("t"), testToolDef("wait")
			spec, waitSpec := makeSpec(t, def, DirectExecution), makeSpec(t, waitDef, policy)
			current := newRun(t)
			current, modelStep := advanceToExecuting(t, current, testRequest(def, waitDef), []ToolSpec{spec, waitSpec})
			bindings := make([]ToolCallBinding, 5)
			result := sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls}
			for i := range bindings {
				callSpec := spec
				if i == 2 {
					callSpec = waitSpec
				}
				providerID := "c" + string(rune('1'+i))
				bindings[i] = makeBinding(t, modelStep, i, providerID, callSpec, `{}`)
				result.ToolCalls = append(result.ToolCalls, sdk.ToolCall{ToolCallID: providerID, ToolName: callSpec.Name, Input: `{}`})
			}
			frozen, err := FreezeModelResult(result)
			if err != nil {
				t.Fatal(err)
			}
			current = fold(t, current, mustDecide(t, current, SubmitModelResult{StepID: modelStep, Result: frozen, Calls: bindings}))
			stepID := current.Current.(ToolStep).RefValue.ID
			// Retain one executing call, one completed result and one known failure.
			for _, i := range []int{0, 3} {
				current = fold(t, current, mustDecide(t, current, StartToolCall{StepID: stepID, CallID: bindings[i].CallID, Claim: "attempt"}))
			}
			current = fold(t, current, mustDecide(t, current, SubmitToolResult{StepID: stepID, CallID: bindings[3].CallID,
				Result: ToolExecutionResult{Output: cj(`"done"`)}}))
			current = fold(t, current, mustDecide(t, current, SubmitToolFailure{StepID: stepID, CallID: bindings[4].CallID,
				Failure: ToolFailure{Class: FailureExecution, Message: "failed"}, Outcome: ToolOutcomeKnown}))
			facts := mustDecide(t, current, CancelRun{})
			if len(facts) != 4 {
				t.Fatalf("cancel facts = %d, want three failures and RunEnded", len(facts))
			}
			for i := 0; i < 3; i++ {
				failure, ok := facts[i].(ToolCallFailed)
				if !ok || failure.CallID != bindings[i].CallID {
					t.Fatalf("fact %d = %+v", i, facts[i])
				}
				outcome, class := ToolOutcomeKnown, FailureCancelled
				if i == 0 {
					outcome, class = ToolOutcomeUnknown, FailureEffectUnknown
				}
				if failure.Outcome != outcome || failure.Failure.Class != class {
					t.Fatalf("call %d failure = %+v", i, failure)
				}
			}
			ended := fold(t, current, facts)
			if ended.Status != RunStopped || ended.LastToolStep == nil ||
				len(ended.Result.UncertainCalls) != 1 || ended.Result.UncertainCalls[0] != bindings[0].CallID {
				t.Fatalf("cancel result = %+v", ended)
			}
			calls := ended.LastToolStep.Calls
			if calls[3].Status != ToolCompleted || calls[4].Failure.Failure.Class != FailureExecution {
				t.Fatalf("settled calls changed: %+v", calls[3:])
			}
		})
	}
}
