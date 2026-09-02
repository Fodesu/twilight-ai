package run

import "testing"

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

	evil := makeBinding(t, "c1", specDanger, `{"rm":"-rf"}`)
	result := modelResultWithNamedCalls("safe", `{"a":1}`, "c1")
	if _, err := ProtocolV1().Decide(s, SubmitModelResult{StepID: stepID, Result: result, Calls: []ToolCallBinding{evil}}); err == nil {
		t.Fatal("binding for a tool the model never called was accepted")
	}

	tampered := makeBinding(t, "c1", specSafe, `{"a":999}`)
	if _, err := ProtocolV1().Decide(s, SubmitModelResult{StepID: stepID, Result: result, Calls: []ToolCallBinding{tampered}}); err == nil {
		t.Fatal("binding with tampered arguments was accepted")
	}
}

func TestRegressionToolStepIDReproducible(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, ApprovalRequired)
	s := newRun(t)
	s, stepID := advanceToExecuting(t, s, testRequest(def), []ToolSpec{spec})
	b := makeBinding(t, "c1", spec, `{}`)
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
	b := makeBinding(t, "c1", spec, `{}`)
	facts := mustDecide(t, s, SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []ToolCallBinding{b}})
	opened := facts[1].(ToolStepOpened)
	s = fold(t, s, facts)
	s = fold(t, s, mustDecide(t, s, StartToolCall{StepID: opened.StepID, CallID: "c1"}))

	_, err := ProtocolV1().Evolve(s, ToolCallFailed{
		StepID:  opened.StepID,
		CallID:  "c1",
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
