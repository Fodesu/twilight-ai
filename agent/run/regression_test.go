package run

import (
	"context"
	"testing"

	"github.com/memohai/twilight/sdk"
)

// Regression tests for the code-review findings on the phase A/B
// implementation. Each test pins one fixed defect.

// Finding 1: a result carrying tool calls with zero bindings must not
// silently complete the run.
func TestRegressionZeroBindingsWithToolCallsRejected(t *testing.T) {
	s := newRun(t, testConfig())
	s, stepID := advanceToExecuting(t, s, testRequest(), nil)
	result := modelResultWithCalls("c1") // has tool calls
	if _, err := Decide(s, SubmitModelResult{StepID: stepID, Result: result, Calls: nil}); err == nil {
		t.Fatal("result with tool calls and no bindings completed the run")
	}
}

// Finding 2: a binding naming a different tool / different arguments than the
// model result must be rejected even with self-consistent digests.
func TestRegressionBindingMustMatchModelResult(t *testing.T) {
	safe := testToolDef("safe")
	danger := testToolDef("danger")
	specSafe := makeSpec(t, safe, DirectExecution)
	specDanger := makeSpec(t, danger, DirectExecution)
	s := newRun(t, testConfig())
	s, stepID := advanceToExecuting(t, s, testRequest(safe, danger), []ToolSpec{specSafe, specDanger})

	// Model called "safe" with {"a":1}; binding claims "danger" with {"rm":"-rf"}.
	evil := makeBinding(t, "c1", specDanger, `{"rm":"-rf"}`)
	result := modelResultWithNamedCalls("safe", `{"a":1}`, "c1")
	if _, err := Decide(s, SubmitModelResult{StepID: stepID, Result: result, Calls: []ToolCallBinding{evil}}); err == nil {
		t.Fatal("binding for a tool the model never called was accepted")
	}

	// Same tool, different arguments: also rejected.
	tampered := makeBinding(t, "c1", specSafe, `{"a":999}`)
	if _, err := Decide(s, SubmitModelResult{StepID: stepID, Result: result, Calls: []ToolCallBinding{tampered}}); err == nil {
		t.Fatal("binding with tampered arguments was accepted")
	}
}

// Finding 4: protocol JSON follows RFC 8785 / IEEE-754 semantics, so the
// PostgreSQL JSONB rendering of a number has the same digest as its wire
// spelling. Exact large identifiers must be JSON strings.
func TestRegressionJCSNumberSemantics(t *testing.T) {
	d1, err := digestToolCallBinding("c", "", DirectExecution, cj(`{"n":1e+21}`))
	if err != nil {
		t.Fatal(err)
	}
	d2, err := digestToolCallBinding("c", "", DirectExecution, cj(`{"n":1000000000000000000000}`))
	if err != nil {
		t.Fatal(err)
	}
	if d1 != d2 {
		t.Fatal("equivalent JCS numbers produced different binding digests")
	}

	id1, err := digestToolCallBinding("c", "", DirectExecution, cj(`{"channel_id":"9007199254740993"}`))
	if err != nil {
		t.Fatal(err)
	}
	id2, err := digestToolCallBinding("c", "", DirectExecution, cj(`{"channel_id":"9007199254740992"}`))
	if err != nil {
		t.Fatal(err)
	}
	if id1 == id2 {
		t.Fatal("distinct string identifiers collided in a binding digest")
	}
}

// Finding 5 + 10: the ToolStep ID must reproduce from the persisted StepRef
// digest, and the digest is carried in the fact (not recomputed by Evolve).
func TestRegressionToolStepIDReproducible(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, ApprovalRequired) // Response-filled path
	s := newRun(t, testConfig())
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

// Finding 3: invalid tool output cannot enter the authority contract. Tools
// return CanonicalJSON, so malformed JSON is rejected at construction rather
// than later inside BuildEnvelope/DigestCommand.
func TestRegressionInvalidToolOutputCannotBeConstructed(t *testing.T) {
	if _, err := ParseCanonicalJSON([]byte(`{broken`)); err == nil {
		t.Fatal("malformed JSON constructed as CanonicalJSON")
	}
}

// Finding 7: an abandoned ask-user call has a failure exit via RejectToolCall.
func TestRegressionExternalResponseCanBeRejected(t *testing.T) {
	def := testToolDef("ask")
	spec := makeSpec(t, def, ExternalResponse)
	s := newRun(t, testConfig())
	s, stepID := advanceToExecuting(t, s, testRequest(def), []ToolSpec{spec})
	b := makeBinding(t, "c1", spec, `{}`)
	facts := mustDecide(t, s, SubmitModelResult{StepID: stepID, Result: modelResultWithNamedCalls("ask", `{}`, "c1"), Calls: []ToolCallBinding{b}})
	opened := facts[1].(ToolStepOpened)
	s = fold(t, s, facts)
	respID := opened.Calls[0].Response.ID

	facts, err := Decide(s, RejectToolCall{StepID: opened.StepID, CallID: "c1", ResponseID: respID,
		ResponseDigest: responseDecisionDigest(t, ResponseExternal, ResponseDecisionRejected, "user dismissed"), Reason: "user dismissed"})
	if err != nil {
		t.Fatalf("external-response call cannot be rejected: %v", err)
	}
	failed := facts[0].(ToolCallFailed)
	if failed.Failure.Class != FailurePermissionDenied || failed.Outcome != ToolOutcomeKnown {
		t.Fatalf("failed = %+v", failed)
	}
	s = fold(t, s, facts)
	if s.Status != RunActive || s.Current != nil {
		t.Fatal("run should continue after abandoning the ask-user call")
	}
}

// Finding 8: duplicate object keys and trailing data are rejected, not
// silently canonicalized.
func TestRegressionCanonicalRejectsAmbiguousInput(t *testing.T) {
	for _, in := range []string{
		`{"a":1,"a":2}`,
		`{"dry_run":true,"dry_run":false}`,
		`{"x":1}]`,
		`{"a":1}}}`,
		`[1,2]]`,
	} {
		if _, err := canonicalJSON([]byte(in)); err == nil {
			t.Fatalf("ambiguous input %q was canonicalized", in)
		}
	}
	// Invalid UTF-8 is rejected rather than collapsed to U+FFFD.
	if _, err := canonicalJSON([]byte("{\"a\":\"\xff\"}")); err == nil {
		t.Fatal("invalid UTF-8 was canonicalized")
	}
}

// Finding 9: Runtime return values are isolated snapshots — mutating them
// must not reach authoritative state or committed events.
func TestRegressionRuntimeReturnsAreIsolated(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, ApprovalRequired)
	rt, stepID, grant := preparedRuntime(t, []sdk.ToolDefinition{def}, []ToolSpec{spec})
	b := makeBinding(t, "c1", spec, `{"k":"original"}`)
	snap, err := rt.Load(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	res := mustCommit(t, rt, "complete-1", snap.Revision, grant,
		SubmitModelResult{StepID: stepID, Result: modelResultWithNamedCalls("t", `{"k":"original"}`, "c1"), Calls: []ToolCallBinding{b}})

	// Mutate byte views returned from immutable CanonicalJSON values.
	opened := res.Events[1].Fact.(ToolStepOpened)
	argBytes := opened.Calls[0].Arguments.RawMessage()
	argBytes[2] = 'X'
	// Mutate the snapshot's waiting payload view.
	snap, _ = rt.Load(context.Background(), "run-1")
	ts := snap.State.Current.(ToolStep)
	payloadBytes := ts.Calls[0].Waiting.Payload.RawMessage()
	payloadBytes[2] = 'Y'

	// Authority must be unchanged: reload and verify the argument bytes.
	fresh, _ := rt.Load(context.Background(), "run-1")
	got := fresh.State.Current.(ToolStep).Calls[0].Arguments
	if got.String() != `{"k":"original"}` {
		t.Fatalf("authoritative arguments mutated through a returned view: %s", got.String())
	}
	stored := recordEvents(t, rt, "run-1")
	for _, e := range stored {
		if f, ok := e.Fact.(ToolStepOpened); ok {
			if f.Calls[0].Arguments.String() != `{"k":"original"}` {
				t.Fatalf("stored event mutated through a returned view: %s", f.Calls[0].Arguments.String())
			}
		}
	}
}

// Finding 11: Evolve rejects illegal ToolCallState combinations.
func TestRegressionEvolveRejectsIllegalCallState(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, DirectExecution)
	s := newRun(t, testConfig())
	s, stepID := advanceToExecuting(t, s, testRequest(def), []ToolSpec{spec})
	b := makeBinding(t, "c1", spec, `{}`)
	facts := mustDecide(t, s, SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []ToolCallBinding{b}})
	opened := facts[1].(ToolStepOpened)
	s = fold(t, s, facts)
	s = fold(t, s, mustDecide(t, s, StartToolCall{StepID: opened.StepID, CallID: "c1"}))

	// Unknown outcome with a non-effect_unknown class is illegal (RUN-MCH-2).
	_, err := Evolve(s, ToolCallFailed{
		StepID:  opened.StepID,
		CallID:  "c1",
		Failure: ToolFailure{Class: FailureExecution},
		Outcome: ToolOutcomeUnknown,
	})
	if err == nil {
		t.Fatal("Evolve accepted an illegal unknown-outcome class")
	}
}

// Low-severity finding: CancelRun cannot forge a system reason.
func TestRegressionCancelReasonFixed(t *testing.T) {
	s := newRun(t, testConfig())
	if _, err := Decide(s, CancelRun{Reason: ReasonStepLimit}); err == nil {
		t.Fatal("CancelRun forged step_limit into the log")
	}
	facts := mustDecide(t, s, CancelRun{})
	end, ok := facts[0].(RunEnded).End.(RunStoppedEnd)
	if !ok || end.Reason != ReasonCancelled {
		t.Fatal("cancel reason not fixed to cancelled")
	}
}
