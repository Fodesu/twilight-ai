package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/memohai/twilight-ai/sdk"
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

// Finding 4: integers above 2^53 keep exact digits; near-adjacent big
// integers must not collide into one digest.
func TestRegressionBigIntegerPrecision(t *testing.T) {
	got, err := canonicalJSON([]byte(`{"channel_id":1234567890123456789}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"channel_id":1234567890123456789}` {
		t.Fatalf("big integer corrupted: %s", got)
	}
	d1, err := digestToolCallBinding("c", "", DirectExecution, cj(`{"n":9007199254740993}`))
	if err != nil {
		t.Fatal(err)
	}
	d2, err := digestToolCallBinding("c", "", DirectExecution, cj(`{"n":9007199254740992}`))
	if err != nil {
		t.Fatal(err)
	}
	if d1 == d2 {
		t.Fatal("adjacent big integers collide into one binding digest")
	}
	// Non-integer numbers still use the ES6 double form.
	got, _ = canonicalJSON([]byte(`{"a":1.0e3}`))
	if string(got) != `{"a":1000}` {
		t.Fatalf("float form changed: %s", got)
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

// Finding 6: a panicking tool settles as Unknown instead of crashing the
// process.
func TestRegressionToolPanicBecomesUnknown(t *testing.T) {
	spec := toolSpec(t, "echo", DirectExecution)
	echo := &fakeTool{ref: "echo", def: spec.Definition.SDK(), policy: DirectExecution,
		execute: func(context.Context, ToolExecutionRequest) ToolExecutionOutcome {
			panic("nil map write")
		}}
	invoker := &fakeInvoker{results: []sdk.ModelResult{toolCallResult("c1")}}
	rt := loopRuntime(t)
	loop, _ := NewLoop(fakeCatalog{invoker}, fakeToolCatalog{map[ToolRef]ExecutableTool{"echo": echo}},
		staticPlanner{specs: []ToolSpec{spec}}, ExecutionPolicy{}, false)

	res, err := loop.Run(context.Background(), rt, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Result.Status != RunFailed || res.Result.Reason != ReasonEffectUnknown {
		t.Fatalf("res = %+v", res.Result)
	}
	if !strings.Contains(res.Result.Failure.Message, "panic") {
		t.Fatalf("failure = %+v", res.Result.Failure)
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
	res := mustCommit(t, rt, "complete-1", 2, grant,
		SubmitModelResult{StepID: stepID, Result: modelResultWithNamedCalls("t", `{"k":"original"}`, "c1"), Calls: []ToolCallBinding{b}})

	// Mutate byte views returned from immutable CanonicalJSON values.
	opened := res.Events[1].Fact.(ToolStepOpened)
	argBytes := opened.Calls[0].Arguments.RawMessage()
	argBytes[2] = 'X'
	// Mutate the snapshot's waiting payload view.
	snap, _ := rt.Load(context.Background())
	ts := snap.State.Current.(ToolStep)
	payloadBytes := ts.Calls[0].Waiting.Payload.RawMessage()
	payloadBytes[2] = 'Y'

	// Authority must be unchanged: reload and verify the argument bytes.
	fresh, _ := rt.Load(context.Background())
	got := fresh.State.Current.(ToolStep).Calls[0].Arguments
	if got.String() != `{"k":"original"}` {
		t.Fatalf("authoritative arguments mutated through a returned view: %s", got.String())
	}
	stored := rt.Events()
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

	// Unknown outcome with a non-effect_unknown class is illegal (spec §4.2).
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

// Finding 14: EventRunFinished fires on terminal.
func TestRegressionRunFinishedEmitted(t *testing.T) {
	rt := loopRuntime(t)
	var kinds []EventKind
	sink := sinkFunc(func(_ context.Context, e Event) error {
		kinds = append(kinds, e.Kind)
		return nil
	})
	loop, _ := NewLoop(fakeCatalog{&fakeInvoker{results: []sdk.ModelResult{textResult("done")}}},
		fakeToolCatalog{}, staticPlanner{}, ExecutionPolicy{}, false)
	if _, err := loop.Run(context.Background(), rt, sink); err != nil {
		t.Fatal(err)
	}
	for _, k := range kinds {
		if k == EventRunFinished {
			return
		}
	}
	t.Fatalf("EventRunFinished never emitted; kinds = %v", kinds)
}

// Finding 15: a ToolSpec whose Ref differs from the definition name routes
// through the catalog by Ref end to end.
func TestRegressionAliasedToolRefExecutes(t *testing.T) {
	def := sdk.ToolDefinition{Name: "read", Parameters: json.RawMessage(`{"type":"object"}`)}
	frozenDef, err := FreezeToolDefinition(def)
	if err != nil {
		t.Fatal(err)
	}
	d, err := DigestToolDefinition(frozenDef)
	if err != nil {
		t.Fatal(err)
	}
	spec := ToolSpec{Ref: "fs.read", Definition: frozenDef, DefinitionDigest: d, Policy: DirectExecution}
	executed := atomic.Bool{}
	tool := &fakeTool{ref: "fs.read", def: def, policy: DirectExecution,
		execute: func(context.Context, ToolExecutionRequest) ToolExecutionOutcome {
			executed.Store(true)
			return ToolExecutionSucceeded{Result: ToolExecutionResult{Output: cj(`"ok"`)}}
		}}
	// Model calls the definition name "read"; catalog keys by Ref "fs.read".
	invoker := &fakeInvoker{results: []sdk.ModelResult{
		func() sdk.ModelResult {
			r := sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls}
			r.ToolCalls = []sdk.ToolCall{{ToolCallID: "c1", ToolName: "read", Input: `{}`}}
			return r
		}(),
		textResult("done"),
	}}
	rt := loopRuntime(t)
	loop, _ := NewLoop(fakeCatalog{invoker}, fakeToolCatalog{map[ToolRef]ExecutableTool{"fs.read": tool}},
		staticPlanner{specs: []ToolSpec{spec}}, ExecutionPolicy{}, false)

	res, err := loop.Run(context.Background(), rt, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !executed.Load() {
		t.Fatal("aliased tool never executed")
	}
	if res.Result.Status != RunCompleted {
		t.Fatalf("res = %+v", res.Result)
	}
}

// Low-severity finding: CancelRun cannot forge a system reason.
func TestRegressionCancelReasonFixed(t *testing.T) {
	s := newRun(t, testConfig())
	if _, err := Decide(s, CancelRun{Reason: ReasonStepLimit}); err == nil {
		t.Fatal("CancelRun forged step_limit into the log")
	}
	facts := mustDecide(t, s, CancelRun{})
	if facts[0].(RunEnded).Reason != ReasonCancelled {
		t.Fatal("cancel reason not fixed to cancelled")
	}
}

// Finding 13: a stream that closes Parts but returns (nil, nil) from Result
// fails the call instead of panicking the Loop.
func TestRegressionStreamNilResult(t *testing.T) {
	rt := loopRuntime(t)
	loop, _ := NewLoop(fakeCatalog{nilResultStreamer{}}, fakeToolCatalog{}, staticPlanner{}, ExecutionPolicy{}, true)
	res, err := loop.Run(context.Background(), rt, nil)
	if err != nil {
		t.Fatal(err)
	}
	// SubmitModelFailure path: run fails with provider_failure, no panic.
	if res.Result.Status != RunFailed {
		t.Fatalf("res = %+v", res.Result)
	}
}

type sinkFunc func(context.Context, Event) error

func (f sinkFunc) Emit(ctx context.Context, e Event) error { return f(ctx, e) }

type nilResultStreamer struct{}

func (nilResultStreamer) Generate(context.Context, sdk.Request) (sdk.ModelResult, error) {
	return sdk.ModelResult{}, errors.New("generate should not be called when streaming")
}

func (nilResultStreamer) Stream(context.Context, sdk.Request) (sdk.ModelStream, error) {
	parts := make(chan sdk.StreamPart)
	close(parts)
	return sdk.ModelStream{
		Parts:  parts,
		Result: func() (*sdk.ModelResult, error) { return nil, nil },
	}, nil
}
