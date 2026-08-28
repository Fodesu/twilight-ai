package agent

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/memohai/twilight-ai/sdk"
)

// Event-sourcing arbitration tests (spec §5.1): the log is the source of
// truth; the snapshot is a rebuildable same-transaction projection; the
// revision watermark witnesses log-tail completeness.

// fullRunRuntime drives one complete run (prepare -> model -> tool -> done)
// and returns the runtime.
func fullRunRuntime(t *testing.T) *MemoryRuntime {
	t.Helper()
	def := testToolDef("t")
	spec := makeSpec(t, def, DirectExecution)
	rt, stepID, grant := preparedRuntime(t, []sdk.ToolDefinition{def}, []ToolSpec{spec})
	b := makeBinding(t, "c1", spec, `{}`)
	res := mustCommit(t, rt, "complete-1", 2, grant,
		SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []ToolCallBinding{b}})
	toolStep := res.Events[1].Fact.(ToolStepOpened).StepID
	sRes := mustCommit(t, rt, "start-c1", res.Snapshot.Revision, "", StartToolCall{StepID: toolStep, CallID: "c1"})
	mustCommit(t, rt, "done-c1", sRes.Snapshot.Revision, sRes.Grant,
		SubmitToolResult{StepID: toolStep, CallID: "c1", Result: ToolExecutionResult{Output: json.RawMessage(`"ok"`)}})
	mustCommit(t, rt, DeriveModelRequestCommandID("run-1", 5), 5, "",
		func() PrepareModelRequest {
			snap, _ := rt.Load(context.Background())
			prep, _ := buildPrepareFromSnap(t, snap, testRequest(), nil)
			return prep
		}())
	start := mustCommit(t, rt, "start-2", 6, "", StartModelExecution{StepID: currentStepID(t, rt)})
	final, err := FreezeModelResult(sdk.ModelResult{Text: "final", FinishReason: sdk.FinishReasonStop})
	if err != nil {
		t.Fatal(err)
	}
	mustCommit(t, rt, "done-2", start.Snapshot.Revision, start.Grant,
		SubmitModelResult{StepID: currentStepID(t, rt), Result: final})
	return rt
}

func currentStepID(t *testing.T, rt *MemoryRuntime) StepID {
	t.Helper()
	snap, err := rt.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snap.State.Current == nil {
		t.Fatal("no current step")
	}
	return snap.State.Current.Ref().ID
}

// A healthy runtime rebuilds without divergence: with a correct
// implementation the arbitration branch never fires.
func TestRebuildHealthyIsNoop(t *testing.T) {
	rt := fullRunRuntime(t)
	before, _ := rt.Load(context.Background())
	diverged, err := rt.Rebuild()
	if err != nil {
		t.Fatal(err)
	}
	if diverged {
		t.Fatal("healthy runtime reported divergence on rebuild")
	}
	after, _ := rt.Load(context.Background())
	if !statesEquivalent(before.State, after.State) || before.Revision != after.Revision {
		t.Fatal("rebuild changed a healthy state")
	}
}

// A corrupted snapshot is repaired from the log, and the divergence is
// reported for audit.
func TestRebuildRepairsCorruptedSnapshot(t *testing.T) {
	rt := fullRunRuntime(t)
	want, _ := rt.Load(context.Background())

	// Out-of-band write: corrupt the authoritative snapshot directly.
	rt.mu.Lock()
	rt.state.ModelSteps = 99
	rt.state.Status = RunActive
	rt.state.Result = nil
	rt.mu.Unlock()

	diverged, err := rt.Rebuild()
	if err != nil {
		t.Fatal(err)
	}
	if !diverged {
		t.Fatal("rebuild did not report the repaired divergence")
	}
	got, _ := rt.Load(context.Background())
	if !statesEquivalent(want.State, got.State) {
		t.Fatal("rebuild did not restore the log-derived state")
	}
	if got.State.Status != RunCompleted || got.State.ModelSteps != 2 {
		t.Fatalf("rebuilt state = %+v", got.State.Status)
	}
}

// A log tail below the watermark halts with ErrLogTruncated: accepted facts
// are gone and continuing would repeat gated executions.
func TestRebuildHaltsOnTruncatedTail(t *testing.T) {
	rt := fullRunRuntime(t)
	rt.mu.Lock()
	// Simulate selective damage: drop the last transition's events while the
	// watermark (separate storage in a durable adapter) survives.
	last := rt.log[len(rt.log)-1].Revision
	var kept []AgentEvent
	for _, e := range rt.log {
		if e.Revision < last {
			kept = append(kept, e)
		}
	}
	rt.log = kept
	rt.mu.Unlock()

	_, err := rt.Rebuild()
	if !errors.Is(err, ErrLogTruncated) {
		t.Fatalf("err = %v, want ErrLogTruncated", err)
	}
}

// A gap in the middle of the log is log damage, not a rebuild input.
func TestFoldRejectsInteriorGap(t *testing.T) {
	rt := fullRunRuntime(t)
	rt.mu.Lock()
	var holed []AgentEvent
	for _, e := range rt.log {
		if e.Revision == 3 { // drop one interior transition
			continue
		}
		holed = append(holed, e)
	}
	log := holed
	initial := cloneMachineState(rt.initial)
	rt.mu.Unlock()

	if _, _, err := FoldEvents(initial, log); err == nil {
		t.Fatal("interior gap folded silently")
	}
}

// A tampered fact fails its digest check during fold.
func TestFoldRejectsTamperedFact(t *testing.T) {
	rt := fullRunRuntime(t)
	rt.mu.Lock()
	log := cloneEvents(rt.log)
	initial := cloneMachineState(rt.initial)
	rt.mu.Unlock()

	for i := range log {
		if f, ok := log[i].Fact.(ToolCallCompleted); ok {
			f.Result.Output = json.RawMessage(`"tampered"`)
			log[i].Fact = f
			break
		}
	}
	if _, _, err := FoldEvents(initial, log); err == nil {
		t.Fatal("tampered fact folded silently")
	}
}

// ModelStepPrepared is self-contained: the binding digest folds verbatim and
// reproduces the step identity without recomputation.
func TestRegressionPreparedFactSelfContained(t *testing.T) {
	s := newRun(t, testConfig())
	prep, cmdID := buildPrepare(t, s, testRequest(), nil)
	facts := mustDecide(t, s, prep)
	fact := facts[0].(ModelStepPrepared)
	if fact.BindingDigest == "" {
		t.Fatal("ModelStepPrepared carries no binding digest")
	}
	if DeriveModelStepID(s.RunID, cmdID, fact.BindingDigest) != fact.StepID {
		t.Fatal("carried binding digest does not reproduce the step ID")
	}
	s = fold(t, s, facts)
	ms := s.Current.(ModelStep)
	if ms.RefValue.Digest != fact.BindingDigest {
		t.Fatal("Evolve did not fold the carried digest verbatim")
	}
}

// Golden event stream: a fixed v1 command sequence folds to frozen state
// bytes. If this test fails, either the canonical encoding or Evolve's
// folding semantics changed — both are permanent contracts of SchemaVersion 1
// (fix the code, not the constant), unless the protocol itself is still
// pre-release and the change is deliberate (then re-freeze the constant in
// the same commit that changes the protocol).
func TestGoldenEventStreamV1(t *testing.T) {
	rt := fullRunRuntime(t)
	folded, maxRev, err := FoldEvents(cloneMachineState(rt.initial), rt.Events())
	if err != nil {
		t.Fatal(err)
	}
	if maxRev != 8 {
		t.Fatalf("golden stream has %d transitions, want 8", maxRev)
	}
	stateBytes, err := marshalCanonical(stateComparable(folded))
	if err != nil {
		t.Fatal(err)
	}
	got := string(sha256Digest(stateBytes))
	const frozen = "sha256:004694c1e4ffc3d5ba55bbc833b4cc912943959115836cdc29e1620d063afccb"
	if got != frozen {
		t.Fatalf("golden v1 state digest changed:\n got %s\nwant %s\nstate: %s", got, frozen, stateBytes)
	}
}
