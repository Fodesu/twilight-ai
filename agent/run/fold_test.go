package run

import (
	"context"
	"errors"
	"testing"

	"github.com/memohai/twilight/sdk"
)

// Event-sourcing arbitration tests (RUN-SCP-1, RUN-CMT-2): the complete canonical
// TransitionRecord is the diagnostic commit record; the snapshot is a
// rebuildable same-transaction projection; the head revision witnesses
// log-tail completeness.

// fullRunRuntime drives one complete run (prepare -> model -> tool -> done)
// and returns the runtime.
func fullRunRuntime(t *testing.T) Runtime {
	t.Helper()
	def := testToolDef("t")
	spec := makeSpec(t, def, DirectExecution)
	rt, stepID, grant := preparedRuntime(t, []sdk.ToolDefinition{def}, []ToolSpec{spec})
	b := makeBinding(t, "c1", spec, `{}`)
	snap, err := rt.Load(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	res := mustCommit(t, rt, "complete-1", snap.Revision, grant,
		SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []ToolCallBinding{b}})
	toolStep := res.Events[1].Fact.(ToolStepOpened).StepID
	sRes := mustCommit(t, rt, "start-c1", res.Snapshot.Revision, "", StartToolCall{StepID: toolStep, CallID: "c1"})
	mustCommit(t, rt, "done-c1", sRes.Snapshot.Revision, sRes.Grant,
		SubmitToolResult{StepID: toolStep, CallID: "c1", Result: ToolExecutionResult{Output: cj(`"ok"`)}})
	snap, err = rt.Load(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	prep, cmdID := buildPrepareFromSnap(t, snap, testRequest(), nil)
	prepared := mustCommit(t, rt, cmdID, snap.Revision, "", prep)
	start := mustCommit(t, rt, "start-2", prepared.Snapshot.Revision, "", StartModelExecution{StepID: currentStepID(t, rt)})
	final, err := FreezeModelResult(sdk.ModelResult{Text: "final", FinishReason: sdk.FinishReasonStop})
	if err != nil {
		t.Fatal(err)
	}
	mustCommit(t, rt, "done-2", start.Snapshot.Revision, start.Grant,
		SubmitModelResult{StepID: currentStepID(t, rt), Result: final})
	return rt
}

func currentStepID(t *testing.T, rt Runtime) StepID {
	t.Helper()
	snap, err := rt.Load(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	switch cur := snap.State.Current.(type) {
	case ModelStep:
		return cur.Ref().ID
	case ToolStep:
		return cur.Ref().ID
	default:
		t.Fatalf("current = %T, want ModelStep or ToolStep", snap.State.Current)
		return ""
	}
}

// A healthy runtime rebuilds without divergence: with a correct
// implementation the arbitration branch never fires.
func TestRebuildHealthyIsNoop(t *testing.T) {
	rt := fullRunRuntime(t)
	before, _ := rt.Load(context.Background(), "run-1")
	diverged, err := rebuildRun(t, rt, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if diverged {
		t.Fatal("healthy runtime reported divergence on rebuild")
	}
	after, _ := rt.Load(context.Background(), "run-1")
	if !statesEquivalent(&before.State, &after.State) || before.Revision != after.Revision {
		t.Fatal("rebuild changed a healthy state")
	}
}

// A corrupted snapshot is repaired from the log, and the divergence is
// reported for audit.
func TestRebuildRepairsCorruptedSnapshot(t *testing.T) {
	rt := fullRunRuntime(t)
	want, _ := rt.Load(context.Background(), "run-1")

	// Out-of-band write: corrupt the authoritative snapshot directly.
	entry := memoryEntry(t, rt)
	entry.mu.Lock()
	entry.state.ModelSteps = 99
	entry.state.Status = RunActive
	entry.state.Result = nil
	entry.mu.Unlock()

	diverged, err := rebuildRun(t, rt, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if !diverged {
		t.Fatal("rebuild did not report the repaired divergence")
	}
	got, _ := rt.Load(context.Background(), "run-1")
	if !statesEquivalent(&want.State, &got.State) {
		t.Fatal("rebuild did not restore the log-derived state")
	}
	if got.State.Status != RunCompleted || got.State.ModelSteps != 2 {
		t.Fatalf("rebuilt state = %+v", got.State.Status)
	}
}

// A log tail below the head revision halts with ErrLogTruncated: accepted
// facts are gone and continuing would repeat gated executions.
func TestRebuildHaltsOnTruncatedTail(t *testing.T) {
	rt := fullRunRuntime(t)
	entry := memoryEntry(t, rt)
	entry.mu.Lock()
	// Simulate selective damage: drop the last transition while the head
	// revision (separate storage in a durable adapter) survives.
	entry.log = cloneTransitionRecords(entry.log[:len(entry.log)-1])
	entry.mu.Unlock()

	_, err := rebuildRun(t, rt, "run-1")
	if !errors.Is(err, ErrLogTruncated) {
		t.Fatalf("err = %v, want ErrLogTruncated", err)
	}
}

func TestRebuildHaltsOnPartialTailTransition(t *testing.T) {
	rt := fullRunRuntime(t)
	entry := memoryEntry(t, rt)
	entry.mu.Lock()
	last := &entry.log[len(entry.log)-1]
	if len(last.Events) < 2 {
		t.Fatal("test requires a multi-event tail transition")
	}
	last.Events = last.Events[:len(last.Events)-1]
	entry.mu.Unlock()

	_, err := rebuildRun(t, rt, "run-1")
	if err == nil {
		t.Fatal("partial tail transition folded silently")
	}
}

func TestFoldRejectsDamagedLog(t *testing.T) {
	rt := fullRunRuntime(t)
	entry := memoryEntry(t, rt)
	entry.mu.Lock()
	initial := cloneMachineState(&entry.header.InitialState)
	transitions := cloneTransitionRecords(entry.log)
	events := flattenTransitionRecords(entry.log)
	entry.mu.Unlock()

	t.Run("interior transition gap", func(t *testing.T) {
		var holed []TransitionRecord
		for i := range transitions {
			if transitions[i].Revision == 3 {
				continue
			}
			holed = append(holed, cloneTransitionRecord(&transitions[i]))
		}
		if _, _, err := FoldTransitions(cloneMachineState(&initial), holed); err == nil {
			t.Fatal("interior transition gap folded silently")
		}
	})
	t.Run("interior event gap", func(t *testing.T) {
		var holed []AgentEvent
		for i := range events {
			if events[i].Revision == 3 {
				continue
			}
			holed = append(holed, events[i])
		}
		if _, _, err := FoldEvents(cloneMachineState(&initial), holed); err == nil {
			t.Fatal("interior event gap folded silently")
		}
	})
	t.Run("run id mismatch", func(t *testing.T) {
		log := flattenTransitionRecords(transitions)
		log[0].RunID = "other-run"
		if _, _, err := FoldEvents(cloneMachineState(&initial), log); err == nil {
			t.Fatal("run mismatch folded silently")
		}
	})
	t.Run("command identity change", func(t *testing.T) {
		log := flattenTransitionRecords(transitions)
		for i := range log {
			if log[i].Index == 1 {
				log[i].CommandID = "other-command"
				break
			}
		}
		if _, _, err := FoldEvents(cloneMachineState(&initial), log); err == nil {
			t.Fatal("same-revision command identity change folded silently")
		}
	})
	t.Run("unsupported schema version", func(t *testing.T) {
		log := flattenTransitionRecords(transitions)
		log[0].SchemaVersion = 99
		if _, _, err := FoldEvents(cloneMachineState(&initial), log); err == nil {
			t.Fatal("unsupported schema version folded silently")
		}
	})
	t.Run("tampered fact", func(t *testing.T) {
		log := flattenTransitionRecords(transitions)
		for i := range log {
			if f, ok := log[i].Fact.(ToolCallCompleted); ok {
				f.Result.Output = cj(`"tampered"`)
				log[i].Fact = f
				break
			}
		}
		if _, _, err := FoldEvents(cloneMachineState(&initial), log); err == nil {
			t.Fatal("tampered fact folded silently")
		}
	})
}

// ModelStepPrepared is self-contained: the binding digest folds verbatim and
// reproduces the step identity without recomputation.
func TestRegressionPreparedFactSelfContained(t *testing.T) {
	s := newRun(t)
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

// Golden event stream for the current pre-release v1 command sequence. It
// protects the current fold result; deliberately changing the pre-release
// protocol requires re-freezing the fixture in the same change. After v1 is
// published, this becomes a permanent compatibility fixture.
func TestGoldenEventStreamV1(t *testing.T) {
	rt := fullRunRuntime(t)
	record, err := rt.Record(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	folded, maxRev, err := FoldTransitions(cloneMachineState(&record.Header.InitialState), record.Transitions)
	if err != nil {
		t.Fatal(err)
	}
	if maxRev != 9 {
		t.Fatalf("golden stream has %d transitions, want 9", maxRev)
	}
	stateBytes, err := ProtocolV1.EncodeMachineState(&folded)
	if err != nil {
		t.Fatal(err)
	}
	got := string(sha256Digest(stateBytes))
	const frozen = "sha256:b76adb269cc9821c6415ab93b355725fbd0054cb5a0ca26af7ab937648708d25"
	if got != frozen {
		t.Fatalf("golden v1 state digest changed:\n got %s\nwant %s\nstate: %s", got, frozen, stateBytes)
	}
}
