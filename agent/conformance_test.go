package agent

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/memohai/twilight-ai/sdk"
)

// Conformance tests over the Runtime contract, run against MemoryRuntime
// (spec §14.3). The same suite is the seed of agent/runtimetest for
// MemohRuntime.

func newTestRuntime(t *testing.T, cfg RunConfig) *MemoryRuntime {
	t.Helper()
	s, err := Initialize("run-1", cfg, NextRun(AgentInput{ID: "seed", Payload: json.RawMessage(`{"q":"hi"}`)}))
	if err != nil {
		t.Fatal(err)
	}
	return NewMemoryRuntime(s)
}

func commitCmd(t *testing.T, rt Runtime, id CommandID, base uint64, grant ExecutionGrant, cmd AgentCommand) (CommitResult, error) {
	t.Helper()
	env, err := BuildEnvelope("run-1", id, cmd)
	if err != nil {
		t.Fatal(err)
	}
	return rt.Commit(context.Background(), CommitRequest{BaseRevision: base, Grant: grant, Command: env})
}

func mustCommit(t *testing.T, rt Runtime, id CommandID, base uint64, grant ExecutionGrant, cmd AgentCommand) CommitResult {
	t.Helper()
	res, err := commitCmd(t, rt, id, base, grant, cmd)
	if err != nil {
		t.Fatalf("commit %T: %v", cmd, err)
	}
	return res
}

// preparedRuntime returns a runtime advanced to an Executing ModelStep, plus
// stepID and the model grant.
func preparedRuntime(t *testing.T, tools []sdk.ToolDefinition, specs []ToolSpec) (*MemoryRuntime, StepID, ExecutionGrant) {
	t.Helper()
	rt := newTestRuntime(t, RunConfig{Model: "m-1", ModelRejectLimit: 2})
	snap, _ := rt.Load(context.Background())
	req := testRequest(tools...)
	prep, cmdID := buildPrepareFromSnap(t, snap, req, specs)
	mustCommit(t, rt, cmdID, snap.Revision, "", prep)
	start := mustCommit(t, rt, "start-1", 1, "", StartModelExecution{StepID: prep.StepID})
	if start.Grant == "" {
		t.Fatal("accepted start returned no grant")
	}
	return rt, prep.StepID, start.Grant
}

func buildPrepareFromSnap(t *testing.T, snap RuntimeSnapshot, req sdk.Request, specs []ToolSpec) (PrepareModelRequest, CommandID) {
	t.Helper()
	reqDigest, err := DigestRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	toolsDigest, err := DigestToolSpecs(specs)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := DigestModelStepBinding(snap.State.Config.Model, reqDigest, toolsDigest)
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
		StepID: stepID, Model: snap.State.Config.Model, Request: req,
		RequestDigest: reqDigest, InputIDs: ids, Tools: specs, ToolsDigest: toolsDigest,
	}, cmdID
}

func TestConformanceIdempotentReplay(t *testing.T) {
	rt := newTestRuntime(t, RunConfig{Model: "m-1"})
	res1 := mustCommit(t, rt, "cancel-1", 0, "", CancelRun{})
	if res1.Status != CommitAccepted || len(res1.Events) != 1 {
		t.Fatalf("res1 = %+v", res1)
	}
	// Same CommandID + digest replays: AlreadyApplied with the original group.
	res2 := mustCommit(t, rt, "cancel-1", 0, "", CancelRun{})
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
	// Same CommandID, different content: conflict.
	_, err := commitCmd(t, rt, "cancel-1", 0, "", CancelRun{Reason: "other"})
	if err != ErrCommandConflict {
		t.Fatalf("err = %v, want ErrCommandConflict", err)
	}
}

func TestConformanceRevisionAndIndex(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, DirectExecution)
	rt, stepID, grant := preparedRuntime(t, []sdk.ToolDefinition{def}, []ToolSpec{spec})

	b := makeBinding(t, "c1", spec, `{}`)
	res := mustCommit(t, rt, "complete-1", 2, grant,
		SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []ToolCallBinding{b}})
	// One transition, two facts: shared Revision, contiguous Index, snapshot
	// revision equals the transition revision.
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

func TestConformanceStartGrantLifecycle(t *testing.T) {
	rt, stepID, grant := preparedRuntime(t, nil, nil)

	// Completion without grant is stale.
	_, err := commitCmd(t, rt, "done-x", 2, "", SubmitModelResult{StepID: stepID, Result: sdk.ModelResult{}})
	if !errors.Is(err, ErrStaleRuntime) {
		t.Fatalf("grantless completion err = %v, want ErrStaleRuntime", err)
	}
	// Replayed start is AlreadyApplied and must not re-grant.
	res := mustCommit(t, rt, "start-1", 1, "", StartModelExecution{StepID: stepID})
	if res.Status != CommitAlreadyApplied || res.Grant != "" {
		t.Fatalf("replayed start: %+v", res)
	}
	// Owner completes with the minted grant.
	res = mustCommit(t, rt, "done-1", 2, grant, SubmitModelResult{StepID: stepID, Result: sdk.ModelResult{Text: "ok"}})
	if res.Status != CommitAccepted || res.Snapshot.State.Status != RunCompleted {
		t.Fatalf("completion: %+v", res.Snapshot.State.Status)
	}
}

func TestConformanceCallLocalRebase(t *testing.T) {
	defA, defB := testToolDef("a"), testToolDef("b")
	specA := makeSpec(t, defA, DirectExecution)
	specB := makeSpec(t, defB, DirectExecution)
	rt, stepID, grant := preparedRuntime(t, []sdk.ToolDefinition{defA, defB}, []ToolSpec{specA, specB})

	bA := makeBinding(t, "cA", specA, `{}`)
	bA.ToolRef = "a"
	bB := makeBinding(t, "cB", specB, `{}`)
	bB.ToolRef = "b"
	res := mustCommit(t, rt, "complete-1", 2, grant,
		SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("cA", "cB"), Calls: []ToolCallBinding{bA, bB}})
	toolStep := res.Events[1].Fact.(ToolStepOpened).StepID
	base := res.Snapshot.Revision

	// A starts, advancing the revision.
	startA := mustCommit(t, rt, "start-A", base, "", StartToolCall{StepID: toolStep, CallID: "cA"})
	// B starts on the now-stale base revision: call-local rebase accepts it.
	startB := mustCommit(t, rt, "start-B", base, "", StartToolCall{StepID: toolStep, CallID: "cB"})
	if startB.Status != CommitAccepted || startB.Grant == "" {
		t.Fatal("stale-base start of an untouched Pending call must rebase")
	}
	// A's completion on a stale base with A's grant also rebases.
	doneA := mustCommit(t, rt, "done-A", base, startA.Grant,
		SubmitToolResult{StepID: toolStep, CallID: "cA", Result: ToolExecutionResult{Output: json.RawMessage(`1`)}})
	if doneA.Status != CommitAccepted {
		t.Fatal("owner completion on stale base must rebase")
	}
	// Second start of the same call must not rebase (no longer Pending).
	_, err := commitCmd(t, rt, "start-A2", base, "", StartToolCall{StepID: toolStep, CallID: "cA"})
	if !errors.Is(err, ErrStaleRuntime) {
		t.Fatalf("restart of settled call err = %v, want ErrStaleRuntime", err)
	}
	_ = startB
}

func TestConformancePrepareIsHardCAS(t *testing.T) {
	rt := newTestRuntime(t, RunConfig{Model: "m-1"})
	snap, _ := rt.Load(context.Background())
	prep, cmdID := buildPrepareFromSnap(t, snap, testRequest(), nil)
	mustCommit(t, rt, cmdID, snap.Revision, "", prep)

	// A concurrent planner on the same revision with a DIFFERENT request:
	// same derived CommandID, different digest -> conflict.
	otherReq := testRequest()
	otherReq.System = "different"
	prep2, cmdID2 := buildPrepareFromSnap(t, snap, otherReq, nil)
	if cmdID2 != cmdID {
		t.Fatal("same revision must derive the same command id")
	}
	_, err := commitCmd(t, rt, cmdID2, snap.Revision, "", prep2)
	if err != ErrCommandConflict {
		t.Fatalf("err = %v, want ErrCommandConflict", err)
	}
	// The SAME request replays as AlreadyApplied.
	res := mustCommit(t, rt, cmdID, snap.Revision, "", prep)
	if res.Status != CommitAlreadyApplied {
		t.Fatalf("status = %v", res.Status)
	}
}

func TestConformanceCancelRebasesAndUnknownWins(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, DirectExecution)
	rt, stepID, grant := preparedRuntime(t, []sdk.ToolDefinition{def}, []ToolSpec{spec})
	b := makeBinding(t, "c1", spec, `{}`)
	res := mustCommit(t, rt, "complete-1", 2, grant,
		SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []ToolCallBinding{b}})
	toolStep := res.Events[1].Fact.(ToolStepOpened).StepID
	startRes := mustCommit(t, rt, "start-c1", res.Snapshot.Revision, "", StartToolCall{StepID: toolStep, CallID: "c1"})

	// Unknown commits first and terminates the run.
	unknown := mustCommit(t, rt, "unk-1", startRes.Snapshot.Revision, startRes.Grant,
		SubmitToolFailure{StepID: toolStep, CallID: "c1", Outcome: ToolOutcomeUnknown})
	if unknown.Snapshot.State.Status != RunFailed {
		t.Fatal("unknown did not fail the run")
	}
	// Cancel arrives late on an ancient base revision: terminal wins, cancel
	// cannot rewrite the outcome.
	_, err := commitCmd(t, rt, "cancel-late", 0, "", CancelRun{})
	if err != ErrRunTerminal {
		t.Fatalf("late cancel err = %v, want ErrRunTerminal", err)
	}
}

func TestConformanceCancelOnStaleBase(t *testing.T) {
	rt, _, _ := preparedRuntime(t, nil, nil)
	// Cancel with BaseRevision 0 while authority is at 2: run-control rebases.
	res := mustCommit(t, rt, "cancel-1", 0, "", CancelRun{})
	if res.Status != CommitAccepted || res.Snapshot.State.Status != RunStopped {
		t.Fatalf("cancel: %+v", res.Snapshot.State.Status)
	}
}

func TestConformanceReplayFoldMatchesState(t *testing.T) {
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

	// Fold the event log with Evolve from the initial state; it must match
	// the live snapshot (spec §9.1: replay uses only Evolve).
	initial, err := Initialize("run-1", RunConfig{Model: "m-1", ModelRejectLimit: 2},
		NextRun(AgentInput{ID: "seed", Payload: json.RawMessage(`{"q":"hi"}`)}))
	if err != nil {
		t.Fatal(err)
	}
	replayed := initial
	var lastRev uint64
	for _, e := range rt.Events() {
		if e.Revision < lastRev {
			t.Fatal("event log out of order")
		}
		lastRev = e.Revision
		replayed, err = Evolve(replayed, e.Fact)
		if err != nil {
			t.Fatalf("replay Evolve: %v", err)
		}
	}
	live, _ := rt.Load(context.Background())
	a, _ := json.Marshal(stateSnapshotForTest(live.State))
	bts, _ := json.Marshal(stateSnapshotForTest(replayed))
	if string(a) != string(bts) {
		t.Fatalf("replay diverged:\n live   %s\n replay %s", a, bts)
	}
	if live.Revision != lastRev {
		t.Fatalf("snapshot revision %d != last event revision %d", live.Revision, lastRev)
	}
}

func TestConformanceAcceptInputByInputID(t *testing.T) {
	rt := newTestRuntime(t, RunConfig{Model: "m-1"})
	in := AgentInput{ID: "in-9", Payload: json.RawMessage(`{"t":"x"}`)}
	id := DeriveInputCommandID("run-1", in.ID)
	res1 := mustCommit(t, rt, id, 0, "", NextStep(in))
	if res1.Status != CommitAccepted {
		t.Fatal("first accept rejected")
	}
	// Same input replayed through the derived id: AlreadyApplied.
	res2 := mustCommit(t, rt, id, 0, "", NextStep(in))
	if res2.Status != CommitAlreadyApplied {
		t.Fatalf("status = %v", res2.Status)
	}
	// Same InputID, different payload: conflict (derived id collides, digest
	// differs).
	_, err := commitCmd(t, rt, id, 0, "", NextStep(AgentInput{ID: "in-9", Payload: json.RawMessage(`{"t":"y"}`)}))
	if err != ErrCommandConflict {
		t.Fatalf("err = %v, want ErrCommandConflict", err)
	}
}

func TestConformanceDerivedCommandIDEnforced(t *testing.T) {
	// Derived-identity families cannot bypass their idempotency index with a
	// caller-minted CommandID (spec §5.5).
	rt := newTestRuntime(t, RunConfig{Model: "m-1"})
	_, err := commitCmd(t, rt, "random-id", 0, "", NextStep(AgentInput{ID: "in-1", Payload: json.RawMessage(`1`)}))
	if err == nil {
		t.Fatal("AcceptInput with non-derived CommandID accepted")
	}
	// Response commands are checked the same way.
	_, err = commitCmd(t, rt, "random-id-2", 0, "", ApproveToolCall{StepID: "s", CallID: "c", ResponseID: "r"})
	if err == nil {
		t.Fatal("ApproveToolCall with non-derived CommandID accepted")
	}
}

func TestConformancePrepareRejectionDoesNotLivelock(t *testing.T) {
	// A planner that omits pending InputIDs produces a Prepare the authority
	// rejects at the same revision forever; the Loop must surface an error
	// instead of spinning (loop.go planAndPrepare guard).
	rt := newTestRuntime(t, RunConfig{Model: "m-1"})
	loop, err := NewLoop(fakeCatalog{&fakeInvoker{}}, fakeToolCatalog{},
		badPlanner{}, ExecutionPolicy{}, false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = loop.Run(context.Background(), rt, nil)
	if err == nil {
		t.Fatal("prepare livelock not surfaced")
	}
}

// badPlanner never consumes pending inputs, so its Prepare is always rejected.
type badPlanner struct{}

func (badPlanner) Plan(_ context.Context, hint PlanningHint) (RequestPlan, error) {
	return RequestPlan{Model: hint.Model, Request: sdk.Request{Model: string(hint.Model)}}, nil
}
