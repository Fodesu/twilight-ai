package runtimetest

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/memohai/twilight/agent/artifact"
	"github.com/memohai/twilight/agent/es"
	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/session"
	"github.com/memohai/twilight/agent/session/chatlog"
	"github.com/memohai/twilight/agent/session/extension"
	runmod "github.com/memohai/twilight/agent/session/run"
	"github.com/memohai/twilight/agent/turn"
)

// Run executes the RUN-CMP-2 Runtime conformance suite against stores made
// by newStore.
func Run(t *testing.T, newStore Factory) {
	t.Helper()
	for name, fn := range map[string]func(*testing.T, Factory){
		"Creation":           testCreation,
		"ReplayAndBase":      testReplayAndBase,
		"InputQueue":         testInputQueue,
		"Grants":             testGrants,
		"CommitComposition":  testCommitComposition,
		"Admission":          testAdmission,
		"SettlementSnapshot": testSettlementSnapshot,
		"PrepareCAS":         testPrepareCASIgnoresOtherModules,
		"LeaseAndCommit":     testLeaseAndCommit,
		"Projection":         testProjection,
		"Isolation":          testIsolation,
		"ExpiryRecovery":     testExpiryRecovery,
		"Renewal":            testRenewal,
		"FrozenValues":       testFrozenValues,
	} {
		t.Run(name, func(t *testing.T) { fn(t, newStore) })
	}
}

// --- 建立与寻址 -----------------------------------------------------------------------

func testCreation(t *testing.T, newStore Factory) {
	h := newHarness(t, newStore(t), 0)
	h.startRun("t1", "r1", input("in-1"))
	snap := h.load("r1")
	if snap.State.Owner != "t1" || snap.State.Attempt != 1 || len(snap.State.PendingInputs) != 1 || snap.SchemaVersion != run.SchemaVersion1 {
		t.Fatalf("created state = %+v", snap.State)
	}
	if snap.Position.Revision != h.head().Revision || snap.Position.Index != 3 {
		t.Fatalf("position = %+v, want revision %d index 3 (started, delivered, created, accepted)", snap.Position, h.head().Revision)
	}
	// Unknown RunID.
	if _, err := h.rt.Load(h.ctx, sid, "nope"); !errors.Is(err, run.ErrRunNotFound) {
		t.Fatalf("load unknown = %v", err)
	}
	if _, err := h.rt.Record(h.ctx, sid, "nope"); !errors.Is(err, run.ErrRunNotFound) {
		t.Fatalf("record unknown = %v", err)
	}
	env, _ := run.ProtocolV1().BuildEnvelope(sid, "nope", run.DeriveInputCommandID("nope", "x"), run.AcceptInput{Input: input("x")})
	if _, err := h.rt.Commit(h.ctx, sid, run.CommitRequest{Command: env}); !errors.Is(err, run.ErrRunNotFound) {
		t.Fatalf("commit unknown = %v", err)
	}
	// Schema disagreement is a hard error, not a retriable rejection.
	env, _ = run.ProtocolV1().BuildEnvelope(sid, "r1", run.DeriveInputCommandID("r1", "in-2"), run.AcceptInput{Input: input("in-2")})
	env.SchemaVersion = 2
	_, err := h.rt.Commit(h.ctx, sid, run.CommitRequest{Command: env})
	if err == nil || errors.Is(err, run.ErrStaleRuntime) || errors.Is(err, run.ErrCommandConflict) {
		t.Fatalf("schema mismatch = %v, want a non-retriable error", err)
	}
	// Terminated Run: Load returns the terminal state, Commit is terminal.
	h.mustCommit("r1", "cancel-1", run.RunPosition{}, "", run.CancelRun{})
	term := h.load("r1")
	if term.State.Status != run.RunStopped || term.State.Result == nil {
		t.Fatalf("terminal load = %+v", term.State)
	}
	rec := h.record("r1")
	if !run.StatesEquivalent(&rec.Snapshot.State, &term.State) || rec.Snapshot.Position != term.Position {
		t.Fatal("terminal Load and Record disagree")
	}
	if _, err := h.commit("r1", run.DeriveInputCommandID("r1", "late"), run.RunPosition{}, "", run.AcceptInput{Input: input("late")}); !errors.Is(err, run.ErrRunTerminal) {
		t.Fatalf("commit on terminal = %v", err)
	}
	// A second created for the same RunID breaks the fold.
	h.mustApply(h.startGroup("t2", "r1", 1))
	if _, err := h.rt.Load(h.ctx, sid, "r1"); err == nil {
		t.Fatal("duplicate created folded silently")
	}
}

// --- 重放与 Base ----------------------------------------------------------------------

func testReplayAndBase(t *testing.T, newStore Factory) {
	h := newHarness(t, newStore(t), 0)
	h.startRun("t1", "r1", input("in-1"))
	first := h.mustCommit("r1", run.DeriveInputCommandID("r1", "in-2"), run.RunPosition{}, "", run.AcceptInput{Input: input("in-2")})
	if first.Status != run.CommitAccepted {
		t.Fatal("first accept not accepted")
	}
	again := h.mustCommit("r1", run.DeriveInputCommandID("r1", "in-2"), run.RunPosition{}, "", run.AcceptInput{Input: input("in-2")})
	if again.Status != run.CommitAlreadyApplied || again.Commit.CommitID != first.Commit.CommitID || again.Commit.CommitDigest != first.Commit.CommitDigest {
		t.Fatalf("replay = %+v", again)
	}
	if h.head().Revision != first.Commit.Revision {
		t.Fatal("replay appended a commit")
	}
	// Prepare is a hard CAS on the Run's own position.
	snap := h.load("r1")
	stale := run.RunPosition{Revision: snap.Position.Revision - 1}
	cmd, id := h.preparedCommand(run.RuntimeSnapshot{State: snap.State, Position: stale, SchemaVersion: snap.SchemaVersion}, false)
	if _, err := h.commit("r1", id, stale, "", cmd); !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("stale prepare = %v", err)
	}
	cmd, id = h.preparedCommand(snap, false)
	prepared := h.mustCommit("r1", id, snap.Position, "", cmd)
	// Non-prepare commands accept a zero or stale Base (call-local rebase).
	h.mustCommit("r1", run.DeriveInputCommandID("r1", "in-3"), run.RunPosition{}, "", run.AcceptInput{Input: input("in-3")})
	// Terminal replay: an accepted command replays after termination.
	h.mustCommit("r1", "cancel", prepared.Snapshot.Position, "", run.CancelRun{})
	replay := h.mustCommit("r1", run.DeriveInputCommandID("r1", "in-3"), run.RunPosition{}, "", run.AcceptInput{Input: input("in-3")})
	if replay.Status != run.CommitAlreadyApplied || !replay.Snapshot.State.Status.Terminal() {
		t.Fatalf("terminal replay = %+v", replay)
	}
	if _, err := h.commit("r1", run.DeriveInputCommandID("r1", "in-4"), run.RunPosition{}, "", run.AcceptInput{Input: input("in-4")}); !errors.Is(err, run.ErrRunTerminal) {
		t.Fatalf("new command after terminal = %v", err)
	}
	// Derived-identity families must use their derived CommandID.
	if _, err := h.commit("r1", "random", run.RunPosition{}, "", run.AcceptInput{Input: input("in-5")}); !errors.Is(err, run.ErrCommandConflict) && !errors.Is(err, run.ErrRunTerminal) {
		t.Fatalf("non-derived id = %v", err)
	}
}

// --- 输入入队 --------------------------------------------------------------------------

func testInputQueue(t *testing.T, newStore Factory) {
	h := newHarness(t, newStore(t), 0)
	h.startRun("t1", "r1", input("in-1"))
	// Prepared: the input queues and Next asks to withdraw.
	step := h.prepare("r1", false)
	h.mustCommit("r1", run.DeriveInputCommandID("r1", "in-2"), run.RunPosition{}, "", run.AcceptInput{Input: input("in-2")})
	snap := h.load("r1")
	eff, err := run.Next(snap.State)
	if err != nil {
		t.Fatal(err)
	}
	if w, ok := eff.(run.WithdrawPrepared); !ok || w.StepID != step {
		t.Fatalf("effect while Prepared with input = %#v", eff)
	}
	h.mustCommit("r1", run.DeriveWithdrawCommandID("r1", step), snap.Position, "", run.WithdrawPreparedStep{StepID: step})
	snap = h.load("r1")
	if _, open := snap.State.Current.(run.Open); !open || len(snap.State.PendingInputs) != 1 || snap.State.ModelSteps != 0 {
		t.Fatalf("after withdraw = %+v", snap.State)
	}
	cmd, id := h.preparedCommand(snap, false)
	if len(cmd.InputIDs) != 1 || cmd.InputIDs[0] != "in-2" {
		t.Fatalf("replanned prepare consumes %v", cmd.InputIDs)
	}
	h.mustCommit("r1", id, snap.Position, "", cmd)
	// Executing: the input queues; a result without calls reopens instead of ending.
	grant, claim := h.startModel("r1", cmd.StepID)
	h.mustCommit("r1", run.DeriveInputCommandID("r1", "in-3"), run.RunPosition{}, "", run.AcceptInput{Input: input("in-3")})
	res := h.mustCommit("r1", run.DeriveSettlementCommandID("r1", cmd.StepID, "", claim), run.RunPosition{}, grant, run.SubmitModelResult{StepID: cmd.StepID, Result: textResult("a")})
	if res.Snapshot.State.Status != run.RunActive {
		t.Fatal("run ended with a pending input")
	}
	if _, open := res.Snapshot.State.Current.(run.Open); !open || len(res.Snapshot.State.PendingInputs) != 1 {
		t.Fatalf("after result with pending input = %+v", res.Snapshot.State)
	}
	// ToolStep: the input queues as well.
	h2 := newHarness(t, newStore(t), 0)
	h2.startRun("t1", "r2", input("in-1"))
	h2.openToolStep("r2", 1)
	res = h2.mustCommit("r2", run.DeriveInputCommandID("r2", "in-9"), run.RunPosition{}, "", run.AcceptInput{Input: input("in-9")})
	if _, ok := res.Snapshot.State.Current.(run.ToolStep); !ok || len(res.Snapshot.State.PendingInputs) != 1 {
		t.Fatalf("accept on tool step = %+v", res.Snapshot.State)
	}
}

// --- grant -----------------------------------------------------------------------------

func testGrants(t *testing.T, newStore Factory) {
	h := newHarness(t, newStore(t), 0)
	h.startRun("t1", "r1", input("in-1"))
	h.startRun("t2", "r2", input("in-1"))
	step, grant, claim := h.executingModel("r1", false)
	lease, ok := h.lease("r1", step, "")
	if !ok || run.ExecutionGrant(lease.Token) != grant || lease.Holder != string(claim) {
		t.Fatalf("lease = %+v ok=%v, grant %s", lease, ok, grant)
	}
	// Same-claim replay returns the same grant; another claim is refused.
	replay := h.mustCommit("r1", run.DeriveStartCommandID("r1", step, "", claim), run.RunPosition{}, "", run.StartModelExecution{StepID: step, Claim: claim})
	if replay.Status != run.CommitAlreadyApplied || replay.Grant != grant {
		t.Fatalf("start replay = %+v", replay)
	}
	other := h.claim()
	if _, err := h.commit("r1", run.DeriveStartCommandID("r1", step, "", other), run.RunPosition{}, "", run.StartModelExecution{StepID: step, Claim: other}); !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("second claim start = %v", err)
	}
	// Settlement needs the live grant of its own target.
	settle := run.SubmitModelResult{StepID: step, Result: textResult("done")}
	settleID := run.DeriveSettlementCommandID("r1", step, "", claim)
	if _, err := h.commit("r1", settleID, run.RunPosition{}, "", settle); !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("grantless settlement = %v", err)
	}
	if _, err := h.commit("r1", settleID, run.RunPosition{}, "wrong", settle); !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("wrong grant = %v", err)
	}
	step2, grant2, _ := h.executingModel("r2", false)
	if _, err := h.commit("r1", settleID, run.RunPosition{}, grant2, settle); !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("cross-run grant = %v", err)
	}
	_ = step2
	res := h.mustCommit("r1", settleID, run.RunPosition{}, grant, settle)
	if !res.Snapshot.State.Status.Terminal() {
		t.Fatal("settlement did not end the run")
	}
	// After settlement the start replays with an empty grant and the lease is gone.
	replay = h.mustCommit("r1", run.DeriveStartCommandID("r1", step, "", claim), run.RunPosition{}, "", run.StartModelExecution{StepID: step, Claim: claim})
	if replay.Status != run.CommitAlreadyApplied || replay.Grant != "" {
		t.Fatalf("start replay after settlement = %+v", replay)
	}
	if _, ok := h.lease("r1", step, ""); ok {
		t.Fatal("lease survived settlement")
	}
}

// --- commit 组成 ----------------------------------------------------------------------------

func testCommitComposition(t *testing.T, newStore Factory) {
	h := newHarness(t, newStore(t), 0)
	h.startRun("t1", "r1", input("in-1"))
	step, grant, claim := h.executingModel("r1", true)
	result, bindings := h.toolCallResult(step, 1)
	before := h.head()
	res := h.mustCommit("r1", run.DeriveSettlementCommandID("r1", step, "", claim), run.RunPosition{}, grant,
		run.SubmitModelResult{StepID: step, Result: result, Calls: bindings})
	if h.head().Revision != before.Revision+1 {
		t.Fatal("one command did not produce exactly one commit")
	}
	types := eventTypes(&res.Commit)
	want := []session.EventType{runmod.Prefix + "model_step_completed", runmod.Prefix + "tool_step_opened", chatlog.TypeAssistant}
	if strings.Join(asStrings(types), ",") != strings.Join(asStrings(want), ",") {
		t.Fatalf("commit events = %v, want %v", types, want)
	}
	// The companion's SourceDigest equals the fact's ResultDigest.
	var resultDigest es.Digest
	for _, f := range h.record("r1").Facts {
		if c, ok := f.(run.ModelStepCompleted); ok {
			resultDigest = c.ResultDigest
		}
	}
	decoded, err := h.registry.Decode(res.Commit.Events[2])
	if err != nil {
		t.Fatal(err)
	}
	if a := decoded.Value.(chatlog.AssistantPayload).Assistant; a.SourceDigest != resultDigest || a.TurnID != "t1" {
		t.Fatalf("assistant = %+v, want SourceDigest %s", a, resultDigest)
	}
	// Attach follows the companion; twilight/run/ events are refused.
	ts := res.Snapshot.State.Current.(run.ToolStep)
	call := ts.Calls[0].CallID
	toolGrant, toolClaim := h.startTool("r1", ts.RefValue.ID, call)
	output := run.MustParseCanonicalJSON(`{"ok":true}`)
	if _, err := h.commit("r1", run.DeriveSettlementCommandID("r1", ts.RefValue.ID, call, toolClaim), run.RunPosition{}, toolGrant,
		run.SubmitToolResult{StepID: ts.RefValue.ID, CallID: call, Result: run.ToolExecutionResult{Output: output}},
		run.ModuleEvent{Type: runmod.Prefix + "input_accepted", Value: runmod.Event{RunID: "r1", Fact: run.InputAccepted{Input: input("x")}}}); err == nil {
		t.Fatal("Attach with a twilight/run/ event accepted")
	}
	res = h.mustCommit("r1", run.DeriveSettlementCommandID("r1", ts.RefValue.ID, call, toolClaim), run.RunPosition{}, toolGrant,
		run.SubmitToolResult{StepID: ts.RefValue.ID, CallID: call, Result: run.ToolExecutionResult{Output: output}},
		run.ModuleEvent{Type: chatlog.TypeInputDelivered, Value: chatlog.InputDeliveredPayload{InputID: "in-attach", TurnID: "t1"}})
	types = eventTypes(&res.Commit)
	if len(types) != 3 || types[0] != runmod.Prefix+"tool_call_completed" || types[1] != chatlog.TypeToolResult || types[2] != chatlog.TypeInputDelivered {
		t.Fatalf("commit events = %v", types)
	}
	outputDigest, _ := run.ProtocolV1().DigestToolOutput(output)
	decoded, _ = h.registry.Decode(res.Commit.Events[1])
	if r := decoded.Value.(chatlog.ToolResultPayload).ToolResult; r.SourceDigest != outputDigest || r.Status != chatlog.ToolSuccess {
		t.Fatalf("tool_result = %+v", r)
	}
}

func asStrings(types []session.EventType) []string {
	out := make([]string, len(types))
	for i, t := range types {
		out[i] = string(t)
	}
	return out
}

// --- admission -------------------------------------------------------------------------------

func testAdmission(t *testing.T, newStore Factory) {
	h := newHarness(t, newStore(t), 0)
	h.startRun("t1", "r1", input("in-1"))
	ref := artifact.Ref{Scheme: "cas", Authority: "local", Key: "k1", Durability: artifact.EventBound, Integrity: &artifact.Integrity{Algorithm: "sha256", Value: "x"}}
	binding, err := artifact.NewBinding("b1", ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.bindings.CreateBinding(h.ctx, binding); err != nil {
		t.Fatal(err)
	}
	attach := func(bindingID artifact.BindingID) run.ModuleEvent {
		a := chatlog.Assistant{ID: "a-attach", TurnID: "t1", Parts: chatlog.Parts{chatlog.ReferencePart{BindingID: bindingID, Name: "f"}}}
		a.Digest, _ = chatlog.DigestAssistant(&a)
		return run.ModuleEvent{Type: chatlog.TypeAssistant, Value: chatlog.AssistantPayload{Assistant: a}}
	}
	before := h.head()
	// Unregistered binding: the whole commit is refused and nothing is written.
	if _, err := h.commit("r1", run.DeriveInputCommandID("r1", "in-2"), run.RunPosition{}, "", run.AcceptInput{Input: input("in-2")}, attach("missing")); err == nil {
		t.Fatal("unregistered binding admitted")
	}
	if h.head() != before {
		t.Fatal("refused commit wrote to the stream")
	}
	if len(h.load("r1").State.PendingInputs) != 1 {
		t.Fatal("refused commit changed the Run")
	}
	// Registered binding: commit and claim in the same transaction.
	res := h.mustCommit("r1", run.DeriveInputCommandID("r1", "in-2"), run.RunPosition{}, "", run.AcceptInput{Input: input("in-2")}, attach("b1"))
	claimID := extension.DeriveClaimID(session.ProtocolVersion1, sid, res.Commit.CommitID, mustSet(t, h, "b1").RefSetDigest)
	entry, ok, err := h.store.ControlGet(h.ctx, sid, extension.ClaimNamespace, string(claimID))
	if err != nil || !ok || !strings.Contains(string(entry.Value), `"active"`) {
		t.Fatalf("claim entry = %s ok=%v err=%v", entry.Value, ok, err)
	}
}

func mustSet(t *testing.T, h *harness, ids ...artifact.BindingID) artifact.BindingSet {
	t.Helper()
	set, err := artifact.SetBuilder{Resolver: h.bindings}.Build(h.ctx, ids)
	if err != nil {
		t.Fatal(err)
	}
	return set
}

// --- 结算返回值 --------------------------------------------------------------------------------

func testSettlementSnapshot(t *testing.T, newStore Factory) {
	h := newHarness(t, newStore(t), 0)
	h.startRun("t1", "r1", input("in-1"))
	step, grant, claim := h.executingModel("r1", false)
	res := h.mustCommit("r1", run.DeriveSettlementCommandID("r1", step, "", claim), run.RunPosition{}, grant, run.SubmitModelResult{StepID: step, Result: textResult("done")})
	if res.Snapshot.State.Status != run.RunCompleted || res.Snapshot.State.Result == nil || res.Snapshot.State.Result.Status != run.RunCompleted {
		t.Fatalf("settlement snapshot = %+v", res.Snapshot.State)
	}
	rec := h.record("r1")
	if !run.StatesEquivalent(&rec.Snapshot.State, &res.Snapshot.State) || rec.Snapshot.Position != res.Snapshot.Position {
		t.Fatalf("settlement snapshot %+v disagrees with record %+v", res.Snapshot, rec.Snapshot)
	}
	// The completed turn is settled in the same commit (companion).
	types := eventTypes(&res.Commit)
	if types[len(types)-1] != turn.TypeCompleted {
		t.Fatalf("terminal commit events = %v, want turn/completed last", types)
	}
}

// --- Prepare hard CAS 对其他模块不敏感 ---------------------------------------------------------------

func testPrepareCASIgnoresOtherModules(t *testing.T, newStore Factory) {
	h := newHarness(t, newStore(t), 0)
	h.startRun("t1", "r1", input("in-1"))
	h.startRun("t2", "r2", input("in-1"))
	snap := h.load("r1")
	cmd, id := h.preparedCommand(snap, false)
	// Other modules and another Run write after the planner loaded.
	h.submitInputs(input("late"))
	h.prepare("r2", false)
	after := h.load("r1")
	if after.Position != snap.Position {
		t.Fatalf("foreign writes moved r1 position %+v -> %+v", snap.Position, after.Position)
	}
	if after.Head == snap.Head {
		t.Fatal("session head did not move")
	}
	if _, err := h.commit("r1", id, snap.Position, "", cmd); err != nil {
		t.Fatalf("prepare against a moved session head: %v", err)
	}
}

// --- 租约与 commit -------------------------------------------------------------------------------

func testLeaseAndCommit(t *testing.T, newStore Factory) {
	h := newHarness(t, newStore(t), 0)
	h.startRun("t1", "r1", input("in-1"))
	step, ids := h.openToolStep("r1", 2)
	g0, c0 := h.startTool("r1", step, ids[0])
	if l, ok := h.lease("r1", step, ids[0]); !ok || l.Holder != string(c0) || run.ExecutionGrant(l.Token) != g0 {
		t.Fatalf("lease after start = %+v ok=%v", l, ok)
	}
	h.mustCommit("r1", run.DeriveSettlementCommandID("r1", step, ids[0], c0), run.RunPosition{}, g0,
		run.SubmitToolResult{StepID: step, CallID: ids[0], Result: run.ToolExecutionResult{Output: run.MustParseCanonicalJSON(`1`)}})
	if _, ok := h.lease("r1", step, ids[0]); ok {
		t.Fatal("lease survived settlement")
	}
	// Terminal commit releases every live lease of the Run.
	h.startTool("r1", step, ids[1])
	h.mustCommit("r1", "cancel", run.RunPosition{}, "", run.CancelRun{})
	if _, ok := h.lease("r1", step, ids[1]); ok {
		t.Fatal("lease survived the terminal commit")
	}
	if got := h.load("r1").State.Result.UncertainCalls; len(got) != 1 || got[0] != ids[1] {
		t.Fatalf("uncertain calls = %v", got)
	}
}

// --- 投影 -------------------------------------------------------------------------------------------

func testProjection(t *testing.T, newStore Factory) {
	h := newHarness(t, newStore(t), 0)
	h.startRun("t1", "r1", input("in-1"))
	snapshotOf := func() session.SnapshotResult {
		res, err := h.store.LoadSnapshot(h.ctx, session.SnapshotRequest{SessionID: sid, ProjectionKey: session.ProjectionKey(runmod.MachineProjectionID), ProjectionVersion: 1})
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	step, _, _ := h.executingModel("r1", true)
	if snapshotOf().Found {
		t.Fatal("snapshot written while the Run is mid-step")
	}
	toolStep, ids := func() (run.StepID, []run.CallID) {
		g, _ := h.lease("r1", step, "")
		result, bindings := h.toolCallResult(step, 1)
		res := h.mustCommit("r1", run.DeriveSettlementCommandID("r1", step, "", run.ExecutionClaim(g.Holder)), run.RunPosition{}, run.ExecutionGrant(g.Token),
			run.SubmitModelResult{StepID: step, Result: result, Calls: bindings})
		return res.Snapshot.State.Current.(run.ToolStep).RefValue.ID, []run.CallID{bindings[0].CallID}
	}()
	grant, claim := h.startTool("r1", toolStep, ids[0])
	res := h.mustCommit("r1", run.DeriveSettlementCommandID("r1", toolStep, ids[0], claim), run.RunPosition{}, grant,
		run.SubmitToolResult{StepID: toolStep, CallID: ids[0], Result: run.ToolExecutionResult{Output: run.MustParseCanonicalJSON(`1`)}})
	if _, open := res.Snapshot.State.Current.(run.Open); !open {
		t.Fatalf("after tool settlement current = %T", res.Snapshot.State.Current)
	}
	snap := snapshotOf()
	if !snap.Found || snap.Snapshot.Through.Revision != res.Commit.Revision-1 {
		t.Fatalf("snapshot after return to Open = %+v, want Through = commit revision - 1", snap.Snapshot)
	}
	// Projection state, Load and Record agree for the active Run.
	m := h.machine()
	loaded := h.load("r1")
	rec := h.record("r1")
	if active, ok := m.Active["r1"]; !ok || !run.StatesEquivalent(&active, &loaded.State) || !run.StatesEquivalent(&active, &rec.Snapshot.State) {
		t.Fatal("projection, Load and Record disagree")
	}
	// Terminal Run leaves the projection; Load and Record still answer.
	h.mustCommit("r1", "cancel", run.RunPosition{}, "", run.CancelRun{})
	if _, still := h.machine().Active["r1"]; still {
		t.Fatal("terminal run still in the projection")
	}
	if h.load("r1").State.Status != run.RunStopped || h.record("r1").Snapshot.State.Status != run.RunStopped {
		t.Fatal("terminal run not readable")
	}
	// An illegal fact sequence does not fold.
	if _, err := run.FoldRun([]run.Fact{run.InputAccepted{Input: input("x")}}); err == nil {
		t.Fatal("fold without created succeeded")
	}
}

// --- 隔离 --------------------------------------------------------------------------------------------

func testIsolation(t *testing.T, newStore Factory) {
	h := newHarness(t, newStore(t), 0)
	h.startRun("t1", "r1", input("in-1"))
	h.startRun("t2", "r2", input("in-1"))
	p1 := h.load("r1").Position
	h.prepare("r2", false)
	h.submitInputs(input("noise"))
	h.mustApply(extension.SemanticGroup{CommitID: "turn-noise", Events: []extension.TypedEvent{{Type: turn.TypeStarted, RecordedAtUnixMilli: 1,
		Value: turn.StartedPayload{TurnID: "t9", ExecutionBinding: turn.ExecutionBindingRef{ID: "b", Digest: "sha256:b"}, Companion: turn.CompanionV1Version, PlanDigest: "sha256:p"}}}})
	if h.load("r1").Position != p1 {
		t.Fatal("r2, chatlog or turn writes moved r1")
	}
	for _, f := range h.record("r1").Facts {
		if c, ok := f.(run.RunCreated); ok && c.RunID != "r1" {
			t.Fatal("record of r1 contains another run")
		}
	}
	if len(h.record("r1").Facts) != 2 || len(h.record("r2").Facts) != 3 {
		t.Fatalf("facts r1=%d r2=%d", len(h.record("r1").Facts), len(h.record("r2").Facts))
	}
}

// --- lease 过期 recovery ----------------------------------------------------------------------------

func testExpiryRecovery(t *testing.T, newStore Factory) {
	const ttl = time.Minute
	h := newHarness(t, newStore(t), ttl)
	h.startRun("t1", "r1", input("in-1"))
	h.startRun("t2", "r2", input("in-1"))
	// r1: executing model; r2: two executing tool calls started at different times.
	h.executingModel("r1", false)
	requestDigest := h.load("r1").State.Current.(run.ModelStep).RequestDigest
	toolStep, ids := h.openToolStep("r2", 2)
	_, c0 := h.startTool("r2", toolStep, ids[0])
	h.clock.Advance(ttl / 2)
	h.startTool("r2", toolStep, ids[1])

	// Live leases refuse grantless recovery and RecoverExpired finds nothing.
	if n, err := h.rt.RecoverExpired(h.ctx); err != nil || n != 0 {
		t.Fatalf("recover with live leases = %d %v", n, err)
	}
	unknown := run.SubmitToolFailure{StepID: toolStep, CallID: ids[0], Failure: run.ToolFailure{Class: run.FailureEffectUnknown}, Outcome: run.ToolOutcomeUnknown}
	if _, err := h.commit("r2", run.DeriveToolRecoveryCommandID("r2", toolStep, ids[0], c0), run.RunPosition{}, "", unknown); !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("grantless unknown on a live lease = %v", err)
	}
	// Expire r1's model lease and r2's first call only.
	h.clock.Advance(ttl/2 + time.Second)
	// A recovery CommandID derived from another holder is refused.
	if _, err := h.commit("r2", run.DeriveToolRecoveryCommandID("r2", toolStep, ids[0], "intruder"), run.RunPosition{}, "", unknown); !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("recovery with a foreign holder = %v", err)
	}
	n, err := h.rt.RecoverExpired(h.ctx)
	if err != nil || n != 2 {
		t.Fatalf("recovered = %d %v, want 2", n, err)
	}
	r1 := h.load("r1").State.Current.(run.ModelStep)
	if r1.Status != run.ModelPrepared || r1.RequestDigest != requestDigest {
		t.Fatalf("model after recovery = %+v", r1)
	}
	if _, err := h.rt.FrozenRequest(h.ctx, requestDigest); err != nil {
		t.Fatalf("frozen request after recovery: %v", err)
	}
	ts := h.load("r2").State.Current.(run.ToolStep)
	if ts.Calls[0].Status != run.ToolFailed || ts.Calls[0].Failure == nil || ts.Calls[0].Failure.Outcome != run.ToolOutcomeUnknown {
		t.Fatalf("expired call = %+v", ts.Calls[0])
	}
	if ts.Calls[1].Status != run.ToolExecuting {
		t.Fatalf("sibling call = %+v, want still Executing", ts.Calls[1])
	}
	if _, ok := h.lease("r2", toolStep, ids[0]); ok {
		t.Fatal("recovered lease not deleted")
	}
	if _, ok := h.lease("r2", toolStep, ids[1]); !ok {
		t.Fatal("sibling lease deleted")
	}
	if n, err := h.rt.RecoverExpired(h.ctx); err != nil || n != 0 {
		t.Fatalf("second RecoverExpired = %d %v, want 0", n, err)
	}
	// TTL zero: recovery never runs.
	h0 := newHarness(t, newStore(t), 0)
	h0.startRun("t1", "r1", input("in-1"))
	h0.executingModel("r1", false)
	h0.clock.Advance(time.Hour)
	if n, err := h0.rt.RecoverExpired(h0.ctx); err != nil || n != 0 {
		t.Fatalf("RecoverExpired with TTL 0 = %d %v", n, err)
	}
}

// --- lease 续期 ---------------------------------------------------------------------------------------

func testRenewal(t *testing.T, newStore Factory) {
	const ttl = time.Minute
	h := newHarness(t, newStore(t), ttl)
	h.startRun("t1", "r1", input("in-1"))
	step, grant, claim := h.executingModel("r1", false)
	h.clock.Advance(ttl / 2)
	if err := h.rt.RenewLease(h.ctx, sid, "r1", step, "", grant); err != nil {
		t.Fatalf("renew: %v", err)
	}
	h.clock.Advance(ttl/2 + time.Second) // past the original deadline, inside the renewed one
	if n, _ := h.rt.RecoverExpired(h.ctx); n != 0 {
		t.Fatal("renewed lease was recovered at its original deadline")
	}
	if err := h.rt.RenewLease(h.ctx, sid, "r1", step, "", ""); !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("renew with empty grant = %v", err)
	}
	if err := h.rt.RenewLease(h.ctx, sid, "r1", step, "", "wrong"); !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("renew with wrong grant = %v", err)
	}
	h.mustCommit("r1", run.DeriveSettlementCommandID("r1", step, "", claim), run.RunPosition{}, grant, run.SubmitModelResult{StepID: step, Result: textResult("done")})
	if err := h.rt.RenewLease(h.ctx, sid, "r1", step, "", grant); !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("renew after settlement = %v", err)
	}
	// TTL zero: renewal only validates the grant.
	h0 := newHarness(t, newStore(t), 0)
	h0.startRun("t1", "r1", input("in-1"))
	step0, grant0, _ := h0.executingModel("r1", false)
	if err := h0.rt.RenewLease(h0.ctx, sid, "r1", step0, "", grant0); err != nil {
		t.Fatalf("renew with TTL 0: %v", err)
	}
	if l, _ := h0.lease("r1", step0, ""); l.DeadlineUnixMilli != 0 {
		t.Fatalf("TTL 0 renew set a deadline: %d", l.DeadlineUnixMilli)
	}
}

// --- FrozenValueStore --------------------------------------------------------------------------------

func testFrozenValues(t *testing.T, newStore Factory) {
	h := newHarness(t, newStore(t), 0)
	h.startRun("t1", "r1", input("in-1"))
	step, grant, claim := h.executingModel("r1", false)
	digest := h.load("r1").State.Current.(run.ModelStep).RequestDigest
	body, _, err := h.frozen.Get(h.ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.frozen.Put(h.ctx, digest, body); err != nil {
		t.Fatalf("idempotent put: %v", err)
	}
	if _, err := h.rt.FrozenRequest(h.ctx, "sha256:unknown"); !errors.Is(err, run.ErrFrozenValueMissing) {
		t.Fatalf("unknown digest = %v", err)
	}
	h.mustCommit("r1", run.DeriveSettlementCommandID("r1", step, "", claim), run.RunPosition{}, grant, run.SubmitModelResult{StepID: step, Result: textResult("done")})
	h.frozen.Delete(digest)
	if _, err := h.rt.Record(h.ctx, sid, "r1"); err != nil {
		t.Fatalf("record after dropping the settled body: %v", err)
	}
}
