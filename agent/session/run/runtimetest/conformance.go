package runtimetest

import (
	"errors"
	"strings"
	"testing"

	"github.com/memohai/twilight/agent/artifact"
	"github.com/memohai/twilight/agent/es"
	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/session"
	"github.com/memohai/twilight/agent/session/chatlog"
	"github.com/memohai/twilight/agent/session/extension"
	runmod "github.com/memohai/twilight/agent/session/run"
	"github.com/memohai/twilight/agent/turn"
)

// Run executes the RUN-CMP-2 Runtime conformance suite against fixtures made
// by factory.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	for name, fn := range map[string]func(*testing.T, Factory){
		"Creation":           testCreation,
		"ReplayAndBase":      testReplayAndBase,
		"InputQueue":         testInputQueue,
		"StartAndClaim":      testStartAndClaim,
		"GroupComposition":   testGroupComposition,
		"Admission":          testAdmission,
		"SettlementSnapshot": testSettlementSnapshot,
		"PrepareCAS":         testPrepareCASIgnoresOtherModules,
		"Projection":         testProjection,
		"Isolation":          testIsolation,
		"Takeover":           testTakeover,
		"OwnershipLost":      testOwnershipLost,
		"FrozenValues":       testFrozenValues,
	} {
		t.Run(name, func(t *testing.T) { fn(t, factory) })
	}
}

// --- 建立与寻址 -----------------------------------------------------------------------

func testCreation(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startRun("t1", "r1", input("in-1"))
	snap := h.load("r1")
	if snap.State.Owner != "t1" || snap.State.Attempt != 1 || len(snap.State.PendingInputs) != 1 || snap.SchemaVersion != run.SchemaVersion1 {
		t.Fatalf("created state = %+v", snap.State)
	}
	// Position is the Seq of the Run's last row: the start group is submitted
	// (0), started (1), delivered (2), created (3), accepted (4).
	if snap.Position != h.head().Next-1 || snap.Position != 4 {
		t.Fatalf("position = %d, want %d (last row of the start group)", snap.Position, h.head().Next-1)
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
	h.mustCommit("r1", "cancel-1", 0, run.CancelRun{})
	term := h.load("r1")
	if term.State.Status != run.RunStopped || term.State.Result == nil {
		t.Fatalf("terminal load = %+v", term.State)
	}
	rec := h.record("r1")
	if !run.StatesEquivalent(&rec.Snapshot.State, &term.State) || rec.Snapshot.Position != term.Position {
		t.Fatal("terminal Load and Record disagree")
	}
	if _, err := h.commit("r1", run.DeriveInputCommandID("r1", "late"), 0, run.AcceptInput{Input: input("late")}); !errors.Is(err, run.ErrRunTerminal) {
		t.Fatalf("commit on terminal = %v", err)
	}
	// A second created for the same RunID is refused by the projection, so the
	// Writer rejects the group before it reaches the stream.
	group := h.startGroup("t2", "r1", 1)
	res, err := h.writer().Commit(h.ctx, func(extension.View) (*extension.SemanticGroup, error) { return &group, nil })
	if err != nil || res.Outcome != extension.CommitInvalid {
		t.Fatalf("duplicate created = %+v %v, want invalid", res, err)
	}
}

// --- 重放与 Base ----------------------------------------------------------------------

func testReplayAndBase(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startRun("t1", "r1", input("in-1"))
	first := h.mustCommit("r1", run.DeriveInputCommandID("r1", "in-2"), 0, run.AcceptInput{Input: input("in-2")})
	if first.Status != run.CommitAccepted {
		t.Fatal("first accept not accepted")
	}
	again := h.mustCommit("r1", run.DeriveInputCommandID("r1", "in-2"), 0, run.AcceptInput{Input: input("in-2")})
	if again.Status != run.CommitAlreadyApplied || len(again.Events) != len(first.Events) || again.Events[0].Digest != first.Events[0].Digest {
		t.Fatalf("replay = %+v", again)
	}
	if h.head().Next != first.Snapshot.Head.Next {
		t.Fatal("replay appended rows")
	}
	// Prepare is a hard CAS on the Run's own position.
	snap := h.load("r1")
	stale := snap.Position - 1
	cmd, id := h.preparedCommand(run.RuntimeSnapshot{State: snap.State, Position: stale, SchemaVersion: snap.SchemaVersion}, false)
	if _, err := h.commit("r1", id, stale, cmd); !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("stale prepare = %v", err)
	}
	cmd, id = h.preparedCommand(snap, false)
	prepared := h.mustCommit("r1", id, snap.Position, cmd)
	// Non-prepare commands accept a zero or stale Base (call-local rebase).
	h.mustCommit("r1", run.DeriveInputCommandID("r1", "in-3"), 0, run.AcceptInput{Input: input("in-3")})
	// Terminal replay: an accepted command replays after termination.
	h.mustCommit("r1", "cancel", prepared.Snapshot.Position, run.CancelRun{})
	replay := h.mustCommit("r1", run.DeriveInputCommandID("r1", "in-3"), 0, run.AcceptInput{Input: input("in-3")})
	if replay.Status != run.CommitAlreadyApplied || !replay.Snapshot.State.Status.Terminal() {
		t.Fatalf("terminal replay = %+v", replay)
	}
	if _, err := h.commit("r1", run.DeriveInputCommandID("r1", "in-4"), 0, run.AcceptInput{Input: input("in-4")}); !errors.Is(err, run.ErrRunTerminal) {
		t.Fatalf("new command after terminal = %v", err)
	}
	// Derived-identity families must use their derived CommandID.
	if _, err := h.commit("r1", "random", 0, run.AcceptInput{Input: input("in-5")}); !errors.Is(err, run.ErrCommandConflict) && !errors.Is(err, run.ErrRunTerminal) {
		t.Fatalf("non-derived id = %v", err)
	}
}

// --- 输入入队 --------------------------------------------------------------------------

func testInputQueue(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startRun("t1", "r1", input("in-1"))
	// Prepared: the input queues and Next asks to withdraw.
	step := h.prepare("r1", false)
	h.mustCommit("r1", run.DeriveInputCommandID("r1", "in-2"), 0, run.AcceptInput{Input: input("in-2")})
	snap := h.load("r1")
	eff, err := run.Next(snap.State)
	if err != nil {
		t.Fatal(err)
	}
	if w, ok := eff.(run.WithdrawPrepared); !ok || w.StepID != step {
		t.Fatalf("effect while Prepared with input = %#v", eff)
	}
	h.mustCommit("r1", run.DeriveWithdrawCommandID("r1", step), snap.Position, run.WithdrawPreparedStep{StepID: step})
	snap = h.load("r1")
	if _, open := snap.State.Current.(run.Open); !open || len(snap.State.PendingInputs) != 1 || snap.State.ModelSteps != 0 {
		t.Fatalf("after withdraw = %+v", snap.State)
	}
	cmd, id := h.preparedCommand(snap, false)
	if len(cmd.InputIDs) != 1 || cmd.InputIDs[0] != "in-2" {
		t.Fatalf("replanned prepare consumes %v", cmd.InputIDs)
	}
	h.mustCommit("r1", id, snap.Position, cmd)
	// Executing: the input queues; a result without calls reopens instead of ending.
	claim := h.startModel("r1", cmd.StepID)
	h.mustCommit("r1", run.DeriveInputCommandID("r1", "in-3"), 0, run.AcceptInput{Input: input("in-3")})
	res := h.mustCommit("r1", run.DeriveSettlementCommandID("r1", cmd.StepID, "", claim), 0, run.SubmitModelResult{StepID: cmd.StepID, Result: textResult("a")})
	if res.Snapshot.State.Status != run.RunActive {
		t.Fatal("run ended with a pending input")
	}
	if _, open := res.Snapshot.State.Current.(run.Open); !open || len(res.Snapshot.State.PendingInputs) != 1 {
		t.Fatalf("after result with pending input = %+v", res.Snapshot.State)
	}
	// ToolStep: the input queues as well.
	h2 := newHarness(t, factory(t))
	h2.startRun("t1", "r2", input("in-1"))
	h2.openToolStep("r2", 1)
	res = h2.mustCommit("r2", run.DeriveInputCommandID("r2", "in-9"), 0, run.AcceptInput{Input: input("in-9")})
	if _, ok := res.Snapshot.State.Current.(run.ToolStep); !ok || len(res.Snapshot.State.PendingInputs) != 1 {
		t.Fatalf("accept on tool step = %+v", res.Snapshot.State)
	}
}

// --- start 与 claim ------------------------------------------------------------------------

func testStartAndClaim(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startRun("t1", "r1", input("in-1"))
	step, claim := h.executingModel("r1", false)
	// Same-claim replay is AlreadyApplied; another claim finds the target taken.
	replay := h.mustCommit("r1", run.DeriveStartCommandID("r1", step, "", claim), 0, run.StartModelExecution{StepID: step, Claim: claim})
	if replay.Status != run.CommitAlreadyApplied {
		t.Fatalf("start replay = %+v", replay)
	}
	other := h.claim()
	if _, err := h.commit("r1", run.DeriveStartCommandID("r1", step, "", other), 0, run.StartModelExecution{StepID: step, Claim: other}); !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("second claim start = %v", err)
	}
	// Starts and recoveries without a claim are conflicts.
	if _, err := h.commit("r1", run.DeriveStartCommandID("r1", step, "", ""), 0, run.StartModelExecution{StepID: step}); !errors.Is(err, run.ErrCommandConflict) {
		t.Fatalf("claimless start = %v", err)
	}
	// Settlement under the attempt's claim; its replay is AlreadyApplied.
	settleID := run.DeriveSettlementCommandID("r1", step, "", claim)
	res := h.mustCommit("r1", settleID, 0, run.SubmitModelResult{StepID: step, Result: textResult("done")})
	if !res.Snapshot.State.Status.Terminal() {
		t.Fatal("settlement did not end the run")
	}
	again := h.mustCommit("r1", settleID, 0, run.SubmitModelResult{StepID: step, Result: textResult("done")})
	if again.Status != run.CommitAlreadyApplied {
		t.Fatalf("settlement replay = %+v", again)
	}
	// After settlement the start still replays; a new command is terminal.
	replay = h.mustCommit("r1", run.DeriveStartCommandID("r1", step, "", claim), 0, run.StartModelExecution{StepID: step, Claim: claim})
	if replay.Status != run.CommitAlreadyApplied {
		t.Fatalf("start replay after settlement = %+v", replay)
	}
}

// --- 组的组成 ----------------------------------------------------------------------------

func testGroupComposition(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startRun("t1", "r1", input("in-1"))
	step, claim := h.executingModel("r1", true)
	result, bindings := h.toolCallResult(step, 1)
	before := h.head()
	res := h.mustCommit("r1", run.DeriveSettlementCommandID("r1", step, "", claim), 0,
		run.SubmitModelResult{StepID: step, Result: result, Calls: bindings})
	if h.head().Next != before.Next+session.Seq(len(res.Events)) {
		t.Fatal("one command did not produce exactly one group")
	}
	for i, e := range res.Events {
		if e.CommitID != session.CommitID(run.DeriveSettlementCommandID("r1", step, "", claim)) || int(e.Index) != i || e.Last != (i == len(res.Events)-1) {
			t.Fatalf("row %d markers = %+v", i, e)
		}
	}
	types := eventTypes(res.Events)
	want := []session.EventType{runmod.Prefix + "model_step_completed", runmod.Prefix + "tool_step_opened", chatlog.TypeAssistant}
	if strings.Join(asStrings(types), ",") != strings.Join(asStrings(want), ",") {
		t.Fatalf("group events = %v, want %v", types, want)
	}
	if res.Snapshot.Position != res.Events[1].Seq {
		t.Fatalf("position = %d, want the last run row %d", res.Snapshot.Position, res.Events[1].Seq)
	}
	// The companion's SourceDigest equals the fact's ResultDigest.
	var resultDigest es.Digest
	for _, f := range h.record("r1").Facts {
		if c, ok := f.(run.ModelStepCompleted); ok {
			resultDigest = c.ResultDigest
		}
	}
	decoded, err := h.registry.Decode(res.Events[2])
	if err != nil {
		t.Fatal(err)
	}
	if a := decoded.Value.(chatlog.AssistantPayload).Assistant; a.SourceDigest != resultDigest || a.TurnID != "t1" {
		t.Fatalf("assistant = %+v, want SourceDigest %s", a, resultDigest)
	}
	// Attach follows the companion; twilight/run/ events are refused.
	ts := res.Snapshot.State.Current.(run.ToolStep)
	call := ts.Calls[0].CallID
	toolClaim := h.startTool("r1", ts.RefValue.ID, call)
	output := run.MustParseCanonicalJSON(`{"ok":true}`)
	if _, err := h.commit("r1", run.DeriveSettlementCommandID("r1", ts.RefValue.ID, call, toolClaim), 0,
		run.SubmitToolResult{StepID: ts.RefValue.ID, CallID: call, Result: run.ToolExecutionResult{Output: output}},
		run.ModuleEvent{Type: runmod.Prefix + "input_accepted", Value: runmod.Event{RunID: "r1", Fact: run.InputAccepted{Input: input("x")}}}); err == nil {
		t.Fatal("Attach with a twilight/run/ event accepted")
	}
	h.submitInputs(input("in-attach"))
	res = h.mustCommit("r1", run.DeriveSettlementCommandID("r1", ts.RefValue.ID, call, toolClaim), 0,
		run.SubmitToolResult{StepID: ts.RefValue.ID, CallID: call, Result: run.ToolExecutionResult{Output: output}},
		run.ModuleEvent{Type: chatlog.TypeInputDelivered, Value: chatlog.InputDeliveredPayload{InputID: "in-attach", TurnID: "t1"}})
	types = eventTypes(res.Events)
	if len(types) != 3 || types[0] != runmod.Prefix+"tool_call_completed" || types[1] != chatlog.TypeToolResult || types[2] != chatlog.TypeInputDelivered {
		t.Fatalf("group events = %v", types)
	}
	outputDigest, _ := run.ProtocolV1().DigestToolOutput(output)
	decoded, _ = h.registry.Decode(res.Events[1])
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

func testAdmission(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
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
	// Unregistered binding: the whole group is refused and nothing is written.
	if _, err := h.commit("r1", run.DeriveInputCommandID("r1", "in-2"), 0, run.AcceptInput{Input: input("in-2")}, attach("missing")); err == nil {
		t.Fatal("unregistered binding admitted")
	}
	if h.head() != before {
		t.Fatal("refused commit wrote to the stream")
	}
	if len(h.load("r1").State.PendingInputs) != 1 {
		t.Fatal("refused commit changed the Run")
	}
	// Registered binding: the claim is Active once the group is committed.
	res := h.mustCommit("r1", run.DeriveInputCommandID("r1", "in-2"), 0, run.AcceptInput{Input: input("in-2")}, attach("b1"))
	claimID := extension.DeriveClaimID(session.ProtocolVersion1, sid, res.Events[0].CommitID, mustSet(t, h, "b1").RefSetDigest)
	claim, ok, err := h.ledger.LookupClaim(h.ctx, claimID)
	if err != nil || !ok || claim.State != artifact.ClaimActive || claim.Owner != extension.CommitOwner(sid, res.Events[0].CommitID) {
		t.Fatalf("claim = %+v ok=%v err=%v", claim, ok, err)
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

func testSettlementSnapshot(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startRun("t1", "r1", input("in-1"))
	step, claim := h.executingModel("r1", false)
	res := h.mustCommit("r1", run.DeriveSettlementCommandID("r1", step, "", claim), 0, run.SubmitModelResult{StepID: step, Result: textResult("done")})
	if res.Snapshot.State.Status != run.RunCompleted || res.Snapshot.State.Result == nil || res.Snapshot.State.Result.Status != run.RunCompleted {
		t.Fatalf("settlement snapshot = %+v", res.Snapshot.State)
	}
	rec := h.record("r1")
	if !run.StatesEquivalent(&rec.Snapshot.State, &res.Snapshot.State) || rec.Snapshot.Position != res.Snapshot.Position {
		t.Fatalf("settlement snapshot %+v disagrees with record %+v", res.Snapshot, rec.Snapshot)
	}
	// The completed turn is settled in the same group (companion).
	types := eventTypes(res.Events)
	if types[len(types)-1] != turn.TypeCompleted {
		t.Fatalf("terminal group events = %v, want turn/completed last", types)
	}
}

// --- Prepare hard CAS 对其他模块不敏感 ---------------------------------------------------------------

func testPrepareCASIgnoresOtherModules(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startRun("t1", "r1", input("in-1"))
	h.startRun("t2", "r2", input("in-b"))
	snap := h.load("r1")
	cmd, id := h.preparedCommand(snap, false)
	// Other modules and another Run write after the planner loaded.
	h.submitInputs(input("late"))
	h.prepare("r2", false)
	after := h.load("r1")
	if after.Position != snap.Position {
		t.Fatalf("foreign writes moved r1 position %d -> %d", snap.Position, after.Position)
	}
	if after.Head == snap.Head {
		t.Fatal("session head did not move")
	}
	if _, err := h.commit("r1", id, snap.Position, cmd); err != nil {
		t.Fatalf("prepare against a moved session head: %v", err)
	}
}

// --- 投影 -------------------------------------------------------------------------------------------

func testProjection(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startRun("t1", "r1", input("in-1"))
	cached := func() (session.Head, bool) {
		_, through, ok, err := h.cache.Load(h.ctx, sid, runmod.MachineProjectionID, runmod.MachineProjection.Version)
		if err != nil {
			t.Fatal(err)
		}
		return through, ok
	}
	step, claim := h.executingModel("r1", true)
	if _, ok := cached(); ok {
		t.Fatal("projection cached while the Run is mid-step")
	}
	result, bindings := h.toolCallResult(step, 1)
	opened := h.mustCommit("r1", run.DeriveSettlementCommandID("r1", step, "", claim), 0,
		run.SubmitModelResult{StepID: step, Result: result, Calls: bindings})
	toolStep := opened.Snapshot.State.Current.(run.ToolStep).RefValue.ID
	callID := bindings[0].CallID
	toolClaim := h.startTool("r1", toolStep, callID)
	res := h.mustCommit("r1", run.DeriveSettlementCommandID("r1", toolStep, callID, toolClaim), 0,
		run.SubmitToolResult{StepID: toolStep, CallID: callID, Result: run.ToolExecutionResult{Output: run.MustParseCanonicalJSON(`1`)}})
	if _, open := res.Snapshot.State.Current.(run.Open); !open {
		t.Fatalf("after tool settlement current = %T", res.Snapshot.State.Current)
	}
	if through, ok := cached(); !ok || through != res.Snapshot.Head {
		t.Fatalf("cache after return to Open = %+v ok=%v, want head %+v", through, ok, res.Snapshot.Head)
	}
	// The cached state plus tail equals the Writer's state.
	observer := extension.NewProjectionReader(h.store, h.registry, h.cache)
	fromCache, _, err := observer.Load(h.ctx, sid, runmod.MachineProjectionID, runmod.MachineProjection.Version)
	if err != nil {
		t.Fatal(err)
	}
	m := h.machine()
	loaded := h.load("r1")
	rec := h.record("r1")
	active, ok := m.Active["r1"]
	if !ok || !run.StatesEquivalent(&active, &loaded.State) || !run.StatesEquivalent(&active, &rec.Snapshot.State) {
		t.Fatal("projection, Load and Record disagree")
	}
	if fromCache := fromCache.(runmod.Machine).Active["r1"]; !run.StatesEquivalent(&fromCache, &active) {
		t.Fatal("observer's cache+tail disagrees with the writer's projection")
	}
	// Terminal Run leaves Active, stays in Ended; Load and Record still answer.
	h.mustCommit("r1", "cancel", 0, run.CancelRun{})
	m = h.machine()
	if _, still := m.Active["r1"]; still {
		t.Fatal("terminal run still in the projection")
	}
	if _, ended := m.Ended["r1"]; !ended {
		t.Fatal("terminal run not remembered in Ended")
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

func testIsolation(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startRun("t1", "r1", input("in-1"))
	h.startRun("t2", "r2", input("in-b"))
	p1 := h.load("r1").Position
	h.prepare("r2", false)
	h.submitInputs(input("noise"))
	h.mustApply(extension.SemanticGroup{CommitID: "turn-noise", Events: []extension.TypedEvent{{Type: turn.TypeStarted, RecordedAtUnixMilli: 1,
		Value: turn.StartedPayload{TurnID: "t9", Profile: turn.ProfileRef{ID: "b", Digest: "sha256:b"}, Companion: turn.CompanionV1Version}}}})
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

// --- 接管处置 -----------------------------------------------------------------------------------------

func testTakeover(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startRun("t1", "r1", input("in-1"))
	h.startRun("t2", "r2", input("in-b"))
	// r1: executing model; r2: one executing and one pending tool call.
	h.executingModel("r1", false)
	requestDigest := h.load("r1").State.Current.(run.ModelStep).RequestDigest
	toolStep, ids := h.openToolStep("r2", 2)
	h.startTool("r2", toolStep, ids[0])

	h.takeover()
	n, err := h.rt.RecoverInterrupted(h.ctx, sid)
	if err != nil || n != 2 {
		t.Fatalf("RecoverInterrupted = %d %v, want 2", n, err)
	}
	r1 := h.load("r1")
	ms := r1.State.Current.(run.ModelStep)
	if r1.State.Status != run.RunActive || ms.Status != run.ModelPrepared || ms.RequestDigest != requestDigest {
		t.Fatalf("model after takeover = %+v", r1.State)
	}
	if _, err := h.rt.FrozenRequest(h.ctx, requestDigest); err != nil {
		t.Fatalf("frozen request after takeover: %v", err)
	}
	r2 := h.load("r2")
	ts := r2.State.Current.(run.ToolStep)
	if r2.State.Status != run.RunActive || ts.Calls[0].Status != run.ToolFailed || ts.Calls[0].Failure == nil || ts.Calls[0].Failure.Outcome != run.ToolOutcomeUnknown {
		t.Fatalf("executing call after takeover = %+v", ts.Calls[0])
	}
	if ts.Calls[1].Status != run.ToolPending {
		t.Fatalf("pending sibling = %+v, want untouched", ts.Calls[1])
	}
	// The Unknown travels with its chatlog tool_result in one group.
	rec := h.record("r2")
	var unknownSeq session.Seq
	for _, e := range rec.Events {
		if strings.HasSuffix(string(e.Type), "tool_call_failed") {
			unknownSeq = e.Seq
		}
	}
	page, err := h.store.Read(h.ctx, session.ReadRequest{SessionID: sid, From: unknownSeq})
	if err != nil || len(page.Events) < 2 || page.Events[1].Type != chatlog.TypeToolResult || page.Events[1].CommitID != page.Events[0].CommitID {
		t.Fatalf("rows after the Unknown = %v %v", eventTypes(page.Events), err)
	}
	decoded, _ := h.registry.Decode(page.Events[1])
	if tr := decoded.Value.(chatlog.ToolResultPayload).ToolResult; tr.Status != chatlog.ToolUnknown || tr.SourceDigest != "" {
		t.Fatalf("tool_result = %+v", tr)
	}
	// Same owner repeats: idempotent, nothing new.
	head := h.head()
	if n, err := h.rt.RecoverInterrupted(h.ctx, sid); err != nil || n != 0 || h.head() != head {
		t.Fatalf("second RecoverInterrupted = %d %v", n, err)
	}
	// Another takeover with nothing Executing does nothing.
	h.takeover()
	if n, err := h.rt.RecoverInterrupted(h.ctx, sid); err != nil || n != 0 {
		t.Fatalf("RecoverInterrupted with no executing target = %d %v", n, err)
	}
}

// --- 所有权失效 ----------------------------------------------------------------------------------------

func testOwnershipLost(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startRun("t1", "r1", input("in-1"))
	step, claim := h.executingModel("r1", false)
	old := h.takeover()
	head := h.head()
	_, err := h.commitWith(old, "r1", run.DeriveSettlementCommandID("r1", step, "", claim), 0, run.SubmitModelResult{StepID: step, Result: textResult("late")})
	if !errors.Is(err, run.ErrOwnershipLost) {
		t.Fatalf("old owner commit = %v, want ErrOwnershipLost", err)
	}
	if h.head() != head {
		t.Fatal("fenced commit reached the stream")
	}
	if _, err := old.Load(h.ctx, sid, "r1"); !errors.Is(err, run.ErrOwnershipLost) {
		t.Fatalf("old owner load = %v, want ErrOwnershipLost", err)
	}
	// The new owner is unaffected.
	if h.load("r1").State.Current.(run.ModelStep).Status != run.ModelExecuting {
		t.Fatal("new owner's view changed")
	}
}

// --- FrozenValueStore --------------------------------------------------------------------------------

func testFrozenValues(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startRun("t1", "r1", input("in-1"))
	step, claim := h.executingModel("r1", false)
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
	h.mustCommit("r1", run.DeriveSettlementCommandID("r1", step, "", claim), 0, run.SubmitModelResult{StepID: step, Result: textResult("done")})
	h.frozen.Delete(digest)
	if _, err := h.rt.Record(h.ctx, sid, "r1"); err != nil {
		t.Fatalf("record after dropping the settled body: %v", err)
	}
}
