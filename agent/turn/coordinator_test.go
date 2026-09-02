package turn_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/run/loop"
	"github.com/memohai/twilight/agent/turn"
	"github.com/memohai/twilight/sdk"
)

// --- application doubles ------------------------------------------------------

type scriptModel struct {
	results []sdk.ModelResult
	calls   atomic.Int32
}

func (m *scriptModel) ResolveModel(run.ModelRef) (loop.ModelInvoker, error) { return m, nil }
func (m *scriptModel) Generate(ctx context.Context, _ sdk.Request) (sdk.ModelResult, error) {
	if err := ctx.Err(); err != nil {
		return sdk.ModelResult{}, err
	}
	n := int(m.calls.Add(1)) - 1
	if n >= len(m.results) {
		return sdk.ModelResult{}, errors.New("no scripted result")
	}
	return m.results[n], nil
}

type echoTool struct {
	spec  run.ToolSpec
	def   sdk.ToolDefinition
	block chan struct{} // when non-nil, first execution blocks until closed
	ran   atomic.Int32
}

func newEchoTool(t testing.TB, policy run.ResponsePolicy) *echoTool {
	t.Helper()
	def := sdk.ToolDefinition{Name: "echo", Parameters: []byte(`{"type":"object"}`)}
	frozen, err := run.FreezeToolDefinition(def)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := run.ProtocolV1().DigestToolDefinition(frozen)
	if err != nil {
		t.Fatal(err)
	}
	return &echoTool{def: def, spec: run.ToolSpec{Ref: "echo", Definition: frozen, DefinitionDigest: digest, Policy: policy}}
}

func (e *echoTool) ResolveTool(ref run.ToolRef) (loop.ExecutableTool, error) {
	if ref != "echo" {
		return nil, fmt.Errorf("unknown tool %q", ref)
	}
	return e, nil
}
func (e *echoTool) Ref() run.ToolRef                          { return "echo" }
func (e *echoTool) Definition() sdk.ToolDefinition            { return e.def }
func (e *echoTool) ResponsePolicy() run.ResponsePolicy        { return e.spec.Policy }
func (e *echoTool) ValidateArguments(run.CanonicalJSON) error { return nil }
func (e *echoTool) Execute(_ context.Context, req loop.ToolExecutionRequest) loop.ToolExecutionOutcome {
	if e.ran.Add(1) == 1 && e.block != nil {
		<-e.block
		return loop.ToolExecutionUnknown{Failure: run.ToolFailure{Class: run.FailureEffectUnknown}}
	}
	return loop.ToolExecutionSucceeded{Result: run.ToolExecutionResult{Output: req.Arguments}}
}

// planner replays delivered inputs as user messages and the last tool step
// as tool messages, which is all the scripted model needs.
type planner struct{ spec run.ToolSpec }

func (p planner) Plan(_ context.Context, hint run.PlanningHint) (loop.RequestPlan, error) {
	var msgs []sdk.Message
	ids := make([]run.InputID, 0, len(hint.Inputs))
	for _, in := range hint.Inputs {
		ids = append(ids, in.ID)
		msgs = append(msgs, sdk.UserMessage(in.Payload.String()))
	}
	if hint.LastToolStep != nil {
		for _, c := range hint.LastToolStep.Calls {
			msgs = append(msgs, sdk.ToolMessage(sdk.ToolResultPart{ToolCallID: c.ProviderCallID, ToolName: "echo", Result: c.Status.String()}))
		}
	}
	return loop.RequestPlan{Model: "m-1", Request: sdk.Request{Model: "m-1", Messages: msgs, Tools: []sdk.ToolDefinition{p.spec.Definition.SDK()}},
		InputIDs: ids, Tools: []run.ToolSpec{p.spec}}, nil
}

func toolCall(id string) sdk.ModelResult {
	return sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls, Usage: sdk.Usage{TotalTokens: 1},
		ToolCalls: []sdk.ToolCall{{ToolCallID: id, ToolName: "echo", Input: `{"x":1}`}}}
}

func text(s string) sdk.ModelResult {
	return sdk.ModelResult{Text: s, FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}
}

type harness struct {
	t     *testing.T
	log   *turn.MemoryLog
	rt    run.Runtime
	coord *turn.Coordinator
	tool  *echoTool
	ref   turn.Ref
}

func newHarness(t *testing.T, rt run.Runtime, policy run.ResponsePolicy, results ...sdk.ModelResult) *harness {
	t.Helper()
	tool := newEchoTool(t, policy)
	l, err := loop.New(&scriptModel{results: results}, tool, planner{tool.spec}, loop.ExecutionPolicy{}, false)
	if err != nil {
		t.Fatal(err)
	}
	log := turn.NewMemoryLog()
	return &harness{t: t, log: log, rt: rt, tool: tool,
		coord: &turn.Coordinator{Log: log, Runtime: rt, Driver: turn.LoopDriver{Loop: l, Runtime: rt}},
		ref:   turn.Ref{Session: "s-1", Turn: "t-1"}}
}

func (h *harness) events() []turn.Event {
	h.t.Helper()
	evs, err := h.log.Replay(context.Background(), h.ref.Session)
	if err != nil {
		h.t.Fatal(err)
	}
	return evs
}

func (h *harness) types() []string {
	evs := h.events()
	out := make([]string, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.Type)
	}
	return out
}

func requireTypes(t *testing.T, got, want []string) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("event types\n got  %v\n want %v", got, want)
	}
}

// --- tests --------------------------------------------------------------------

func TestStartDrivesToCompletionAndMaterializes(t *testing.T) {
	rt := run.NewRuntime(run.NewMemoryStore())
	h := newHarness(t, rt, run.DirectExecution, toolCall("c1"), text("done"))
	res, err := h.coord.Start(context.Background(), turn.StartRequest{Ref: h.ref, RunID: "run-1",
		Inputs: []run.AgentInput{{ID: "in-1", Payload: run.MustParseCanonicalJSON(`{"text":"hi"}`)}}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Disposition != turn.DispositionFinished || res.Settlement != turn.SettlementCompleted || res.Result.Model.Text != "done" {
		t.Fatalf("res = %+v", res)
	}
	requireTypes(t, h.types(), []string{
		turn.EventTurnStarted, turn.EventInputDelivered,
		turn.EventAssistant, turn.EventToolResult, turn.EventAssistant,
		turn.EventTurnCompleted,
	})
	// Materialized events carry their Run position and the coverage watermark
	// equals the terminal revision.
	evs := h.events()
	if evs[2].Revision == 0 || evs[len(evs)-1].Revision != 9 {
		t.Fatalf("positions: %+v", evs)
	}
	// Start again with the same RunID is idempotent: no new events.
	again, err := h.coord.Start(context.Background(), turn.StartRequest{Ref: h.ref, RunID: "run-1"})
	if err != nil || again.Disposition != turn.DispositionFinished || len(h.events()) != len(evs) {
		t.Fatalf("idempotent start: %+v %v events=%d", again, err, len(h.events()))
	}
	if _, err := h.coord.Start(context.Background(), turn.StartRequest{Ref: h.ref, RunID: "run-2"}); !errors.Is(err, turn.ErrTurnConflict) {
		t.Fatalf("second primary run err = %v", err)
	}
}

func TestApprovalWaitsThenResumes(t *testing.T) {
	rt := run.NewRuntime(run.NewMemoryStore())
	h := newHarness(t, rt, run.ApprovalRequired, toolCall("c1"), text("done"))
	res, err := h.coord.Start(context.Background(), turn.StartRequest{Ref: h.ref, RunID: "run-1",
		Inputs: []run.AgentInput{{ID: "in-1", Payload: run.MustParseCanonicalJSON(`{"text":"hi"}`)}}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Disposition != turn.DispositionWaitingForResponse || len(res.Waiting) != 1 {
		t.Fatalf("res = %+v", res)
	}
	// The assistant that asked for the tool is already materialized while we wait.
	requireTypes(t, h.types(), []string{turn.EventTurnStarted, turn.EventInputDelivered, turn.EventAssistant})

	w := res.Waiting[0]
	digest, err := run.ProtocolV1().DigestToolResponseDecision(w.Kind, run.ResponseDecisionApproved, "")
	if err != nil {
		t.Fatal(err)
	}
	snap, err := rt.Load(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	env, err := run.ProtocolV1().BuildEnvelope("run-1", run.DeriveResponseCommandID("run-1", w.StepID, w.CallID, w.ID),
		run.ApproveToolCall{StepID: w.StepID, CallID: w.CallID, ResponseID: w.ID, ResponseDigest: digest})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Commit(context.Background(), run.CommitRequest{BaseRevision: snap.Revision, Command: env}); err != nil {
		t.Fatal(err)
	}
	res, err = h.coord.Resume(context.Background(), h.ref)
	if err != nil || res.Disposition != turn.DispositionFinished {
		t.Fatalf("resume: %+v %v", res, err)
	}
	requireTypes(t, h.types(), []string{turn.EventTurnStarted, turn.EventInputDelivered, turn.EventAssistant,
		turn.EventToolResult, turn.EventAssistant, turn.EventTurnCompleted})
}

func TestStopCancelsAndSettlesAsFailedStopped(t *testing.T) {
	rt := run.NewRuntime(run.NewMemoryStore())
	h := newHarness(t, rt, run.ApprovalRequired, toolCall("c1"))
	if _, err := h.coord.Start(context.Background(), turn.StartRequest{Ref: h.ref, RunID: "run-1",
		Inputs: []run.AgentInput{{ID: "in-1", Payload: run.MustParseCanonicalJSON(`{}`)}}}); err != nil {
		t.Fatal(err)
	}
	res, err := h.coord.Stop(context.Background(), h.ref)
	if err != nil || res.Disposition != turn.DispositionFinished || res.Settlement != turn.SettlementStopped {
		t.Fatalf("stop: %+v %v", res, err)
	}
	evs := h.events()
	last := evs[len(evs)-1]
	var p turn.SettledPayload
	if err := last.Payload.Decode(&p); err != nil {
		t.Fatal(err)
	}
	if last.Type != turn.EventTurnFailed || p.Settlement != turn.SettlementStopped || p.FailureClass != string(run.ReasonCancelled) {
		t.Fatalf("settlement event = %s %+v", last.Type, p)
	}
	// Stop again is idempotent.
	if _, err := h.coord.Stop(context.Background(), h.ref); err != nil || len(h.events()) != len(evs) {
		t.Fatalf("second stop: %v events=%d", err, len(h.events()))
	}
}

// A driver whose process dies mid tool call: the Turn is resumed by a fresh
// coordinator after lease expiry. Nothing about the Turn lives outside the
// log and the Runtime, so the new coordinator rebuilds and finishes it.
func TestResumeAfterCrashRecoversAndSettles(t *testing.T) {
	clock := time.Unix(1000, 0)
	rt := run.NewRuntimeWithOptions(run.NewMemoryStore(), run.RuntimeOptions{LeaseTTL: time.Second, Now: func() time.Time { return clock }})
	h := newHarness(t, rt, run.DirectExecution, toolCall("c1"), text("after crash"))
	h.tool.block = make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := h.coord.Start(ctx, turn.StartRequest{Ref: h.ref, RunID: "run-1",
			Inputs: []run.AgentInput{{ID: "in-1", Payload: run.MustParseCanonicalJSON(`{}`)}}})
		done <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		snap, err := rt.Load(context.Background(), "run-1")
		if err == nil && len(run.ExecutingCalls(snap.State)) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("tool never started")
		}
		time.Sleep(time.Millisecond)
	}
	// "Crash": cancel the driver's context and let the blocked worker return
	// Unknown against a cancelled ctx; then drop every in-process artifact.
	cancel()
	close(h.tool.block)
	<-done

	clock = clock.Add(2 * time.Second)
	if _, err := rt.RecoverExpired(context.Background()); err != nil {
		t.Fatal(err)
	}

	// New process: new Loop, new Coordinator, same log and Runtime.
	h2 := newHarness(t, rt, run.DirectExecution, text("after crash"))
	h2.log = h.log
	h2.coord.Log = h.log
	res, err := h2.coord.Resume(context.Background(), h.ref)
	if err != nil || res.Disposition != turn.DispositionFinished || res.Settlement != turn.SettlementCompleted {
		t.Fatalf("resume after crash: %+v %v", res, err)
	}
	types := h2.types()
	// Exactly one tool_result, and it is the Unknown recovery, followed by the
	// second assistant and the settlement.
	unknown := 0
	for _, e := range h2.events() {
		if e.Type != turn.EventToolResult {
			continue
		}
		var p turn.ToolResultPayload
		if err := e.Payload.Decode(&p); err != nil {
			t.Fatal(err)
		}
		if p.Status == turn.ToolUnknown {
			unknown++
		}
	}
	if unknown != 1 {
		t.Fatalf("unknown tool results = %d in %v", unknown, types)
	}
	if types[len(types)-1] != turn.EventTurnCompleted {
		t.Fatalf("types = %v", types)
	}
}
