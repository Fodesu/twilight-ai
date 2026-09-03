package turn_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
	results  []sdk.ModelResult
	calls    atomic.Int32
	mu       sync.Mutex
	requests []sdk.Request // every request the model received, in order
}

func (m *scriptModel) ResolveModel(run.ModelRef) (loop.ModelInvoker, error) { return m, nil }
func (m *scriptModel) Generate(ctx context.Context, req sdk.Request) (sdk.ModelResult, error) {
	if err := ctx.Err(); err != nil {
		return sdk.ModelResult{}, err
	}
	m.mu.Lock()
	m.requests = append(m.requests, req)
	m.mu.Unlock()
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
	model *scriptModel
	ref   turn.Ref
}

func newHarness(t *testing.T, rt run.Runtime, policy run.ResponsePolicy, results ...sdk.ModelResult) *harness {
	return newHarnessOnLog(t, rt, turn.NewMemoryLog(), policy, results...)
}

// newHarnessOnLog builds a fresh Loop and Coordinator over an existing log,
// which is how a second process or a second Turn joins the same session.
func newHarnessOnLog(t *testing.T, rt run.Runtime, log *turn.MemoryLog, policy run.ResponsePolicy, results ...sdk.ModelResult) *harness {
	t.Helper()
	tool := newEchoTool(t, policy)
	model := &scriptModel{results: results}
	planner := &turn.ContextPlanner{Log: log, Session: "s-1", Model: "m-1", Tools: []run.ToolSpec{tool.spec}, System: "test system"}
	l, err := loop.New(model, tool, planner, loop.ExecutionPolicy{}, false)
	if err != nil {
		t.Fatal(err)
	}
	return &harness{t: t, log: log, rt: rt, tool: tool, model: model,
		coord: &turn.Coordinator{Log: log, Runtime: rt, Driver: turn.LoopDriver{Loop: l, Runtime: rt}},
		ref:   turn.Ref{Session: "s-1", Turn: "t-1"}}
}

// roles returns the role sequence of the i-th request the model saw.
func (h *harness) roles(i int) []string {
	h.t.Helper()
	h.model.mu.Lock()
	defer h.model.mu.Unlock()
	if i >= len(h.model.requests) {
		h.t.Fatalf("model saw %d requests, want index %d", len(h.model.requests), i)
	}
	out := make([]string, 0, len(h.model.requests[i].Messages))
	for _, m := range h.model.requests[i].Messages {
		out = append(out, string(m.Role))
	}
	return out
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
	// The second model request carries the whole conversation so far, not
	// only the boundary facts: system, the user input, the assistant tool
	// call and its result.
	requireTypes(t, h.roles(0), []string{"system", "user"})
	requireTypes(t, h.roles(1), []string{"system", "user", "assistant", "tool"})
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
	h2 := newHarnessOnLog(t, rt, h.log, run.DirectExecution, text("after crash"))
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

// A second Turn on the same session plans from the whole session history: the
// model sees the first Turn's input, tool exchange and answer before the new
// input. Nothing about the first Turn is held in memory by the second Loop.
func TestSecondTurnSeesFirstTurnConversation(t *testing.T) {
	rt := run.NewRuntime(run.NewMemoryStore())
	h1 := newHarness(t, rt, run.DirectExecution, toolCall("c1"), text("first answer"))
	if _, err := h1.coord.Start(context.Background(), turn.StartRequest{Ref: h1.ref, RunID: "run-1",
		Inputs: []run.AgentInput{{ID: "in-1", Payload: run.MustParseCanonicalJSON(`{"text":"first question"}`)}}}); err != nil {
		t.Fatal(err)
	}
	h2 := newHarnessOnLog(t, rt, h1.log, run.DirectExecution, text("second answer"))
	h2.ref = turn.Ref{Session: "s-1", Turn: "t-2"}
	res, err := h2.coord.Start(context.Background(), turn.StartRequest{Ref: h2.ref, RunID: "run-2",
		Inputs: []run.AgentInput{{ID: "in-2", Payload: run.MustParseCanonicalJSON(`{"text":"second question"}`)}}})
	if err != nil || res.Disposition != turn.DispositionFinished || res.Result.Model.Text != "second answer" {
		t.Fatalf("second turn: %+v %v", res, err)
	}
	requireTypes(t, h2.roles(0), []string{"system", "user", "assistant", "tool", "assistant", "user"})
	req := h2.model.requests[0]
	if got := req.Messages[1].Content[0].(sdk.TextPart).Text; got != "first question" {
		t.Fatalf("first user message = %q", got)
	}
	if got := req.Messages[5].Content[0].(sdk.TextPart).Text; got != "second question" {
		t.Fatalf("second user message = %q", got)
	}
	// The log now holds both Turns in order.
	types := h2.types()
	if types[0] != turn.EventTurnStarted || types[len(types)-1] != turn.EventTurnCompleted {
		t.Fatalf("types = %v", types)
	}
}
