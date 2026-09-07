package ref_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/memohai/twilight/agent/ref"
	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/run/loop"
	"github.com/memohai/twilight/agent/session"
	"github.com/memohai/twilight/agent/session/chatlog"
	"github.com/memohai/twilight/agent/turn"
	"github.com/memohai/twilight/sdk"
)

// gateTool blocks each execution until released, so tests can act mid-step.
type gateTool struct {
	started chan struct{}
	release chan struct{}
}

func (t *gateTool) Ref() run.ToolRef { return "lookup" }
func (t *gateTool) Definition() sdk.ToolDefinition {
	return sdk.ToolDefinition{Name: "lookup", Parameters: []byte(`{"type":"object"}`)}
}
func (t *gateTool) ResponsePolicy() run.ResponsePolicy        { return run.DirectExecution }
func (t *gateTool) ValidateArguments(run.CanonicalJSON) error { return nil }
func (t *gateTool) Execute(_ context.Context, req loop.ToolExecutionRequest) loop.ToolExecutionOutcome {
	t.started <- struct{}{}
	<-t.release
	return loop.ToolExecutionSucceeded{Result: run.ToolExecutionResult{Output: req.Arguments}}
}

// scriptedRequests records every request the model saw and answers from a
// script: tool call first, then text.
type scriptedRequests struct {
	seen    []sdk.Request
	answers []sdk.ModelResult
}

func (m *scriptedRequests) Generate(_ context.Context, req sdk.Request) (sdk.ModelResult, error) {
	m.seen = append(m.seen, req)
	if len(m.answers) == 0 {
		return sdk.ModelResult{Text: "done", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}, nil
	}
	next := m.answers[0]
	m.answers = m.answers[1:]
	return next, nil
}

type gateCatalog struct{ tool *gateTool }

func (c gateCatalog) ResolveTool(ref run.ToolRef) (loop.ExecutableTool, error) { return c.tool, nil }

func toolCallAnswer() sdk.ModelResult {
	return sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls, Usage: sdk.Usage{TotalTokens: 1},
		ToolCalls: []sdk.ToolCall{{ToolCallID: "c1", ToolName: "lookup", Input: `{"q":"weather"}`}}}
}

func setup(t *testing.T, model loop.ModelInvoker, tool *gateTool) (*ref.Memory, turn.ExecutionBindingRef, session.SessionID) {
	t.Helper()
	m, err := ref.New(ref.Options{LeaseTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	const sid session.SessionID = "s-1"
	if err := m.CreateSession(context.Background(), sid); err != nil {
		t.Fatal(err)
	}
	def, err := run.FreezeToolDefinition(tool.Definition())
	if err != nil {
		t.Fatal(err)
	}
	binding, err := m.Bindings.Register("b1", ref.Binding{
		Public: ref.BindingPublic{Model: "m-1", SystemPrompt: "be brief", Tools: []ref.PublicTool{{Ref: tool.Ref(), Definition: def, Policy: run.DirectExecution}}},
		Models: modelCatalog{model}, Tools: gateCatalog{tool},
	})
	if err != nil {
		t.Fatal(err)
	}
	return m, binding, sid
}

// An input delivered while a tool call is Executing queues on the Run, is
// delivered to the same Turn in the same commit, and reaches the model in the
// next request together with the tool result (TRN-DLV, RUN-LOP-8).
func TestDeliverMidTurnReachesNextModelRequest(t *testing.T) {
	ctx := context.Background()
	tool := &gateTool{started: make(chan struct{}, 1), release: make(chan struct{})}
	model := &scriptedRequests{answers: []sdk.ModelResult{toolCallAnswer()}}
	m, binding, sid := setup(t, model, tool)

	first, err := m.SubmitInput(ctx, sid, "in-1", "what is the weather?")
	if err != nil {
		t.Fatal(err)
	}
	ref1 := turn.TurnRef{SessionID: sid, TurnID: "t1"}
	done := make(chan turn.TurnResponse, 1)
	go func() {
		resp, err := m.Coordinator.Start(ctx, turn.StartRequest{Ref: ref1, Inputs: []run.AgentInput{first}, ExecutionBinding: binding, Companion: turn.CompanionV1Version})
		if err != nil {
			t.Error(err)
		}
		done <- resp
	}()
	<-tool.started

	second, err := m.SubmitInput(ctx, sid, "in-2", "and tomorrow?")
	if err != nil {
		t.Fatal(err)
	}
	driver := &ref.SessionDriver{Coordinator: m.Coordinator, Memory: m, Binding: binding, Companion: turn.CompanionV1Version, NewTurnID: func() turn.TurnID { return "t2" }}
	// Deliver commits AcceptInput + input_delivered without waiting for the
	// tool; Drive is skipped because the Run is already being driven here, so
	// route through the Coordinator directly in a goroutine.
	deliverDone := make(chan error, 1)
	go func() {
		_, err := driver.Send(ctx, sid, []run.AgentInput{second})
		deliverDone <- err
	}()
	// The Deliver commit lands while the tool runs; the Loop sees PendingInputs
	// at its next Load. Release the tool and let both drivers finish.
	waitFor(t, func() bool {
		surface, err := m.TurnSurface(ctx, sid)
		return err == nil && len(surface.Turns["t1"].InputIDs) == 2
	})
	close(tool.release)
	if err := <-deliverDone; err != nil && !errors.Is(err, loop.ErrRunAlreadyRunning) {
		t.Fatalf("deliver: %v", err)
	}
	resp := <-done
	if resp.Status != turn.TurnCompleted {
		t.Fatalf("turn status = %s, want completed", resp.Status)
	}
	if len(model.seen) != 2 {
		t.Fatalf("model requests = %d, want 2", len(model.seen))
	}
	last := model.seen[1].Messages
	var users []string
	for _, msg := range last {
		if msg.Role == sdk.MessageRoleUser {
			users = append(users, msg.Content[0].(sdk.TextPart).Text)
		}
	}
	if len(users) != 2 || users[1] != "and tomorrow?" {
		t.Fatalf("second request user messages = %v", users)
	}
	if last[len(last)-2].Role != sdk.MessageRoleTool {
		t.Fatalf("tool result did not precede the delivered input: %+v", roles(last))
	}
	chat, err := m.ChatlogSurface(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	if got := chat.Inputs["in-2"]; got.Status != chatlog.InputDelivered || got.Input.TurnID != "t1" {
		t.Fatalf("in-2 = %+v, want delivered to t1", got)
	}
}

// Stop settles the Turn as stopped in the same commit as CancelRun; a later
// Send opens a new Turn whose planner sees the stopped Turn's content.
func TestStopSettlesTurnAndNextSendStartsNewTurn(t *testing.T) {
	ctx := context.Background()
	tool := &gateTool{started: make(chan struct{}, 1), release: make(chan struct{})}
	model := &scriptedRequests{answers: []sdk.ModelResult{toolCallAnswer()}}
	m, binding, sid := setup(t, model, tool)
	first, _ := m.SubmitInput(ctx, sid, "in-1", "hello")
	ref1 := turn.TurnRef{SessionID: sid, TurnID: "t1"}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = m.Coordinator.Start(ctx, turn.StartRequest{Ref: ref1, Inputs: []run.AgentInput{first}, ExecutionBinding: binding, Companion: turn.CompanionV1Version})
	}()
	<-tool.started

	resp, err := m.Coordinator.Stop(ctx, turn.StopRequest{Ref: ref1, Reason: "user"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != turn.TurnStopped || resp.Disposition != turn.ResumeFinished || resp.End == nil {
		t.Fatalf("stop response = %+v", resp)
	}
	if _, stopped := (*resp.End).(run.RunStoppedEnd); !stopped {
		t.Fatalf("end = %#v, want RunStoppedEnd", *resp.End)
	}
	close(tool.release)
	<-done

	// The abandoned worker's settlement was rejected; the Run is terminal.
	record, err := m.Runtime.Record(ctx, sid, resp.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Snapshot.State.Status != run.RunStopped || len(record.Snapshot.State.Result.UncertainCalls) != 1 {
		t.Fatalf("stopped run = %+v", record.Snapshot.State.Result)
	}

	second, _ := m.SubmitInput(ctx, sid, "in-2", "again")
	driver := &ref.SessionDriver{Coordinator: m.Coordinator, Memory: m, Binding: binding, Companion: turn.CompanionV1Version, NewTurnID: func() turn.TurnID { return "t2" }}
	resp2, err := driver.Send(ctx, sid, []run.AgentInput{second})
	if err != nil {
		t.Fatal(err)
	}
	if resp2.Ref.TurnID != "t2" || resp2.Status != turn.TurnCompleted {
		t.Fatalf("second send = %+v", resp2)
	}
	// The new Turn's request carried the stopped Turn's assistant tool call and
	// its unknown tool_result (REF-PLN-6), then the new input.
	last := model.seen[len(model.seen)-1].Messages
	if got := roles(last); len(got) != 5 || got[0] != "system" || got[1] != "user" || got[2] != "assistant" || got[3] != "tool" || got[4] != "user" {
		t.Fatalf("roles = %v", got)
	}
}

func roles(msgs []sdk.Message) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = string(m.Role)
	}
	return out
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(time.Millisecond)
	}
}
