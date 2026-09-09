package ref_test

import (
	"context"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/ref"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/turn"
	"github.com/felinics/twilight/sdk"
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

func toolCallAnswer() sdk.ModelResult {
	return sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls, Usage: sdk.Usage{TotalTokens: 1},
		ToolCalls: []sdk.ToolCall{{ToolCallID: "c1", ToolName: "lookup", Input: `{"q":"weather"}`}}}
}

func setup(t *testing.T, model loop.ModelInvoker, tool *gateTool) (*ref.Memory, turn.ProfileRef, session.SessionID) {
	t.Helper()
	m, err := ref.New(ref.Options{})
	if err != nil {
		t.Fatal(err)
	}
	const sid session.SessionID = "s-1"
	if err := m.CreateSession(context.Background(), sid); err != nil {
		t.Fatal(err)
	}
	agent, err := ref.NewAgent("m-1", model, ref.WithTool(tool), ref.WithSystemPrompt("be brief"))
	if err != nil {
		t.Fatal(err)
	}
	profile, err := m.Agents.Register("b1", agent)
	if err != nil {
		t.Fatal(err)
	}
	return m, profile, sid
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
		resp, err := m.Coordinator.Start(ctx, turn.StartRequest{Ref: ref1, Inputs: []run.AgentInput{first}, Profile: binding, Companion: turn.CompanionV1Version})
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
	driver := &ref.SessionDriver{Coordinator: m.Coordinator, Memory: m, Profile: binding, Companion: turn.CompanionV1Version, NewTurnID: func() turn.TurnID { return "t2" }}
	// Deliver commits AcceptInput + input_delivered without waiting for the
	// tool; the Run is already driven here, so the response reports
	// already_driving (or finished when the running driver settles first).
	deliverDone := make(chan turn.TurnResponse, 1)
	go func() {
		resp, err := driver.Send(ctx, sid, []run.AgentInput{second})
		if err != nil {
			t.Error(err)
		}
		deliverDone <- resp
	}()
	// The Deliver commit lands while the tool runs; the Loop sees PendingInputs
	// at its next Load. Release the tool and let both drivers finish.
	waitFor(t, func() bool {
		surface, err := m.TurnSurface(ctx, sid)
		return err == nil && len(surface.Turns["t1"].InputIDs) == 2
	})
	close(tool.release)
	if resp := <-deliverDone; resp.Disposition != turn.ResumeAlreadyDriving && resp.Disposition != turn.ResumeFinished {
		t.Fatalf("deliver disposition = %s", resp.Disposition)
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
		_, _ = m.Coordinator.Start(ctx, turn.StartRequest{Ref: ref1, Inputs: []run.AgentInput{first}, Profile: binding, Companion: turn.CompanionV1Version})
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
	driver := &ref.SessionDriver{Coordinator: m.Coordinator, Memory: m, Profile: binding, Companion: turn.CompanionV1Version, NewTurnID: func() turn.TurnID { return "t2" }}
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
