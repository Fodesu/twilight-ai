package host_test

import (
	"context"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/host"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/turn"
	"github.com/felinics/twilight/sdk"
)

// setup composes a Host over an in-memory store with one model and one tool,
// registers the preset and opens a Session whose new Turns are named t2, t3, ...
func setup(t *testing.T, model loop.ModelInvoker, tool *gateTool, opts host.SessionOptions) (*host.Host, turn.PresetRef, session.SessionID, *host.Session) {
	t.Helper()
	tools := []loop.ExecutableTool{}
	if tool != nil {
		tools = append(tools, tool)
	}
	h := newHost(host.Ports{}, map[run.ModelRef]loop.ModelInvoker{"m-1": model}, tools...)
	const sid session.SessionID = "s-1"
	if err := h.CreateSession(context.Background(), sid); err != nil {
		t.Fatal(err)
	}
	preset, err := h.Presets.Register("b1", mustPreset("m-1", tools, host.WithSystemPrompt("be brief")))
	if err != nil {
		t.Fatal(err)
	}
	opts.Preset = preset
	next := 2
	if opts.NewTurnID == nil {
		opts.NewTurnID = func() turn.TurnID { id := turn.TurnID("t" + string(rune('0'+next))); next++; return id }
	}
	s, err := h.OpenSession(context.Background(), sid, opts)
	if err != nil {
		t.Fatal(err)
	}
	return h, preset, sid, s
}

// An input delivered while a tool call is Executing queues on the Run, is
// delivered to the same Turn in the same commit, and reaches the model in the
// next request together with the tool result (TRN-DLV, RUN-LOP-8).
func TestDeliverMidTurnReachesNextModelRequest(t *testing.T) {
	ctx := context.Background()
	tool := &gateTool{started: make(chan struct{}, 1), release: make(chan struct{})}
	model := &scriptedRequests{answers: []sdk.ModelResult{toolCallAnswer()}}
	h, preset, sid, s := setup(t, model, tool, host.SessionOptions{})

	first, err := h.SubmitInput(ctx, sid, "in-1", "what is the weather?")
	if err != nil {
		t.Fatal(err)
	}
	ref1 := turn.TurnRef{SessionID: sid, TurnID: "t1"}
	done := make(chan turn.TurnResponse, 1)
	go func() {
		resp, err := h.Coordinator.Start(ctx, turn.StartRequest{Ref: ref1, Inputs: []run.AgentInput{first}, Preset: preset, Companion: turn.CompanionV1Version})
		if err == nil {
			// The Coordinator only commits; the host drives (HST-DRV-1).
			resp, err = h.Drive(ctx, ref1)
		}
		if err != nil {
			t.Error(err)
		}
		done <- resp
	}()
	<-tool.started

	second, err := h.SubmitInput(ctx, sid, "in-2", "and tomorrow?")
	if err != nil {
		t.Fatal(err)
	}
	// Deliver commits AcceptInput + input_delivered without waiting for the
	// tool; the Run is already driven here, so the response reports
	// already_driving (or finished when the running driver settles first).
	deliverDone := make(chan turn.TurnResponse, 1)
	go func() {
		resp, err := s.Route(ctx, []run.AgentInput{second})
		if err != nil {
			t.Error(err)
		}
		deliverDone <- resp
	}()
	// The Deliver commit lands while the tool runs; the Loop sees PendingInputs
	// at its next Load. Release the tool and let both drivers finish.
	waitFor(t, func() bool {
		surface, err := h.TurnSurface(ctx, sid)
		return err == nil && len(surface.Turns["t1"].InputIDs) == 2
	})
	close(tool.release)
	if resp := <-deliverDone; resp.Disposition != host.ResumeAlreadyDriving && resp.Disposition != turn.ResumeFinished {
		t.Fatalf("deliver disposition = %s", resp.Disposition)
	}
	resp := <-done
	if resp.Status != turn.TurnCompleted {
		t.Fatalf("turn status = %s, want completed", resp.Status)
	}
	seen := model.requests()
	if len(seen) != 2 {
		t.Fatalf("model requests = %d, want 2", len(seen))
	}
	last := seen[1].Messages
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
	chat, err := h.ChatlogSurface(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := chat.Inputs.Get("in-2"); got.Status != chatlog.InputDelivered || got.Input.TurnID != "t1" {
		t.Fatalf("in-2 = %+v, want delivered to t1", got)
	}
}

// Stop settles the Turn as stopped in the same commit as CancelRun; a later
// Route opens a new Turn whose prompt builder sees the stopped Turn's content.
func TestStopSettlesTurnAndNextSendStartsNewTurn(t *testing.T) {
	ctx := context.Background()
	tool := &gateTool{started: make(chan struct{}, 1), release: make(chan struct{})}
	model := &scriptedRequests{answers: []sdk.ModelResult{toolCallAnswer()}}
	h, preset, sid, s := setup(t, model, tool, host.SessionOptions{})
	first, _ := h.SubmitInput(ctx, sid, "in-1", "hello")
	ref1 := turn.TurnRef{SessionID: sid, TurnID: "t1"}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := h.Coordinator.Start(ctx, turn.StartRequest{Ref: ref1, Inputs: []run.AgentInput{first}, Preset: preset, Companion: turn.CompanionV1Version}); err == nil {
			_, _ = h.Drive(ctx, ref1)
		}
	}()
	<-tool.started

	resp, err := h.Coordinator.Stop(ctx, turn.StopRequest{Ref: ref1, Reason: "user"})
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
	record, err := h.Runtime.Record(ctx, sid, resp.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Snapshot.State.Status != run.RunStopped || len(record.Snapshot.State.Result.UncertainCalls) != 1 {
		t.Fatalf("stopped run = %+v", record.Snapshot.State.Result)
	}

	second, _ := h.SubmitInput(ctx, sid, "in-2", "again")
	resp2, err := s.Route(ctx, []run.AgentInput{second})
	if err != nil {
		t.Fatal(err)
	}
	if resp2.Ref.TurnID != "t2" || resp2.Status != turn.TurnCompleted {
		t.Fatalf("second send = %+v", resp2)
	}
	// The new Turn's request carried the stopped Turn's assistant tool call and
	// its unknown tool_result (DEC-PMT-6), then the new input.
	seen := model.requests()
	last := seen[len(seen)-1].Messages
	if got := roles(last); len(got) != 5 || got[0] != "system" || got[1] != "user" || got[2] != "assistant" || got[3] != "tool" || got[4] != "user" {
		t.Fatalf("roles = %v", got)
	}
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
