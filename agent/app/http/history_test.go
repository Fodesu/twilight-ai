package http_test

import (
	"context"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/app"
	ownerhttp "github.com/felinics/twilight/agent/app/http"
	"github.com/felinics/twilight/agentcore/inbox"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
)

func TestHistorySnapshotAndResumeCommand(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	a, client := newOwner(t)
	const sid session.SessionID = "history-reopen"
	if err := client.Ensure(ctx, sid); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Open(ctx, sid, ownerhttp.OpenRequest{Preset: "p1"}); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"first", "second"} {
		command, err := app.NewCommand(inbox.CommandID(text), app.CommandSubmit, app.SubmitCommand{InputID: run.InputID("in-" + text), Text: text})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.Enqueue(ctx, sid, command); err != nil {
			t.Fatal(err)
		}
		if result, err := client.Await(ctx, sid, command.ID); err != nil || result.Status != inbox.StatusApplied {
			t.Fatalf("submit: %+v %v", result, err)
		}
		opened, _ := a.Opened(sid)
		if err := opened.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	history, err := client.History(ctx, sid)
	if err != nil || history.ProtocolVersion != 1 || history.Head.Next == 0 || len(history.Messages) != 4 {
		t.Fatalf("history: %+v %v", history, err)
	}
	for i, want := range []string{"first", "echo: first", "second", "echo: second"} {
		message := history.Messages[i]
		if message.Text != want || message.Position.Commit >= history.Head.Next {
			t.Fatalf("message %d: %+v", i, message)
		}
	}
	// The HTTP face must honor resume even when the Session is ALREADY open.
	// No work is a harmless applied kick, not a fake new Turn or assistant reply.
	resume, _ := app.NewCommand("resume", app.CommandResume, app.ResumeCommand{})
	if _, err := client.Enqueue(ctx, sid, resume); err != nil {
		t.Fatal(err)
	}
	if result, err := client.Await(ctx, sid, resume.ID); err != nil || result.Status != inbox.StatusApplied {
		t.Fatalf("resume: %+v %v", result, err)
	}
	opened, _ := a.Opened(sid)
	if err := opened.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	stale, _ := app.NewCommand("stale-resume", app.CommandResume, app.ResumeCommand{TurnID: "unobserved"})
	if _, err := client.Enqueue(ctx, sid, stale); err != nil {
		t.Fatal(err)
	}
	if result, err := client.Await(ctx, sid, stale.ID); err != nil || result.Status != inbox.StatusRejected {
		t.Fatalf("stale resume: %+v %v", result, err)
	}
	if err := client.Close(ctx, sid); err != nil {
		t.Fatal(err)
	}
	restored, err := client.History(ctx, sid)
	if err != nil || len(restored.Messages) != 4 {
		t.Fatalf("closed history: %+v %v", restored, err)
	}
	if _, err := client.Open(ctx, sid, ownerhttp.OpenRequest{Preset: "p1"}); err != nil {
		t.Fatal(err)
	}
	// The exact snapshot boundary yields new durable events, not the old prefix.
	streamCtx, stop := context.WithCancel(ctx)
	defer stop()
	events := make(chan ownerhttp.Event, 32)
	go func() {
		_ = client.EventsFrom(streamCtx, sid, restored.Head.Next, func(e ownerhttp.Event) bool { events <- e; return true })
	}()
	next, _ := app.NewCommand("third", app.CommandSubmit, app.SubmitCommand{InputID: "in-third", Text: "third"})
	if _, err := client.Enqueue(ctx, sid, next); err != nil {
		t.Fatal(err)
	}
	for {
		event := awaitHistoryEvent(t, ctx, events)
		if event.Type == "" {
			continue
		}
		if event.Position.Commit < restored.Head.Next {
			t.Fatalf("replayed snapshot: %+v", event)
		}
		return
	}
}

func awaitHistoryEvent(t *testing.T, ctx context.Context, events <-chan ownerhttp.Event) ownerhttp.Event {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return ownerhttp.Event{}
	}
}
