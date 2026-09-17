package host_test

import (
	"context"
	"testing"

	"github.com/felinics/twilight/agent/host"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/turn"
	"github.com/felinics/twilight/sdk"
)

// HST-FRK-1/2: forking before a Turn yields a child whose conversation ends
// where the Turn's inputs were still undelivered. Resuming the child
// regenerates the Turn from the same input; withdrawing the input and sending
// another edits it. The parent is unchanged either way, and both children
// read the frozen bodies of the shared prefix.
func TestForkBeforeTurnRegeneratesAndEdits(t *testing.T) {
	ctx := context.Background()
	model := &scriptedRequests{answers: []sdk.ModelResult{
		{Text: "first answer", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}},
		{Text: "second answer", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}},
		{Text: "regenerated", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}},
		{Text: "edited answer", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}},
	}}
	store, content := session.NewMemoryStore(), memoryContent()
	h := newHost(host.Ports{Store: store, Content: content, Ownership: session.OpenOptions{Takeover: true}}, map[run.ModelRef]loop.ModelInvoker{"m-1": model})
	preset, err := h.Presets.Register("b1", mustPreset("m-1", nil))
	if err != nil {
		t.Fatal(err)
	}
	names := func(prefix string) func() turn.TurnID {
		n := 0
		return func() turn.TurnID { n++; return turn.TurnID(prefix + string(rune('0'+n))) }
	}
	open := func(sid session.SessionID, prefix string) *host.Session {
		s, err := h.OpenSession(ctx, sid, host.SessionOptions{Preset: preset, NewTurnID: names(prefix)})
		if err != nil {
			t.Fatalf("open %s: %v", sid, err)
		}
		return s
	}
	parent := open("parent", "p")
	for _, text := range []string{"hello", "how are you"} {
		if results, err := parent.Send(ctx, text); err != nil || len(results) != 1 || results[0].Status != turn.TurnCompleted {
			t.Fatalf("send %q = %+v %v", text, results, err)
		}
	}
	if err := parent.Close(ctx); err != nil {
		t.Fatal(err)
	}
	parentHead, _ := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "parent"})

	// Regenerate: fork before the second Turn, open the child, resume it.
	header, err := h.ForkBeforeTurn(ctx, "parent", "p2", "regen")
	if err != nil {
		t.Fatal(err)
	}
	parentHeader, _ := h.Store.Header(ctx, "parent")
	if header.Parent == nil || header.Parent.Segment != session.SegmentIDOf(parentHeader) {
		t.Fatalf("fork header = %+v, want an edge to the parent's segment", header)
	}
	regen := open("regen", "r")
	chat, err := h.ChatlogSurface(ctx, "regen")
	if err != nil {
		t.Fatal(err)
	}
	if pending := chat.SubmittedInputs(); len(pending) != 1 || chat.Assistants.Len() != 1 {
		t.Fatalf("child before resume: pending=%d assistants=%d, want the second input undelivered and one reply", len(pending), chat.Assistants.Len())
	}
	// No Turn is active at the fork point; the undelivered input is a backlog
	// Drain starts a new Turn from.
	resp, ok, err := regen.Drain(ctx)
	if err != nil || !ok || resp.Status != turn.TurnCompleted {
		t.Fatalf("regen drain = %+v ok=%v %v", resp, ok, err)
	}
	if reply := lastReply(t, h, "regen"); reply != "regenerated" {
		t.Fatalf("regenerated reply = %q", reply)
	}
	reqs := model.requests()
	if last := messageTexts(reqs[len(reqs)-1]); len(last) != 3 || last[0] != "user: hello" || last[1] != "assistant: first answer" || last[2] != "user: how are you" {
		t.Fatalf("regenerated request = %v", last)
	}
	if err := regen.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// Edit: fork the same point, withdraw the original input, send another.
	if _, err := h.ForkBeforeTurn(ctx, "parent", "p2", "edit"); err != nil {
		t.Fatal(err)
	}
	edit := open("edit", "e")
	chat, _ = h.ChatlogSurface(ctx, "edit")
	pending := chat.SubmittedInputs()
	if err := h.WithdrawInput(ctx, "edit", run.InputID(pending[0].ID), "edited"); err != nil {
		t.Fatal(err)
	}
	if err := h.WithdrawInput(ctx, "edit", run.InputID(pending[0].ID), "edited"); err == nil {
		t.Fatal("withdrawing a withdrawn input succeeded")
	}
	results, err := edit.Send(ctx, "how is the weather")
	if err != nil || len(results) != 1 || results[0].Reply != "edited answer" {
		t.Fatalf("edit send = %+v %v", results, err)
	}
	reqs = model.requests()
	if last := messageTexts(reqs[len(reqs)-1]); len(last) != 3 || last[2] != "user: how is the weather" {
		t.Fatalf("edited request = %v", last)
	}
	chat, _ = h.ChatlogSurface(ctx, "edit")
	if v, _ := chat.Inputs.Get(pending[0].ID); v.Status != chatlog.InputWithdrawn {
		t.Fatalf("withdrawn input status = %s", v.Status)
	}
	if err := edit.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// The parent is untouched; each child holds only its own commits after
	// the shared prefix.
	after, _ := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "parent"})
	if after.Head != parentHead.Head {
		t.Fatalf("parent head moved: %+v -> %+v", parentHead.Head, after.Head)
	}
	for _, sid := range []session.SessionID{"regen", "edit"} {
		page, _ := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid})
		if len(page.Commits) <= int(header.Parent.Seq)+1 || page.Commits[header.Parent.Seq+1].PrevDigest != header.Parent.Digest {
			t.Fatalf("%s does not continue from the anchor", sid)
		}
	}
	// A Turn started by the first commit has no prefix; an unknown Turn is a
	// conflict; a Session cannot fork itself.
	if _, err := h.ForkBeforeTurn(ctx, "parent", "p9", "x"); err == nil {
		t.Fatal("fork before an unknown turn succeeded")
	}
	if _, err := h.Fork(ctx, host.ForkRequest{Parent: "parent", At: 0, Child: "parent"}); err == nil {
		t.Fatal("self fork succeeded")
	}
}

// lastReply materializes the text of the last assistant entry of a Session's
// context through the Host's content store.
func lastReply(t *testing.T, h *host.Host, sid session.SessionID) string {
	t.Helper()
	state, _, err := h.Projection(context.Background(), sid, chatlog.ContextProjectionID, chatlog.ContextProjection.Version)
	if err != nil {
		t.Fatal(err)
	}
	entries := state.(chatlog.Context).Entries
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].Kind == chatlog.EntryAssistant {
			m, err := chatlog.NewMaterializer(h.Content()).Entry(context.Background(), &entries[i])
			if err != nil {
				t.Fatal(err)
			}
			return m.Text()
		}
	}
	return ""
}
