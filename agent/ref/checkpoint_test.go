package ref_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/felinics/twilight/agent/ref"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/turn"
	"github.com/felinics/twilight/sdk"
)

// compactAwareModel answers turns with numbered replies and compactor
// requests (ref.CompactorSystemPrompt) with a fixed summary, recording every
// request.
type compactAwareModel struct {
	mu      sync.Mutex
	seen    []sdk.Request
	replies int
}

func (m *compactAwareModel) Generate(_ context.Context, req sdk.Request) (sdk.ModelResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seen = append(m.seen, req)
	if len(req.Messages) > 0 && req.Messages[0].Role == sdk.MessageRoleSystem && messageText(req.Messages[0]) == ref.CompactorSystemPrompt {
		return sdk.ModelResult{Text: "summary-of-the-past", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}, nil
	}
	m.replies++
	return sdk.ModelResult{Text: fmt.Sprintf("reply-%d", m.replies), FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}, nil
}

func (m *compactAwareModel) requests() []sdk.Request {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]sdk.Request(nil), m.seen...)
}

func messageText(m sdk.Message) string {
	var b strings.Builder
	for _, part := range m.Content {
		if t, ok := part.(sdk.TextPart); ok {
			b.WriteString(t.Text)
		}
	}
	return b.String()
}

func messageTexts(req sdk.Request) []string {
	out := make([]string, 0, len(req.Messages))
	for _, msg := range req.Messages {
		out = append(out, string(msg.Role)+": "+messageText(msg))
	}
	return out
}

func openCompactSession(t *testing.T, store session.Store, model *compactAwareModel, opts ref.SessionOptions) (*ref.Memory, *ref.Session) {
	t.Helper()
	m, err := ref.New(ref.Options{Store: store, Ownership: session.OpenOptions{Takeover: true}})
	if err != nil {
		t.Fatal(err)
	}
	agent, err := ref.NewAgent("m-1", model)
	if err != nil {
		t.Fatal(err)
	}
	profile, err := m.Agents.Register("b1", agent)
	if err != nil {
		t.Fatal(err)
	}
	opts.Profile = profile
	s, err := m.OpenSession(context.Background(), "s-ckpt", opts)
	if err != nil {
		t.Fatal(err)
	}
	return m, s
}

// An explicit Compact shrinks the next model request to the summary plus the
// retained suffix, and a restarted process assembles exactly the same context
// from the checkpointed log (CHT-EVT-3, REF-CKP-1).
func TestCompactShrinksContextAndReplaysAcrossRestart(t *testing.T) {
	ctx := context.Background()
	store := session.NewMemoryStore()
	model := &compactAwareModel{}
	m, s := openCompactSession(t, store, model, ref.SessionOptions{CompactRetainEntries: 1})

	for _, text := range []string{"one", "two"} {
		if _, err := s.Send(ctx, text); err != nil {
			t.Fatal(err)
		}
	}
	before := model.requests()
	grown := before[len(before)-1] // user one, assistant reply-1, user two
	if len(grown.Messages) != 3 {
		t.Fatalf("pre-compact request = %v", messageTexts(grown))
	}

	id, ok, err := s.Compact(ctx)
	if err != nil || !ok {
		t.Fatalf("compact = %s %v %v", id, ok, err)
	}
	chat, err := m.ChatlogSurface(ctx, "s-ckpt")
	if err != nil {
		t.Fatal(err)
	}
	if v := chat.Checkpoints[id]; v.Status != chatlog.CheckpointActive {
		t.Fatalf("checkpoint = %+v", v)
	}

	if _, err := s.Send(ctx, "three"); err != nil {
		t.Fatal(err)
	}
	reqs := model.requests()
	compacted := reqs[len(reqs)-1]
	want := []string{"assistant: summary-of-the-past", "assistant: reply-2", "user: three"}
	if got := messageTexts(compacted); !equalStrings(got, want) {
		t.Fatalf("post-compact request = %v, want %v", got, want)
	}

	// "Restart": a second assembly over the same store must assemble the next
	// request as exactly the settled continuation of the first process's view.
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	model2 := &compactAwareModel{}
	model2.replies = 3 // keep reply numbering aligned for readability only
	_, s2 := openCompactSession(t, store, model2, ref.SessionOptions{CompactRetainEntries: 1})
	if _, err := s2.Send(ctx, "four"); err != nil {
		t.Fatal(err)
	}
	reqs2 := model2.requests()
	next := messageTexts(reqs2[len(reqs2)-1])
	wantNext := append(messageTexts(compacted), "assistant: reply-3", "user: four")
	if !equalStrings(next, wantNext) {
		t.Fatalf("restarted request = %v, want %v", next, wantNext)
	}
}

// The automatic policy compacts after settlement once the context passes the
// threshold; failures reach CompactWarn only (REF-CKP-1).
func TestAutoCompactAfterSettlement(t *testing.T) {
	ctx := context.Background()
	var warned []error
	model := &compactAwareModel{}
	m, s := openCompactSession(t, store4(t), model, ref.SessionOptions{
		CompactAfterEntries: 3, CompactRetainEntries: 1,
		CompactWarn: func(err error) { warned = append(warned, err) },
	})
	if _, err := s.Send(ctx, "one"); err != nil { // 2 entries, below threshold
		t.Fatal(err)
	}
	if _, err := s.Send(ctx, "two"); err != nil { // 4 entries, compacts
		t.Fatal(err)
	}
	if len(warned) != 0 {
		t.Fatalf("warnings = %v", warned)
	}
	chat, err := m.ChatlogSurface(ctx, "s-ckpt")
	if err != nil {
		t.Fatal(err)
	}
	if len(chat.Checkpoints) != 1 {
		t.Fatalf("checkpoints = %+v", chat.Checkpoints)
	}
	state, _, err := m.Projection(ctx, "s-ckpt", chatlog.ContextProjectionID, chatlog.ContextProjection.Version)
	if err != nil {
		t.Fatal(err)
	}
	if entries := state.(chatlog.Context).Entries; len(entries) != 2 || entries[0].Kind != chatlog.EntrySummary {
		t.Fatalf("entries = %+v", entries)
	}
}

// Compact refuses while a Turn is active: compaction is a between-turns
// policy (REF-CKP-1).
func TestCompactRefusesWhileTurnActive(t *testing.T) {
	ctx := context.Background()
	tool := &gateTool{started: make(chan struct{}, 1), release: make(chan struct{})}
	model := &scriptedRequests{answers: []sdk.ModelResult{toolCallAnswer()}}
	m, profile, sid := setup(t, model, tool)
	s, err := m.OpenSession(ctx, sid, ref.SessionOptions{Profile: profile, CompactRetainEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := s.Send(ctx, "one")
		done <- err
	}()
	<-tool.started
	if _, _, err := s.Compact(ctx); !errors.Is(err, turn.ErrConflict) {
		t.Fatalf("compact mid-turn = %v, want conflict", err)
	}
	close(tool.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func store4(t *testing.T) session.Store {
	t.Helper()
	return session.NewMemoryStore()
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
