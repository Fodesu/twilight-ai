package app_test

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/target"
	"github.com/felinics/twilight/agent/turn"
	"github.com/felinics/twilight/sdk"
)

var (
	ws1 = run.TargetRef{Kind: "workspace", ID: "ws-1"}
	ws2 = run.TargetRef{Kind: "workspace", ID: "ws-2"}
)

// targetTool reports the target each of its tool effects executes against.
type targetTool struct {
	seen chan *run.TargetRef
}

func (t *targetTool) Ref() run.ToolRef { return "lookup" }
func (t *targetTool) Definition() sdk.ToolDefinition {
	return sdk.ToolDefinition{Name: "lookup", Parameters: []byte(`{"type":"object"}`)}
}
func (t *targetTool) ResponsePolicy() run.ResponsePolicy        { return run.DirectExecution }
func (t *targetTool) ValidateArguments(run.CanonicalJSON) error { return nil }
func (t *targetTool) Execute(_ context.Context, req loop.ToolExecutionRequest) loop.ToolExecutionOutcome {
	t.seen <- req.Target
	return loop.ToolExecutionSucceeded{Result: run.ToolExecutionResult{Output: req.Arguments}}
}

// The bound target is a Session fact the default resolver hands to every tool
// effect (APP-TGT-1, RUN-LOP-9); binding the current target again writes
// nothing.
func TestBindTargetReachesToolEffects(t *testing.T) {
	ctx := context.Background()
	tool := &targetTool{seen: make(chan *run.TargetRef, 1)}
	model := &scriptedRequests{answers: []sdk.ModelResult{toolCallAnswer()}}
	store := session.NewMemoryStore()
	h := newHost(app.Config{Store: store}, map[run.ModelRef]loop.ModelInvoker{"m-1": model}, tool)
	preset, err := h.RegisterPreset("b1", mustPreset("m-1", []loop.ExecutableTool{tool}))
	if err != nil {
		t.Fatal(err)
	}
	const sid session.SessionID = "s-1"
	s, err := h.OpenSession(ctx, sid, app.SessionOptions{Preset: preset})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.Target(ctx); err != nil || got != nil {
		t.Fatalf("unbound target = %v %v", got, err)
	}
	if err := s.BindTarget(ctx, run.TargetRef{Kind: "workspace"}); err == nil {
		t.Fatal("an incomplete target was bound")
	}
	if err := s.BindTarget(ctx, ws1); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Target(ctx); err != nil || got == nil || *got != ws1 {
		t.Fatalf("target = %v %v, want %v", got, err, ws1)
	}
	commits := func() int {
		page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid})
		if err != nil {
			t.Fatal(err)
		}
		return len(page.Commits)
	}
	before := commits()
	if err := s.BindTarget(ctx, ws1); err != nil {
		t.Fatal(err)
	}
	if after := commits(); after != before {
		t.Fatalf("rebinding the current target wrote %d commits", after-before)
	}

	results, err := s.Send(ctx, "what is the weather?")
	if err != nil || len(results) != 1 || results[0].Status != turn.TurnCompleted {
		t.Fatalf("send = %+v %v", results, err)
	}
	if seen := <-tool.seen; seen == nil || *seen != ws1 {
		t.Fatalf("tool effect target = %v, want %v", seen, ws1)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// The binding changes between Turns only (APP-TGT-1).
func TestBindTargetRefusesWhileTurnActive(t *testing.T) {
	ctx := context.Background()
	tool := &gateTool{started: make(chan struct{}, 1), release: make(chan struct{})}
	model := &scriptedRequests{answers: []sdk.ModelResult{toolCallAnswer()}}
	_, _, _, s := setup(t, model, tool, app.SessionOptions{})
	done := make(chan error, 1)
	go func() {
		_, err := s.Send(ctx, "one")
		done <- err
	}()
	<-tool.started
	if err := s.BindTarget(ctx, ws1); !errors.Is(err, turn.ErrConflict) {
		t.Fatalf("bind mid-turn = %v, want conflict", err)
	}
	close(tool.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := s.BindTarget(ctx, ws1); err != nil {
		t.Fatalf("bind after settlement = %v", err)
	}
}

// The binding is of session lineage: a fork child starts with its parent's
// target and rebinding the child leaves the parent unchanged (SES-FRK-5).
func TestForkChildInheritsTarget(t *testing.T) {
	ctx := context.Background()
	model := &scriptedRequests{}
	h := newHost(app.Config{Store: session.NewMemoryStore()}, map[run.ModelRef]loop.ModelInvoker{"m-1": model})
	preset, err := h.RegisterPreset("b1", mustPreset("m-1", nil))
	if err != nil {
		t.Fatal(err)
	}
	open := func(sid session.SessionID, prefix string) *app.Session {
		n := 0
		s, err := h.OpenSession(ctx, sid, app.SessionOptions{Preset: preset,
			NewTurnID: func() turn.TurnID { n++; return turn.TurnID(prefix + string(rune('0'+n))) }})
		if err != nil {
			t.Fatalf("open %s: %v", sid, err)
		}
		return s
	}
	parent := open("parent", "p")
	if err := parent.BindTarget(ctx, ws1); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"hello", "again"} {
		if _, err := parent.Send(ctx, text); err != nil {
			t.Fatal(err)
		}
	}
	if err := parent.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := h.ForkBeforeTurn(ctx, "parent", "p2", "child"); err != nil {
		t.Fatal(err)
	}
	child := open("child", "c")
	if got, err := child.Target(ctx); err != nil || got == nil || *got != ws1 {
		t.Fatalf("inherited target = %v %v, want %v", got, err, ws1)
	}
	if err := child.BindTarget(ctx, ws2); err != nil {
		t.Fatal(err)
	}
	if got, err := child.Target(ctx); err != nil || got == nil || *got != ws2 {
		t.Fatalf("child target = %v %v, want %v", got, err, ws2)
	}
	if cur, err := target.Read(ctx, h.Authority.Projections, "parent"); err != nil || cur.Target == nil || *cur.Target != ws1 {
		t.Fatalf("parent target = %+v %v, want %v", cur, err, ws1)
	}
	if err := child.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
