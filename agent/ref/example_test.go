package ref_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/memohai/twilight/agent/ref"
	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/run/loop"
	"github.com/memohai/twilight/agent/session"
	"github.com/memohai/twilight/agent/session/chatlog"
	"github.com/memohai/twilight/agent/turn"
	"github.com/memohai/twilight/sdk"
)

// Example_recoverableTurn drives one Turn through a process crash on the
// single Session stream.
//
// Process 1 creates the Session, submits the user input and starts the Turn.
// The model asks for a tool; the tool never returns and the process dies while
// the call is Executing with a live lease. Nothing is written on the way down.
//
// Process 2 reopens the same Session store. The lease has expired, so
// RecoverExpired settles the abandoned call as Unknown in the same commit as
// its chatlog tool_result, the Run stays Active, and Resume drives the Loop:
// the planner reads the conversation back from the chatlog projection and
// the Turn completes. The turn surface and the chatlog then agree on what
// happened without any cross-store reconciliation.
func Example_recoverableTurn() {
	ctx := context.Background()
	const sid session.SessionID = "session-1"
	const leaseTTL = 30 * time.Second
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}

	// Shared "durable" state: the Session store and the frozen request bodies.
	store := session.NewMemoryStore()
	frozen := run.NewMemoryFrozenValues()
	tool := &lookupTool{block: make(chan struct{})}

	// ---- process 1 ----------------------------------------------------------
	p1, err := ref.New(ref.Options{Store: store, Frozen: frozen, LeaseTTL: leaseTTL, Now: clock.Now})
	if err != nil {
		panic(err)
	}
	if err := p1.CreateSession(ctx, sid); err != nil {
		panic(err)
	}
	binding1, err := p1.Bindings.Register("weather-agent", newBinding(tool))
	if err != nil {
		panic(err)
	}
	input, err := p1.SubmitInput(ctx, sid, "in-1", "what is the weather?")
	if err != nil {
		panic(err)
	}
	ref1 := turn.TurnRef{SessionID: sid, TurnID: "turn-1"}
	startDone := make(chan error, 1)
	go func() {
		_, err := p1.Coordinator.Start(ctx, turn.StartRequest{Ref: ref1, Inputs: []run.AgentInput{input},
			ExecutionBinding: binding1, Companion: turn.CompanionV1Version})
		startDone <- err
	}()
	runID := waitForExecutingCall(ctx, p1, sid, ref1.TurnID)
	fmt.Println("process 1: tool call is Executing; process crashes")

	// ---- process 2 ----------------------------------------------------------
	clock.Advance(2 * leaseTTL)
	p2, err := ref.New(ref.Options{Store: store, Frozen: frozen, LeaseTTL: leaseTTL, Now: clock.Now})
	if err != nil {
		panic(err)
	}
	// The binding is re-registered from the same public configuration, so the
	// ref the Session recorded still resolves.
	if _, err := p2.Bindings.Register("weather-agent", newBinding(tool)); err != nil {
		panic(err)
	}
	snap, err := p2.Runtime.Load(ctx, sid, runID)
	if err != nil {
		panic(err)
	}
	fmt.Printf("process 2: run active = %v, needs recovery = %v\n", snap.State.Status == run.RunActive, run.NeedsRecovery(snap.State))

	recovered, err := p2.Runtime.RecoverExpired(ctx)
	if err != nil {
		panic(err)
	}
	chat, err := p2.ChatlogSurface(ctx, sid)
	if err != nil {
		panic(err)
	}
	fmt.Printf("recovered %d lease; chatlog has %d tool_result(s) with status %s\n", recovered, len(chat.ToolResults), toolResultStatus(&chat))

	resp, err := p2.Coordinator.Resume(ctx, turn.TurnRequest{Ref: ref1})
	if err != nil {
		panic(err)
	}
	fmt.Printf("process 2: turn %s, disposition %s, attempt %d\n", resp.Status, resp.Disposition, resp.Attempt)

	record, err := p2.Runtime.Record(ctx, sid, runID)
	if err != nil {
		panic(err)
	}
	chat, _ = p2.ChatlogSurface(ctx, sid)
	fmt.Printf("record: %d run facts fold to the projection; chatlog entries: %d\n", len(record.Facts), len(chat.EntryOrder))

	// Let the abandoned worker exit; its settlement is rejected because the
	// lease it held was released by recovery.
	close(tool.block)
	<-startDone

	// Output:
	// process 1: tool call is Executing; process crashes
	// process 2: run active = true, needs recovery = true
	// recovered 1 lease; chatlog has 1 tool_result(s) with status unknown
	// process 2: turn completed, disposition finished, attempt 1
	// record: 12 run facts fold to the projection; chatlog entries: 4
}

func toolResultStatus(s *chatlog.Surface) string {
	for _, r := range s.ToolResults {
		return string(r.Status)
	}
	return "none"
}

func waitForExecutingCall(ctx context.Context, m *ref.Memory, sid session.SessionID, turnID turn.TurnID) run.RunID {
	deadline := time.Now().Add(10 * time.Second)
	for {
		surface, err := m.TurnSurface(ctx, sid)
		if err == nil {
			if v, ok := surface.Turns[turnID]; ok && v.ActiveRun != "" {
				snap, err := m.Runtime.Load(ctx, sid, v.ActiveRun)
				if err == nil && len(run.ExecutingCalls(snap.State)) == 1 {
					return v.ActiveRun
				}
			}
		}
		if time.Now().After(deadline) {
			panic("tool call never started")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func newBinding(tool *lookupTool) ref.Binding {
	def, err := run.FreezeToolDefinition(tool.Definition())
	if err != nil {
		panic(err)
	}
	return ref.Binding{
		Public: ref.BindingPublic{Model: "m-1", Tools: []ref.PublicTool{{Ref: tool.Ref(), Definition: def, Policy: run.DirectExecution}}},
		Models: modelCatalog{&scriptedModel{}},
		Tools:  toolCatalog{tool},
		Policy: loop.ExecutionPolicy{LeaseRenewInterval: 5 * time.Second},
	}
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

type modelCatalog struct{ m loop.ModelInvoker }

func (c modelCatalog) ResolveModel(run.ModelRef) (loop.ModelInvoker, error) { return c.m, nil }

type toolCatalog struct{ tool *lookupTool }

func (c toolCatalog) ResolveTool(ref run.ToolRef) (loop.ExecutableTool, error) {
	if ref != c.tool.Ref() {
		return nil, fmt.Errorf("unknown tool %q", ref)
	}
	return c.tool, nil
}

// scriptedModel asks for the tool until a tool result is in the conversation,
// then answers.
type scriptedModel struct{}

func (scriptedModel) Generate(_ context.Context, req sdk.Request) (sdk.ModelResult, error) {
	if n := len(req.Messages); n > 0 && req.Messages[n-1].Role == sdk.MessageRoleTool {
		return sdk.ModelResult{Text: "done", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}, nil
	}
	return sdk.ModelResult{
		FinishReason: sdk.FinishReasonToolCalls,
		Usage:        sdk.Usage{TotalTokens: 1},
		ToolCalls:    []sdk.ToolCall{{ToolCallID: "c1", ToolName: "lookup", Input: `{"q":"weather"}`}},
	}, nil
}

// lookupTool blocks on its first execution until block is closed.
type lookupTool struct {
	block chan struct{}
	ran   atomic.Bool
}

func (t *lookupTool) Ref() run.ToolRef { return "lookup" }
func (t *lookupTool) Definition() sdk.ToolDefinition {
	return sdk.ToolDefinition{Name: "lookup", Parameters: []byte(`{"type":"object","properties":{"q":{"type":"string"}}}`)}
}
func (t *lookupTool) ResponsePolicy() run.ResponsePolicy        { return run.DirectExecution }
func (t *lookupTool) ValidateArguments(run.CanonicalJSON) error { return nil }
func (t *lookupTool) Execute(_ context.Context, req loop.ToolExecutionRequest) loop.ToolExecutionOutcome {
	if t.ran.CompareAndSwap(false, true) {
		<-t.block
		return loop.ToolExecutionUnknown{Failure: run.ToolFailure{Class: run.FailureEffectUnknown, Message: "process died"}}
	}
	return loop.ToolExecutionSucceeded{Result: run.ToolExecutionResult{Output: req.Arguments}}
}
