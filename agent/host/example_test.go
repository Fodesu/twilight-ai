package host_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/felinics/twilight/agent/host"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/turn"
	"github.com/felinics/twilight/sdk"
)

// Example_recoverableTurn drives one Turn through a process crash on the
// single Session stream.
//
// Process 1 owns the Session (Epoch 1), submits the user input and starts the
// Turn. The model asks for a tool; the tool never returns and the process dies
// while the call is Executing. Nothing is written on the way down.
//
// Process 2 reopens the same Session store with Takeover — the crashed owner
// never closed — and takes the Session over (Epoch 2). Its Executor is fresh,
// so no attempt reattaches: the takeover disposition settles the abandoned
// call as Unknown in the same group as its chatlog tool_result, the Run stays
// Active, and Drive runs the Loop: the planner reads the conversation back
// from the chatlog projection and the Turn completes. The dead process's
// worker finally returns and its settlement is fenced by the kernel: nothing
// of Epoch 1 reaches the stream after the takeover.
func Example_recoverableTurn() {
	ctx := context.Background()
	const sid session.SessionID = "session-1"
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}

	// Shared "durable" state: the Session store and the content store of frozen request bodies.
	store := session.NewMemoryStore()
	content := memoryContent()
	tool := &lookupTool{block: make(chan struct{})}
	profile := mustProfile("m-1", []loop.ExecutableTool{tool})

	// ---- process 1 ----------------------------------------------------------
	p1 := newHost(host.Ports{Store: store, Content: content, Clock: clock.Now},
		map[run.ModelRef]loop.ModelInvoker{"m-1": &scriptedModel{}}, tool)
	if err := p1.CreateSession(ctx, sid); err != nil {
		panic(err)
	}
	if _, err := p1.Open(ctx, sid); err != nil {
		panic(err)
	}
	profile1, err := p1.Profiles.Register("weather-agent", profile)
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
			Profile: profile1, Companion: turn.CompanionV1Version})
		if err == nil {
			// The Coordinator only commits; the host drives (HST-DRV-1).
			_, err = p1.Drive(ctx, ref1)
		}
		startDone <- err
	}()
	runID := waitForExecutingCall(ctx, p1, sid, ref1.TurnID)
	fmt.Println("process 1: tool call is Executing; process crashes")

	// ---- process 2 ----------------------------------------------------------
	p2 := newHost(host.Ports{Store: store, Content: content, Ownership: session.OpenOptions{Takeover: true}, Clock: clock.Now},
		map[run.ModelRef]loop.ModelInvoker{"m-1": &scriptedModel{}}, tool)
	// The profile is re-registered from the same public configuration, so the
	// ref the Session recorded still resolves.
	if _, err := p2.Profiles.Register("weather-agent", profile); err != nil {
		panic(err)
	}
	recovered, err := p2.Open(ctx, sid)
	if err != nil {
		panic(err)
	}
	chat, err := p2.ChatlogSurface(ctx, sid)
	if err != nil {
		panic(err)
	}
	fmt.Printf("process 2: took over; %d executing target disposed; chatlog has %d tool_result(s) with status %s\n", recovered, chat.ToolResults.Len(), toolResultStatus(&chat))

	resp, err := p2.Drive(ctx, ref1)
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

	// Let the abandoned worker exit; its settlement is fenced because process
	// 1's Epoch was superseded.
	close(tool.block)
	err = <-startDone
	fmt.Printf("process 1: %v\n", errorsIsOwnershipLost(err))
	after, _ := p2.Runtime.Record(ctx, sid, runID)
	fmt.Printf("stream unchanged by the fenced worker: %v\n", len(after.Facts) == len(record.Facts))

	// Output:
	// process 1: tool call is Executing; process crashes
	// process 2: took over; 1 executing target disposed; chatlog has 1 tool_result(s) with status unknown
	// process 2: turn completed, disposition finished, attempt 1
	// record: 12 run facts fold to the projection; chatlog entries: 4
	// process 1: ownership lost
	// stream unchanged by the fenced worker: true
}

func toolResultStatus(s *chatlog.Surface) string {
	status := "none"
	s.ToolResults.Range(func(_ chatlog.ToolResultID, r chatlog.ToolResult) bool {
		status = string(r.Status)
		return false
	})
	return status
}

func waitForExecutingCall(ctx context.Context, h *host.Host, sid session.SessionID, turnID turn.TurnID) run.RunID {
	deadline := time.Now().Add(10 * time.Second)
	for {
		surface, err := h.TurnSurface(ctx, sid)
		if err == nil {
			if v, ok := surface.Turns[turnID]; ok && v.ActiveRun != "" {
				snap, err := h.Runtime.Load(ctx, sid, v.ActiveRun)
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
		return loop.ToolExecutionSucceeded{Result: run.ToolExecutionResult{Output: req.Arguments}}
	}
	return loop.ToolExecutionSucceeded{Result: run.ToolExecutionResult{Output: req.Arguments}}
}
