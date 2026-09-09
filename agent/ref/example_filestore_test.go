package ref_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/memohai/twilight/agent/ref"
	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/run/loop"
	"github.com/memohai/twilight/agent/session"
	"github.com/memohai/twilight/agent/session/filestore"
	"github.com/memohai/twilight/agent/turn"
	"github.com/memohai/twilight/sdk"
)

// Example_jsonlPrototype is the full prototype on the JSONL file store: one
// Session directory on disk carries the whole agent.
//
// Turn 1 shows steer and queue: while its tool call executes, a second Send
// routes to Deliver (the input joins the running Turn) and a third input is
// only submitted (it queues). After the Turn settles, OnTurnSettled starts
// Turn 2 from the queued input.
//
// Turn 2 shows resume: the process "crashes" while its tool call executes.
// A second Store instance over the same directory — a new process — opens
// with Takeover, disposes the abandoned call and Resume completes the Turn.
// The dead process's late settlement is fenced by owner.json. The log stays
// one JSONL file, readable with standard tools.
func Example_jsonlPrototype() {
	ctx := context.Background()
	const sid session.SessionID = "session-jsonl"
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	root, err := os.MkdirTemp("", "twilight-jsonl-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(root)

	tool := &stagedTool{}
	frozen := run.NewMemoryFrozenValues()

	// ---- process 1 ----------------------------------------------------------
	store1, err := filestore.New(root)
	if err != nil {
		panic(err)
	}
	p1, err := ref.New(ref.Options{Store: store1, Frozen: frozen, Now: clock.Now})
	if err != nil {
		panic(err)
	}
	if err := p1.CreateSession(ctx, sid); err != nil {
		panic(err)
	}
	if _, err := p1.Open(ctx, sid); err != nil {
		panic(err)
	}
	model1 := &scriptedRequests{answers: []sdk.ModelResult{protoToolCall("call-1"), protoText("done"), protoToolCall("call-2")}}
	profile1, err := p1.Agents.Register("jsonl-agent", protoAgent(model1, tool))
	if err != nil {
		panic(err)
	}
	turnSeq := 0
	driver := &ref.SessionDriver{Coordinator: p1.Coordinator, Memory: p1, Profile: profile1, Companion: turn.CompanionV1Version,
		NewTurnID: func() turn.TurnID { turnSeq++; return turn.TurnID(fmt.Sprintf("turn-%d", turnSeq)) }}

	// Turn 1: Send starts the Turn; the model asks for the tool, which blocks.
	stage1 := tool.stage()
	in1, err := p1.SubmitInput(ctx, sid, "in-1", "what is the weather?")
	if err != nil {
		panic(err)
	}
	turn1Done := make(chan turn.TurnResponse, 1)
	go func() {
		resp, err := driver.Send(ctx, sid, []run.AgentInput{in1})
		if err != nil {
			panic(err)
		}
		turn1Done <- resp
	}()
	<-stage1.started

	// Steer: a second Send while turn-1 runs routes to Deliver (REF-DRV-1).
	in2, err := p1.SubmitInput(ctx, sid, "in-2", "and tomorrow?")
	if err != nil {
		panic(err)
	}
	steerDone := make(chan struct{})
	go func() {
		defer close(steerDone)
		// Deliver into the running Turn returns already_driving, not an error.
		if _, err := driver.Send(ctx, sid, []run.AgentInput{in2}); err != nil {
			panic(err)
		}
	}()
	waitUntil(func() bool {
		surface, err := p1.TurnSurface(ctx, sid)
		return err == nil && len(surface.Turns["turn-1"].InputIDs) == 2
	})
	<-steerDone
	chat, err := p1.ChatlogSurface(ctx, sid)
	if err != nil {
		panic(err)
	}
	fmt.Printf("steer: in-2 %s to turn-1 while its tool call executes\n", chat.Inputs["in-2"].Status)

	// Queue: in-3 is only submitted; nothing delivers it into the running Turn.
	if _, err := p1.SubmitInput(ctx, sid, "in-3", "book a table"); err != nil {
		panic(err)
	}
	chat, _ = p1.ChatlogSurface(ctx, sid)
	fmt.Printf("queue: %d input pending while turn-1 runs\n", len(chat.SubmittedInputs()))

	close(stage1.release)
	resp1 := <-turn1Done
	fmt.Printf("turn-1: %s\n", resp1.Status)

	// Turn 2 opens from the backlog (REF-DRV-2); its tool call blocks and the
	// process dies while the call is Executing.
	stage2 := tool.stage()
	turn2Err := make(chan error, 1)
	go func() {
		_, _, err := driver.OnTurnSettled(ctx, sid)
		turn2Err <- err
	}()
	<-stage2.started
	fmt.Println("turn-2: started from the queued input; tool call is Executing; process 1 crashes")

	// ---- process 2: a new Store instance over the same directory -------------
	store2, err := filestore.New(root)
	if err != nil {
		panic(err)
	}
	p2, err := ref.New(ref.Options{Store: store2, Frozen: frozen, Ownership: session.OpenOptions{Takeover: true}, Now: clock.Now})
	if err != nil {
		panic(err)
	}
	if _, err := p2.Agents.Register("jsonl-agent", protoAgent(&scriptedRequests{}, tool)); err != nil {
		panic(err)
	}
	recovered, err := p2.Open(ctx, sid)
	if err != nil {
		panic(err)
	}
	fmt.Printf("process 2: took over; %d executing target disposed\n", recovered)

	resp2, err := p2.Coordinator.Resume(ctx, turn.TurnRequest{Ref: turn.TurnRef{SessionID: sid, TurnID: "turn-2"}})
	if err != nil {
		panic(err)
	}
	fmt.Printf("turn-2: %s, disposition %s, attempt %d\n", resp2.Status, resp2.Disposition, resp2.Attempt)

	// The dead process's worker returns; owner.json fences its settlement.
	close(stage2.release)
	fmt.Printf("process 1: %v\n", errorsIsOwnershipLost(<-turn2Err))

	// The whole Session is one JSONL file: one event per line, digest-chained.
	page, err := store2.Read(ctx, session.ReadRequest{SessionID: sid})
	if err != nil {
		panic(err)
	}
	raw, err := os.ReadFile(store2.LogPath(sid))
	if err != nil {
		panic(err)
	}
	fmt.Printf("log.jsonl: %d lines, chain verified over %d rows\n", strings.Count(string(raw), "\n"), len(page.Events))
	fmt.Printf("first row: %s; last row: %s\n", page.Events[0].Type, page.Events[len(page.Events)-1].Type)

	// Output:
	// steer: in-2 delivered to turn-1 while its tool call executes
	// queue: 1 input pending while turn-1 runs
	// turn-1: completed
	// turn-2: started from the queued input; tool call is Executing; process 1 crashes
	// process 2: took over; 1 executing target disposed
	// turn-2: completed, disposition finished, attempt 1
	// process 1: ownership lost
	// log.jsonl: 41 lines, chain verified over 41 rows
	// first row: twilight/chatlog/input_submitted; last row: twilight/turn/completed
}

func waitUntil(cond func() bool) {
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			panic("condition not reached")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func protoToolCall(id string) sdk.ModelResult {
	return sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls, Usage: sdk.Usage{TotalTokens: 1},
		ToolCalls: []sdk.ToolCall{{ToolCallID: id, ToolName: "lookup", Input: `{"q":"weather"}`}}}
}

func protoText(text string) sdk.ModelResult {
	return sdk.ModelResult{Text: text, FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}
}

func protoAgent(model loop.ModelInvoker, tool *stagedTool) ref.Agent {
	agent, err := ref.NewAgent("m-1", model, ref.WithTool(tool))
	if err != nil {
		panic(err)
	}
	return agent
}

// stagedTool blocks each staged execution until its stage is released;
// executions beyond the staged ones run straight through.
type stagedTool struct {
	mu     sync.Mutex
	stages []*toolStage
}

type toolStage struct {
	started chan struct{}
	release chan struct{}
}

func (t *stagedTool) stage() *toolStage {
	st := &toolStage{started: make(chan struct{}), release: make(chan struct{})}
	t.mu.Lock()
	t.stages = append(t.stages, st)
	t.mu.Unlock()
	return st
}

func (t *stagedTool) Ref() run.ToolRef { return "lookup" }
func (t *stagedTool) Definition() sdk.ToolDefinition {
	return sdk.ToolDefinition{Name: "lookup", Parameters: []byte(`{"type":"object","properties":{"q":{"type":"string"}}}`)}
}
func (t *stagedTool) ResponsePolicy() run.ResponsePolicy        { return run.DirectExecution }
func (t *stagedTool) ValidateArguments(run.CanonicalJSON) error { return nil }
func (t *stagedTool) Execute(_ context.Context, req loop.ToolExecutionRequest) loop.ToolExecutionOutcome {
	t.mu.Lock()
	var st *toolStage
	if len(t.stages) > 0 {
		st = t.stages[0]
		t.stages = t.stages[1:]
	}
	t.mu.Unlock()
	if st != nil {
		close(st.started)
		<-st.release
	}
	return loop.ToolExecutionSucceeded{Result: run.ToolExecutionResult{Output: req.Arguments}}
}
