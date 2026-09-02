package run_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/run/loop"
	"github.com/memohai/twilight/agent/run/sqlitestore"
	"github.com/memohai/twilight/sdk"
)

// Example_recoverableRun drives one Run through a process crash.
//
// Process 1 creates the Run on a SQLite Store, accepts the user input and
// runs the Loop until the model asks for a tool. The tool never returns: the
// process dies while the call is Executing and its lease is live. Nothing is
// written on the way down.
//
// Process 2 reopens the same database. The lease has expired, so
// RecoverExpired settles the abandoned call as Unknown (the effect may or may
// not have happened) and the Run stays Active on the same RunID. The Loop
// then plans the next model request from the committed tool outcome and the
// Run completes. Record verifies the whole transition log against the stored
// snapshot.
func Example_recoverableRun() {
	dir, err := os.MkdirTemp("", "twilight-run-example")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	dbPath := filepath.Join(dir, "runs.db")
	ctx := context.Background()

	// A controllable clock stands in for wall time so lease expiry is
	// deterministic. Production hosts leave RuntimeOptions.Now nil.
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	const leaseTTL = 30 * time.Second

	// The application side: model, tool and planner survive the "crash" here
	// only because both processes live in one test binary.
	tool := &lookupTool{block: make(chan struct{})}
	app := newExampleApp(tool)

	// ---- process 1 ----------------------------------------------------------
	store1, err := sqlitestore.Open(dbPath)
	if err != nil {
		panic(err)
	}
	rt1 := run.NewRuntimeWithOptions(store1, run.RuntimeOptions{LeaseTTL: leaseTTL, Now: clock.Now})

	newRun, err := run.BuildNewRun("run-1", "example")
	if err != nil {
		panic(err)
	}
	if _, err := rt1.Create(ctx, newRun); err != nil {
		panic(err)
	}
	input := run.AgentInput{ID: "in-1", Payload: run.MustParseCanonicalJSON(`{"text":"what is the weather?"}`)}
	env, err := run.ProtocolV1().BuildEnvelope("run-1", run.DeriveInputCommandID("run-1", input.ID), run.AcceptInput{Input: input})
	if err != nil {
		panic(err)
	}
	if _, err := rt1.Commit(ctx, run.CommitRequest{Command: env}); err != nil {
		panic(err)
	}

	loop1, err := loop.New(app.models(), app.tools(), app, loop.ExecutionPolicy{LeaseRenewInterval: 5 * time.Second}, false)
	if err != nil {
		panic(err)
	}
	loop1Done := make(chan error, 1)
	go func() {
		_, err := loop1.Run(ctx, rt1, "run-1", nil)
		loop1Done <- err
	}()
	waitForExecutingCall(ctx, rt1, "run-1", "c1")
	fmt.Println("process 1: tool call c1 is Executing; process crashes")

	// The crash: the process disappears without settling. Closing the Store
	// is the closest single-binary equivalent; the abandoned worker's later
	// writes fail, exactly as a dead process writes nothing.
	if err := store1.Close(); err != nil {
		panic(err)
	}

	// ---- process 2 ----------------------------------------------------------
	clock.Advance(2 * leaseTTL)
	store2, err := sqlitestore.Open(dbPath)
	if err != nil {
		panic(err)
	}
	defer store2.Close()
	rt2 := run.NewRuntimeWithOptions(store2, run.RuntimeOptions{LeaseTTL: leaseTTL, Now: clock.Now})

	snap, err := rt2.Load(ctx, "run-1")
	if err != nil {
		panic(err)
	}
	fmt.Printf("process 2: reopened at revision %d, run %s, needs recovery = %v\n",
		snap.Revision, statusName(snap.State.Status), run.NeedsRecovery(snap.State))

	// Hosts run this on a timer (run.RunExpiredRecovery); one pass is enough here.
	recovered, err := rt2.RecoverExpired(ctx)
	if err != nil {
		panic(err)
	}
	snap, err = rt2.Load(ctx, "run-1")
	if err != nil {
		panic(err)
	}
	call := snap.State.LastToolStep.Calls[0]
	fmt.Printf("recovered %d lease: call %s is %s (%s), run %s\n",
		recovered, call.CallID, call.Status, call.Failure.Failure.Class, statusName(snap.State.Status))

	loop2, err := loop.New(app.models(), app.tools(), app, loop.ExecutionPolicy{LeaseRenewInterval: 5 * time.Second}, false)
	if err != nil {
		panic(err)
	}
	result, err := loop2.Run(ctx, rt2, "run-1", nil)
	if err != nil {
		panic(err)
	}
	fmt.Printf("process 2: run %s: %q\n", statusName(result.Result.Status), result.Result.Model.Text)

	record, err := rt2.Record(ctx, "run-1")
	if err != nil {
		panic(err)
	}
	fmt.Printf("record: %d transitions fold to the stored snapshot\n", len(record.Transitions))

	// Let the abandoned worker exit; its settlement fails against the closed
	// Store, which is the crash we simulated.
	close(tool.block)
	<-loop1Done

	// Output:
	// process 1: tool call c1 is Executing; process crashes
	// process 2: reopened at revision 5, run active, needs recovery = true
	// recovered 1 lease: call c1 is Failed (effect_unknown), run active
	// process 2: run completed: "done"
	// record: 9 transitions fold to the stored snapshot
}

// waitForExecutingCall polls Load until callID on the current ToolStep is
// Executing, which is the point at which a start has been accepted and its
// lease is live.
func waitForExecutingCall(ctx context.Context, rt run.Runtime, runID run.RunID, callID run.CallID) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		snap, err := rt.Load(ctx, runID)
		if err != nil {
			panic(err)
		}
		for _, id := range run.ExecutingCalls(snap.State) {
			if id == callID {
				return
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	panic("tool call never reached Executing")
}

func statusName(s run.RunStatus) string {
	switch s {
	case run.RunActive:
		return "active"
	case run.RunCompleted:
		return "completed"
	case run.RunStopped:
		return "stopped"
	default:
		return "failed"
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

// exampleApp is the application side of the Loop: the RequestPlanner, plus
// the model and tool catalogs exposed through models() and tools(). A real
// host injects its provider client, tool registry and context assembly here.
type exampleApp struct {
	tool *lookupTool
	spec run.ToolSpec
}

func newExampleApp(tool *lookupTool) *exampleApp {
	frozen, err := run.FreezeToolDefinition(tool.Definition())
	if err != nil {
		panic(err)
	}
	digest, err := run.ProtocolV1().DigestToolDefinition(frozen)
	if err != nil {
		panic(err)
	}
	return &exampleApp{tool: tool, spec: run.ToolSpec{
		Ref: tool.Ref(), Definition: frozen, DefinitionDigest: digest, Policy: run.DirectExecution,
	}}
}

// Plan projects the Run boundary facts into the next sdk.Request: the pending
// user inputs, plus the committed outcome of the previous tool step.
func (a *exampleApp) Plan(_ context.Context, hint run.PlanningHint) (loop.RequestPlan, error) {
	var messages []sdk.Message
	ids := make([]run.InputID, 0, len(hint.Inputs))
	for _, in := range hint.Inputs {
		ids = append(ids, in.ID)
		var body struct {
			Text string `json:"text"`
		}
		if err := in.Payload.Decode(&body); err != nil {
			return loop.RequestPlan{}, err
		}
		messages = append(messages, sdk.UserMessage(body.Text))
	}
	if hint.LastToolStep != nil {
		for _, call := range hint.LastToolStep.Calls {
			outcome := "ok"
			if call.Status == run.ToolFailed {
				outcome = call.Failure.Failure.Class
			}
			messages = append(messages, sdk.ToolMessage(sdk.ToolResultPart{
				ToolCallID: string(call.CallID), ToolName: string(call.ToolRef), Result: outcome,
			}))
		}
	}
	return loop.RequestPlan{
		Model:    "m-1",
		Request:  sdk.Request{Model: "m-1", Messages: messages, Tools: []sdk.ToolDefinition{a.tool.Definition()}},
		InputIDs: ids,
		Tools:    []run.ToolSpec{a.spec},
	}, nil
}

type modelCatalog struct{ app *exampleApp }
type toolCatalog struct{ app *exampleApp }

func (a *exampleApp) models() loop.ModelCatalog { return modelCatalog{a} }
func (a *exampleApp) tools() loop.ToolCatalog   { return toolCatalog{a} }

func (c modelCatalog) ResolveModel(run.ModelRef) (loop.ModelInvoker, error) { return c.app, nil }

func (c toolCatalog) ResolveTool(ref run.ToolRef) (loop.ExecutableTool, error) {
	if ref != c.app.tool.Ref() {
		return nil, fmt.Errorf("unknown tool %q", ref)
	}
	return c.app.tool, nil
}

// Generate is a scripted model: it asks for the tool until a tool result is
// present in the conversation, then answers.
func (a *exampleApp) Generate(_ context.Context, req sdk.Request) (sdk.ModelResult, error) {
	if n := len(req.Messages); n > 0 && req.Messages[n-1].Role == sdk.MessageRoleTool {
		return sdk.ModelResult{Text: "done", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}, nil
	}
	return sdk.ModelResult{
		FinishReason: sdk.FinishReasonToolCalls,
		Usage:        sdk.Usage{TotalTokens: 1},
		ToolCalls:    []sdk.ToolCall{{ToolCallID: "c1", ToolName: "lookup", Input: `{"q":"weather"}`}},
	}, nil
}

// lookupTool blocks on its first execution until block is closed, standing in
// for a tool call that is in flight when the process dies.
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
