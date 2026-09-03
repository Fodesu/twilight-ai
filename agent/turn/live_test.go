//go:build live

// Live end-to-end test: agent/turn -> agent/run/loop -> a real model through
// the sdk. Runs only with `go test -tags live` and the environment below.
//
//	TWILIGHT_LIVE_BASE_URL   OpenAI-compatible chat completions base URL
//	TWILIGHT_LIVE_API_KEY    bearer key
//	TWILIGHT_LIVE_MODEL      model id (default gpt-5.4)
package turn_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/run/loop"
	"github.com/memohai/twilight/agent/turn"
	"github.com/memohai/twilight/provider/openai/completions"
	"github.com/memohai/twilight/sdk"
)

// calculator is a real tool: it evaluates "a op b" and returns the number.
type calculator struct{ spec run.ToolSpec }

func newCalculator(t *testing.T) *calculator {
	t.Helper()
	def := sdk.ToolDefinition{
		Name:        "calculator",
		Description: "Evaluate an arithmetic expression of the form '<int> <op> <int>' where op is + - * /.",
		Parameters:  []byte(`{"type":"object","properties":{"expression":{"type":"string"}},"required":["expression"]}`),
	}
	frozen, err := run.FreezeToolDefinition(def)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := run.ProtocolV1().DigestToolDefinition(frozen)
	if err != nil {
		t.Fatal(err)
	}
	return &calculator{spec: run.ToolSpec{Ref: "calculator", Definition: frozen, DefinitionDigest: digest, Policy: run.DirectExecution}}
}

func (c *calculator) ResolveTool(ref run.ToolRef) (loop.ExecutableTool, error) {
	if ref != "calculator" {
		return nil, fmt.Errorf("unknown tool %q", ref)
	}
	return c, nil
}
func (c *calculator) Ref() run.ToolRef                          { return "calculator" }
func (c *calculator) Definition() sdk.ToolDefinition            { return c.spec.Definition.SDK() }
func (c *calculator) ResponsePolicy() run.ResponsePolicy        { return run.DirectExecution }
func (c *calculator) ValidateArguments(run.CanonicalJSON) error { return nil }
func (c *calculator) Execute(_ context.Context, req loop.ToolExecutionRequest) loop.ToolExecutionOutcome {
	var args struct {
		Expression string `json:"expression"`
	}
	if err := req.Arguments.Decode(&args); err != nil {
		return loop.ToolExecutionFailed{Failure: run.ToolFailure{Class: run.FailureInvalidArguments, Message: err.Error()}}
	}
	fields := strings.Fields(strings.NewReplacer("*", " * ", "+", " + ", "-", " - ", "/", " / ").Replace(args.Expression))
	if len(fields) != 3 {
		return loop.ToolExecutionFailed{Failure: run.ToolFailure{Class: run.FailureInvalidArguments, Message: "expected '<int> <op> <int>'"}}
	}
	a, errA := strconv.Atoi(fields[0])
	b, errB := strconv.Atoi(fields[2])
	if errA != nil || errB != nil {
		return loop.ToolExecutionFailed{Failure: run.ToolFailure{Class: run.FailureInvalidArguments, Message: "operands must be integers"}}
	}
	var v int
	switch fields[1] {
	case "+":
		v = a + b
	case "-":
		v = a - b
	case "*":
		v = a * b
	case "/":
		if b == 0 {
			return loop.ToolExecutionFailed{Failure: run.ToolFailure{Class: run.FailureExecution, Message: "division by zero"}}
		}
		v = a / b
	default:
		return loop.ToolExecutionFailed{Failure: run.ToolFailure{Class: run.FailureInvalidArguments, Message: "unknown operator"}}
	}
	out, _ := run.CanonicalJSONFromValue(map[string]int{"result": v})
	return loop.ToolExecutionSucceeded{Result: run.ToolExecutionResult{Output: out}}
}

type liveCatalog struct{ model *sdk.Model }

func (c liveCatalog) ResolveModel(run.ModelRef) (loop.ModelInvoker, error) { return c.model, nil }

func TestLiveTurnWithRealModelAndTool(t *testing.T) {
	baseURL, apiKey := os.Getenv("TWILIGHT_LIVE_BASE_URL"), os.Getenv("TWILIGHT_LIVE_API_KEY")
	if baseURL == "" || apiKey == "" {
		t.Skip("TWILIGHT_LIVE_BASE_URL / TWILIGHT_LIVE_API_KEY not set")
	}
	modelID := os.Getenv("TWILIGHT_LIVE_MODEL")
	if modelID == "" {
		modelID = "gpt-5.4"
	}
	provider := completions.New(completions.WithAPIKey(apiKey), completions.WithBaseURL(baseURL))
	model := provider.ChatModel(modelID)

	calc := newCalculator(t)
	rt := run.NewRuntime(run.NewMemoryStore())
	log := turn.NewMemoryLog()
	planner := &turn.ContextPlanner{
		Log: log, Session: "live-s1", Model: run.ModelRef(modelID), Tools: []run.ToolSpec{calc.spec},
		System: "You are a precise assistant. Use the calculator tool for any arithmetic. Reply with just the final number once you have it.",
	}
	l, err := loop.New(liveCatalog{model}, calc, planner, loop.ExecutionPolicy{}, false)
	if err != nil {
		t.Fatal(err)
	}
	coord := &turn.Coordinator{Log: log, Runtime: rt, Driver: turn.LoopDriver{Loop: l, Runtime: rt}}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	ref := turn.Ref{Session: "live-s1", Turn: "live-t1"}
	res, err := coord.Start(ctx, turn.StartRequest{Ref: ref, RunID: "live-run-1",
		Inputs: []run.AgentInput{{ID: "in-1", Payload: run.MustParseCanonicalJSON(`{"text":"What is 17 * 23? Use the calculator."}`)}}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Disposition != turn.DispositionFinished || res.Settlement != turn.SettlementCompleted {
		if res.Result != nil && res.Result.Failure != nil {
			t.Fatalf("turn %s: reason=%s class=%s message=%s", res.Settlement, res.Result.Reason, res.Result.Failure.Class, res.Result.Failure.Message)
		}
		t.Fatalf("turn did not complete: disposition=%s settlement=%s result=%+v", res.Disposition, res.Settlement, res.Result)
	}
	if !strings.Contains(res.Result.Model.Text, "391") {
		t.Fatalf("final answer %q does not contain 391", res.Result.Model.Text)
	}

	events, err := log.Replay(ctx, ref.Session)
	if err != nil {
		t.Fatal(err)
	}
	var sawToolResult bool
	t.Logf("=== session log: %d events", len(events))
	for _, e := range events {
		t.Logf("seq=%d %-32s rev=%d idx=%d\n%s", e.Seq, e.Type, e.Revision, e.Index, e.Payload.String())
		if e.Type == turn.EventToolResult {
			var p turn.ToolResultPayload
			if err := e.Payload.Decode(&p); err != nil {
				t.Fatal(err)
			}
			if p.Status != turn.ToolSuccess || !strings.Contains(p.Output.String(), "391") {
				t.Fatalf("tool result = %+v", p)
			}
			sawToolResult = true
		}
	}
	if !sawToolResult {
		t.Fatal("model did not call the tool")
	}
	record, err := rt.Record(ctx, "live-run-1")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("=== run record: %d transitions, usage %+v", len(record.Transitions), record.Snapshot.State.Usage)
	for _, tr := range record.Transitions {
		for _, ev := range tr.Events {
			t.Logf("rev=%d idx=%d %-22s command=%s", ev.Revision, ev.Index, ev.Type, ev.CommandID)
		}
	}
	if out := os.Getenv("TWILIGHT_LIVE_RECORD_OUT"); out != "" {
		raw, err := json.MarshalIndent(record, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(out, raw, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("=== full RunRecord written to %s (%d bytes)", out, len(raw))
	}
}
