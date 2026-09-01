package loop

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	. "github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/sdk"
)

func TestRegressionToolPanicBecomesUnknown(t *testing.T) {
	spec := toolSpec(t, "echo", DirectExecution)
	echo := &fakeTool{ref: "echo", def: spec.Definition.SDK(), policy: DirectExecution,
		execute: func(context.Context, ToolExecutionRequest) ToolExecutionOutcome {
			panic("nil map write")
		}}
	invoker := &fakeInvoker{results: []sdk.ModelResult{toolCallResult("c1")}}
	rt := loopRuntime(t)
	interpreter, _ := New(fakeCatalog{invoker}, fakeToolCatalog{map[ToolRef]ExecutableTool{"echo": echo}},
		staticPlanner{specs: []ToolSpec{spec}}, ExecutionPolicy{}, false)

	res, err := interpreter.Run(context.Background(), rt, "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Result.Status != RunFailed || res.Result.Reason != ReasonEffectUnknown {
		t.Fatalf("res = %+v", res.Result)
	}
	if !strings.Contains(res.Result.Failure.Message, "panic") {
		t.Fatalf("failure = %+v", res.Result.Failure)
	}
}

func TestRegressionRunFinishedEmitted(t *testing.T) {
	rt := loopRuntime(t)
	var kinds []EventKind
	sink := sinkFunc(func(_ context.Context, e Event) error {
		kinds = append(kinds, e.Kind)
		return nil
	})
	interpreter, _ := New(fakeCatalog{&fakeInvoker{results: []sdk.ModelResult{textResult("done")}}},
		fakeToolCatalog{}, staticPlanner{}, ExecutionPolicy{}, false)
	if _, err := interpreter.Run(context.Background(), rt, "run-1", sink); err != nil {
		t.Fatal(err)
	}
	for _, k := range kinds {
		if k == EventRunFinished {
			return
		}
	}
	t.Fatalf("EventRunFinished never emitted; kinds = %v", kinds)
}

func TestRegressionAliasedToolRefExecutes(t *testing.T) {
	def := sdk.ToolDefinition{Name: "read", Parameters: json.RawMessage(`{"type":"object"}`)}
	frozenDef, err := FreezeToolDefinition(def)
	if err != nil {
		t.Fatal(err)
	}
	d, err := DigestToolDefinition(SchemaVersion1, frozenDef)
	if err != nil {
		t.Fatal(err)
	}
	spec := ToolSpec{Ref: "fs.read", Definition: frozenDef, DefinitionDigest: d, Policy: DirectExecution}
	executed := atomic.Bool{}
	tool := &fakeTool{ref: "fs.read", def: def, policy: DirectExecution,
		execute: func(context.Context, ToolExecutionRequest) ToolExecutionOutcome {
			executed.Store(true)
			return ToolExecutionSucceeded{Result: ToolExecutionResult{Output: cj(`"ok"`)}}
		}}
	invoker := &fakeInvoker{results: []sdk.ModelResult{
		func() sdk.ModelResult {
			r := sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls}
			r.ToolCalls = []sdk.ToolCall{{ToolCallID: "c1", ToolName: "read", Input: `{}`}}
			return r
		}(),
		textResult("done"),
	}}
	rt := loopRuntime(t)
	interpreter, _ := New(fakeCatalog{invoker}, fakeToolCatalog{map[ToolRef]ExecutableTool{"fs.read": tool}},
		staticPlanner{specs: []ToolSpec{spec}}, ExecutionPolicy{}, false)

	res, err := interpreter.Run(context.Background(), rt, "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !executed.Load() {
		t.Fatal("aliased tool never executed")
	}
	if res.Result.Status != RunCompleted {
		t.Fatalf("res = %+v", res.Result)
	}
}

func TestRegressionStreamNilResult(t *testing.T) {
	rt := loopRuntime(t)
	interpreter, _ := New(fakeCatalog{nilResultStreamer{}}, fakeToolCatalog{}, staticPlanner{}, ExecutionPolicy{}, true)
	res, err := interpreter.Run(context.Background(), rt, "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Result.Status != RunFailed {
		t.Fatalf("res = %+v", res.Result)
	}
}

type sinkFunc func(context.Context, Event) error

func (f sinkFunc) Emit(ctx context.Context, e Event) error { return f(ctx, e) }

type nilResultStreamer struct{}

func (nilResultStreamer) Generate(context.Context, sdk.Request) (sdk.ModelResult, error) {
	return sdk.ModelResult{}, errors.New("generate should not be called when streaming")
}

func (nilResultStreamer) Stream(context.Context, sdk.Request) (sdk.ModelStream, error) {
	parts := make(chan sdk.StreamPart)
	close(parts)
	return sdk.ModelStream{Parts: parts, Result: func() (*sdk.ModelResult, error) { return nil, nil }}, nil
}
