package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/app"
	executorhttp "github.com/felinics/twilight/agent/executor/http"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/effect"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/extension"
	"github.com/felinics/twilight/agent/session/filestore"
	runmod "github.com/felinics/twilight/agent/session/run"
	"github.com/felinics/twilight/agent/turn"
	"github.com/felinics/twilight/sdk"
)

// This transport exercises the production wire codec and handlers without a
// listener. Disconnect severs this authority's connection while the Worker
// retains the execution, as it does when the authority process disappears.
type handlerTransport struct {
	handler http.Handler
	stopped chan struct{}
	once    sync.Once
}

func newHandlerTransport(handler http.Handler) *handlerTransport {
	return &handlerTransport{handler: handler, stopped: make(chan struct{})}
}

func (tr *handlerTransport) disconnect() { tr.once.Do(func() { close(tr.stopped) }) }

func (tr *handlerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	select {
	case <-tr.stopped:
		return nil, errors.New("test: authority connection closed")
	default:
	}
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()
	finished := make(chan *http.Response, 1)
	go func() {
		recorder := httptest.NewRecorder()
		tr.handler.ServeHTTP(recorder, req.Clone(ctx))
		finished <- recorder.Result()
	}()
	select {
	case response := <-finished:
		return response, nil
	case <-tr.stopped:
		cancel()
		response := <-finished
		_ = response.Body.Close()
		return nil, errors.New("test: authority connection closed")
	case <-req.Context().Done():
		cancel()
		response := <-finished
		_ = response.Body.Close()
		return nil, req.Context().Err()
	}
}

type remoteGateTool struct {
	started chan loop.ToolExecutionRequest
	release chan struct{}
	once    sync.Once
	calls   atomic.Int32
}

func (*remoteGateTool) Ref() run.ToolRef { return "gate" }
func (*remoteGateTool) Definition() sdk.ToolDefinition {
	return sdk.ToolDefinition{Name: "gate", Description: "wait for the test release",
		Parameters: []byte(`{"type":"object","properties":{},"additionalProperties":false}`)}
}
func (*remoteGateTool) ResponsePolicy() run.ResponsePolicy        { return run.DirectExecution }
func (*remoteGateTool) ValidateArguments(run.CanonicalJSON) error { return nil }
func (tool *remoteGateTool) unblock()                             { tool.once.Do(func() { close(tool.release) }) }
func (tool *remoteGateTool) Execute(ctx context.Context, req loop.ToolExecutionRequest) loop.ToolExecutionOutcome {
	tool.calls.Add(1)
	select {
	case tool.started <- req:
	case <-ctx.Done():
		return loop.ToolExecutionUnknown{Failure: run.ToolFailure{Class: run.FailureEffectUnknown, Message: ctx.Err().Error()}}
	}
	select {
	case <-tool.release:
		return loop.ToolExecutionSucceeded{Result: run.ToolExecutionResult{Output: run.MustParseCanonicalJSON(`{"released":true}`)}}
	case <-ctx.Done():
		return loop.ToolExecutionUnknown{Failure: run.ToolFailure{Class: run.FailureEffectUnknown, Message: ctx.Err().Error()}}
	}
}

type remoteGateModel struct {
	calls atomic.Int32
	final chan sdk.Request
}

func (model *remoteGateModel) Generate(ctx context.Context, req sdk.Request) (sdk.ModelResult, error) {
	model.calls.Add(1)
	for _, message := range req.Messages {
		if message.Role == sdk.MessageRoleTool {
			select {
			case model.final <- req:
			case <-ctx.Done():
				return sdk.ModelResult{}, ctx.Err()
			}
			return sdk.ModelResult{Text: "the original gate completed", FinishReason: sdk.FinishReasonStop}, nil
		}
	}
	return sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls,
		ToolCalls: []sdk.ToolCall{{ToolCallID: "gate-call", ToolName: "gate", Input: `{}`}}}, nil
}

func TestRemoteAuthorityReopensRunningTool(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	workerRoot, authorityRoot := t.TempDir(), t.TempDir()
	const sid session.SessionID = "remote-reopen"
	gate := &remoteGateTool{started: make(chan loop.ToolExecutionRequest, 2), release: make(chan struct{})}
	defer gate.unblock()
	model := &remoteGateModel{final: make(chan sdk.Request, 2)}
	models := map[run.ModelRef]loop.ModelInvoker{"test-model": model}
	tools := []loop.ExecutableTool{gate}
	preset, err := app.NewPreset("test-model", tools)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := newExecutor(ctx, workerRoot, models, tools)
	if err != nil {
		t.Fatal(err)
	}
	handler := (&executorhttp.Server{Worker: worker}).Handler()
	firstTransport := newHandlerTransport(handler)
	defer firstTransport.disconnect()
	first, _, err := remoteTestApplication(authorityRoot, preset, app.ExecutorConfig{Mode: app.ExecutorRemote,
		Endpoint: "http://worker", HTTP: &http.Client{Transport: firstTransport}})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close(context.Background())
	firstRef, err := first.PresetRef("cli")
	if err != nil {
		t.Fatal(err)
	}
	firstSession, err := first.OpenSession(ctx, sid, app.SessionOptions{Preset: firstRef})
	if err != nil {
		t.Fatal(err)
	}
	sent := make(chan error, 1)
	go func() {
		_, err := firstSession.Send(ctx, "use the gate, then report its result")
		sent <- err
	}()
	var started loop.ToolExecutionRequest
	select {
	case started = <-gate.started:
	case err := <-sent:
		t.Fatalf("send finished before the tool started: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Graceful drive cancellation intentionally cancels tools. A connection
	// loss instead leaves the remote execution available for the next owner.
	firstTransport.disconnect()
	select {
	case err := <-sent:
		if err == nil {
			t.Fatal("disconnected authority unexpectedly completed its Send")
		}
	case <-ctx.Done():
		t.Fatal("the old authority did not stop reading: ", ctx.Err())
	}
	if err := firstSession.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(ctx); err != nil {
		t.Fatal(err)
	}

	secondTransport := newHandlerTransport(handler)
	defer secondTransport.disconnect()
	second, store, err := remoteTestApplication(authorityRoot, preset, app.ExecutorConfig{Mode: app.ExecutorRemote,
		Endpoint: "http://worker", HTTP: &http.Client{Transport: secondTransport}})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close(context.Background())
	secondRef, err := second.PresetRef("cli")
	if err != nil {
		t.Fatal(err)
	}
	events := second.Events(ctx, sid)
	openCtx, stopOpen := context.WithCancel(ctx)
	secondSession, err := second.OpenSession(openCtx, sid, app.SessionOptions{Preset: secondRef})
	stopOpen()
	if err != nil {
		t.Fatal(err)
	}
	if secondSession.Recovered != 0 {
		t.Fatalf("disposed %d executions; the live tool should be reattached", secondSession.Recovered)
	}
	gate.unblock()
	for completed := false; !completed; {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatal("events ended before recovery completed")
			}
			if event.Err != nil {
				t.Fatal(event.Err)
			}
			completed = event.Row.Type == turn.TypeCompleted
		case <-ctx.Done():
			t.Fatal("reopened authority did not complete: ", ctx.Err())
		}
	}
	if got := gate.calls.Load(); got != 1 {
		t.Fatalf("gate executed %d times, want 1", got)
	}
	if got := model.calls.Load(); got != 2 {
		t.Fatalf("model executed %d times, want the original call and one follow-up", got)
	}
	select {
	case request := <-model.final:
		assertRemoteToolPair(t, request)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	status, err := secondSession.Status(ctx)
	if err != nil || status.Active != "" || len(status.Failed) != 0 {
		t.Fatalf("recovered session status = %+v, %v", status, err)
	}
	registry, err := extension.BuildRegistry(session.ProtocolVersion1, chatlog.Module, runmod.Module, turn.Module)
	if err != nil {
		t.Fatal(err)
	}
	page, err := store.Read(ctx, session.ReadRequest{SessionID: sid})
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, row := range page.Events {
		decoded, err := registry.Decode(row)
		if err != nil {
			t.Fatal(err)
		}
		event, ok := decoded.Value.(runmod.Event)
		if !ok {
			continue
		}
		counts[run.FactType(event.Fact)]++
		if event.RunID != started.RunID {
			t.Fatalf("recovery changed RunID: %s != %s", event.RunID, started.RunID)
		}
		switch fact := event.Fact.(type) {
		case run.ToolCallStarted:
			if fact.StepID != started.StepID || fact.CallID != started.CallID || fact.Claim != started.Claim {
				t.Fatalf("tool start changed identity: %+v", fact)
			}
		case run.ToolCallCompleted:
			if fact.StepID != started.StepID || fact.CallID != started.CallID {
				t.Fatalf("tool completion changed identity: %+v", fact)
			}
		case run.RunEnded:
			if _, ok := fact.End.(run.RunCompletedEnd); !ok {
				t.Fatalf("run ended with %T", fact.End)
			}
		}
	}
	for kind, want := range map[string]int{"run_created": 1, "tool_step_opened": 1, "tool_call_started": 1,
		"tool_call_completed": 1, "tool_call_failed": 0, "model_step_completed": 2, "run_ended": 1} {
		if got := counts[kind]; got != want {
			t.Errorf("%s facts = %d, want %d", kind, got, want)
		}
	}

	// All outcomes are now durable. A new Worker with an empty local backend
	// reads the same result without executing the tool or model again.
	recreated, err := newExecutor(ctx, workerRoot, models, tools)
	if err != nil {
		t.Fatal(err)
	}
	recreatedTransport := newHandlerTransport((&executorhttp.Server{Worker: recreated}).Handler())
	defer recreatedTransport.disconnect()
	client := &executorhttp.Client{BaseURL: "http://worker", HTTP: &http.Client{Transport: recreatedTransport}}
	key := effect.AssignmentKey{Session: sid, RunID: started.RunID, StepID: started.StepID, CallID: started.CallID, Claim: started.Claim}
	attachment, err := client.Attach(ctx, key)
	if err != nil || attachment.State != effect.AttachmentTerminal {
		t.Fatalf("recreated worker attachment = %+v, %v", attachment, err)
	}
	outcome, err := client.GetOutcome(ctx, key)
	if err != nil || outcome.Key != key || outcome.Err != nil {
		t.Fatalf("recreated worker outcome = %+v, %v", outcome, err)
	}
	result, ok := outcome.Tool.(loop.ToolExecutionSucceeded)
	if !ok || result.Result.Output.String() != `{"released":true}` {
		t.Fatalf("recreated worker lost the tool result: %+v", outcome.Tool)
	}
	if gate.calls.Load() != 1 || model.calls.Load() != 2 {
		t.Fatal("reading persisted outcomes re-executed an effect")
	}
}

func remoteTestApplication(root string, preset turn.AgentPreset, exec app.ExecutorConfig) (*app.Application, *filestore.Store, error) {
	store, err := filestore.New(root)
	if err != nil {
		return nil, nil, err
	}
	content, err := filestore.NewContentStore(root, runmod.FrozenAuthority, filestore.ContentStoreOptions{})
	if err != nil {
		return nil, nil, err
	}
	application, err := app.Build(app.Config{
		Store: store, Content: content, Executor: exec,
		Presets:   []app.Preset{{ID: "cli", Value: preset}},
		Ownership: session.OpenOptions{Takeover: true},
	})
	return application, store, err
}

func assertRemoteToolPair(t *testing.T, request sdk.Request) {
	t.Helper()
	var callCount, resultCount int
	for _, message := range request.Messages {
		for _, part := range message.Content {
			switch part := part.(type) {
			case sdk.ToolCallPart:
				callCount++
				if part.ToolCallID != "gate-call" || part.ToolName != "gate" || message.Role != sdk.MessageRoleAssistant {
					t.Fatalf("unexpected assistant tool call: %+v", part)
				}
			case sdk.ToolResultPart:
				resultCount++
				if callCount != 1 || part.ToolCallID != "gate-call" || part.ToolName != "gate" || part.IsError || message.Role != sdk.MessageRoleTool {
					t.Fatalf("tool result does not match its preceding call: %+v", part)
				}
			}
		}
	}
	if callCount != 1 || resultCount != 1 {
		t.Fatalf("tool pairing: %d calls, %d results", callCount, resultCount)
	}
}
