package host_test

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/felinics/twilight/agent/artifact"
	"github.com/felinics/twilight/agent/host"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
	runmod "github.com/felinics/twilight/agent/session/run"
	"github.com/felinics/twilight/agent/turn"
	"github.com/felinics/twilight/sdk"
)

// newHost composes a colocated Host for tests: a LocalExecutor over the given
// models and tools; the Runtime still writes request bodies to ports.Content
// (RUN-WIR-4) and the executor never reads them back (RUN-EXE-7). ports.Content
// and ports.Executor are filled in; the other ports are taken as given.
func newHost(ports host.Ports, models map[run.ModelRef]loop.ModelInvoker, tools ...loop.ExecutableTool) *host.Host {
	if ports.Content == nil {
		ports.Content = memoryContent()
	}
	cat, err := host.NewCatalog(models, tools...)
	if err != nil {
		panic(err)
	}
	exec, err := host.NewLocalExecutor(cat, nil, false)
	if err != nil {
		panic(err)
	}
	ports.Executor = exec
	h, err := host.New(ports)
	if err != nil {
		panic(err)
	}
	return h
}

// memoryContent is an in-process cas store under the frozen authority.
func memoryContent() artifact.ContentStore {
	store, err := artifact.NewMemoryContentStore(runmod.FrozenAuthority, artifact.MemoryContentStoreOptions{})
	if err != nil {
		panic(err)
	}
	return store
}

// mustPreset builds the one-model AgentPreset the tests register.
func mustPreset(model run.ModelRef, tools []loop.ExecutableTool, opts ...host.PresetOption) turn.AgentPreset {
	p, err := host.NewPreset(model, tools, opts...)
	if err != nil {
		panic(err)
	}
	return p
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

// scriptedRequests records every request the model saw and answers from a
// script: tool call first, then text.
type scriptedRequests struct {
	mu      sync.Mutex
	seen    []sdk.Request
	answers []sdk.ModelResult
}

func (m *scriptedRequests) Generate(_ context.Context, req sdk.Request) (sdk.ModelResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seen = append(m.seen, req)
	if len(m.answers) == 0 {
		return sdk.ModelResult{Text: "done", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}, nil
	}
	next := m.answers[0]
	m.answers = m.answers[1:]
	return next, nil
}

func (m *scriptedRequests) requests() []sdk.Request {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]sdk.Request(nil), m.seen...)
}

func toolCallAnswer() sdk.ModelResult {
	return sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls, Usage: sdk.Usage{TotalTokens: 1},
		ToolCalls: []sdk.ToolCall{{ToolCallID: "c1", ToolName: "lookup", Input: `{"q":"weather"}`}}}
}

// gateTool blocks each execution until released, so tests can act mid-step.
type gateTool struct {
	started chan struct{}
	release chan struct{}
}

func (t *gateTool) Ref() run.ToolRef { return "lookup" }
func (t *gateTool) Definition() sdk.ToolDefinition {
	return sdk.ToolDefinition{Name: "lookup", Parameters: []byte(`{"type":"object"}`)}
}
func (t *gateTool) ResponsePolicy() run.ResponsePolicy        { return run.DirectExecution }
func (t *gateTool) ValidateArguments(run.CanonicalJSON) error { return nil }
func (t *gateTool) Execute(_ context.Context, req loop.ToolExecutionRequest) loop.ToolExecutionOutcome {
	t.started <- struct{}{}
	<-t.release
	return loop.ToolExecutionSucceeded{Result: run.ToolExecutionResult{Output: req.Arguments}}
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

func roles(msgs []sdk.Message) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = string(m.Role)
	}
	return out
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

func errorsIsOwnershipLost(err error) string {
	if err == nil {
		return "no error"
	}
	for e := err; e != nil; {
		if e == run.ErrOwnershipLost {
			return "ownership lost"
		}
		u, ok := e.(interface{ Unwrap() error })
		if !ok {
			break
		}
		e = u.Unwrap()
	}
	return err.Error()
}
