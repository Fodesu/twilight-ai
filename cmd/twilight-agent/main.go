// Command twilight-agent is a line-oriented CLI agent over the agent core:
// the JSONL file store carries the Session, the reference assembly wires the
// runtime, and the REPL routes each line through SessionDriver.Send — so a
// line typed while a Turn runs steers it (Deliver), a line that cannot be
// delivered stays queued and opens the next Turn after settlement, and a
// restart over the same root takes the Session over and resumes.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/memohai/twilight/agent/ref"
	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/run/loop"
	"github.com/memohai/twilight/agent/session"
	"github.com/memohai/twilight/agent/session/chatlog"
	"github.com/memohai/twilight/agent/session/filestore"
	"github.com/memohai/twilight/agent/turn"
	"github.com/memohai/twilight/provider/openai/completions"
	"github.com/memohai/twilight/sdk"
)

func main() {
	var (
		root     = flag.String("root", "./.twilight", "session store root directory (one subdirectory per session)")
		sid      = flag.String("session", "default", "session id; reopening the same id resumes its history")
		provider = flag.String("provider", "openai-completions", "model provider (only openai-completions)")
		baseURL  = flag.String("base-url", "", "provider base URL (default: the provider's public endpoint)")
		apiKey   = flag.String("api-key", "", "provider API key (default: $TWILIGHT_API_KEY)")
		modelID  = flag.String("model", "", "model id, e.g. gpt-4o or deepseek-chat (required unless -mock)")
		compat   = flag.String("compat", "", "provider compatibility profile: deepseek")
		system   = flag.String("system", "", "system prompt")
		mock     = flag.Bool("mock", false, "offline mode: scripted model plus a built-in `now` tool, no API key")
	)
	flag.Parse()
	if err := run_(*root, session.SessionID(*sid), *provider, *baseURL, *apiKey, *modelID, *compat, *system, *mock); err != nil {
		fmt.Fprintln(os.Stderr, "twilight-agent:", err)
		os.Exit(1)
	}
}

func run_(root string, sid session.SessionID, provider, baseURL, apiKey, modelID, compat, system string, mock bool) error {
	ctx := context.Background()
	binding, err := buildBinding(mock, provider, baseURL, apiKey, modelID, compat, system)
	if err != nil {
		return err
	}

	store, err := filestore.New(root)
	if err != nil {
		return err
	}
	m, err := ref.New(ref.Options{Store: store, Ownership: session.OpenOptions{Takeover: true}, Sink: printSink{}})
	if err != nil {
		return err
	}
	if _, err := store.Header(ctx, sid); err != nil {
		if !session.IsCode(err, session.ErrNotFound) {
			return err
		}
		if err := m.CreateSession(ctx, sid); err != nil {
			return err
		}
	}
	disposed, err := m.Open(ctx, sid)
	if err != nil {
		return err
	}
	bindingRef, err := m.Bindings.Register("cli", binding)
	if err != nil {
		return err
	}

	idBase := time.Now().UnixMilli()
	idSeq := 0
	driver := &ref.SessionDriver{Coordinator: m.Coordinator, Memory: m, Binding: bindingRef, Companion: turn.CompanionV1Version,
		NewTurnID: func() turn.TurnID { idSeq++; return turn.TurnID(fmt.Sprintf("turn-%d-%d", idBase, idSeq)) }}
	nextInputID := func() run.InputID { idSeq++; return run.InputID(fmt.Sprintf("in-%d-%d", idBase, idSeq)) }

	fmt.Printf("session %s — log at %s\n", sid, store.LogPath(sid))
	if disposed > 0 {
		fmt.Printf("takeover: %d executing target disposed\n", disposed)
	}

	settled := make(chan settleResult, 8)
	inflight := 0
	spawn := func(fn func() (turn.TurnResponse, error)) {
		inflight++
		go func() {
			resp, err := fn()
			settled <- settleResult{resp: resp, err: err}
		}()
	}

	// A restart lands here with the interrupted Turn still active: resume it.
	tsurf, err := m.TurnSurface(ctx, sid)
	if err != nil {
		return err
	}
	if v, ok := tsurf.Active(); ok {
		fmt.Printf("resuming turn %s\n", v.TurnID)
		req := turn.TurnRequest{Ref: turn.TurnRef{SessionID: sid, TurnID: v.TurnID}}
		spawn(func() (turn.TurnResponse, error) { return m.Coordinator.Resume(ctx, req) })
	}
	for _, v := range tsurf.Turns {
		if v.Status == turn.TurnAttemptFailed {
			fmt.Printf("turn %s failed; /retry to retry it\n", v.TurnID)
		}
	}

	handle := func(res settleResult, allowBacklog bool) {
		inflight--
		switch {
		case res.skip:
		case res.err == nil:
			fmt.Printf("turn %s: %s (%s)\n", res.resp.Ref.TurnID, res.resp.Status, res.resp.Disposition)
			if text := lastAssistantText(ctx, m, sid); text != "" {
				fmt.Println(text)
			}
		case errors.Is(res.err, loop.ErrRunAlreadyRunning):
			fmt.Println("steer: input delivered into the running turn")
		default:
			fmt.Fprintln(os.Stderr, "error:", res.err)
		}
		if !allowBacklog || inflight > 0 {
			return
		}
		// Drain the backlog: submitted, undelivered inputs open the next Turn.
		chat, err := m.ChatlogSurface(ctx, sid)
		if err != nil || len(chat.SubmittedInputs()) == 0 {
			return
		}
		inflight++
		go func() {
			resp, ok, err := driver.OnTurnSettled(ctx, sid)
			settled <- settleResult{resp: resp, err: err, skip: err == nil && !ok}
		}()
	}

	lines := make(chan string)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()

	for {
		select {
		case res := <-settled:
			handle(res, true)
		case line, ok := <-lines:
			if !ok {
				line = "/quit"
			}
			switch line = strings.TrimSpace(line); {
			case line == "":
			case line == "/quit":
				for inflight > 0 {
					select {
					case res := <-settled:
						handle(res, false)
					case <-time.After(60 * time.Second):
						fmt.Fprintln(os.Stderr, "timed out waiting for the running turn")
						inflight = 0
					}
				}
				return m.Close(ctx)
			case line == "/log":
				fmt.Println(store.LogPath(sid))
			case line == "/retry":
				tsurf, err := m.TurnSurface(ctx, sid)
				if err != nil {
					fmt.Fprintln(os.Stderr, "error:", err)
					continue
				}
				retried := false
				for id, v := range tsurf.Turns {
					if v.Status == turn.TurnAttemptFailed {
						req := turn.RetryRequest{Ref: turn.TurnRef{SessionID: sid, TurnID: id}, Reason: "cli retry"}
						spawn(func() (turn.TurnResponse, error) { return m.Coordinator.Retry(ctx, req) })
						retried = true
						break
					}
				}
				if !retried {
					fmt.Println("no failed turn to retry")
				}
			case strings.HasPrefix(line, "/"):
				fmt.Println("commands: /quit /log /retry")
			default:
				in, err := m.SubmitInput(ctx, sid, nextInputID(), line)
				if err != nil {
					fmt.Fprintln(os.Stderr, "error:", err)
					continue
				}
				// Send routes by itself: Deliver into the active Turn (steer),
				// or Start a new one. An input the running Turn cannot accept
				// stays submitted and the backlog drain picks it up.
				spawn(func() (turn.TurnResponse, error) { return driver.Send(ctx, sid, []run.AgentInput{in}) })
			}
		}
	}
}

type settleResult struct {
	resp turn.TurnResponse
	err  error
	skip bool
}

func lastAssistantText(ctx context.Context, m *ref.Memory, sid session.SessionID) string {
	chat, err := m.ChatlogSurface(ctx, sid)
	if err != nil {
		return ""
	}
	for i := len(chat.EntryOrder) - 1; i >= 0; i-- {
		e := chat.EntryOrder[i]
		if e.Kind != chatlog.EntryAssistant {
			continue
		}
		var b strings.Builder
		for _, part := range chat.Assistants[chatlog.AssistantID(e.ID)].Parts {
			if t, ok := part.(chatlog.TextPart); ok {
				b.WriteString(t.Text)
			}
		}
		return b.String()
	}
	return ""
}

// printSink surfaces tool activity while a Turn runs (observation only).
type printSink struct{}

func (printSink) Emit(_ context.Context, e loop.Event) error {
	switch e.Kind {
	case loop.EventToolStarted:
		fmt.Printf("tool call %s started\n", e.CallID)
	case loop.EventToolCompleted:
		fmt.Printf("tool call %s completed\n", e.CallID)
	}
	return nil
}

// --- binding -------------------------------------------------------------------

func buildBinding(mock bool, provider, baseURL, apiKey, modelID, compat, system string) (ref.Binding, error) {
	if mock {
		tool := nowTool{}
		def, err := run.FreezeToolDefinition(tool.Definition())
		if err != nil {
			return ref.Binding{}, err
		}
		return ref.Binding{
			Public: ref.BindingPublic{Model: "mock", SystemPrompt: system,
				Tools: []ref.PublicTool{{Ref: tool.Ref(), Definition: def, Policy: run.DirectExecution}}},
			Models: modelCatalog{mockModel{}},
			Tools:  singleTool{tool},
		}, nil
	}
	if provider != "openai-completions" {
		return ref.Binding{}, fmt.Errorf("unsupported provider %q (only openai-completions)", provider)
	}
	if modelID == "" {
		return ref.Binding{}, errors.New("-model is required (or use -mock)")
	}
	if apiKey == "" {
		apiKey = os.Getenv("TWILIGHT_API_KEY")
	}
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "warning: no API key (-api-key or $TWILIGHT_API_KEY)")
	}
	var opts []completions.Option
	if baseURL != "" {
		opts = append(opts, completions.WithBaseURL(baseURL))
	}
	if apiKey != "" {
		opts = append(opts, completions.WithAPIKey(apiKey))
	}
	switch compat {
	case "":
	case "deepseek":
		opts = append(opts, completions.WithDeepSeekChatCompletionsCompat())
	default:
		return ref.Binding{}, fmt.Errorf("unsupported compat %q (only deepseek)", compat)
	}
	invoker := providerModel{model: &sdk.Model{ID: modelID, Provider: completions.New(opts...), Type: sdk.ModelTypeChat}}
	return ref.Binding{
		Public: ref.BindingPublic{Model: run.ModelRef(modelID), SystemPrompt: system},
		Models: modelCatalog{invoker},
		Tools:  singleTool{},
	}, nil
}

type providerModel struct{ model *sdk.Model }

func (p providerModel) Generate(ctx context.Context, req sdk.Request) (sdk.ModelResult, error) {
	return sdk.Generate(ctx, p.model, req)
}

type modelCatalog struct{ m loop.ModelInvoker }

func (c modelCatalog) ResolveModel(run.ModelRef) (loop.ModelInvoker, error) { return c.m, nil }

// singleTool resolves the one built-in tool; the zero value resolves nothing.
type singleTool struct{ tool loop.ExecutableTool }

func (c singleTool) ResolveTool(r run.ToolRef) (loop.ExecutableTool, error) {
	if c.tool == nil || c.tool.Ref() != r {
		return nil, fmt.Errorf("unknown tool %q", r)
	}
	return c.tool, nil
}

// mockModel answers once a tool result is in the conversation and reports how
// many messages it saw, so a restart over the same session shows the context
// growing; otherwise it asks for the built-in tool first.
type mockModel struct{}

func (mockModel) Generate(_ context.Context, req sdk.Request) (sdk.ModelResult, error) {
	for _, msg := range req.Messages {
		if msg.Role == sdk.MessageRoleTool {
			return sdk.ModelResult{Text: fmt.Sprintf("mock: %d messages in context", len(req.Messages)),
				FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}, nil
		}
	}
	return sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls, Usage: sdk.Usage{TotalTokens: 1},
		ToolCalls: []sdk.ToolCall{{ToolCallID: fmt.Sprintf("call-%d", len(req.Messages)), ToolName: "now", Input: `{}`}}}, nil
}

type nowTool struct{}

func (nowTool) Ref() run.ToolRef { return "now" }
func (nowTool) Definition() sdk.ToolDefinition {
	return sdk.ToolDefinition{Name: "now", Description: "current UTC time", Parameters: []byte(`{"type":"object","properties":{}}`)}
}
func (nowTool) ResponsePolicy() run.ResponsePolicy        { return run.DirectExecution }
func (nowTool) ValidateArguments(run.CanonicalJSON) error { return nil }
func (nowTool) Execute(context.Context, loop.ToolExecutionRequest) loop.ToolExecutionOutcome {
	out := run.MustParseCanonicalJSON(fmt.Sprintf(`{"now":%q}`, time.Now().UTC().Format(time.RFC3339)))
	return loop.ToolExecutionSucceeded{Result: run.ToolExecutionResult{Output: out}}
}
