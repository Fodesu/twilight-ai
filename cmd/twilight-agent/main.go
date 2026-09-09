// Command twilight-agent is a line-oriented CLI agent over the agent core:
// the JSONL file store carries the Session and ref.Session is the host
// object. Each stdin line goes through Session.Send — a line typed while a
// Turn runs steers it (already_driving), a line the running Turn cannot
// accept queues and opens the next Turn after settlement, and a restart over
// the same root takes the Session over and resumes.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
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
	agent, err := buildAgent(mock, provider, baseURL, apiKey, modelID, compat, system)
	if err != nil {
		return err
	}

	store, err := filestore.New(root)
	if err != nil {
		return err
	}
	// Frozen request bodies persist next to the session log, so a restart can
	// replay the request of a ModelStep that was executing at the crash.
	frozen, err := filestore.NewFrozenValues(root)
	if err != nil {
		return err
	}
	m, err := ref.New(ref.Options{Store: store, Frozen: frozen, Ownership: session.OpenOptions{Takeover: true}, Sink: printSink{}})
	if err != nil {
		return err
	}
	profile, err := m.Agents.Register("cli", agent)
	if err != nil {
		return err
	}
	s, err := m.OpenSession(ctx, sid, ref.SessionOptions{Profile: profile})
	if err != nil {
		return err
	}
	fmt.Printf("session %s — log at %s\n", sid, store.LogPath(sid))
	if s.Recovered > 0 {
		fmt.Printf("takeover: %d executing target disposed\n", s.Recovered)
	}

	// Turns run in per-call goroutines; /quit cancels them. A cancelled Turn
	// stays Active in the log and the next start resumes it.
	driveCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup

	status, err := s.Status(ctx)
	if err != nil {
		return err
	}
	if status.Active != "" {
		fmt.Printf("resuming turn %s\n", status.Active)
		wg.Add(1)
		go func() {
			defer wg.Done()
			results, _, err := s.Resume(driveCtx)
			report(results, err)
		}()
	}
	for _, id := range status.Failed {
		fmt.Printf("turn %s failed; /retry to retry it\n", id)
	}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		switch line := strings.TrimSpace(scanner.Text()); {
		case line == "":
		case line == "/quit":
			return shutdown(ctx, s, cancel, &wg)
		case line == "/log":
			fmt.Println(store.LogPath(sid))
		case line == "/retry":
			wg.Add(1)
			go func() {
				defer wg.Done()
				results, ok, err := s.Retry(driveCtx)
				if err == nil && !ok {
					fmt.Println("no failed turn to retry")
					return
				}
				report(results, err)
			}()
		case strings.HasPrefix(line, "/"):
			fmt.Println("commands: /quit /log /retry")
		default:
			wg.Add(1)
			go func(text string) {
				defer wg.Done()
				results, err := s.Send(driveCtx, text)
				report(results, err)
			}(line)
		}
	}
	return shutdown(ctx, s, cancel, &wg)
}

func report(results []ref.Result, err error) {
	for _, r := range results {
		if r.Disposition == turn.ResumeAlreadyDriving {
			fmt.Println("steer: input delivered into the running turn")
			continue
		}
		fmt.Printf("turn %s: %s (%s)\n", r.TurnID, r.Status, r.Disposition)
		if r.Reply != "" {
			fmt.Println(r.Reply)
		}
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "error:", err)
	}
}

// shutdown waits for running turns, then cancels the stragglers: a cancelled
// Turn stays Active in the log and the next start resumes it.
func shutdown(ctx context.Context, s *ref.Session, cancel func(), wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		fmt.Fprintln(os.Stderr, "cancelling the running turn; it resumes on the next start")
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			fmt.Fprintln(os.Stderr, "timed out waiting for the running turn")
		}
	}
	return s.Close(ctx)
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

// --- agent -------------------------------------------------------------------

func buildAgent(mock bool, provider, baseURL, apiKey, modelID, compat, system string) (ref.Agent, error) {
	if mock {
		return ref.NewAgent("mock", mockModel{}, ref.WithTool(nowTool{}), ref.WithSystemPrompt(system))
	}
	if provider != "openai-completions" {
		return nil, fmt.Errorf("unsupported provider %q (only openai-completions)", provider)
	}
	if modelID == "" {
		return nil, errors.New("-model is required (or use -mock)")
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
		return nil, fmt.Errorf("unsupported compat %q (only deepseek)", compat)
	}
	invoker := providerModel{model: &sdk.Model{ID: modelID, Provider: completions.New(opts...), Type: sdk.ModelTypeChat}}
	return ref.NewAgent(run.ModelRef(modelID), invoker, ref.WithSystemPrompt(system))
}

type providerModel struct{ model *sdk.Model }

func (p providerModel) Generate(ctx context.Context, req sdk.Request) (sdk.ModelResult, error) {
	return sdk.Generate(ctx, p.model, req)
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
