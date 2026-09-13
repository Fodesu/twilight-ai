// Command twilight-agent is a line-oriented CLI agent over the agent core: a
// colocated Host — the JSONL file store carries the Session, a LocalExecutor
// runs the model and the built-in tool in this process — and host.Session is
// the facade. Each stdin line goes through Session.Send — a line typed while
// a Turn runs steers it (already_driving), a line the running Turn cannot
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

	"github.com/felinics/twilight/agent/host"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/filestore"
	"github.com/felinics/twilight/agent/turn"
	"github.com/felinics/twilight/provider/openai/completions"
	"github.com/felinics/twilight/sdk"
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
		compactN = flag.Int("compact-after", 0, "auto-compact the context after this many entries (0 disables; /compact always works)")
	)
	flag.Parse()
	if err := run_(*root, session.SessionID(*sid), *provider, *baseURL, *apiKey, *modelID, *compat, *system, *mock, *compactN); err != nil {
		fmt.Fprintln(os.Stderr, "twilight-agent:", err)
		os.Exit(1)
	}
}

func run_(root string, sid session.SessionID, provider, baseURL, apiKey, modelID, compat, system string, mock bool, compactAfter int) error {
	ctx := context.Background()
	catalog, profile, err := buildAgent(mock, provider, baseURL, apiKey, modelID, compat, system)
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
	// The effect layer: the model and the tool run in this process behind the
	// Executor port; the Host itself holds neither.
	executor, err := host.NewLocalExecutor(catalog, frozen, nil, false)
	if err != nil {
		return err
	}
	h, err := host.New(host.Ports{Store: store, Frozen: frozen, Executor: executor,
		Observers: []loop.EventSink{printSink{}}, Ownership: session.OpenOptions{Takeover: true},
		Warn: func(err error) { fmt.Fprintln(os.Stderr, "host:", err) }})
	if err != nil {
		return err
	}
	profileRef, err := h.Profiles.Register("cli", profile)
	if err != nil {
		return err
	}
	s, err := h.OpenSession(ctx, sid, host.SessionOptions{Profile: profileRef, CompactAfterEntries: compactAfter,
		CompactWarn: func(err error) { fmt.Fprintln(os.Stderr, "compact:", err) }})
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
		case line == "/compact":
			wg.Add(1)
			go func() {
				defer wg.Done()
				id, ok, err := s.Compact(driveCtx)
				switch {
				case err != nil:
					fmt.Fprintln(os.Stderr, "compact:", err)
				case !ok:
					fmt.Println("nothing to compact")
				default:
					fmt.Printf("compacted: checkpoint %s\n", id)
				}
			}()
		case strings.HasPrefix(line, "/"):
			fmt.Println("commands: /quit /log /retry /compact")
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

func report(results []host.Result, err error) {
	for _, r := range results {
		if r.Disposition == host.ResumeAlreadyDriving {
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
func shutdown(ctx context.Context, s *host.Session, cancel func(), wg *sync.WaitGroup) error {
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

// buildAgent returns the two halves of the agent: the effect catalog the
// LocalExecutor serves (model client, tool implementation) and the Profile
// the authority records (model ref, frozen tool definition, system prompt).
func buildAgent(mock bool, provider, baseURL, apiKey, modelID, compat, system string) (*host.Catalog, turn.Profile, error) {
	if mock {
		tool := nowTool{}
		catalog, err := host.NewCatalog(map[run.ModelRef]loop.ModelInvoker{"mock": mockModel{}}, tool)
		if err != nil {
			return nil, turn.Profile{}, err
		}
		profile, err := host.NewProfile("mock", []loop.ExecutableTool{tool}, host.WithSystemPrompt(system))
		return catalog, profile, err
	}
	if provider != "openai-completions" {
		return nil, turn.Profile{}, fmt.Errorf("unsupported provider %q (only openai-completions)", provider)
	}
	if modelID == "" {
		return nil, turn.Profile{}, errors.New("-model is required (or use -mock)")
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
		return nil, turn.Profile{}, fmt.Errorf("unsupported compat %q (only deepseek)", compat)
	}
	// *sdk.Model is itself the ModelInvoker: it exposes
	// Generate(context.Context, sdk.Request) (sdk.ModelResult, error), so a
	// hand-written wrapper would only be ceremony between two identical shapes.
	invoker := &sdk.Model{ID: modelID, Provider: completions.New(opts...), Type: sdk.ModelTypeChat}
	catalog, err := host.NewCatalog(map[run.ModelRef]loop.ModelInvoker{run.ModelRef(modelID): invoker})
	if err != nil {
		return nil, turn.Profile{}, err
	}
	profile, err := host.NewProfile(run.ModelRef(modelID), nil, host.WithSystemPrompt(system))
	return catalog, profile, err
}

// mockModel answers once a tool result is in the conversation and reports how
// many messages it saw, so a restart over the same session shows the context
// growing; otherwise it asks for the built-in tool first. A compactor request
// (host.CompactorSystemPrompt) gets a fixed summary for deterministic smoke.
type mockModel struct{}

func (mockModel) Generate(_ context.Context, req sdk.Request) (sdk.ModelResult, error) {
	if len(req.Messages) > 0 && req.Messages[0].Role == sdk.MessageRoleSystem && messageText(req.Messages[0]) == host.CompactorSystemPrompt {
		return sdk.ModelResult{Text: "mock summary of the compacted conversation",
			FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}, nil
	}
	for _, msg := range req.Messages {
		if msg.Role == sdk.MessageRoleTool {
			return sdk.ModelResult{Text: fmt.Sprintf("mock: %d messages in context", len(req.Messages)),
				FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}, nil
		}
	}
	return sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls, Usage: sdk.Usage{TotalTokens: 1},
		ToolCalls: []sdk.ToolCall{{ToolCallID: fmt.Sprintf("call-%d", len(req.Messages)), ToolName: "now", Input: `{}`}}}, nil
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
