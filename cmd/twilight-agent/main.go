// Command twilight-agent runs an interactive agent, an HTTP authority, or a
// standalone execution worker. Each process owns its file store. The authority
// resumes its Session on startup and can reattach work on a surviving worker.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/filestore"
	runmod "github.com/felinics/twilight/agent/session/run"
	"github.com/felinics/twilight/agent/turn"
	"github.com/felinics/twilight/provider/openai/completions"
	"github.com/felinics/twilight/sdk"
)

type agentOptions struct {
	root, provider, baseURL, apiKey, modelID, compat, system string
	sid                                                      session.SessionID
	mock                                                     bool
	compactAfter                                             int
	endpoint, mode, address                                  string
	toolDelay                                                time.Duration
}

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
		endpoint = flag.String("executor-url", "", "execute remotely through this executor HTTP endpoint")
		mode     = flag.String("mode", "cli", "cli (interactive), agent (HTTP authority), or worker (HTTP executor)")
		listen   = flag.String("listen", "", "loopback listen address (agent: 127.0.0.1:8088, worker: 127.0.0.1:8089)")
		delay    = flag.Duration("tool-delay", 0, "delay the now tool to exercise recovery, e.g. 30s")
	)
	flag.Parse()
	var err error
	switch {
	case *delay < 0:
		err = errors.New("-tool-delay must be non-negative")
	case *mode != "cli" && *mode != "agent" && *mode != "worker":
		err = errors.New("-mode must be cli, agent, or worker")
	case *mode == "worker" && *endpoint != "":
		err = errors.New("worker mode uses local model and tool implementations; omit -executor-url")
	case *mode == "worker":
		if *listen == "" {
			*listen = "127.0.0.1:8089"
		}
		models, tools, _, buildErr := buildAgent(*mock, *provider, *baseURL, *apiKey, *modelID, *compat, *system)
		if buildErr != nil {
			err = buildErr
			break
		}
		tools[0] = nowTool{delay: *delay}
		err = serveExecutor(*root, *listen, models, tools)
	default:
		if *mode == "agent" && *listen == "" {
			*listen = "127.0.0.1:8088"
		}
		err = runAgent(agentOptions{root: *root, sid: session.SessionID(*sid), provider: *provider,
			baseURL: *baseURL, apiKey: *apiKey, modelID: *modelID, compat: *compat, system: *system,
			mock: *mock, compactAfter: *compactN, endpoint: *endpoint, toolDelay: *delay, mode: *mode, address: *listen})
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "twilight-agent:", err)
		os.Exit(1)
	}
}

func runAgent(o agentOptions) error {
	ctx := context.Background()
	var listener net.Listener
	if o.mode == "agent" {
		var err error
		listener, err = listenLoopback(o.address)
		if err != nil {
			return err
		}
		defer listener.Close()
	}
	var (
		models map[run.ModelRef]loop.ModelInvoker
		tools  []loop.ExecutableTool
		preset turn.AgentPreset
		err    error
	)
	if o.endpoint == "" {
		models, tools, preset, err = buildAgent(o.mock, o.provider, o.baseURL, o.apiKey, o.modelID, o.compat, o.system)
		if err == nil {
			tools[0] = nowTool{delay: o.toolDelay}
		}
	} else {
		if o.mock {
			o.modelID = "mock"
		}
		preset, err = app.NewPreset(run.ModelRef(o.modelID), []loop.ExecutableTool{nowTool{}}, app.WithSystemPrompt(o.system))
	}
	if err != nil {
		return err
	}
	execConfig := app.ExecutorConfig{Mode: app.ExecutorLocal, Models: models, Tools: tools}
	if o.endpoint != "" {
		execConfig = app.ExecutorConfig{Mode: app.ExecutorRemote, Endpoint: o.endpoint}
	}

	store, err := filestore.New(o.root)
	if err != nil {
		return err
	}
	// Frozen request bodies persist as cas content next to the session log, so
	// a restart can replay the request of a ModelStep that was executing at
	// the crash.
	content, err := filestore.NewContentStore(o.root, runmod.FrozenAuthority, filestore.ContentStoreOptions{})
	if err != nil {
		return err
	}
	// The application builder selects the local effect profile and wires the
	// authority, content store and preset registry in one place.
	a, err := app.Build(app.Config{
		Store:     store,
		Content:   content,
		Executor:  execConfig,
		Presets:   []app.Preset{{ID: "cli", Value: preset}},
		Ownership: session.OpenOptions{Takeover: true},
		Warn:      func(err error) { fmt.Fprintln(os.Stderr, "agent:", err) },
	})
	if err != nil {
		return err
	}
	defer a.Close(ctx)
	// Every observation derives from the committed stream: tool activity is
	// read off the Session's event stream, not from the executor.
	eventsCtx, stopEvents := context.WithCancel(ctx)
	defer stopEvents()
	go printToolActivity(a.Events(eventsCtx, o.sid))
	presetRef, err := a.PresetRef("cli")
	if err != nil {
		return err
	}
	s, err := a.OpenSession(ctx, o.sid, app.SessionOptions{Preset: presetRef, CompactAfterEntries: o.compactAfter,
		CompactWarn: func(err error) { fmt.Fprintln(os.Stderr, "compact:", err) }})
	if err != nil {
		return err
	}
	fmt.Printf("session %s - log at %s\n", o.sid, store.LogPath(o.sid))
	if o.endpoint != "" {
		fmt.Printf("executor %s\n", o.endpoint)
	}
	if s.Recovered > 0 {
		fmt.Printf("takeover: %d executing target disposed\n", s.Recovered)
	}
	if o.mode == "agent" {
		return serveAgent(listener, s, store, o.sid)
	}

	// Interactive commands share one lifetime, bounded by shutdown's grace.
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
			fmt.Println(store.LogPath(o.sid))
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
	return errors.Join(scanner.Err(), shutdown(ctx, s, cancel, &wg))
}

func report(results []app.Result, err error) {
	for _, r := range results {
		if r.Disposition == app.ResumeAlreadyDriving {
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

// shutdown waits for running turns, then cancels outstanding drives.
func shutdown(ctx context.Context, s *app.Session, cancel func(), wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		fmt.Fprintln(os.Stderr, "cancelling outstanding drives")
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			fmt.Fprintln(os.Stderr, "timed out waiting for the running turn")
		}
	}
	return s.Close(ctx)
}

// printToolActivity surfaces tool activity from the Session's event stream:
// the run facts that start and settle a tool call, decoded by the Host.
func printToolActivity(events <-chan app.Event) {
	for e := range events {
		ev, ok := e.Value.(runmod.Event)
		if !ok {
			continue
		}
		switch f := ev.Fact.(type) {
		case run.ToolCallStarted:
			fmt.Printf("tool call %s started\n", f.CallID)
		case run.ToolCallCompleted:
			fmt.Printf("tool call %s completed\n", f.CallID)
		}
	}
}

// --- agent -------------------------------------------------------------------

// buildAgent returns the effect capabilities and the AgentPreset. The app
// builder performs the actual authority/executor assembly.
func buildAgent(mock bool, provider, baseURL, apiKey, modelID, compat, system string) (map[run.ModelRef]loop.ModelInvoker, []loop.ExecutableTool, turn.AgentPreset, error) {
	if mock {
		tool := nowTool{}
		models := map[run.ModelRef]loop.ModelInvoker{"mock": mockModel{}}
		preset, err := app.NewPreset("mock", []loop.ExecutableTool{tool}, app.WithSystemPrompt(system))
		return models, []loop.ExecutableTool{tool}, preset, err
	}
	if provider != "openai-completions" {
		return nil, nil, turn.AgentPreset{}, fmt.Errorf("unsupported provider %q (only openai-completions)", provider)
	}
	if modelID == "" {
		return nil, nil, turn.AgentPreset{}, errors.New("-model is required (or use -mock)")
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
		return nil, nil, turn.AgentPreset{}, fmt.Errorf("unsupported compat %q (only deepseek)", compat)
	}
	// *sdk.Model is itself the ModelInvoker: it exposes
	// Generate(context.Context, sdk.Request) (sdk.ModelResult, error), so a
	// hand-written wrapper would only be ceremony between two identical shapes.
	invoker := &sdk.Model{ID: modelID, Provider: completions.New(opts...), Type: sdk.ModelTypeChat}
	models := map[run.ModelRef]loop.ModelInvoker{run.ModelRef(modelID): invoker}
	tools := []loop.ExecutableTool{nowTool{}}
	preset, err := app.NewPreset(run.ModelRef(modelID), tools, app.WithSystemPrompt(system))
	return models, tools, preset, err
}

// mockModel answers once a tool result is in the conversation and reports how
// many messages it saw, so a restart over the same session shows the context
// growing; otherwise it asks for the built-in tool first. A compactor request
// (app.CompactorSystemPrompt) gets a fixed summary for deterministic smoke.
type mockModel struct{}

func (mockModel) Generate(_ context.Context, req sdk.Request) (sdk.ModelResult, error) {
	if len(req.Messages) > 0 && req.Messages[0].Role == sdk.MessageRoleSystem && messageText(req.Messages[0]) == app.CompactorSystemPrompt {
		return sdk.ModelResult{Text: "mock summary of the compacted conversation",
			FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}, nil
	}
	for i := len(req.Messages) - 1; i >= 0; i-- {
		msg := req.Messages[i]
		if msg.Role == sdk.MessageRoleUser {
			break
		}
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

type nowTool struct{ delay time.Duration }

func (nowTool) Ref() run.ToolRef { return "now" }
func (nowTool) Definition() sdk.ToolDefinition {
	return sdk.ToolDefinition{Name: "now", Description: "current UTC time", Parameters: []byte(`{"type":"object","properties":{}}`)}
}
func (nowTool) ResponsePolicy() run.ResponsePolicy        { return run.DirectExecution }
func (nowTool) ValidateArguments(run.CanonicalJSON) error { return nil }
func (t nowTool) Execute(ctx context.Context, _ loop.ToolExecutionRequest) loop.ToolExecutionOutcome {
	if t.delay > 0 {
		fmt.Fprintf(os.Stderr, "now: started (delay %s)\n", t.delay)
		timer := time.NewTimer(t.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return loop.ToolExecutionFailed{Failure: run.ToolFailure{Class: run.FailureCancelled, Message: ctx.Err().Error()}}
		}
	}
	out := run.MustParseCanonicalJSON(fmt.Sprintf(`{"now":%q}`, time.Now().UTC().Format(time.RFC3339)))
	return loop.ToolExecutionSucceeded{Result: run.ToolExecutionResult{Output: out}}
}
