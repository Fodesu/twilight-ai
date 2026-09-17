// Package app provides the application-level composition boundary for the
// agent runtime. It turns concrete infrastructure and effect capabilities
// into a runnable authority without making callers assemble Host ports by
// hand. The core packages remain independent of this package.
package app

import (
	"context"
	"errors"
	"fmt"
	stdhttp "net/http"
	"sync"

	"github.com/felinics/twilight/agent/artifact"
	"github.com/felinics/twilight/agent/decision"
	"github.com/felinics/twilight/agent/executor/http"
	"github.com/felinics/twilight/agent/host"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/effect"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session"
	runmod "github.com/felinics/twilight/agent/session/run"
	"github.com/felinics/twilight/agent/turn"
)

// ExecutorMode selects the effect implementation built by Build.
type ExecutorMode string

const (
	// ExecutorLocal runs model and tool effects in this process.
	ExecutorLocal ExecutorMode = "local"
	// ExecutorRemote forwards effects to an executor/http Server.
	ExecutorRemote ExecutorMode = "remote"
)

// ExecutorConfig describes how the application obtains its effect Port. Port
// takes precedence when supplied, which is useful for tests and custom
// transports. Local and Remote are the standard deployment profiles.
type ExecutorConfig struct {
	Mode ExecutorMode
	Port effect.Port

	// Models and Tools are required for ExecutorLocal. They are ignored by
	// ExecutorRemote because implementations live in the worker process.
	Models map[run.ModelRef]loop.ModelInvoker
	Tools  []loop.ExecutableTool

	// Endpoint and HTTP configure ExecutorRemote.
	Endpoint string
	HTTP     *stdhttp.Client
}

// Preset is an authority-side decision identity to register during Build.
type Preset struct {
	ID    turn.PresetID
	Value turn.AgentPreset
}

// Config contains application assembly choices. It deliberately contains
// concrete ports rather than a file format: YAML, environment variables and
// command-line flags can be decoded into this type by an outer deployment
// package later.
type Config struct {
	Store   session.Store
	Content artifact.ContentStore

	Executor ExecutorConfig
	Presets  []Preset

	Ownership      session.OpenOptions
	TargetResolver loop.TargetResolver
	Warn           func(error)
}

// Session and SessionOptions are the application-facing conversation facade
// types. Their implementation remains in the authority package.
type Session = host.Session
type SessionOptions = host.SessionOptions
type Result = host.Result
type Event = host.Event
type ForkRequest = host.ForkRequest

// ResumeAlreadyDriving is returned when another driver already owns a Run.
const ResumeAlreadyDriving = host.ResumeAlreadyDriving

// CompactorSystemPrompt is kept here for deterministic model test doubles and
// applications that need to recognize the built-in compaction request.
const CompactorSystemPrompt = host.CompactorSystemPrompt

// Application is the public authority facade returned by Build. The
// underlying authority implementation is deliberately hidden from callers;
// the application package exposes only the operations needed at the
// composition boundary.
type Application struct {
	authority *host.Host

	mu   sync.RWMutex
	refs map[turn.PresetID]turn.PresetRef
}

// Build assembles an authority application from typed dependencies and a
// deployment-neutral executor profile. It is the single convenience entry
// point for normal applications; callers only need to construct infrastructure
// and register model/tool implementations.
func Build(c Config) (*Application, error) {
	content := c.Content
	if content == nil {
		var err error
		content, err = artifact.NewMemoryContentStore(runmod.FrozenAuthority, artifact.MemoryContentStoreOptions{})
		if err != nil {
			return nil, fmt.Errorf("app: create default content store: %w", err)
		}
	}
	port, err := buildExecutor(c.Executor)
	if err != nil {
		return nil, err
	}

	h, err := host.New(host.Ports{
		Store:          c.Store,
		Content:        content,
		Executor:       port,
		TargetResolver: c.TargetResolver,
		Ownership:      c.Ownership,
		Warn:           c.Warn,
	})
	if err != nil {
		return nil, err
	}
	refs := make(map[turn.PresetID]turn.PresetRef, len(c.Presets))
	for _, preset := range c.Presets {
		if preset.ID == "" {
			return nil, errors.New("app: preset requires an id")
		}
		ref, err := h.Presets.Register(preset.ID, preset.Value)
		if err != nil {
			return nil, fmt.Errorf("app: register preset %q: %w", preset.ID, err)
		}
		refs[preset.ID] = ref
	}
	return &Application{authority: h, refs: refs}, nil
}

// RegisterPreset adds or replaces an authority-side decision identity after
// Build. Most applications should provide presets in Config instead.
func (a *Application) RegisterPreset(id turn.PresetID, preset turn.AgentPreset) (turn.PresetRef, error) {
	ref, err := a.authority.Presets.Register(id, preset)
	if err != nil {
		return turn.PresetRef{}, err
	}
	a.mu.Lock()
	a.refs[id] = ref
	a.mu.Unlock()
	return ref, nil
}

// PresetRef returns the digest-checked reference for a configured preset.
func (a *Application) PresetRef(id turn.PresetID) (turn.PresetRef, error) {
	a.mu.RLock()
	ref, ok := a.refs[id]
	a.mu.RUnlock()
	if !ok {
		return turn.PresetRef{}, fmt.Errorf("app: unknown preset %q", id)
	}
	return ref, nil
}

// OpenSession opens a session using a registered preset.
func (a *Application) OpenSession(ctx context.Context, sid session.SessionID, opts SessionOptions) (*Session, error) {
	return a.authority.OpenSession(ctx, sid, opts)
}

// Fork creates a child session from a parent's ledger prefix (HST-FRK-1).
func (a *Application) Fork(ctx context.Context, req ForkRequest) (session.SessionHeader, error) {
	return a.authority.Fork(ctx, req)
}

// ForkBeforeTurn forks a session at the commit before the named turn started
// (HST-FRK-2), so the turn's inputs can be regenerated or edited in the child.
func (a *Application) ForkBeforeTurn(ctx context.Context, parent session.SessionID, turnID turn.TurnID, child session.SessionID) (session.SessionHeader, error) {
	return a.authority.ForkBeforeTurn(ctx, parent, turnID, child)
}

// DeleteSession drops a session's root; forks that inherit its commits keep
// reading them until Collect reclaims what nothing reaches (SES-GC).
func (a *Application) DeleteSession(ctx context.Context, sid session.SessionID) error {
	return a.authority.DeleteSession(ctx, sid)
}

// Collect reclaims unreachable session segments.
func (a *Application) Collect(ctx context.Context) (session.CollectReport, error) {
	return a.authority.Collect(ctx)
}

// WithdrawInput marks a submitted, undelivered input as withdrawn.
func (a *Application) WithdrawInput(ctx context.Context, sid session.SessionID, id run.InputID, reason string) error {
	return a.authority.WithdrawInput(ctx, sid, id, reason)
}

// Close releases sessions owned by the application.
func (a *Application) Close(ctx context.Context) error {
	return a.authority.Close(ctx)
}

// Drive advances a turn to its next quiescent point.
func (a *Application) Drive(ctx context.Context, ref turn.TurnRef) (turn.TurnResponse, error) {
	return a.authority.Drive(ctx, ref)
}

// Events returns the committed/provisional observation stream for a session.
func (a *Application) Events(ctx context.Context, sid session.SessionID) <-chan Event {
	return a.authority.Events(ctx, sid)
}

func buildExecutor(c ExecutorConfig) (effect.Port, error) {
	if c.Port != nil {
		return c.Port, nil
	}
	switch c.Mode {
	case "", ExecutorLocal:
		catalog, err := host.NewCatalog(c.Models, c.Tools...)
		if err != nil {
			return nil, err
		}
		port, err := host.NewLocalExecutor(catalog, nil, false)
		if err != nil {
			return nil, err
		}
		return port, nil
	case ExecutorRemote:
		if c.Endpoint == "" {
			return nil, errors.New("app: remote executor requires an endpoint")
		}
		return &http.Client{BaseURL: c.Endpoint, HTTP: c.HTTP}, nil
	default:
		return nil, fmt.Errorf("app: unknown executor mode %q", c.Mode)
	}
}

// NewPreset is the application-facing helper for constructing the common
// preset shape. Tool implementations are used only to freeze their public
// definitions; they are not stored in the preset.
func NewPreset(model run.ModelRef, tools []loop.ExecutableTool, opts ...PresetOption) (turn.AgentPreset, error) {
	if model == "" {
		return turn.AgentPreset{}, errors.New("app: preset requires a model")
	}
	p := turn.AgentPreset{SchemaVersion: 1, Model: model, Prompt: decision.PromptContextV1}
	seen := make(map[run.ToolRef]struct{}, len(tools))
	for _, tool := range tools {
		if tool == nil {
			return turn.AgentPreset{}, errors.New("app: preset got a nil tool")
		}
		if _, ok := seen[tool.Ref()]; ok {
			return turn.AgentPreset{}, fmt.Errorf("app: duplicate tool %q", tool.Ref())
		}
		seen[tool.Ref()] = struct{}{}
		definition, err := run.FreezeToolDefinition(tool.Definition())
		if err != nil {
			return turn.AgentPreset{}, err
		}
		p.Tools = append(p.Tools, turn.PublicTool{
			Ref: tool.Ref(), Definition: definition, Policy: tool.ResponsePolicy(),
		})
	}
	for _, opt := range opts {
		opt(&p)
	}
	if err := turn.ValidatePreset(&p); err != nil {
		return turn.AgentPreset{}, err
	}
	return p, nil
}

// NewPresetFromDefinitions constructs a preset for an authority that does not
// have local tool implementations. The definitions are already frozen public
// protocol values and can safely come from configuration.
func NewPresetFromDefinitions(model run.ModelRef, tools []turn.PublicTool, opts ...PresetOption) (turn.AgentPreset, error) {
	if model == "" {
		return turn.AgentPreset{}, errors.New("app: preset requires a model")
	}
	p := turn.AgentPreset{
		SchemaVersion: 1,
		Model:         model,
		Prompt:        decision.PromptContextV1,
		Tools:         append([]turn.PublicTool(nil), tools...),
	}
	for _, opt := range opts {
		opt(&p)
	}
	if err := turn.ValidatePreset(&p); err != nil {
		return turn.AgentPreset{}, err
	}
	return p, nil
}

// PresetOption tunes NewPresetFromDefinitions without requiring a local tool
// implementation.
type PresetOption func(*turn.AgentPreset)

// WithSystemPrompt sets the instruction included in the preset digest.
func WithSystemPrompt(s string) PresetOption {
	return func(p *turn.AgentPreset) { p.SystemPrompt = s }
}

// WithPrompt selects the decision component used by the preset.
func WithPrompt(ref turn.PromptBuilderRef) PresetOption {
	return func(p *turn.AgentPreset) { p.Prompt = ref }
}

// WithStreaming selects streaming model execution.
func WithStreaming(on bool) PresetOption {
	return func(p *turn.AgentPreset) { p.Streaming = on }
}

// WithScheduling selects tool scheduling.
func WithScheduling(s run.ToolScheduling) PresetOption {
	return func(p *turn.AgentPreset) { p.Scheduling = s }
}

// WithMalformedRetries sets malformed model response retries.
func WithMalformedRetries(n uint8) PresetOption {
	return func(p *turn.AgentPreset) { p.MalformedRetries = n }
}
