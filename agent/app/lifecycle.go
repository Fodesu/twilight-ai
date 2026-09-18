package app

import (
	"context"

	"github.com/felinics/twilight/agent/decision"
	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/run/model/sdkconv"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/extension"
	"github.com/felinics/twilight/agent/turn"
)

// --- session lifecycle, forwarded from the Authority ---------------------------------

// Fork creates a child session from a parent's ledger prefix (AUTH-FRK-1).
func (app *Application) Fork(ctx context.Context, req ForkRequest) (session.SegmentHeader, error) {
	return app.Authority.Fork(ctx, req)
}

// ForkBeforeTurn forks a session at the commit before the named turn started
// (AUTH-FRK-2), so the turn's inputs can be regenerated or edited in the child.
func (app *Application) ForkBeforeTurn(ctx context.Context, parent session.SessionID, turnID turn.TurnID, child session.SessionID) (session.SegmentHeader, error) {
	return app.Authority.ForkBeforeTurn(ctx, parent, turnID, child)
}

// DeleteSession drops a session's root (AUTH-FRK-3).
func (app *Application) DeleteSession(ctx context.Context, sid session.SessionID) error {
	return app.Authority.DeleteSession(ctx, sid)
}

// Collect reclaims unreachable session segments (SES-GC-2).
func (app *Application) Collect(ctx context.Context) (session.CollectReport, error) {
	return app.Authority.Collect(ctx)
}

// ChatlogSurface reads the chatlog surface of a Session.
func (app *Application) ChatlogSurface(ctx context.Context, sid session.SessionID) (chatlog.Surface, error) {
	return chatlog.ReadSurface(ctx, app.Authority.Projections, sid)
}

// TurnSurface reads the turn surface of a Session.
func (app *Application) TurnSurface(ctx context.Context, sid session.SessionID) (turn.TurnSurface, error) {
	return turn.ReadSurface(ctx, app.Authority.Projections, sid)
}

// Projection reads any registered projection of a Session (APP-MEM-1).
func (app *Application) Projection(ctx context.Context, sid session.SessionID, id extension.ProjectionID, v extension.ProjectionVersion) (any, session.Head, error) {
	return app.Authority.Projection(ctx, sid, id, v)
}

// Content materializes the frozen bodies projections name (CHT-MAT-1).
func (app *Application) Content() chatlog.ContentResolver { return app.Authority.Content }

// CreateSession creates the Session.
func (app *Application) CreateSession(ctx context.Context, sid session.SessionID) error {
	return app.Authority.CreateSession(ctx, sid, jsonstable.Value{})
}

// EnsureSession creates the stream when it does not exist yet.
func (app *Application) EnsureSession(ctx context.Context, sid session.SessionID) error {
	return app.Authority.EnsureSession(ctx, sid)
}

// --- presets ------------------------------------------------------------------------

// PresetOption tunes NewPreset and NewPresetFromDefinitions.
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

// NewPreset constructs the common preset shape. Tool implementations are
// used only to freeze their public definitions; they are not stored in the
// preset.
func NewPreset(model run.ModelRef, tools []loop.ExecutableTool, opts ...PresetOption) (turn.AgentPreset, error) {
	defs := make([]turn.PublicTool, 0, len(tools))
	seen := make(map[run.ToolRef]struct{}, len(tools))
	for _, tool := range tools {
		if tool == nil {
			return turn.AgentPreset{}, errNilTool
		}
		if _, ok := seen[tool.Ref()]; ok {
			return turn.AgentPreset{}, &duplicateToolError{tool.Ref()}
		}
		seen[tool.Ref()] = struct{}{}
		definition, err := sdkconv.FreezeToolDefinition(tool.Definition())
		if err != nil {
			return turn.AgentPreset{}, err
		}
		defs = append(defs, turn.PublicTool{Ref: tool.Ref(), Definition: definition, Policy: tool.ResponsePolicy()})
	}
	return NewPresetFromDefinitions(model, defs, opts...)
}

// NewPresetFromDefinitions constructs a preset from already frozen public
// tool definitions, for an authority without local tool implementations.
func NewPresetFromDefinitions(model run.ModelRef, tools []turn.PublicTool, opts ...PresetOption) (turn.AgentPreset, error) {
	if model == "" {
		return turn.AgentPreset{}, errNoModel
	}
	p := turn.AgentPreset{SchemaVersion: 1, Model: model, Prompt: decision.PromptContextV1, Tools: append([]turn.PublicTool(nil), tools...)}
	for _, opt := range opts {
		opt(&p)
	}
	if err := turn.ValidatePreset(&p); err != nil {
		return turn.AgentPreset{}, err
	}
	return p, nil
}
