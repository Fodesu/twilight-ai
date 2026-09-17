package host

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/decision"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/turn"
)

// --- presets --------------------------------------------------------------------

// PresetOption tunes NewPreset.
type PresetOption func(*turn.AgentPreset)

func WithSystemPrompt(s string) PresetOption { return func(p *turn.AgentPreset) { p.SystemPrompt = s } }
func WithStreaming(on bool) PresetOption     { return func(p *turn.AgentPreset) { p.Streaming = on } }

// WithPromptBuilder selects the decision component; the default is
// decision.PromptContextV1.
func WithPromptBuilder(ref turn.PromptBuilderRef) PresetOption {
	return func(p *turn.AgentPreset) { p.Prompt = ref }
}

// WithScheduling sets how one step's tool calls run (parallel by default).
func WithScheduling(s run.ToolScheduling) PresetOption {
	return func(p *turn.AgentPreset) { p.Scheduling = s }
}

// WithMalformedRetries bounds the retries of one model step after malformed
// results; zero fails the Run on the first.
func WithMalformedRetries(n uint8) PresetOption {
	return func(p *turn.AgentPreset) { p.MalformedRetries = n }
}

// NewPreset builds the common one-model Preset: the tools' frozen
// definitions and response policies enter it, their implementations do not.
// The same tools are then served by the Executor's catalog.
func NewPreset(model run.ModelRef, tools []loop.ExecutableTool, opts ...PresetOption) (turn.AgentPreset, error) {
	if model == "" {
		return turn.AgentPreset{}, errors.New("host: preset requires a model ref")
	}
	p := turn.AgentPreset{SchemaVersion: 1, Model: model, Prompt: decision.PromptContextV1}
	seen := map[run.ToolRef]struct{}{}
	for _, t := range tools {
		if _, dup := seen[t.Ref()]; dup {
			return turn.AgentPreset{}, fmt.Errorf("host: duplicate tool %q", t.Ref())
		}
		seen[t.Ref()] = struct{}{}
		def, err := run.FreezeToolDefinition(t.Definition())
		if err != nil {
			return turn.AgentPreset{}, err
		}
		p.Tools = append(p.Tools, turn.PublicTool{Ref: t.Ref(), Definition: def, Policy: t.ResponsePolicy()})
	}
	for _, opt := range opts {
		opt(&p)
	}
	if err := turn.ValidatePreset(&p); err != nil {
		return turn.AgentPreset{}, err
	}
	return p, nil
}
