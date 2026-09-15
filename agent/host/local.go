package host

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/decision"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/turn"
)

// Catalog is a static effect catalog for a colocated deployment: the model
// invokers and tool implementations one process serves. It is the
// implementation side of the effect layer; nothing in it enters an AgentPreset.
type Catalog struct {
	models map[run.ModelRef]loop.ModelInvoker
	tools  map[run.ToolRef]loop.ExecutableTool
}

// NewCatalog builds a Catalog; models maps each ModelRef to its invoker.
func NewCatalog(models map[run.ModelRef]loop.ModelInvoker, tools ...loop.ExecutableTool) (*Catalog, error) {
	c := &Catalog{models: make(map[run.ModelRef]loop.ModelInvoker, len(models)), tools: make(map[run.ToolRef]loop.ExecutableTool, len(tools))}
	for ref, inv := range models {
		if ref == "" || inv == nil {
			return nil, errors.New("host: catalog requires a model ref and an invoker")
		}
		c.models[ref] = inv
	}
	for _, t := range tools {
		if t == nil {
			return nil, errors.New("host: catalog got a nil tool")
		}
		if _, dup := c.tools[t.Ref()]; dup {
			return nil, fmt.Errorf("host: duplicate tool %q", t.Ref())
		}
		c.tools[t.Ref()] = t
	}
	return c, nil
}

func (c *Catalog) ResolveModel(ref run.ModelRef) (loop.ModelInvoker, error) {
	inv, ok := c.models[ref]
	if !ok {
		return nil, fmt.Errorf("host: unknown model %q", ref)
	}
	return inv, nil
}

func (c *Catalog) ResolveTool(ref run.ToolRef) (loop.ExecutableTool, error) {
	t, ok := c.tools[ref]
	if !ok {
		return nil, fmt.Errorf("host: unknown tool %q", ref)
	}
	return t, nil
}

// NewLocalExecutor is the colocated Executor: effects run in goroutines of
// this process against the Catalog, and provisional observations go to sink.
// Model assignments must carry the request inline (RUN-EXE-7); the Host still
// writes frozen bodies to Ports.Content (RUN-WIR-4) for the Session record,
// and the executor never reads them back. streaming selects
// StreamingModelInvoker when an invoker offers it.
func NewLocalExecutor(cat *Catalog, sink loop.EventSink, streaming bool) (loop.Executor, error) {
	if cat == nil {
		return nil, errors.New("host: nil catalog")
	}
	return loop.NewLocalExecutor(cat, cat, sink, streaming)
}

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
