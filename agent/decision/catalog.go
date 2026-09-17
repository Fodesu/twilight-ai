package decision

import (
	"fmt"

	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/turn"
)

// PromptBuilderFactory builds the PromptBuilder of one PromptBuilderRef for
// one AgentPreset. The factory is pure configuration: the builder it returns
// reads state only through the Sources (DEC-CAT-1).
type PromptBuilderFactory func(turn.AgentPreset, Sources) loop.PromptBuilder

// PromptBuilders resolves PromptBuilderRefs on the authority side (DEC-CAT-1).
// It is the decision layer's only registry: every other decision input is
// data on the AgentPreset itself.
type PromptBuilders struct {
	factories map[turn.PromptBuilderRef]PromptBuilderFactory
}

// NewPromptBuilders builds a registry; empty refs and nil factories are
// rejected, duplicates conflict.
func NewPromptBuilders(entries map[turn.PromptBuilderRef]PromptBuilderFactory) (*PromptBuilders, error) {
	c := &PromptBuilders{factories: make(map[turn.PromptBuilderRef]PromptBuilderFactory, len(entries))}
	for ref, f := range entries {
		if err := c.Register(ref, f); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// Register adds one prompt builder; the ref must be new.
func (c *PromptBuilders) Register(ref turn.PromptBuilderRef, f PromptBuilderFactory) error {
	if ref == "" || f == nil {
		return fmt.Errorf("decision: prompt builder registration requires a ref and a factory")
	}
	if _, dup := c.factories[ref]; dup {
		return fmt.Errorf("decision: prompt builder %q registered twice", ref)
	}
	c.factories[ref] = f
	return nil
}

// Resolve returns the builder of preset.Prompt or ErrUnknownPromptBuilder
// (DEC-CAT-2).
func (c *PromptBuilders) Resolve(preset turn.AgentPreset, sources Sources) (loop.PromptBuilder, error) {
	if c == nil {
		return nil, fmt.Errorf("decision: no prompt builders configured")
	}
	f, ok := c.factories[preset.Prompt]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownPromptBuilder, preset.Prompt)
	}
	return f(preset, sources), nil
}

// DefaultPromptBuilders holds the first-party prompt builder: the context
// builder (PromptContextV1).
func DefaultPromptBuilders() *PromptBuilders {
	builders, _ := NewPromptBuilders(map[turn.PromptBuilderRef]PromptBuilderFactory{PromptContextV1: NewContextPromptBuilder})
	return builders
}
