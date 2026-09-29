package decision

import (
	"fmt"

	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run/loop"
)

// PromptBuilderFactory builds the PromptBuilder of one PromptBuilderRef for
// one AgentPreset. The factory is pure configuration: the builder it returns
// reads state only through the Sources (DEC-CAT-1).
type PromptBuilderFactory func(preset.AgentPreset, Sources) loop.PromptBuilder

// PromptBuilders resolves PromptBuilderRefs on the Owner side (DEC-CAT-1).
// It is the decision layer's only registry: every other decision input is
// data on the AgentPreset itself.
type PromptBuilders struct {
	factories map[preset.PromptBuilderRef]PromptBuilderFactory
}

// NewPromptBuilders builds a registry; empty refs and nil factories are
// rejected, duplicates conflict.
func NewPromptBuilders(entries map[preset.PromptBuilderRef]PromptBuilderFactory) (*PromptBuilders, error) {
	c := &PromptBuilders{factories: make(map[preset.PromptBuilderRef]PromptBuilderFactory, len(entries))}
	for ref, f := range entries {
		if err := c.Register(ref, f); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// Register adds one prompt builder; the ref must be new.
func (c *PromptBuilders) Register(ref preset.PromptBuilderRef, f PromptBuilderFactory) error {
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
func (c *PromptBuilders) Resolve(ap preset.AgentPreset, sources Sources) (loop.PromptBuilder, error) {
	if c == nil {
		return nil, fmt.Errorf("decision: no prompt builders configured")
	}
	f, ok := c.factories[ap.Prompt]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownPromptBuilder, ap.Prompt)
	}
	return f(ap, sources), nil
}
