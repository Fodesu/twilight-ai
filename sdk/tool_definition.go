package sdk

import (
	"fmt"

	"github.com/google/jsonschema-go/jsonschema"
)

// NewToolDefinition describes a tool whose arguments decode into T: the JSON
// Schema of T becomes Parameters. The SDK stops at the definition; running the
// call the model makes, and decoding its ToolArguments into T, is the
// caller's.
func NewToolDefinition[T any](name, description string) (ToolDefinition, error) {
	if name == "" {
		return ToolDefinition{}, fmt.Errorf("twilightai: tool definition requires a name")
	}
	schema, err := jsonschema.For[T](nil)
	if err != nil {
		return ToolDefinition{}, fmt.Errorf("twilightai: infer schema for tool %q: %w", name, err)
	}
	return ToolDefinition{Name: name, Description: description, Parameters: schema}, nil
}

func cloneReasoningParts(parts []ReasoningPart) []ReasoningPart {
	if parts == nil {
		return nil
	}
	out := make([]ReasoningPart, len(parts))
	for i, p := range parts {
		p.ProviderMetadata = p.ProviderMetadata.Clone()
		out[i] = p
	}
	return out
}

func cloneSources(sources []Source) []Source {
	if sources == nil {
		return nil
	}
	out := make([]Source, len(sources))
	for i, s := range sources {
		s.ProviderMetadata = s.ProviderMetadata.Clone()
		out[i] = s
	}
	return out
}

func cloneToolCalls(calls []ToolCall) []ToolCall {
	if calls == nil {
		return nil
	}
	out := make([]ToolCall, len(calls))
	for i, c := range calls {
		c.Input = c.Input.clone()
		c.ProviderMetadata = c.ProviderMetadata.Clone()
		out[i] = c
	}
	return out
}
