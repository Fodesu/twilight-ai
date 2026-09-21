package sdk

import (
	"encoding/json"
	"fmt"

	"github.com/google/jsonschema-go/jsonschema"
)

// ToolDefinitionFromTool is the provider-facing definition of a Tool: the
// name, description, schema and cache control, without the execute handler.
func ToolDefinitionFromTool(tool Tool) (ToolDefinition, error) {
	if tool.Name == "" {
		return ToolDefinition{}, fmt.Errorf("twilightai: tool definition requires a name")
	}
	return ToolDefinition{
		Name:         tool.Name,
		Description:  tool.Description,
		Parameters:   cloneSchema(tool.Parameters),
		CacheControl: cloneCacheControl(tool.CacheControl),
	}, nil
}

// ToolDefinitionsFromTools converts every tool; nil in, nil out.
func ToolDefinitionsFromTools(tools []Tool) ([]ToolDefinition, error) {
	if tools == nil {
		return nil, nil
	}
	out := make([]ToolDefinition, len(tools))
	for i, tool := range tools {
		def, err := ToolDefinitionFromTool(tool)
		if err != nil {
			return nil, fmt.Errorf("twilightai: tool %q: %w", tool.Name, err)
		}
		out[i] = def
	}
	return out, nil
}

func cloneCacheControl(c *CacheControl) *CacheControl {
	if c == nil {
		return nil
	}
	cc := *c
	return &cc
}

// cloneSchema copies a schema through JSON, the only complete copy of the
// schema type's nested structure.
func cloneSchema(s *jsonschema.Schema) *jsonschema.Schema {
	if s == nil {
		return nil
	}
	raw, err := json.Marshal(s)
	if err != nil {
		return s
	}
	var out jsonschema.Schema
	if err := json.Unmarshal(raw, &out); err != nil {
		return s
	}
	return &out
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

func cloneResponseMetadataPtr(meta *ResponseMetadata) *ResponseMetadata {
	if meta == nil {
		return nil
	}
	out := *meta
	if responseMetadataZero(out) {
		return nil
	}
	return &out
}

func responseMetadataZero(meta ResponseMetadata) bool {
	return meta.ID == "" && meta.ModelID == "" && meta.Timestamp.IsZero() && len(meta.Headers) == 0
}
