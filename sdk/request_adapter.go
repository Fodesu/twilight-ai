package sdk

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/google/jsonschema-go/jsonschema"
)

const toolChoiceFunctionType = "function"

// requestFromGenerateParams projects the provider-level fields of
// GenerateParams into the single-call Request boundary type. Client-side
// orchestration fields such as MaxSteps, callbacks, approvals, and tool
// Execute handlers intentionally do not appear in Request.
//
//nolint:gocritic // hugeParam: the adapter keeps the value-parameter shape its callers use.
func requestFromGenerateParams(params GenerateParams) (Request, error) {
	if params.Model == nil {
		return Request{}, fmt.Errorf("twilightai: request: model is required")
	}
	tools, err := ToolDefinitionsFromTools(params.Tools)
	if err != nil {
		return Request{}, err
	}
	choice, err := toolChoiceFromValue(params.ToolChoice)
	if err != nil {
		return Request{}, err
	}
	return Request{
		Model:            params.Model.ID,
		System:           params.System,
		Messages:         cloneMessages(params.Messages),
		Tools:            tools,
		ToolChoice:       choice,
		ResponseFormat:   cloneResponseFormat(params.ResponseFormat),
		Temperature:      clonePtr(params.Temperature),
		TopP:             clonePtr(params.TopP),
		MaxTokens:        clonePtr(params.MaxTokens),
		StopSequences:    append([]string(nil), params.StopSequences...),
		FrequencyPenalty: clonePtr(params.FrequencyPenalty),
		PresencePenalty:  clonePtr(params.PresencePenalty),
		Seed:             clonePtr(params.Seed),
		ReasoningEffort:  clonePtr(params.ReasoningEffort),
		ReasoningSummary: clonePtr(params.ReasoningSummary),
		PromptCacheKey:   clonePtr(params.PromptCacheKey),
	}, nil
}

// ToolDefinitionFromTool resolves a legacy Tool's Parameters into a detached
// JSON Schema document and drops execution-only fields.
func ToolDefinitionFromTool(tool Tool) (ToolDefinition, error) {
	schema, err := resolveSchema(tool.Parameters)
	if err != nil {
		return ToolDefinition{}, err
	}
	params := json.RawMessage("null")
	if schema != nil {
		params, err = json.Marshal(schema)
		if err != nil {
			return ToolDefinition{}, fmt.Errorf("twilightai: marshal tool schema: %w", err)
		}
	}
	return ToolDefinition{
		Name:         tool.Name,
		Description:  tool.Description,
		Parameters:   append(json.RawMessage(nil), params...),
		CacheControl: cloneCacheControl(tool.CacheControl),
	}, nil
}

// ToolDefinitionsFromTools converts a legacy tool list into provider-neutral
// definitions, preserving order.
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

// toolChoiceFromValue converts a GenerateParams tool-choice value into the
// closed provider-neutral ToolChoice. Supported inputs are "auto", "none",
// "required", a ToolChoice value, or the OpenAI-style function map
// {"type":"function","function":{"name":"..."}}.
func toolChoiceFromValue(choice any) (ToolChoice, error) {
	switch v := choice.(type) {
	case nil:
		return ToolChoice{}, nil
	case ToolChoice:
		return v, nil
	case string:
		switch ToolChoiceMode(v) {
		case "":
			return ToolChoice{}, nil
		case ToolChoiceAuto, ToolChoiceNone, ToolChoiceRequired:
			return ToolChoice{Mode: ToolChoiceMode(v)}, nil
		default:
			return ToolChoice{}, fmt.Errorf("twilightai: unsupported tool choice %q", v)
		}
	case map[string]any:
		return toolChoiceFromMap(v)
	default:
		// Accept JSON-shaped structs by round-tripping into the supported map
		// form; this keeps the adapter additive without making ToolChoice any part
		// of the new Request contract.
		raw, err := json.Marshal(v)
		if err != nil {
			return ToolChoice{}, fmt.Errorf("twilightai: marshal tool choice %T: %w", choice, err)
		}
		var m map[string]any
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(&m); err != nil {
			return ToolChoice{}, fmt.Errorf("twilightai: unmarshal tool choice %T: %w", choice, err)
		}
		return toolChoiceFromMap(m)
	}
}

func toolChoiceFromMap(m map[string]any) (ToolChoice, error) {
	typ, _ := m["type"].(string)
	if typ != toolChoiceFunctionType && typ != "tool" {
		return ToolChoice{}, fmt.Errorf("twilightai: unsupported tool choice type %q", typ)
	}
	fn, _ := m[toolChoiceFunctionType].(map[string]any)
	if fn == nil {
		fn, _ = m["tool"].(map[string]any)
	}
	name, _ := fn["name"].(string)
	if name == "" {
		return ToolChoice{}, fmt.Errorf("twilightai: tool choice requires function.name")
	}
	return ToolChoice{Mode: ToolChoiceTool, Tool: name}, nil
}

// GenerateResultFromModelResult adapts a single-call ModelResult back to the
// legacy result shape. The multi-step fields remain empty.
//
//nolint:gocritic // hugeParam: compatibility adapter preserves ModelResult as the SDK value DTO boundary.
func GenerateResultFromModelResult(result ModelResult) *GenerateResult {
	out := &GenerateResult{
		Text:                 result.Text,
		Reasoning:            result.Reasoning,
		ReasoningParts:       cloneReasoningParts(result.ReasoningParts),
		TextProviderMetadata: cloneMetadataMap(result.TextProviderMetadata),
		FinishReason:         result.FinishReason,
		RawFinishReason:      result.RawFinishReason,
		Usage:                result.Usage,
		Sources:              cloneSources(result.Sources),
		Files:                append([]GeneratedFile(nil), result.Files...),
		ToolCalls:            cloneToolCalls(result.ToolCalls),
	}
	if result.Response != nil {
		out.Response = *cloneResponseMetadataPtr(result.Response)
	}
	return out
}

func clonePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

func cloneCacheControl(c *CacheControl) *CacheControl {
	if c == nil {
		return nil
	}
	cc := *c
	return &cc
}

func cloneResponseFormat(f *ResponseFormat) *ResponseFormat {
	if f == nil {
		return nil
	}
	out := *f
	if f.JSONSchema != nil {
		out.JSONSchema = cloneSchema(f.JSONSchema)
	}
	return &out
}

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

func cloneMessages(messages []Message) []Message {
	if messages == nil {
		return nil
	}
	out := make([]Message, len(messages))
	for i, msg := range messages {
		out[i] = cloneMessage(msg)
	}
	return out
}

func cloneMessage(msg Message) Message {
	out := msg
	out.Usage = clonePtr(msg.Usage)
	if msg.Content != nil {
		out.Content = make([]MessagePart, len(msg.Content))
		for i, part := range msg.Content {
			out.Content[i] = cloneMessagePart(part)
		}
	}
	return out
}

func cloneMessagePart(part MessagePart) MessagePart {
	switch p := part.(type) {
	case TextPart:
		p.CacheControl = cloneCacheControl(p.CacheControl)
		p.ProviderMetadata = cloneMetadataMap(p.ProviderMetadata)
		return p
	case *TextPart:
		if p == nil {
			return nil
		}
		clone := *p
		clone.CacheControl = cloneCacheControl(clone.CacheControl)
		clone.ProviderMetadata = cloneMetadataMap(clone.ProviderMetadata)
		return &clone
	case ReasoningPart:
		p.ProviderMetadata = cloneMetadataMap(p.ProviderMetadata)
		return p
	case *ReasoningPart:
		if p == nil {
			return nil
		}
		clone := *p
		clone.ProviderMetadata = cloneMetadataMap(clone.ProviderMetadata)
		return &clone
	case ImagePart:
		p.CacheControl = cloneCacheControl(p.CacheControl)
		return p
	case *ImagePart:
		if p == nil {
			return nil
		}
		clone := *p
		clone.CacheControl = cloneCacheControl(clone.CacheControl)
		return &clone
	case FilePart:
		p.CacheControl = cloneCacheControl(p.CacheControl)
		return p
	case *FilePart:
		if p == nil {
			return nil
		}
		clone := *p
		clone.CacheControl = cloneCacheControl(clone.CacheControl)
		return &clone
	case ToolCallPart:
		p.CacheControl = cloneCacheControl(p.CacheControl)
		p.ProviderMetadata = cloneMetadataMap(p.ProviderMetadata)
		p.Input = cloneJSONLike(p.Input)
		return p
	case *ToolCallPart:
		if p == nil {
			return nil
		}
		clone := *p
		clone.CacheControl = cloneCacheControl(clone.CacheControl)
		clone.ProviderMetadata = cloneMetadataMap(clone.ProviderMetadata)
		clone.Input = cloneJSONLike(clone.Input)
		return &clone
	case ToolResultPart:
		p.CacheControl = cloneCacheControl(p.CacheControl)
		p.Result = cloneJSONLike(p.Result)
		return p
	case *ToolResultPart:
		if p == nil {
			return nil
		}
		clone := *p
		clone.CacheControl = cloneCacheControl(clone.CacheControl)
		clone.Result = cloneJSONLike(clone.Result)
		return &clone
	default:
		return part
	}
}

func cloneMetadataMap(meta map[string]any) map[string]any {
	if meta == nil {
		return nil
	}
	out := make(map[string]any, len(meta))
	for k, v := range meta {
		out[k] = cloneJSONLike(v)
	}
	return out
}

func cloneJSONLike(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case json.RawMessage:
		return append(json.RawMessage(nil), x...)
	case []byte:
		return append([]byte(nil), x...)
	case map[string]any:
		return cloneMetadataMap(x)
	case map[string]json.RawMessage:
		out := make(map[string]json.RawMessage, len(x))
		for k, v := range x {
			out[k] = append(json.RawMessage(nil), v...)
		}
		return out
	case map[string]string:
		out := make(map[string]string, len(x))
		for k, v := range x {
			out[k] = v
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, v := range x {
			out[i] = cloneJSONLike(v)
		}
		return out
	case []json.RawMessage:
		out := make([]json.RawMessage, len(x))
		for i, v := range x {
			out[i] = append(json.RawMessage(nil), v...)
		}
		return out
	default:
		return v
	}
}

func cloneReasoningParts(parts []ReasoningPart) []ReasoningPart {
	if parts == nil {
		return nil
	}
	out := make([]ReasoningPart, len(parts))
	for i, part := range parts {
		part.ProviderMetadata = cloneMetadataMap(part.ProviderMetadata)
		out[i] = part
	}
	return out
}

func cloneSources(sources []Source) []Source {
	if sources == nil {
		return nil
	}
	out := make([]Source, len(sources))
	for i, source := range sources {
		source.ProviderMetadata = cloneMetadataMap(source.ProviderMetadata)
		out[i] = source
	}
	return out
}

func cloneToolCalls(calls []ToolCall) []ToolCall {
	if calls == nil {
		return nil
	}
	out := make([]ToolCall, len(calls))
	for i, call := range calls {
		call.Input = cloneJSONLike(call.Input)
		call.ProviderMetadata = cloneMetadataMap(call.ProviderMetadata)
		out[i] = call
	}
	return out
}

func cloneResponseMetadataPtr(meta *ResponseMetadata) *ResponseMetadata {
	if meta == nil {
		return nil
	}
	out := *meta
	if !out.Timestamp.IsZero() {
		out.Timestamp = out.Timestamp.Round(0).UTC()
	}
	if meta.Headers != nil {
		out.Headers = make(map[string]string, len(meta.Headers))
		for k, v := range meta.Headers {
			out.Headers[k] = v
		}
	}
	return &out
}

func responseMetadataZero(meta ResponseMetadata) bool {
	return meta.ID == "" && meta.ModelID == "" && meta.Timestamp.IsZero() && len(meta.Headers) == 0
}
