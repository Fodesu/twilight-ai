package sdk

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/google/jsonschema-go/jsonschema"
)

// ModelStreamFromStreamResult adapts a legacy StreamResult into the single-call
// ModelStream boundary. It forwards every stream part while accumulating the
// final ModelResult; callers must consume Parts before calling Result.
func ModelStreamFromStreamResult(stream *StreamResult) ModelStream {
	out := make(chan StreamPart, 64)
	done := make(chan struct{})
	var result ModelResult
	var streamErr error

	go func() {
		defer close(done)
		defer close(out)
		if stream == nil {
			streamErr = fmt.Errorf("twilightai: nil stream result")
			return
		}
		var reasoning reasoningAccumulator
		for part := range stream.Stream {
			switch p := part.(type) {
			case *TextDeltaPart:
				result.Text += p.Text
			case *TextEndPart:
				if p.ProviderMetadata != nil {
					result.TextProviderMetadata = cloneMetadataMap(p.ProviderMetadata)
				}
			case *ReasoningStartPart:
				reasoning.openBlock(p.ID, p.Format, p.Model, cloneMetadataMap(p.ProviderMetadata))
			case *ReasoningDeltaPart:
				reasoning.appendDelta(p.ID, p.Text, p.Format, p.Model, cloneMetadataMap(p.ProviderMetadata))
			case *ReasoningEndPart:
				reasoning.closeBlock(p.ID, p.Format, p.Model, cloneMetadataMap(p.ProviderMetadata))
			case *StreamToolCallPart:
				result.ToolCalls = append(result.ToolCalls, ToolCall{
					ToolCallID:       p.ToolCallID,
					ToolName:         p.ToolName,
					Input:            cloneJSONLike(p.Input),
					ProviderMetadata: cloneMetadataMap(p.ProviderMetadata),
				})
			case *StreamSourcePart:
				source := p.Source
				source.ProviderMetadata = cloneMetadataMap(source.ProviderMetadata)
				result.Sources = append(result.Sources, source)
			case *StreamFilePart:
				result.Files = append(result.Files, p.File)
			case *FinishStepPart:
				result.FinishReason = p.FinishReason
				result.RawFinishReason = p.RawFinishReason
				result.Usage = p.Usage
				result.Response = cloneResponseMetadataPtr(&p.Response)
			case *FinishPart:
				result.FinishReason = p.FinishReason
				result.RawFinishReason = p.RawFinishReason
				result.Usage = p.TotalUsage
			case *ErrorPart:
				if streamErr == nil {
					streamErr = p.Error
				}
			}
			out <- part
		}
		result.ReasoningParts = cloneReasoningParts(reasoning.result())
		result.Reasoning = ReasoningText(result.ReasoningParts)
	}()

	return ModelStream{
		Parts: out,
		Result: func() (*ModelResult, error) {
			<-done
			res := result
			res.ReasoningParts = cloneReasoningParts(res.ReasoningParts)
			res.TextProviderMetadata = cloneMetadataMap(res.TextProviderMetadata)
			res.Sources = cloneSources(res.Sources)
			res.Files = append([]GeneratedFile(nil), res.Files...)
			res.ToolCalls = cloneToolCalls(res.ToolCalls)
			res.Response = cloneResponseMetadataPtr(res.Response)
			return &res, streamErr
		},
	}
}

// RequestFromGenerateParams projects the provider-level fields of legacy
// GenerateParams into the single-call Request boundary type. Client-side
// orchestration fields such as MaxSteps, callbacks, approvals, and tool
// Execute handlers intentionally do not appear in Request.
func RequestFromGenerateParams(params GenerateParams) (Request, error) {
	if params.Model == nil {
		return Request{}, fmt.Errorf("twilightai: request: model is required")
	}
	tools, err := ToolDefinitionsFromTools(params.Tools)
	if err != nil {
		return Request{}, err
	}
	choice, err := ToolChoiceFromLegacy(params.ToolChoice)
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

// GenerateParamsFromRequest adapts a single-call Request back to legacy
// GenerateParams for providers that still implement Provider.DoGenerate and
// Provider.DoStream. The supplied model provides the provider binding that a
// Request intentionally does not persist. Returned tools contain definitions
// only; Execute and RequireApproval stay empty because provider calls only need
// schemas.
func GenerateParamsFromRequest(model *Model, req Request) (GenerateParams, error) {
	if model == nil {
		return GenerateParams{}, fmt.Errorf("twilightai: request: model is required")
	}
	if req.Model != "" && model.ID != "" && req.Model != model.ID {
		return GenerateParams{}, fmt.Errorf("twilightai: request model %q does not match provider model %q", req.Model, model.ID)
	}
	if len(req.ProviderOptions) > 0 {
		return GenerateParams{}, fmt.Errorf("twilightai: request providerOptions require a ModelInvoker provider")
	}
	tools := make([]Tool, len(req.Tools))
	for i, def := range req.Tools {
		tool, err := ToolFromDefinition(def)
		if err != nil {
			return GenerateParams{}, fmt.Errorf("twilightai: request tool %q: %w", def.Name, err)
		}
		tools[i] = tool
	}
	return GenerateParams{
		Model:            model,
		System:           req.System,
		Messages:         cloneMessages(req.Messages),
		Tools:            tools,
		ToolChoice:       req.ToolChoice.Legacy(),
		ResponseFormat:   cloneResponseFormat(req.ResponseFormat),
		Temperature:      clonePtr(req.Temperature),
		TopP:             clonePtr(req.TopP),
		MaxTokens:        clonePtr(req.MaxTokens),
		StopSequences:    append([]string(nil), req.StopSequences...),
		FrequencyPenalty: clonePtr(req.FrequencyPenalty),
		PresencePenalty:  clonePtr(req.PresencePenalty),
		Seed:             clonePtr(req.Seed),
		ReasoningEffort:  clonePtr(req.ReasoningEffort),
		ReasoningSummary: clonePtr(req.ReasoningSummary),
		PromptCacheKey:   clonePtr(req.PromptCacheKey),
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

// ToolFromDefinition adapts a provider-neutral definition back to a legacy
// Tool value for provider calls. The returned Tool has no Execute handler.
func ToolFromDefinition(def ToolDefinition) (Tool, error) {
	var params any
	if len(def.Parameters) > 0 && string(def.Parameters) != "null" {
		var schema jsonschema.Schema
		if err := json.Unmarshal(def.Parameters, &schema); err != nil {
			return Tool{}, fmt.Errorf("unmarshal tool schema: %w", err)
		}
		params = &schema
	}
	return Tool{
		Name:         def.Name,
		Description:  def.Description,
		Parameters:   params,
		CacheControl: cloneCacheControl(def.CacheControl),
	}, nil
}

// ToolChoiceFromLegacy converts the legacy ToolChoice any shape into the
// closed provider-neutral ToolChoice. Supported legacy inputs are "auto",
// "none", "required", a ToolChoice value, or the OpenAI-style function map
// {"type":"function","function":{"name":"..."}}.
func ToolChoiceFromLegacy(choice any) (ToolChoice, error) {
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
	if typ != "function" && typ != "tool" {
		return ToolChoice{}, fmt.Errorf("twilightai: unsupported tool choice type %q", typ)
	}
	fn, _ := m["function"].(map[string]any)
	if fn == nil {
		fn, _ = m["tool"].(map[string]any)
	}
	name, _ := fn["name"].(string)
	if name == "" {
		return ToolChoice{}, fmt.Errorf("twilightai: tool choice requires function.name")
	}
	return ToolChoice{Mode: ToolChoiceTool, Tool: name}, nil
}

// Legacy converts a closed ToolChoice back to the legacy any shape consumed by
// existing providers.
func (c ToolChoice) Legacy() any {
	switch c.Mode {
	case "":
		return nil
	case ToolChoiceAuto, ToolChoiceNone, ToolChoiceRequired:
		return string(c.Mode)
	case ToolChoiceTool:
		return map[string]any{"type": "function", "function": map[string]any{"name": c.Tool}}
	default:
		return nil
	}
}

// ModelResultFromGenerateResult extracts the single-call fields of a legacy
// GenerateResult. Multi-step Steps/Messages, tool execution results, deferred
// approval state, and callbacks are intentionally not part of ModelResult.
func ModelResultFromGenerateResult(result *GenerateResult) ModelResult {
	if result == nil {
		return ModelResult{}
	}
	var response *ResponseMetadata
	if !responseMetadataZero(result.Response) {
		response = cloneResponseMetadataPtr(&result.Response)
	}
	return ModelResult{
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
		Response:             response,
	}
}

// GenerateResultFromModelResult adapts a single-call ModelResult back to the
// legacy result shape. The multi-step fields remain empty.
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
			return p
		}
		clone := cloneMessagePart(*p).(TextPart)
		return &clone
	case ReasoningPart:
		p.ProviderMetadata = cloneMetadataMap(p.ProviderMetadata)
		return p
	case *ReasoningPart:
		if p == nil {
			return p
		}
		clone := cloneMessagePart(*p).(ReasoningPart)
		return &clone
	case ImagePart:
		p.CacheControl = cloneCacheControl(p.CacheControl)
		return p
	case *ImagePart:
		if p == nil {
			return p
		}
		clone := cloneMessagePart(*p).(ImagePart)
		return &clone
	case FilePart:
		p.CacheControl = cloneCacheControl(p.CacheControl)
		return p
	case *FilePart:
		if p == nil {
			return p
		}
		clone := cloneMessagePart(*p).(FilePart)
		return &clone
	case ToolCallPart:
		p.CacheControl = cloneCacheControl(p.CacheControl)
		p.ProviderMetadata = cloneMetadataMap(p.ProviderMetadata)
		p.Input = cloneJSONLike(p.Input)
		return p
	case *ToolCallPart:
		if p == nil {
			return p
		}
		clone := cloneMessagePart(*p).(ToolCallPart)
		return &clone
	case ToolResultPart:
		p.CacheControl = cloneCacheControl(p.CacheControl)
		p.Result = cloneJSONLike(p.Result)
		return p
	case *ToolResultPart:
		if p == nil {
			return p
		}
		clone := cloneMessagePart(*p).(ToolResultPart)
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
