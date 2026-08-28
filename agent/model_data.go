package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/memohai/twilight-ai/sdk"
)

// ProviderMetadata is the agent's persisted representation of provider-owned
// opaque metadata. Each namespace value is detached canonical JSON: callers may
// keep mutating their sdk map, but runtime events/state own these bytes.
type ProviderMetadata map[string]json.RawMessage

type CacheControl struct {
	Type string `json:"type"`
	TTL  string `json:"ttl,omitempty"`
}

type MessageRole string

const (
	MessageRoleUser      MessageRole = "user"
	MessageRoleAssistant MessageRole = "assistant"
	MessageRoleSystem    MessageRole = "system"
	MessageRoleTool      MessageRole = "tool"
	MessageRoleDeveloper MessageRole = "developer"
)

type MessagePartType string

const (
	MessagePartTypeText       MessagePartType = "text"
	MessagePartTypeReasoning  MessagePartType = "reasoning"
	MessagePartTypeImage      MessagePartType = "image"
	MessagePartTypeFile       MessagePartType = "file"
	MessagePartTypeToolCall   MessagePartType = "tool-call"
	MessagePartTypeToolResult MessagePartType = "tool-result"
)

type ReasoningFormat string

const (
	ReasoningFormatUnknown         ReasoningFormat = ""
	ReasoningFormatAnthropic       ReasoningFormat = "anthropic-v1"
	ReasoningFormatOpenAIResponses ReasoningFormat = "openai-responses-v1"
	ReasoningFormatGoogle          ReasoningFormat = "google-v1"
	ReasoningFormatCopilot         ReasoningFormat = "copilot-v1"
	ReasoningFormatOpenAIChat      ReasoningFormat = "openai-chat-v1"
)

// MessagePart is a closed, JSON-stable persisted content block. SDK message
// parts are interface values; the Runtime never stores that open interface.
type MessagePart struct {
	Type MessagePartType `json:"type"`

	// Text / reasoning.
	Text string `json:"text,omitempty"`

	// Reasoning-only identity/provenance.
	ID     string          `json:"id,omitempty"`
	Format ReasoningFormat `json:"format,omitempty"`
	Model  string          `json:"model,omitempty"`

	// Image / file.
	Image     string `json:"image,omitempty"`
	Data      string `json:"data,omitempty"`
	MediaType string `json:"mediaType,omitempty"`
	Filename  string `json:"filename,omitempty"`

	// Tool call / result.
	ToolCallID string          `json:"toolCallId,omitempty"`
	ToolName   string          `json:"toolName,omitempty"`
	Input      json.RawMessage `json:"input,omitempty"`
	Result     json.RawMessage `json:"result,omitempty"`
	IsError    bool            `json:"isError,omitempty"`

	CacheControl     *CacheControl    `json:"cacheControl,omitempty"`
	ProviderMetadata ProviderMetadata `json:"providerMetadata,omitempty"`
}

type Message struct {
	Role    MessageRole   `json:"role"`
	Content []MessagePart `json:"content"`
	Usage   *Usage        `json:"usage,omitempty"`
}

type ResponseFormatType string

const (
	ResponseFormatText       ResponseFormatType = "text"
	ResponseFormatJSONObject ResponseFormatType = "json_object"
	ResponseFormatJSONSchema ResponseFormatType = "json_schema"
)

type ResponseFormat struct {
	Type       ResponseFormatType `json:"type"`
	JSONSchema json.RawMessage    `json:"jsonSchema,omitempty"`
}

type ToolChoiceMode string

const (
	ToolChoiceAuto     ToolChoiceMode = "auto"
	ToolChoiceNone     ToolChoiceMode = "none"
	ToolChoiceRequired ToolChoiceMode = "required"
	ToolChoiceTool     ToolChoiceMode = "tool"
)

type ToolChoice struct {
	Mode ToolChoiceMode `json:"mode,omitempty"`
	Tool string         `json:"tool,omitempty"`
}

type ToolDefinition struct {
	Name         string          `json:"name"`
	Description  string          `json:"description,omitempty"`
	Parameters   json.RawMessage `json:"parameters"`
	CacheControl *CacheControl   `json:"cacheControl,omitempty"`
}

// ModelRequest is the complete persisted input of one model call. It is the
// agent-owned mirror of sdk.Request with no open SDK interfaces or any fields.
type ModelRequest struct {
	Model    string    `json:"model"`
	System   string    `json:"system,omitempty"`
	Messages []Message `json:"messages,omitempty"`

	Tools      []ToolDefinition `json:"tools,omitempty"`
	ToolChoice ToolChoice       `json:"toolChoice,omitzero"`

	ResponseFormat *ResponseFormat `json:"responseFormat,omitempty"`

	Temperature      *float64 `json:"temperature,omitempty"`
	TopP             *float64 `json:"topP,omitempty"`
	MaxTokens        *int     `json:"maxTokens,omitempty"`
	StopSequences    []string `json:"stopSequences,omitempty"`
	FrequencyPenalty *float64 `json:"frequencyPenalty,omitempty"`
	PresencePenalty  *float64 `json:"presencePenalty,omitempty"`
	Seed             *int     `json:"seed,omitempty"`
	ReasoningEffort  *string  `json:"reasoningEffort,omitempty"`
	ReasoningSummary *string  `json:"reasoningSummary,omitempty"`
	PromptCacheKey   *string  `json:"promptCacheKey,omitempty"`

	ProviderOptions map[string]json.RawMessage `json:"providerOptions,omitempty"`
}

type FinishReason string

const (
	FinishReasonStop          FinishReason = "stop"
	FinishReasonLength        FinishReason = "length"
	FinishReasonContentFilter FinishReason = "content-filter"
	FinishReasonToolCalls     FinishReason = "tool-calls"
	FinishReasonError         FinishReason = "error"
	FinishReasonOther         FinishReason = "other"
	FinishReasonUnknown       FinishReason = "unknown"
)

type InputTokenDetail struct {
	NoCacheTokens      int `json:"noCacheTokens"`
	CacheReadTokens    int `json:"cacheReadTokens"`
	CacheWriteTokens   int `json:"cacheWriteTokens"`
	CacheWrite5mTokens int `json:"cacheWrite5mTokens,omitempty"`
	CacheWrite1hTokens int `json:"cacheWrite1hTokens,omitempty"`
}

type OutputTokenDetail struct {
	TextTokens      int `json:"textTokens"`
	ReasoningTokens int `json:"reasoningTokens"`
}

type Usage struct {
	InputTokens        int               `json:"inputTokens"`
	OutputTokens       int               `json:"outputTokens"`
	TotalTokens        int               `json:"totalTokens"`
	ReasoningTokens    int               `json:"reasoningTokens,omitempty"`
	CachedInputTokens  int               `json:"cachedInputTokens,omitempty"`
	InputTokenDetails  InputTokenDetail  `json:"inputTokenDetails,omitempty"`
	OutputTokenDetails OutputTokenDetail `json:"outputTokenDetails,omitempty"`
}

func (u Usage) Add(other Usage) Usage {
	u.InputTokens += other.InputTokens
	u.OutputTokens += other.OutputTokens
	u.TotalTokens += other.TotalTokens
	u.ReasoningTokens += other.ReasoningTokens
	u.CachedInputTokens += other.CachedInputTokens
	u.InputTokenDetails.NoCacheTokens += other.InputTokenDetails.NoCacheTokens
	u.InputTokenDetails.CacheReadTokens += other.InputTokenDetails.CacheReadTokens
	u.InputTokenDetails.CacheWriteTokens += other.InputTokenDetails.CacheWriteTokens
	u.InputTokenDetails.CacheWrite5mTokens += other.InputTokenDetails.CacheWrite5mTokens
	u.InputTokenDetails.CacheWrite1hTokens += other.InputTokenDetails.CacheWrite1hTokens
	u.OutputTokenDetails.TextTokens += other.OutputTokenDetails.TextTokens
	u.OutputTokenDetails.ReasoningTokens += other.OutputTokenDetails.ReasoningTokens
	return u
}

type ReasoningPart struct {
	ID               string           `json:"id,omitempty"`
	Text             string           `json:"text"`
	Format           ReasoningFormat  `json:"format,omitempty"`
	Model            string           `json:"model,omitempty"`
	ProviderMetadata ProviderMetadata `json:"providerMetadata,omitempty"`
}

type Source struct {
	SourceType       string           `json:"sourceType"`
	ID               string           `json:"id"`
	URL              string           `json:"url"`
	Title            string           `json:"title,omitempty"`
	ProviderMetadata ProviderMetadata `json:"providerMetadata,omitempty"`
}

type GeneratedFile struct {
	Data      string `json:"data"`
	MediaType string `json:"mediaType"`
}

type ModelToolCall struct {
	ToolCallID       string           `json:"toolCallId"`
	ToolName         string           `json:"toolName"`
	Input            json.RawMessage  `json:"input"`
	ProviderMetadata ProviderMetadata `json:"providerMetadata,omitempty"`
}

type ResponseMetadata struct {
	ID        string            `json:"id,omitempty"`
	ModelID   string            `json:"modelId,omitempty"`
	Timestamp string            `json:"timestamp,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"`
}

// ModelResult is the persisted output of one model call. It mirrors
// sdk.ModelResult as agent-owned JSON-stable value types.
type ModelResult struct {
	Text                 string           `json:"text"`
	Reasoning            string           `json:"reasoning,omitempty"`
	ReasoningParts       []ReasoningPart  `json:"reasoningParts,omitempty"`
	TextProviderMetadata ProviderMetadata `json:"textProviderMetadata,omitempty"`

	FinishReason    FinishReason `json:"finishReason"`
	RawFinishReason string       `json:"rawFinishReason,omitempty"`
	Usage           Usage        `json:"usage"`

	Sources   []Source        `json:"sources,omitempty"`
	Files     []GeneratedFile `json:"files,omitempty"`
	ToolCalls []ModelToolCall `json:"toolCalls,omitempty"`

	Response *ResponseMetadata `json:"response,omitempty"`
}

func freezeRawJSON(raw json.RawMessage) (json.RawMessage, error) {
	if raw == nil {
		return nil, nil
	}
	canonical, err := canonicalJSON(raw)
	if err != nil {
		return nil, err
	}
	return cloneRaw(canonical), nil
}

func freezeJSONValue(v any) (json.RawMessage, error) {
	if v == nil {
		return json.RawMessage("null"), nil
	}
	if raw, ok := v.(json.RawMessage); ok {
		return freezeRawJSON(raw)
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return freezeRawJSON(raw)
}

func decodeJSONValue(raw json.RawMessage) (any, error) {
	if raw == nil {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var out any
	if err := dec.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func FreezeProviderMetadata(meta map[string]any) (ProviderMetadata, error) {
	if meta == nil {
		return nil, nil
	}
	out := make(ProviderMetadata, len(meta))
	for k, v := range meta {
		raw, err := freezeJSONValue(v)
		if err != nil {
			return nil, fmt.Errorf("provider metadata %q: %w", k, err)
		}
		out[k] = raw
	}
	return out, nil
}

func (m ProviderMetadata) SDK() (map[string]any, error) {
	if m == nil {
		return nil, nil
	}
	out := make(map[string]any, len(m))
	for k, raw := range m {
		v, err := decodeJSONValue(raw)
		if err != nil {
			return nil, fmt.Errorf("provider metadata %q: %w", k, err)
		}
		out[k] = v
	}
	return out, nil
}

func FreezeCacheControl(c *sdk.CacheControl) *CacheControl {
	if c == nil {
		return nil
	}
	return &CacheControl{Type: c.Type, TTL: c.TTL}
}

func (c *CacheControl) SDK() *sdk.CacheControl {
	if c == nil {
		return nil
	}
	return &sdk.CacheControl{Type: c.Type, TTL: c.TTL}
}

func FreezeToolDefinition(def sdk.ToolDefinition) (ToolDefinition, error) {
	params, err := freezeRawJSON(def.Parameters)
	if err != nil {
		return ToolDefinition{}, fmt.Errorf("tool definition parameters: %w", err)
	}
	return ToolDefinition{
		Name:         def.Name,
		Description:  def.Description,
		Parameters:   params,
		CacheControl: FreezeCacheControl(def.CacheControl),
	}, nil
}

func (d ToolDefinition) SDK() sdk.ToolDefinition {
	return sdk.ToolDefinition{
		Name:         d.Name,
		Description:  d.Description,
		Parameters:   cloneRaw(d.Parameters),
		CacheControl: d.CacheControl.SDK(),
	}
}

func FreezeResponseFormat(f *sdk.ResponseFormat) (*ResponseFormat, error) {
	if f == nil {
		return nil, nil
	}
	out := &ResponseFormat{Type: ResponseFormatType(f.Type)}
	if f.JSONSchema != nil {
		raw, err := json.Marshal(f.JSONSchema)
		if err != nil {
			return nil, err
		}
		out.JSONSchema, err = freezeRawJSON(raw)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (f *ResponseFormat) SDK() (*sdk.ResponseFormat, error) {
	if f == nil {
		return nil, nil
	}
	out := &sdk.ResponseFormat{Type: sdk.ResponseFormatType(f.Type)}
	if f.JSONSchema != nil {
		var schema jsonschema.Schema
		if err := json.Unmarshal(f.JSONSchema, &schema); err != nil {
			return nil, err
		}
		out.JSONSchema = &schema
	}
	return out, nil
}

func FreezeToolChoice(choice sdk.ToolChoice) ToolChoice {
	return ToolChoice{Mode: ToolChoiceMode(choice.Mode), Tool: choice.Tool}
}

func (c ToolChoice) SDK() sdk.ToolChoice {
	return sdk.ToolChoice{Mode: sdk.ToolChoiceMode(c.Mode), Tool: c.Tool}
}

func FreezeMessagePart(p sdk.MessagePart) (MessagePart, error) {
	switch part := p.(type) {
	case sdk.TextPart:
		meta, err := FreezeProviderMetadata(part.ProviderMetadata)
		if err != nil {
			return MessagePart{}, err
		}
		return MessagePart{Type: MessagePartTypeText, Text: part.Text, CacheControl: FreezeCacheControl(part.CacheControl), ProviderMetadata: meta}, nil
	case *sdk.TextPart:
		if part == nil {
			return MessagePart{}, fmt.Errorf("nil *sdk.TextPart")
		}
		return FreezeMessagePart(*part)
	case sdk.ReasoningPart:
		meta, err := FreezeProviderMetadata(part.ProviderMetadata)
		if err != nil {
			return MessagePart{}, err
		}
		return MessagePart{Type: MessagePartTypeReasoning, ID: part.ID, Text: part.Text, Format: ReasoningFormat(part.Format), Model: part.Model, ProviderMetadata: meta}, nil
	case *sdk.ReasoningPart:
		if part == nil {
			return MessagePart{}, fmt.Errorf("nil *sdk.ReasoningPart")
		}
		return FreezeMessagePart(*part)
	case sdk.ImagePart:
		return MessagePart{Type: MessagePartTypeImage, Image: part.Image, MediaType: part.MediaType, CacheControl: FreezeCacheControl(part.CacheControl)}, nil
	case *sdk.ImagePart:
		if part == nil {
			return MessagePart{}, fmt.Errorf("nil *sdk.ImagePart")
		}
		return FreezeMessagePart(*part)
	case sdk.FilePart:
		return MessagePart{Type: MessagePartTypeFile, Data: part.Data, MediaType: part.MediaType, Filename: part.Filename, CacheControl: FreezeCacheControl(part.CacheControl)}, nil
	case *sdk.FilePart:
		if part == nil {
			return MessagePart{}, fmt.Errorf("nil *sdk.FilePart")
		}
		return FreezeMessagePart(*part)
	case sdk.ToolCallPart:
		input, err := freezeToolCallInput(part.Input)
		if err != nil {
			return MessagePart{}, err
		}
		meta, err := FreezeProviderMetadata(part.ProviderMetadata)
		if err != nil {
			return MessagePart{}, err
		}
		return MessagePart{Type: MessagePartTypeToolCall, ToolCallID: part.ToolCallID, ToolName: part.ToolName, Input: input, CacheControl: FreezeCacheControl(part.CacheControl), ProviderMetadata: meta}, nil
	case *sdk.ToolCallPart:
		if part == nil {
			return MessagePart{}, fmt.Errorf("nil *sdk.ToolCallPart")
		}
		return FreezeMessagePart(*part)
	case sdk.ToolResultPart:
		result, err := freezeJSONValue(part.Result)
		if err != nil {
			return MessagePart{}, err
		}
		return MessagePart{Type: MessagePartTypeToolResult, ToolCallID: part.ToolCallID, ToolName: part.ToolName, Result: result, IsError: part.IsError, CacheControl: FreezeCacheControl(part.CacheControl)}, nil
	case *sdk.ToolResultPart:
		if part == nil {
			return MessagePart{}, fmt.Errorf("nil *sdk.ToolResultPart")
		}
		return FreezeMessagePart(*part)
	default:
		return MessagePart{}, fmt.Errorf("unsupported sdk.MessagePart %T", p)
	}
}

func (p MessagePart) SDK() (sdk.MessagePart, error) {
	switch p.Type {
	case MessagePartTypeText:
		meta, err := p.ProviderMetadata.SDK()
		if err != nil {
			return nil, err
		}
		return sdk.TextPart{Text: p.Text, CacheControl: p.CacheControl.SDK(), ProviderMetadata: meta}, nil
	case MessagePartTypeReasoning:
		meta, err := p.ProviderMetadata.SDK()
		if err != nil {
			return nil, err
		}
		return sdk.ReasoningPart{ID: p.ID, Text: p.Text, Format: sdk.ReasoningFormat(p.Format), Model: p.Model, ProviderMetadata: meta}, nil
	case MessagePartTypeImage:
		return sdk.ImagePart{Image: p.Image, MediaType: p.MediaType, CacheControl: p.CacheControl.SDK()}, nil
	case MessagePartTypeFile:
		return sdk.FilePart{Data: p.Data, MediaType: p.MediaType, Filename: p.Filename, CacheControl: p.CacheControl.SDK()}, nil
	case MessagePartTypeToolCall:
		meta, err := p.ProviderMetadata.SDK()
		if err != nil {
			return nil, err
		}
		return sdk.ToolCallPart{ToolCallID: p.ToolCallID, ToolName: p.ToolName, Input: cloneRaw(p.Input), CacheControl: p.CacheControl.SDK(), ProviderMetadata: meta}, nil
	case MessagePartTypeToolResult:
		return sdk.ToolResultPart{ToolCallID: p.ToolCallID, ToolName: p.ToolName, Result: cloneRaw(p.Result), IsError: p.IsError, CacheControl: p.CacheControl.SDK()}, nil
	default:
		return nil, fmt.Errorf("unknown message part type %q", p.Type)
	}
}

func FreezeMessage(m sdk.Message) (Message, error) {
	parts := make([]MessagePart, len(m.Content))
	for i, p := range m.Content {
		frozen, err := FreezeMessagePart(p)
		if err != nil {
			return Message{}, fmt.Errorf("message part %d: %w", i, err)
		}
		parts[i] = frozen
	}
	var usage *Usage
	if m.Usage != nil {
		u := UsageFromSDK(*m.Usage)
		usage = &u
	}
	return Message{Role: MessageRole(m.Role), Content: parts, Usage: usage}, nil
}

func (m Message) SDK() (sdk.Message, error) {
	parts := make([]sdk.MessagePart, len(m.Content))
	for i, p := range m.Content {
		part, err := p.SDK()
		if err != nil {
			return sdk.Message{}, fmt.Errorf("message part %d: %w", i, err)
		}
		parts[i] = part
	}
	var usage *sdk.Usage
	if m.Usage != nil {
		u := m.Usage.SDK()
		usage = &u
	}
	return sdk.Message{Role: sdk.MessageRole(m.Role), Content: parts, Usage: usage}, nil
}

func FreezeModelRequest(req sdk.Request) (ModelRequest, error) {
	messages := make([]Message, len(req.Messages))
	for i, m := range req.Messages {
		msg, err := FreezeMessage(m)
		if err != nil {
			return ModelRequest{}, fmt.Errorf("message %d: %w", i, err)
		}
		messages[i] = msg
	}
	tools := make([]ToolDefinition, len(req.Tools))
	for i, t := range req.Tools {
		tool, err := FreezeToolDefinition(t)
		if err != nil {
			return ModelRequest{}, fmt.Errorf("tool %d: %w", i, err)
		}
		tools[i] = tool
	}
	format, err := FreezeResponseFormat(req.ResponseFormat)
	if err != nil {
		return ModelRequest{}, fmt.Errorf("response format: %w", err)
	}
	options := make(map[string]json.RawMessage, len(req.ProviderOptions))
	if req.ProviderOptions != nil {
		for k, v := range req.ProviderOptions {
			frozen, err := freezeRawJSON(v)
			if err != nil {
				return ModelRequest{}, fmt.Errorf("provider option %q: %w", k, err)
			}
			options[k] = frozen
		}
	} else {
		options = nil
	}
	return ModelRequest{
		Model:            req.Model,
		System:           req.System,
		Messages:         messages,
		Tools:            tools,
		ToolChoice:       FreezeToolChoice(req.ToolChoice),
		ResponseFormat:   format,
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
		ProviderOptions:  options,
	}, nil
}

func (r ModelRequest) SDK() (sdk.Request, error) {
	messages := make([]sdk.Message, len(r.Messages))
	for i, m := range r.Messages {
		msg, err := m.SDK()
		if err != nil {
			return sdk.Request{}, fmt.Errorf("message %d: %w", i, err)
		}
		messages[i] = msg
	}
	tools := make([]sdk.ToolDefinition, len(r.Tools))
	for i, t := range r.Tools {
		tools[i] = t.SDK()
	}
	format, err := r.ResponseFormat.SDK()
	if err != nil {
		return sdk.Request{}, fmt.Errorf("response format: %w", err)
	}
	options := make(map[string]json.RawMessage, len(r.ProviderOptions))
	if r.ProviderOptions != nil {
		for k, v := range r.ProviderOptions {
			options[k] = cloneRaw(v)
		}
	} else {
		options = nil
	}
	return sdk.Request{
		Model:            r.Model,
		System:           r.System,
		Messages:         messages,
		Tools:            tools,
		ToolChoice:       r.ToolChoice.SDK(),
		ResponseFormat:   format,
		Temperature:      clonePtr(r.Temperature),
		TopP:             clonePtr(r.TopP),
		MaxTokens:        clonePtr(r.MaxTokens),
		StopSequences:    append([]string(nil), r.StopSequences...),
		FrequencyPenalty: clonePtr(r.FrequencyPenalty),
		PresencePenalty:  clonePtr(r.PresencePenalty),
		Seed:             clonePtr(r.Seed),
		ReasoningEffort:  clonePtr(r.ReasoningEffort),
		ReasoningSummary: clonePtr(r.ReasoningSummary),
		PromptCacheKey:   clonePtr(r.PromptCacheKey),
		ProviderOptions:  options,
	}, nil
}

func UsageFromSDK(u sdk.Usage) Usage {
	return Usage{
		InputTokens:       u.InputTokens,
		OutputTokens:      u.OutputTokens,
		TotalTokens:       u.TotalTokens,
		ReasoningTokens:   u.ReasoningTokens,
		CachedInputTokens: u.CachedInputTokens,
		InputTokenDetails: InputTokenDetail{
			NoCacheTokens:      u.InputTokenDetails.NoCacheTokens,
			CacheReadTokens:    u.InputTokenDetails.CacheReadTokens,
			CacheWriteTokens:   u.InputTokenDetails.CacheWriteTokens,
			CacheWrite5mTokens: u.InputTokenDetails.CacheWrite5mTokens,
			CacheWrite1hTokens: u.InputTokenDetails.CacheWrite1hTokens,
		},
		OutputTokenDetails: OutputTokenDetail{
			TextTokens:      u.OutputTokenDetails.TextTokens,
			ReasoningTokens: u.OutputTokenDetails.ReasoningTokens,
		},
	}
}

func (u Usage) SDK() sdk.Usage {
	return sdk.Usage{
		InputTokens:       u.InputTokens,
		OutputTokens:      u.OutputTokens,
		TotalTokens:       u.TotalTokens,
		ReasoningTokens:   u.ReasoningTokens,
		CachedInputTokens: u.CachedInputTokens,
		InputTokenDetails: sdk.InputTokenDetail{
			NoCacheTokens:      u.InputTokenDetails.NoCacheTokens,
			CacheReadTokens:    u.InputTokenDetails.CacheReadTokens,
			CacheWriteTokens:   u.InputTokenDetails.CacheWriteTokens,
			CacheWrite5mTokens: u.InputTokenDetails.CacheWrite5mTokens,
			CacheWrite1hTokens: u.InputTokenDetails.CacheWrite1hTokens,
		},
		OutputTokenDetails: sdk.OutputTokenDetail{
			TextTokens:      u.OutputTokenDetails.TextTokens,
			ReasoningTokens: u.OutputTokenDetails.ReasoningTokens,
		},
	}
}

func FreezeReasoningPart(p sdk.ReasoningPart) (ReasoningPart, error) {
	meta, err := FreezeProviderMetadata(p.ProviderMetadata)
	if err != nil {
		return ReasoningPart{}, err
	}
	return ReasoningPart{ID: p.ID, Text: p.Text, Format: ReasoningFormat(p.Format), Model: p.Model, ProviderMetadata: meta}, nil
}

func (p ReasoningPart) SDK() (sdk.ReasoningPart, error) {
	meta, err := p.ProviderMetadata.SDK()
	if err != nil {
		return sdk.ReasoningPart{}, err
	}
	return sdk.ReasoningPart{ID: p.ID, Text: p.Text, Format: sdk.ReasoningFormat(p.Format), Model: p.Model, ProviderMetadata: meta}, nil
}

func FreezeSource(s sdk.Source) (Source, error) {
	meta, err := FreezeProviderMetadata(s.ProviderMetadata)
	if err != nil {
		return Source{}, err
	}
	return Source{SourceType: s.SourceType, ID: s.ID, URL: s.URL, Title: s.Title, ProviderMetadata: meta}, nil
}

func (s Source) SDK() (sdk.Source, error) {
	meta, err := s.ProviderMetadata.SDK()
	if err != nil {
		return sdk.Source{}, err
	}
	return sdk.Source{SourceType: s.SourceType, ID: s.ID, URL: s.URL, Title: s.Title, ProviderMetadata: meta}, nil
}

func FreezeGeneratedFile(f sdk.GeneratedFile) GeneratedFile {
	return GeneratedFile{Data: f.Data, MediaType: f.MediaType}
}

func (f GeneratedFile) SDK() sdk.GeneratedFile {
	return sdk.GeneratedFile{Data: f.Data, MediaType: f.MediaType}
}

func freezeToolCallInput(input any) (json.RawMessage, error) {
	args, err := canonicalToolArguments(input)
	if err == nil {
		return cloneRaw(args), nil
	}
	switch input.(type) {
	case string, json.RawMessage:
		return cloneRaw(rawToolArguments(input)), nil
	default:
		return nil, err
	}
}

func FreezeModelToolCall(c sdk.ToolCall) (ModelToolCall, error) {
	input, err := freezeToolCallInput(c.Input)
	if err != nil {
		return ModelToolCall{}, fmt.Errorf("tool call input: %w", err)
	}
	meta, err := FreezeProviderMetadata(c.ProviderMetadata)
	if err != nil {
		return ModelToolCall{}, err
	}
	return ModelToolCall{ToolCallID: c.ToolCallID, ToolName: c.ToolName, Input: input, ProviderMetadata: meta}, nil
}

func (c ModelToolCall) SDK() (sdk.ToolCall, error) {
	meta, err := c.ProviderMetadata.SDK()
	if err != nil {
		return sdk.ToolCall{}, err
	}
	return sdk.ToolCall{ToolCallID: c.ToolCallID, ToolName: c.ToolName, Input: cloneRaw(c.Input), ProviderMetadata: meta}, nil
}

func FreezeResponseMetadata(r *sdk.ResponseMetadata) *ResponseMetadata {
	if r == nil {
		return nil
	}
	out := &ResponseMetadata{ID: r.ID, ModelID: r.ModelID}
	if !r.Timestamp.IsZero() {
		out.Timestamp = r.Timestamp.UTC().Format(time.RFC3339Nano)
	}
	if r.Headers != nil {
		out.Headers = make(map[string]string, len(r.Headers))
		for k, v := range r.Headers {
			out.Headers[k] = v
		}
	}
	return out
}

func (r *ResponseMetadata) SDK() (sdk.ResponseMetadata, error) {
	if r == nil {
		return sdk.ResponseMetadata{}, nil
	}
	out := sdk.ResponseMetadata{ID: r.ID, ModelID: r.ModelID}
	if r.Timestamp != "" {
		t, err := time.Parse(time.RFC3339Nano, r.Timestamp)
		if err != nil {
			return sdk.ResponseMetadata{}, err
		}
		out.Timestamp = t
	}
	if r.Headers != nil {
		out.Headers = make(map[string]string, len(r.Headers))
		for k, v := range r.Headers {
			out.Headers[k] = v
		}
	}
	return out, nil
}

func FreezeModelResult(r sdk.ModelResult) (ModelResult, error) {
	reasoning := make([]ReasoningPart, len(r.ReasoningParts))
	for i, p := range r.ReasoningParts {
		part, err := FreezeReasoningPart(p)
		if err != nil {
			return ModelResult{}, fmt.Errorf("reasoning part %d: %w", i, err)
		}
		reasoning[i] = part
	}
	textMeta, err := FreezeProviderMetadata(r.TextProviderMetadata)
	if err != nil {
		return ModelResult{}, fmt.Errorf("text provider metadata: %w", err)
	}
	sources := make([]Source, len(r.Sources))
	for i, s := range r.Sources {
		source, err := FreezeSource(s)
		if err != nil {
			return ModelResult{}, fmt.Errorf("source %d: %w", i, err)
		}
		sources[i] = source
	}
	files := make([]GeneratedFile, len(r.Files))
	for i, f := range r.Files {
		files[i] = FreezeGeneratedFile(f)
	}
	calls := make([]ModelToolCall, len(r.ToolCalls))
	for i, c := range r.ToolCalls {
		call, err := FreezeModelToolCall(c)
		if err != nil {
			return ModelResult{}, fmt.Errorf("tool call %d: %w", i, err)
		}
		calls[i] = call
	}
	return ModelResult{
		Text:                 r.Text,
		Reasoning:            r.Reasoning,
		ReasoningParts:       reasoning,
		TextProviderMetadata: textMeta,
		FinishReason:         FinishReason(r.FinishReason),
		RawFinishReason:      r.RawFinishReason,
		Usage:                UsageFromSDK(r.Usage),
		Sources:              sources,
		Files:                files,
		ToolCalls:            calls,
		Response:             FreezeResponseMetadata(r.Response),
	}, nil
}

func (r ModelResult) SDK() (sdk.ModelResult, error) {
	reasoning := make([]sdk.ReasoningPart, len(r.ReasoningParts))
	for i, p := range r.ReasoningParts {
		part, err := p.SDK()
		if err != nil {
			return sdk.ModelResult{}, fmt.Errorf("reasoning part %d: %w", i, err)
		}
		reasoning[i] = part
	}
	textMeta, err := r.TextProviderMetadata.SDK()
	if err != nil {
		return sdk.ModelResult{}, fmt.Errorf("text provider metadata: %w", err)
	}
	sources := make([]sdk.Source, len(r.Sources))
	for i, s := range r.Sources {
		source, err := s.SDK()
		if err != nil {
			return sdk.ModelResult{}, fmt.Errorf("source %d: %w", i, err)
		}
		sources[i] = source
	}
	files := make([]sdk.GeneratedFile, len(r.Files))
	for i, f := range r.Files {
		files[i] = f.SDK()
	}
	calls := make([]sdk.ToolCall, len(r.ToolCalls))
	for i, c := range r.ToolCalls {
		call, err := c.SDK()
		if err != nil {
			return sdk.ModelResult{}, fmt.Errorf("tool call %d: %w", i, err)
		}
		calls[i] = call
	}
	response, err := r.Response.SDK()
	if err != nil {
		return sdk.ModelResult{}, fmt.Errorf("response metadata: %w", err)
	}
	var responsePtr *sdk.ResponseMetadata
	if r.Response != nil {
		responsePtr = &response
	}
	return sdk.ModelResult{
		Text:                 r.Text,
		Reasoning:            r.Reasoning,
		ReasoningParts:       reasoning,
		TextProviderMetadata: textMeta,
		FinishReason:         sdk.FinishReason(r.FinishReason),
		RawFinishReason:      r.RawFinishReason,
		Usage:                r.Usage.SDK(),
		Sources:              sources,
		Files:                files,
		ToolCalls:            calls,
		Response:             responsePtr,
	}, nil
}
