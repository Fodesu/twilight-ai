package sdk

import "encoding/json"

// Request is the complete, frozen input of one model call (spec §2.1).
//
// It is pure data: no provider client, no interface values, no callbacks.
// The model is a provider-scoped string ID; provider binding happens when a
// ModelCatalog resolves a ModelInvoker. Everything here participates in
// DigestRequest with no exclusions, so any field change produces a different
// request identity.
type Request struct {
	// Model is the provider-scoped model ID (e.g. "claude-sonnet-5").
	Model string `json:"model"`
	// System is the stable root instruction placed before the conversation.
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

	// ProviderOptions carries provider-specific extensions keyed by provider
	// namespace. Values must be JSON values; they participate in the digest.
	ProviderOptions map[string]json.RawMessage `json:"providerOptions,omitempty"`
}

// ToolDefinition is the provider-neutral, frozen description of one tool.
// Parameters is a resolved JSON Schema document: schema inference from Go
// structs happens before freezing, never after.
type ToolDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters"`
	// CacheControl participates in the digest like every other field.
	CacheControl *CacheControl `json:"cacheControl,omitempty"`
}

type ToolChoiceMode string

const (
	ToolChoiceAuto     ToolChoiceMode = "auto"
	ToolChoiceNone     ToolChoiceMode = "none"
	ToolChoiceRequired ToolChoiceMode = "required"
	ToolChoiceTool     ToolChoiceMode = "tool"
)

// ToolChoice is the closed replacement for the legacy `any` field.
type ToolChoice struct {
	Mode ToolChoiceMode `json:"mode,omitempty"`
	// Tool names the target tool when Mode == ToolChoiceTool.
	Tool string `json:"tool,omitempty"`
}

// BlobRef is a stable, content-addressed reference to binary content inside a
// frozen request. Byte resolution is the responsibility of whoever assembles
// the ModelInvoker; unstable references (expiring URLs) must not enter a
// frozen request.
type BlobRef struct {
	// Digest is "sha256:<64 lowercase hex>" over the raw bytes.
	Digest    string `json:"digest"`
	MediaType string `json:"mediaType"`
	ByteSize  int64  `json:"byteSize"`
}
