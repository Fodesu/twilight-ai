package sdk

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Request is the complete, frozen input of one model call.
//
// It is pure data at the top level: no provider client, no callbacks. The
// model is a provider-scoped string ID; provider binding happens when a
// ModelCatalog resolves a ModelInvoker. Messages, ProviderOptions and
// ResponseFormat.JSONSchema still carry open JSON shapes, so the agent
// runtime freezes a Request into its own canonical run.ModelRequest before
// digesting or persisting it; the request digest is defined there.
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
	// namespace, which is Provider.Name(). Each value is an object whose
	// members are request-body members of that provider's wire request:
	// ApplyProviderOptions merges them in, so a caller can reach a wire feature
	// the SDK does not model, or override one it does. The values stay open
	// JSON here, so the agent runtime freezes the whole request into its own
	// canonical run.ModelRequest before digesting it; the digest covers these
	// values there, not here.
	ProviderOptions map[string]json.RawMessage `json:"providerOptions,omitempty"`
}

// ApplyProviderOptions merges the caller's options for the provider namespace
// into a provider's own wire request. wire must be the request the provider just
// built, and a provider calls this last, so an option can override a member the
// SDK set.
//
// The SDK fixes where the options for a namespace live and how they are applied;
// what they mean stays the provider's property, because wire is the provider's
// own type. Unknown members are an error rather than a silent no-op: an option
// that is quietly dropped is indistinguishable from one that was never set,
// which is the failure this seam exists to make impossible.
func ApplyProviderOptions(namespace string, options map[string]json.RawMessage, wire any) error {
	if len(options) == 0 {
		return nil
	}
	raw, ok := options[namespace]
	if !ok || len(raw) == 0 {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(wire); err != nil {
		return fmt.Errorf("provider options for %q: %w", namespace, err)
	}
	return nil
}

// ToolDefinition is the provider-neutral, frozen description of one tool.
// Parameters is a resolved JSON Schema document: schema inference from Go
// structs happens before freezing, never after.
type ToolDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters"`
	// CacheControl is carried into the frozen request, whose digest covers it
	// like every other field.
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
