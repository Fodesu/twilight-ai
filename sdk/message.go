package sdk

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type MessageRole string

const (
	MessageRoleUser      MessageRole = "user"
	MessageRoleAssistant MessageRole = "assistant"
	MessageRoleSystem    MessageRole = "system"
	MessageRoleTool      MessageRole = "tool"
	MessageRoleDeveloper MessageRole = "developer"
)

var (
	ErrInvalidMessageRole            = errors.New("invalid message role")
	ErrInvalidInstructionContent     = errors.New("system and developer messages only support text content")
	ErrInstructionInsideToolExchange = errors.New("system and developer messages cannot interrupt a tool exchange")
)

// Valid reports whether the role is one of the SDK's supported semantic roles.
func (r MessageRole) Valid() bool {
	switch r {
	case MessageRoleUser, MessageRoleAssistant, MessageRoleSystem, MessageRoleTool, MessageRoleDeveloper:
		return true
	default:
		return false
	}
}

// MessageRoleCapabilities describes which instruction roles a provider can
// serialize without fallback. Providers that do not support a capability map
// the corresponding instruction to a user message at the wire boundary.
type MessageRoleCapabilities struct {
	Developer             bool
	MidConversationSystem bool
}

type MessagePartType string

const (
	MessagePartTypeText       MessagePartType = "text"
	MessagePartTypeReasoning  MessagePartType = "reasoning"
	MessagePartTypeImage      MessagePartType = "image"
	MessagePartTypeFile       MessagePartType = "file"
	MessagePartTypeToolCall   MessagePartType = "tool-call"
	MessagePartTypeToolResult MessagePartType = "tool-result"
)

type MessagePart interface {
	PartType() MessagePartType
}

// CacheControl specifies caching behaviour for a content block.
// Anthropic currently supports Type "ephemeral" with an optional TTL.
// Leave TTL empty for the default 5-minute cache; set TTL to "1h" for a
// 1-hour cache (billed at a higher rate).
type CacheControl struct {
	Type string `json:"type"`          // "ephemeral"
	TTL  string `json:"ttl,omitempty"` // "" (5 min, default) | "1h"
}

// --- Text ---

type TextPart struct {
	Text             string         `json:"text"`
	CacheControl     *CacheControl  `json:"cacheControl,omitempty"`
	ProviderMetadata map[string]any `json:"providerMetadata,omitempty"`
}

func (p TextPart) PartType() MessagePartType { return MessagePartTypeText }

// --- Reasoning ---

// ReasoningFormat identifies the wire dialect of a reasoning part.
//
// It describes what a part looks like and how to send it back, not who
// produced it: the same dialect reaches us from a direct endpoint, Bedrock,
// Vertex or any compatible gateway, and comparing provider names would treat
// those as foreign. An empty format means the dialect is unknown — such a part
// is never replayed, because no provider can verify a token it did not issue.
type ReasoningFormat string

const (
	ReasoningFormatUnknown ReasoningFormat = ""
	// ReasoningFormatAnthropic is the Anthropic Messages thinking dialect:
	// thinking blocks with a per-block signature, plus redacted_thinking.
	ReasoningFormatAnthropic ReasoningFormat = "anthropic-v1"
	// ReasoningFormatOpenAIResponses is the OpenAI Responses reasoning-item
	// dialect. It covers the Codex backend too: that is the same API behind a
	// different base URL, and it produces byte-identical reasoning items.
	ReasoningFormatOpenAIResponses ReasoningFormat = "openai-responses-v1"
	// ReasoningFormatGoogle is the Gemini generateContent dialect: a thought
	// signature bound to the exact part it arrived on.
	ReasoningFormatGoogle ReasoningFormat = "google-v1"
	// ReasoningFormatCopilot is the GitHub Copilot dialect, which wraps each
	// upstream model's own token in a single reasoning_opaque per response.
	ReasoningFormatCopilot ReasoningFormat = "copilot-v1"
	// ReasoningFormatOpenAIChat is the Chat Completions reasoning dialect used
	// by DeepSeek and MiniMax. It carries no opaque token: replaying it affects
	// answer quality, never request validity.
	ReasoningFormatOpenAIChat ReasoningFormat = "openai-chat-v1"
)

// ReasoningPart carries one block of reasoning produced by the model.
//
// A model may emit several blocks in a single assistant turn (Anthropic
// interleaves thinking with tool calls; OpenAI splits a reasoning item into
// several summary parts). Each block is its own ReasoningPart, kept in the
// order the provider emitted it: providers treat a block as an opaque,
// indivisible round-trip unit and reject a modified sequence, so blocks must
// never be merged, reordered, or dropped.
//
// Text may be empty while ProviderMetadata is not — Anthropic's
// redacted_thinking blocks carry only an encrypted blob, and Google may attach
// a thought signature to a part with no text. Such a part still has to be
// replayed, so emptiness is never a reason to skip it.
//
// The opaque token stays in ProviderMetadata under the provider's own key
// rather than becoming a field here: a signature means different things to
// each provider (Anthropic verifies one per block, OpenAI pairs encrypted
// content with an item id, Google signs a specific part), and promoting it
// would imply a shared meaning that does not exist.
type ReasoningPart struct {
	// ID is the provider's own identity for this block, such as an OpenAI
	// rs_… item id. Providers without a block identity leave it empty.
	ID string `json:"id,omitempty"`

	Text string `json:"text"`

	// Format is the wire dialect this block belongs to. Replay it only to a
	// provider that speaks the same dialect.
	Format ReasoningFormat `json:"format,omitempty"`

	ProviderMetadata map[string]any `json:"providerMetadata,omitempty"`
}

func (p ReasoningPart) PartType() MessagePartType { return MessagePartTypeReasoning }

// ReasoningText joins the parts' text for display. The result is a view, not a
// round-trip value: it drops the block boundaries and opaque tokens providers
// require, so never rebuild a request from it.
func ReasoningText(parts []ReasoningPart) string {
	var sb strings.Builder
	for i := range parts {
		sb.WriteString(parts[i].Text)
	}
	return sb.String()
}

// ReasoningMetadata builds a provider-namespaced metadata bag, skipping empty
// values. It returns nil when nothing is left, so callers can tell "no token"
// from "empty token".
func ReasoningMetadata(namespace string, kv map[string]string) map[string]any {
	inner := make(map[string]any, len(kv))
	for key, value := range kv {
		if value != "" {
			inner[key] = value
		}
	}
	if len(inner) == 0 {
		return nil
	}
	return map[string]any{namespace: inner}
}

// ReasoningMetadataString reads one string value out of a provider's namespace.
func ReasoningMetadataString(meta map[string]any, namespace, key string) string {
	if meta == nil {
		return ""
	}
	inner, ok := meta[namespace].(map[string]any)
	if !ok {
		return ""
	}
	value, _ := inner[key].(string)
	return value
}

// reasoningAccumulator folds a reasoning part stream into ordered parts.
//
// Providers delimit blocks with ReasoningStartPart/ReasoningEndPart and carry
// the block identity on every part, so deltas are matched to their block by ID.
// Text deltas concatenate; metadata replaces, because a provider sends the
// opaque token once as a whole at the end of a block (Anthropic's
// signature_delta is a complete signature, not an increment) and a later part
// carries the authoritative value.
type reasoningAccumulator struct {
	parts []ReasoningPart
	byID  map[string]int
}

// openBlock starts a block, or returns the existing one when a provider
// re-announces the same ID.
func (a *reasoningAccumulator) openBlock(id string, format ReasoningFormat, meta map[string]any) int {
	if idx, ok := a.index(id); ok {
		a.merge(idx, format, meta)
		return idx
	}
	a.parts = append(a.parts, ReasoningPart{ID: id, Format: format, ProviderMetadata: meta})
	idx := len(a.parts) - 1
	if id != "" {
		if a.byID == nil {
			a.byID = make(map[string]int)
		}
		a.byID[id] = idx
	}
	return idx
}

// index finds the block a part belongs to. An unidentified part joins the most
// recent block, which is what providers that omit block IDs imply.
func (a *reasoningAccumulator) index(id string) (int, bool) {
	if id != "" {
		idx, ok := a.byID[id]
		return idx, ok
	}
	if len(a.parts) == 0 {
		return 0, false
	}
	return len(a.parts) - 1, true
}

func (a *reasoningAccumulator) merge(idx int, format ReasoningFormat, meta map[string]any) {
	if format != ReasoningFormatUnknown {
		a.parts[idx].Format = format
	}
	if len(meta) == 0 {
		return
	}
	if a.parts[idx].ProviderMetadata == nil {
		a.parts[idx].ProviderMetadata = make(map[string]any, len(meta))
	}
	for key, value := range meta {
		a.parts[idx].ProviderMetadata[key] = value
	}
}

func (a *reasoningAccumulator) appendDelta(id, text string, format ReasoningFormat, meta map[string]any) {
	idx, ok := a.index(id)
	if !ok {
		idx = a.openBlock(id, format, nil)
	}
	a.parts[idx].Text += text
	a.merge(idx, format, meta)
}

// closeBlock attaches the block's final metadata, which is where providers
// deliver the opaque token.
func (a *reasoningAccumulator) closeBlock(id string, format ReasoningFormat, meta map[string]any) {
	idx, ok := a.index(id)
	if !ok {
		idx = a.openBlock(id, format, nil)
	}
	a.merge(idx, format, meta)
}

func (a *reasoningAccumulator) result() []ReasoningPart {
	return a.parts
}

// --- Image ---

type ImagePart struct {
	Image        string        `json:"image"`
	MediaType    string        `json:"mediaType,omitempty"`
	CacheControl *CacheControl `json:"cacheControl,omitempty"`
}

func (p ImagePart) PartType() MessagePartType { return MessagePartTypeImage }

// --- File ---

type FilePart struct {
	Data         string        `json:"data"`
	MediaType    string        `json:"mediaType,omitempty"`
	Filename     string        `json:"filename,omitempty"`
	CacheControl *CacheControl `json:"cacheControl,omitempty"`
}

func (p FilePart) PartType() MessagePartType { return MessagePartTypeFile }

// --- Tool Call (in assistant messages) ---

type ToolCallPart struct {
	ToolCallID       string         `json:"toolCallId"`
	ToolName         string         `json:"toolName"`
	Input            any            `json:"input"`
	CacheControl     *CacheControl  `json:"cacheControl,omitempty"`
	ProviderMetadata map[string]any `json:"providerMetadata,omitempty"`
}

func (p ToolCallPart) PartType() MessagePartType { return MessagePartTypeToolCall }

// --- Tool Result (in tool messages) ---

type ToolResultPart struct {
	ToolCallID   string        `json:"toolCallId"`
	ToolName     string        `json:"toolName"`
	Result       any           `json:"result"`
	IsError      bool          `json:"isError,omitempty"`
	CacheControl *CacheControl `json:"cacheControl,omitempty"`
}

func (p ToolResultPart) PartType() MessagePartType { return MessagePartTypeToolResult }

// --- Message ---

type Message struct {
	Role    MessageRole   `json:"role"`
	Content []MessagePart `json:"content"`
	Usage   *Usage        `json:"usage,omitempty"`
}

// --- Convenience constructors ---

// UserMessage creates a user message with one or more text parts.
func UserMessage(text string, extra ...MessagePart) Message {
	parts := make([]MessagePart, 0, 1+len(extra))
	parts = append(parts, TextPart{Text: text})
	parts = append(parts, extra...)
	return Message{Role: MessageRoleUser, Content: parts}
}

// SystemMessage creates a system message with a single text part.
func SystemMessage(text string) Message {
	return Message{Role: MessageRoleSystem, Content: []MessagePart{TextPart{Text: text}}}
}

// DeveloperMessage creates a developer message with a single text part.
func DeveloperMessage(text string) Message {
	return Message{Role: MessageRoleDeveloper, Content: []MessagePart{TextPart{Text: text}}}
}

// AssistantMessage creates an assistant message with a single text part.
func AssistantMessage(text string) Message {
	return Message{Role: MessageRoleAssistant, Content: []MessagePart{TextPart{Text: text}}}
}

// ToolMessage creates a tool-role message containing one or more ToolResultParts.
func ToolMessage(results ...ToolResultPart) Message {
	parts := make([]MessagePart, len(results))
	for i, r := range results {
		parts[i] = r
	}
	return Message{Role: MessageRoleTool, Content: parts}
}

// --- JSON ---

func (m Message) MarshalJSON() ([]byte, error) {
	if !m.Role.Valid() {
		return nil, fmt.Errorf("%w: %q", ErrInvalidMessageRole, m.Role)
	}

	// Single TextPart with no metadata and no cache control → emit content as a plain string.
	var content any
	if len(m.Content) == 1 {
		if tp, ok := m.Content[0].(TextPart); ok && len(tp.ProviderMetadata) == 0 && tp.CacheControl == nil {
			content = tp.Text
		}
	}
	if content == nil {
		parts := make([]json.RawMessage, 0, len(m.Content))
		for _, p := range m.Content {
			raw, err := marshalPart(p)
			if err != nil {
				return nil, err
			}
			parts = append(parts, raw)
		}
		content = parts
	}
	if m.Usage != nil {
		return json.Marshal(struct {
			Role    MessageRole `json:"role"`
			Content any         `json:"content"`
			Usage   *Usage      `json:"usage,omitempty"`
		}{Role: m.Role, Content: content, Usage: m.Usage})
	}
	return json.Marshal(struct {
		Role    MessageRole `json:"role"`
		Content any         `json:"content"`
	}{Role: m.Role, Content: content})
}

func (m *Message) UnmarshalJSON(data []byte) error {
	var raw struct {
		Role    MessageRole     `json:"role"`
		Content json.RawMessage `json:"content"`
		Usage   *Usage          `json:"usage,omitempty"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if !raw.Role.Valid() {
		return fmt.Errorf("%w: %q", ErrInvalidMessageRole, raw.Role)
	}
	m.Role = raw.Role
	m.Usage = raw.Usage

	// content can be a string or an array of parts.
	if len(raw.Content) > 0 && raw.Content[0] == '"' {
		var s string
		if err := json.Unmarshal(raw.Content, &s); err != nil {
			return fmt.Errorf("unmarshal string content: %w", err)
		}
		m.Content = []MessagePart{TextPart{Text: s}}
		return nil
	}

	var parts []json.RawMessage
	if err := json.Unmarshal(raw.Content, &parts); err != nil {
		return fmt.Errorf("unmarshal content array: %w", err)
	}
	m.Content = make([]MessagePart, 0, len(parts))
	for _, r := range parts {
		p, err := unmarshalPart(r)
		if err != nil {
			return err
		}
		m.Content = append(m.Content, p)
	}
	return nil
}

func marshalPart(p MessagePart) (json.RawMessage, error) {
	type typed struct {
		Type MessagePartType `json:"type"`
	}
	base, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	typeJSON, _ := json.Marshal(typed{Type: p.PartType()})

	// merge {"type":"..."} into the part's JSON
	merged := make(map[string]json.RawMessage)
	if err := json.Unmarshal(typeJSON, &merged); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(base, &merged); err != nil {
		return nil, err
	}
	return json.Marshal(merged)
}

func unmarshalPart(data json.RawMessage) (MessagePart, error) {
	var probe struct {
		Type MessagePartType `json:"type"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return nil, fmt.Errorf("unmarshal message part type: %w", err)
	}
	switch probe.Type {
	case MessagePartTypeText:
		var p TextPart
		if err := json.Unmarshal(data, &p); err != nil {
			return nil, err
		}
		return p, nil
	case MessagePartTypeReasoning:
		var p ReasoningPart
		if err := json.Unmarshal(data, &p); err != nil {
			return nil, err
		}
		return p, nil
	case MessagePartTypeImage:
		var p ImagePart
		if err := json.Unmarshal(data, &p); err != nil {
			return nil, err
		}
		return p, nil
	case MessagePartTypeFile:
		var p FilePart
		if err := json.Unmarshal(data, &p); err != nil {
			return nil, err
		}
		return p, nil
	case MessagePartTypeToolCall:
		var p ToolCallPart
		if err := json.Unmarshal(data, &p); err != nil {
			return nil, err
		}
		return p, nil
	case MessagePartTypeToolResult:
		var p ToolResultPart
		if err := json.Unmarshal(data, &p); err != nil {
			return nil, err
		}
		return p, nil
	default:
		return nil, fmt.Errorf("unknown message part type: %q", probe.Type)
	}
}
