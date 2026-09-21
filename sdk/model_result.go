package sdk

// ModelResult is one complete model response, carrying what a single call
// produced: no auto tool loop, approval, or multi-step accumulation. Steps and
// messages live in the run loop or application.
type ModelResult struct {
	Text string `json:"text"`
	// Reasoning is the parts' text joined for display. Rebuild requests from
	// ReasoningParts, which keeps the per-block opaque tokens.
	Reasoning      string          `json:"reasoning,omitempty"`
	ReasoningParts []ReasoningPart `json:"reasoningParts,omitempty"`
	// TextProviderMetadata carries an opaque token bound to the answer text
	// (e.g. a Google thought signature on a no-tool-call response). The
	// caller-side result type drops this field when it serializes; ModelResult
	// keeps it, because it is persisted inside AgentEvents and must round-trip.
	TextProviderMetadata ProviderMetadata `json:"textProviderMetadata,omitempty"`

	FinishReason    FinishReason `json:"finishReason"`
	RawFinishReason string       `json:"rawFinishReason,omitempty"`
	Usage           Usage        `json:"usage"`

	Sources   []Source        `json:"sources,omitempty"`
	Files     []GeneratedFile `json:"files,omitempty"`
	ToolCalls []ToolCall      `json:"toolCalls,omitempty"`

	// Response is always present; fields the wire did not carry stay zero and
	// the object is omitted from JSON when every field is zero.
	Response ResponseMetadata `json:"response,omitzero"`
}
