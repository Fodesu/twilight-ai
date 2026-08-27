package sdk

// ModelResult is one complete model response (spec §2.1): the single-call
// fields of the legacy GenerateResult with no auto tool loop, approval, or
// multi-step accumulation. Multi-step steps/messages live in the agent or
// the application, never here.
type ModelResult struct {
	Text string `json:"text"`
	// Reasoning is the parts' text joined for display. Rebuild requests from
	// ReasoningParts, which keeps the per-block opaque tokens.
	Reasoning      string          `json:"reasoning,omitempty"`
	ReasoningParts []ReasoningPart `json:"reasoningParts,omitempty"`
	// TextProviderMetadata carries an opaque token bound to the answer text
	// (e.g. a Google thought signature on a no-tool-call response). Unlike the
	// legacy GenerateResult it serializes: ModelResult is persisted inside
	// AgentEvents and must round-trip.
	TextProviderMetadata map[string]any `json:"textProviderMetadata,omitempty"`

	FinishReason    FinishReason `json:"finishReason"`
	RawFinishReason string       `json:"rawFinishReason,omitempty"`
	Usage           Usage        `json:"usage"`

	Sources   []Source        `json:"sources,omitempty"`
	Files     []GeneratedFile `json:"files,omitempty"`
	ToolCalls []ToolCall      `json:"toolCalls,omitempty"`

	// Response is pointer-typed so an absent value actually omits: a
	// struct-typed field with omitempty never omits, which would freeze a
	// zero timestamp and provider wall-clock headers into every canonical
	// fact digest.
	Response *ResponseMetadata `json:"response,omitempty"`
}
