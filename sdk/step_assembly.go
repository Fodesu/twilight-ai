package sdk

// BuildStepMessages assembles the messages produced by one agent step: an
// assistant message carrying reasoning parts, text, and tool calls (with usage
// attached for output tracking), followed by a tool message when results are
// present.
//
// Reasoning blocks lead the assistant message, one part each, in provider
// emission order and are never filtered on empty text: a redacted thinking
// block carries its payload entirely in metadata.
func BuildStepMessages(text string, textMeta map[string]any, reasoning []ReasoningPart,
	calls []ToolCall, results []ToolResultPart, usage *Usage) []Message {
	return buildStepMessages(text, textMeta, reasoning, calls, results, usage)
}
