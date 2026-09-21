package sdk

// BuildStepMessages assembles the messages produced by one agent step: an
// assistant message carrying reasoning parts, text, and tool calls (with usage
// attached for output tracking), followed by a tool message when results are
// present.
//
// Reasoning blocks lead the assistant message, one part each, in provider
// emission order and are never filtered on empty text: a redacted thinking
// block carries its payload entirely in metadata.
func BuildStepMessages(text string, textMeta ProviderMetadata, reasoningParts []ReasoningPart, toolCalls []ToolCall, toolResults []ToolResultPart, usage *Usage) []Message {
	var assistantParts []MessagePart
	for i := range reasoningParts {
		assistantParts = append(assistantParts, reasoningParts[i])
	}
	if text != "" {
		assistantParts = append(assistantParts, TextPart{Text: text, ProviderMetadata: textMeta})
	}
	for _, tc := range toolCalls {
		assistantParts = append(assistantParts, ToolCallPart{
			ToolCallID:       tc.ToolCallID,
			ToolName:         tc.ToolName,
			Input:            tc.Input,
			ProviderMetadata: tc.ProviderMetadata,
		})
	}

	msgs := []Message{{Role: MessageRoleAssistant, Content: assistantParts, Usage: usage}}
	if len(toolResults) > 0 {
		msgs = append(msgs, ToolMessage(toolResults...))
	}
	return msgs
}
