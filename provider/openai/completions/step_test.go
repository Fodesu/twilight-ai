package completions_test

import "github.com/felinics/twilight/sdk"

// assistantStep is the assistant message of one model result as a caller
// replays it: reasoning parts first, in provider order and never filtered on
// empty text, then the text and the tool calls with their provider tokens.
func assistantStep(r *sdk.ModelResult) sdk.Message {
	var parts []sdk.MessagePart
	for _, rp := range r.ReasoningParts {
		parts = append(parts, rp)
	}
	if r.Text != "" {
		parts = append(parts, sdk.TextPart{Text: r.Text, ProviderMetadata: r.TextProviderMetadata})
	}
	for _, tc := range r.ToolCalls {
		parts = append(parts, sdk.ToolCallPart{ToolCallID: tc.ToolCallID, ToolName: tc.ToolName, Input: tc.Input, ProviderMetadata: tc.ProviderMetadata})
	}
	usage := r.Usage
	return sdk.Message{Role: sdk.MessageRoleAssistant, Content: parts, Usage: &usage}
}
