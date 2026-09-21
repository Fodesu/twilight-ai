package sdk

// ToolCall is a tool invocation the model requested.
type ToolCall struct {
	ToolCallID       string           `json:"toolCallId"`
	ToolName         string           `json:"toolName"`
	Input            ToolArguments    `json:"input"`
	ProviderMetadata ProviderMetadata `json:"providerMetadata,omitempty"`
}

// ToolResult pairs a tool call with what the tool returned.
type ToolResult struct {
	ToolCallID string        `json:"toolCallId"`
	ToolName   string        `json:"toolName"`
	Input      ToolArguments `json:"input"`
	Output     ToolOutput    `json:"output"`
}
