package sdk

import (
	"encoding/json"
	"fmt"
)

// ToolOutput is what a tool returned, as the model will read it: plain text,
// or a JSON document. Exactly one of the two fields is set; the zero value is
// an empty text output.
type ToolOutput struct {
	Text string          `json:"text,omitempty"`
	JSON json.RawMessage `json:"json,omitempty"`
}

// TextOutput is a text tool output.
func TextOutput(text string) ToolOutput { return ToolOutput{Text: text} }

// JSONOutput encodes v as a JSON tool output.
func JSONOutput(v any) (ToolOutput, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return ToolOutput{}, fmt.Errorf("twilightai: encode tool output: %w", err)
	}
	return ToolOutput{JSON: raw}, nil
}

// RawJSONOutput wraps an already encoded JSON document.
func RawJSONOutput(raw json.RawMessage) ToolOutput {
	return ToolOutput{JSON: append(json.RawMessage(nil), raw...)}
}

// String is the output as text: the text itself, or the JSON document.
func (o ToolOutput) String() string {
	if o.JSON != nil {
		return string(o.JSON)
	}
	return o.Text
}

// IsJSON reports whether the output is a JSON document.
func (o ToolOutput) IsJSON() bool { return o.JSON != nil }
