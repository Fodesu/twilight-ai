package agent

import (
	"encoding/json"
	"fmt"
)

func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }

// canonicalToolArguments renders a model-provided tool input as canonical
// JSON bytes for binding digests. Failure means the arguments are not valid
// JSON; the caller binds them raw and lets validation fail as
// invalid_arguments.
func canonicalToolArguments(input any) (json.RawMessage, error) {
	switch x := input.(type) {
	case nil:
		return json.RawMessage("null"), nil
	case json.RawMessage:
		return canonicalJSON(x)
	case string:
		// Providers deliver unparsed argument text as a string.
		if len(x) == 0 {
			return json.RawMessage("null"), nil
		}
		return canonicalJSON([]byte(x))
	default:
		raw, err := json.Marshal(x)
		if err != nil {
			return nil, fmt.Errorf("agent: tool arguments: %w", err)
		}
		return canonicalJSON(raw)
	}
}

// rawToolArguments preserves unparsable argument bytes as a JSON string so
// the known invalid_arguments failure keeps the original text for the model.
func rawToolArguments(input any) json.RawMessage {
	switch x := input.(type) {
	case nil:
		return json.RawMessage("null")
	case json.RawMessage:
		quoted, _ := json.Marshal(string(x))
		return quoted
	case string:
		quoted, _ := json.Marshal(x)
		return quoted
	default:
		if raw, err := json.Marshal(x); err == nil {
			return raw
		}
		return json.RawMessage("null")
	}
}
