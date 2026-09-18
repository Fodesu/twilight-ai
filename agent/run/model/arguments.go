package model

import (
	"encoding/json"

	"github.com/felinics/twilight/agent/jsonstable"
)

// CanonicalToolArguments renders a model-provided tool input as canonical JSON
// for binding digests. Failure means the arguments are not valid JSON; the
// caller binds them raw-as-JSON-string and lets validation fail as
// invalid_arguments.
func CanonicalToolArguments(input any) (jsonstable.Value, error) {
	switch x := input.(type) {
	case nil:
		return jsonstable.Parse([]byte("null"))
	case jsonstable.Value:
		return x, nil
	case json.RawMessage:
		return jsonstable.Parse(x)
	case string:
		// Providers deliver unparsed argument text as a string.
		if x == "" {
			return jsonstable.Parse([]byte("null"))
		}
		return jsonstable.Parse([]byte(x))
	default:
		return jsonstable.FromValue(x)
	}
}

// RawToolArguments preserves unparsable argument bytes as a JSON string so the
// known invalid_arguments failure keeps the original text for the model.
func RawToolArguments(input any) jsonstable.Value {
	var raw []byte
	switch x := input.(type) {
	case nil:
		raw = []byte("null")
	case jsonstable.Value:
		return x
	case json.RawMessage:
		raw, _ = json.Marshal(string(x))
	case string:
		raw, _ = json.Marshal(x)
	default:
		var err error
		raw, err = json.Marshal(x)
		if err != nil {
			raw = []byte("null")
		}
	}
	v, err := jsonstable.Parse(raw)
	if err != nil {
		return jsonstable.MustParse("null")
	}
	return v
}
