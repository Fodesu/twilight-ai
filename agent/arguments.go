package agent

import "encoding/json"

func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }

// canonicalToolArguments renders a model-provided tool input as canonical JSON
// for binding digests. Failure means the arguments are not valid JSON; the
// caller binds them raw-as-JSON-string and lets validation fail as
// invalid_arguments.
func canonicalToolArguments(input any) (CanonicalJSON, error) {
	switch x := input.(type) {
	case nil:
		return ParseCanonicalJSON([]byte("null"))
	case CanonicalJSON:
		return x, nil
	case json.RawMessage:
		return ParseCanonicalJSON(x)
	case string:
		// Providers deliver unparsed argument text as a string.
		if len(x) == 0 {
			return ParseCanonicalJSON([]byte("null"))
		}
		return ParseCanonicalJSON([]byte(x))
	default:
		return CanonicalJSONFromValue(x)
	}
}

// rawToolArguments preserves unparsable argument bytes as a JSON string so the
// known invalid_arguments failure keeps the original text for the model.
func rawToolArguments(input any) CanonicalJSON {
	var raw []byte
	switch x := input.(type) {
	case nil:
		raw = []byte("null")
	case CanonicalJSON:
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
	v, err := ParseCanonicalJSON(raw)
	if err != nil {
		return MustParseCanonicalJSON("null")
	}
	return v
}
