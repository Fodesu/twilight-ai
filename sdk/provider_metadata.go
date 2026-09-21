package sdk

import "encoding/json"

// ProviderMetadata carries the opaque tokens a provider attaches to a part
// and needs back on replay -- a reasoning signature, an encrypted reasoning
// payload, a thought signature, an item id -- keyed by the provider's
// namespace and then by the token's name. Values are strings: the tokens are
// opaque to the SDK and to every other provider, and a string survives every
// wire format unchanged. A provider reads only its own namespace.
type ProviderMetadata map[string]map[string]string

// NewProviderMetadata builds the metadata of one namespace, skipping empty
// values. It returns nil when nothing is left, so a caller can tell "no
// token" from "empty token".
func NewProviderMetadata(namespace string, values map[string]string) ProviderMetadata {
	inner := make(map[string]string, len(values))
	for key, value := range values {
		if value != "" {
			inner[key] = value
		}
	}
	if len(inner) == 0 {
		return nil
	}
	return ProviderMetadata{namespace: inner}
}

// Get reads one value; a missing namespace or key is the empty string.
func (m ProviderMetadata) Get(namespace, key string) string {
	if m == nil {
		return ""
	}
	return m[namespace][key]
}

// Merge returns m with every value of other added, other's values winning on
// the same key. Neither argument is modified; a nil result means both were
// empty.
func (m ProviderMetadata) Merge(other ProviderMetadata) ProviderMetadata {
	if len(other) == 0 {
		return m
	}
	out := m.Clone()
	if out == nil {
		out = make(ProviderMetadata, len(other))
	}
	for namespace, values := range other {
		if out[namespace] == nil {
			out[namespace] = make(map[string]string, len(values))
		}
		for key, value := range values {
			out[namespace][key] = value
		}
	}
	return out
}

// Clone returns an independent copy; nil stays nil.
func (m ProviderMetadata) Clone() ProviderMetadata {
	if m == nil {
		return nil
	}
	out := make(ProviderMetadata, len(m))
	for namespace, values := range m {
		inner := make(map[string]string, len(values))
		for key, value := range values {
			inner[key] = value
		}
		out[namespace] = inner
	}
	return out
}

// StringValues renders a bag of values as metadata strings: strings as they
// are, everything else as its JSON encoding. It is how a provider turns a
// response field of another type into an opaque token.
func StringValues(values map[string]any) map[string]string {
	out := make(map[string]string, len(values))
	for key, value := range values {
		switch v := value.(type) {
		case nil:
			continue
		case string:
			out[key] = v
		default:
			raw, err := json.Marshal(v)
			if err != nil {
				continue
			}
			out[key] = string(raw)
		}
	}
	return out
}
