// Package jsonstable provides immutable RFC 8785 (JCS) JSON values for agent
// wire protocols. External bytes are parsed and canonicalized once at the
// boundary; after that Value is safe to store in commands, facts, and
// MachineState.
package jsonstable

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/gowebpki/jcs"
)

// Value is an immutable canonical JSON value. The zero value represents an
// absent value for omitzero fields and marshals as JSON null when required.
type Value struct {
	raw []byte
}

// Parse validates raw JSON and stores its RFC 8785 canonical representation.
// A nil slice returns the zero Value; an empty but non-nil slice is invalid
// JSON.
//
// JCS gives every I-JSON value one cross-language representation. JSON
// numbers therefore have IEEE-754 binary64 semantics. Exact identifiers or
// arbitrary-precision quantities must be represented as JSON strings, not
// JSON numbers.
func Parse(raw []byte) (Value, error) {
	if raw == nil {
		return Value{}, nil
	}
	canonical, err := Canonicalize(raw)
	if err != nil {
		return Value{}, err
	}
	return Value{raw: append([]byte(nil), canonical...)}, nil
}

// MustParse is a convenience for tests and package-level constants.
func MustParse(raw string) Value {
	v, err := Parse([]byte(raw))
	if err != nil {
		panic(err)
	}
	return v
}

// FromValue marshals a Go JSON-shaped value and stores its canonical form.
func FromValue(v any) (Value, error) {
	if existing, ok := v.(Value); ok {
		return existing, nil
	}
	if raw, ok := v.(json.RawMessage); ok {
		return Parse(raw)
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return Value{}, err
	}
	return Parse(raw)
}

// Bytes returns a detached canonical byte slice. The zero Value returns null.
func (v Value) Bytes() []byte {
	if len(v.raw) == 0 {
		return []byte("null")
	}
	return append([]byte(nil), v.raw...)
}

// RawMessage returns a detached json.RawMessage view of the canonical bytes.
func (v Value) RawMessage() json.RawMessage {
	return json.RawMessage(v.Bytes())
}

func (v Value) String() string { return string(v.Bytes()) }

// IsZero reports whether v is absent. It is used by encoding/json's omitzero
// tag; a present JSON null is not zero because its raw bytes are "null".
func (v Value) IsZero() bool { return len(v.raw) == 0 }

func (v Value) Equal(other Value) bool { return bytes.Equal(v.Bytes(), other.Bytes()) }

func (v Value) Decode(dst any) error {
	dec := json.NewDecoder(bytes.NewReader(v.Bytes()))
	dec.UseNumber()
	return dec.Decode(dst)
}

func (v Value) Any() (any, error) {
	dec := json.NewDecoder(bytes.NewReader(v.Bytes()))
	dec.UseNumber()
	var out any
	if err := dec.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func (v Value) MarshalJSON() ([]byte, error) { return v.Bytes(), nil }

func (v *Value) UnmarshalJSON(raw []byte) error {
	parsed, err := Parse(raw)
	if err != nil {
		return err
	}
	*v = parsed
	return nil
}

// Canonicalize transforms JSON into RFC 8785 JSON Canonicalization Scheme
// (JCS) bytes. It delegates parsing and ECMAScript number formatting to the
// RFC 8785 reference-lineage implementation rather than encoding/json or a
// local formatter, so a PostgreSQL JSONB round trip remains digest-stable
// after canonicalization.
func Canonicalize(raw []byte) ([]byte, error) {
	if !utf8.Valid(raw) {
		return nil, errors.New("agent: canonical: invalid UTF-8 input")
	}
	if err := rejectEscapedLoneSurrogates(raw); err != nil {
		return nil, err
	}
	canonical, err := jcs.Transform(raw)
	if err != nil {
		return nil, fmt.Errorf("agent: canonical: %w", err)
	}
	return canonical, nil
}

// rejectEscapedLoneSurrogates prevents the JCS parser's Unicode replacement
// behavior from merging distinct invalid wire values before they reach a
// digest. It validates only escaped UTF-16 structure; full JSON syntax remains
// the responsibility of the RFC 8785 parser.
func rejectEscapedLoneSurrogates(raw []byte) error {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '"' {
			continue
		}
		i++
		for i < len(raw) {
			switch raw[i] {
			case '"':
				goto nextToken
			case '\\':
				if i+1 >= len(raw) {
					return nil // let the JCS parser report syntax.
				}
				if raw[i+1] != 'u' && raw[i+1] != 'U' {
					i += 2
					continue
				}
				if i+6 > len(raw) {
					return nil // let the JCS parser report syntax.
				}
				code, ok := parseHex4(raw[i+2 : i+6])
				if !ok {
					return nil // let the JCS parser report syntax.
				}
				switch {
				case 0xd800 <= code && code <= 0xdbff:
					if i+12 > len(raw) || raw[i+6] != '\\' || (raw[i+7] != 'u' && raw[i+7] != 'U') {
						return errors.New("agent: canonical: escaped lone surrogate")
					}
					low, ok := parseHex4(raw[i+8 : i+12])
					if !ok || low < 0xdc00 || low > 0xdfff {
						return errors.New("agent: canonical: escaped lone surrogate")
					}
					i += 12
				case 0xdc00 <= code && code <= 0xdfff:
					return errors.New("agent: canonical: escaped lone surrogate")
				default:
					i += 6
				}
			default:
				i++
			}
		}
	nextToken:
	}
	return nil
}

func parseHex4(raw []byte) (rune, bool) {
	if len(raw) != 4 {
		return 0, false
	}
	var n rune
	for _, b := range raw {
		n <<= 4
		switch {
		case '0' <= b && b <= '9':
			n += rune(b - '0')
		case 'a' <= b && b <= 'f':
			n += rune(b-'a') + 10
		case 'A' <= b && b <= 'F':
			n += rune(b-'A') + 10
		default:
			return 0, false
		}
	}
	return n, true
}

// MarshalCanonical marshals a Go value and canonicalizes its JSON wire form.
// This is the single path from protocol values to digest input.
func MarshalCanonical(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("agent: canonical: %w", err)
	}
	return Canonicalize(raw)
}
