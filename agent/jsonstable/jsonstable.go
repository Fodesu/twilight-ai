// Package jsonstable provides immutable canonical JSON values for agent wire
// protocols. External bytes are parsed and canonicalized once at the boundary;
// after that Value is safe to store in commands, facts, and MachineState.
package jsonstable

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// Value is an immutable canonical JSON value. The zero value represents an
// absent value for omitzero fields and marshals as JSON null when required.
type Value struct {
	raw []byte
}

// Parse validates raw JSON and stores its canonical representation. A nil
// slice returns the zero Value; an empty but non-nil slice is invalid JSON.
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

// Canonicalize transforms JSON into the protocol's canonical form: RFC 8785
// (JCS) object-key ordering and string escaping, with two deliberate
// deviations required for digest identity — integer tokens keep arbitrary
// precision instead of collapsing through float64, and inputs JCS tolerates
// but that would merge distinct payloads into one digest (duplicate object
// keys, trailing data, invalid UTF-8) are rejected outright.
func Canonicalize(raw []byte) ([]byte, error) {
	// Reject invalid UTF-8 up front: encoding/json silently rewrites broken
	// bytes to U+FFFD during decode, which would merge distinct payloads into
	// one canonical identity before writeCanonicalString could see them.
	if !utf8.Valid(raw) {
		return nil, errors.New("agent: canonical: invalid UTF-8 input")
	}
	// encoding/json also rewrites escaped lone surrogates (\ud800..\udfff) to
	// U+FFFD before writeCanonicalString sees them. Reject them while the raw
	// escape structure is still visible; otherwise distinct payloads collapse
	// into the same digest.
	if err := rejectEscapedLoneSurrogates(raw); err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	v, err := parseCanonicalValue(dec)
	if err != nil {
		return nil, fmt.Errorf("agent: canonical: %w", err)
	}
	// Strict end: any trailing token — including a stray ']' or '}' the
	// decoder's More() would miss — rejects the input.
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.New("agent: canonical: trailing data after JSON value")
	}
	var b bytes.Buffer
	if err := writeCanonical(&b, v); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

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
					return nil // let the JSON decoder report syntax
				}
				if raw[i+1] != 'u' && raw[i+1] != 'U' {
					i += 2
					continue
				}
				if i+6 > len(raw) {
					return nil // let the JSON decoder report syntax
				}
				code, ok := parseHex4(raw[i+2 : i+6])
				if !ok {
					return nil // let the JSON decoder report syntax
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

// parseCanonicalValue decodes one JSON value from the token stream, rejecting
// duplicate object keys: last-wins collapsing would let two byte-distinct
// payloads share one digest.
func parseCanonicalValue(dec *json.Decoder) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	return parseFromToken(dec, tok)
}

func parseFromToken(dec *json.Decoder, tok json.Token) (any, error) {
	delim, ok := tok.(json.Delim)
	if !ok {
		return tok, nil // string, json.Number, bool, or nil
	}
	switch delim {
	case '{':
		m := make(map[string]any)
		for dec.More() {
			keyTok, err := dec.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyTok.(string)
			if !ok {
				return nil, fmt.Errorf("object key %v is not a string", keyTok)
			}
			if _, dup := m[key]; dup {
				return nil, fmt.Errorf("duplicate object key %q", key)
			}
			val, err := parseCanonicalValue(dec)
			if err != nil {
				return nil, err
			}
			m[key] = val
		}
		if _, err := dec.Token(); err != nil { // consume '}'
			return nil, err
		}
		return m, nil
	case '[':
		a := []any{}
		for dec.More() {
			val, err := parseCanonicalValue(dec)
			if err != nil {
				return nil, err
			}
			a = append(a, val)
		}
		if _, err := dec.Token(); err != nil { // consume ']'
			return nil, err
		}
		return a, nil
	default:
		return nil, fmt.Errorf("unexpected delimiter %v", delim)
	}
}

func writeCanonical(b *bytes.Buffer, v any) error {
	switch x := v.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		if x {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case json.Number:
		s, err := canonicalNumber(x)
		if err != nil {
			return err
		}
		b.WriteString(s)
	case string:
		if err := writeCanonicalString(b, x); err != nil {
			return err
		}
	case []any:
		b.WriteByte('[')
		for i, e := range x {
			if i > 0 {
				b.WriteByte(',')
			}
			if err := writeCanonical(b, e); err != nil {
				return err
			}
		}
		b.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return utf16Less(keys[i], keys[j]) })
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			if err := writeCanonicalString(b, k); err != nil {
				return err
			}
			b.WriteByte(':')
			if err := writeCanonical(b, x[k]); err != nil {
				return err
			}
		}
		b.WriteByte('}')
	default:
		return fmt.Errorf("agent: canonical: unsupported value %T", v)
	}
	return nil
}

// utf16Less orders strings by their UTF-16 code units (RFC 8785 §3.2.3).
func utf16Less(a, b string) bool {
	ua := utf16.Encode([]rune(a))
	ub := utf16.Encode([]rune(b))
	for i := 0; i < len(ua) && i < len(ub); i++ {
		if ua[i] != ub[i] {
			return ua[i] < ub[i]
		}
	}
	return len(ua) < len(ub)
}

// writeCanonicalString emits a JSON string with JCS minimal escaping. Invalid
// UTF-8 is rejected: silently replacing broken bytes with U+FFFD would merge
// distinct payloads into one canonical identity.
func writeCanonicalString(b *bytes.Buffer, s string) error {
	b.WriteByte('"')
	for i, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\b':
			b.WriteString(`\b`)
		case '\t':
			b.WriteString(`\t`)
		case '\n':
			b.WriteString(`\n`)
		case '\f':
			b.WriteString(`\f`)
		case '\r':
			b.WriteString(`\r`)
		default:
			if r < 0x20 {
				fmt.Fprintf(b, `\u%04x`, r)
			} else if r == utf8.RuneError {
				if _, size := utf8.DecodeRuneInString(s[i:]); size == 1 {
					return errors.New("agent: canonical: invalid UTF-8 in string")
				}
				b.WriteRune(r) // a genuine U+FFFD character
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return nil
}

// canonicalNumber renders a JSON number. Integer tokens keep their exact
// digits (arbitrary precision): forcing them through float64 per strict JCS
// would corrupt 64-bit identifiers above 2^53 and collide near-adjacent
// values into one digest. Non-integer tokens use the ES6 double form.
func canonicalNumber(n json.Number) (string, error) {
	s := n.String()
	if !strings.ContainsAny(s, ".eE") {
		neg := strings.HasPrefix(s, "-")
		digits := strings.TrimPrefix(s, "-")
		digits = strings.TrimLeft(digits, "0")
		if digits == "" {
			return "0", nil // 0 and -0 canonicalize to "0"
		}
		if neg {
			return "-" + digits, nil
		}
		return digits, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return "", fmt.Errorf("agent: canonical: bad number %q: %w", s, err)
	}
	return formatES6Float(f)
}

// formatES6Float implements ES6 Number::toString for finite doubles.
func formatES6Float(x float64) (string, error) {
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return "", errors.New("agent: canonical: non-finite number")
	}
	if x == 0 {
		return "0", nil // negative zero canonicalizes to "0"
	}
	neg := math.Signbit(x)
	if neg {
		x = -x
	}
	// Shortest round-trip digits in exponential form: d[.ddd]e±dd
	s := strconv.FormatFloat(x, 'e', -1, 64)
	ePos := strings.IndexByte(s, 'e')
	mant := strings.Replace(s[:ePos], ".", "", 1)
	exp, err := strconv.Atoi(s[ePos+1:])
	if err != nil {
		return "", err
	}
	k := len(mant)
	n := exp + 1 // value = 0.d1..dk × 10^n

	var out string
	switch {
	case k <= n && n <= 21:
		out = mant + strings.Repeat("0", n-k)
	case 0 < n && n <= 21:
		out = mant[:n] + "." + mant[n:]
	case -6 < n && n <= 0:
		out = "0." + strings.Repeat("0", -n) + mant
	default:
		e := n - 1
		m := mant[:1]
		if k > 1 {
			m += "." + mant[1:]
		}
		if e < 0 {
			out = m + "e-" + strconv.Itoa(-e)
		} else {
			out = m + "e+" + strconv.Itoa(e)
		}
	}
	if neg {
		out = "-" + out
	}
	return out, nil
}

// MarshalCanonical marshals a Go value with encoding/json and canonicalizes
// the result. This is the single path from protocol values to digest input.
func MarshalCanonical(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("agent: canonical: %w", err)
	}
	return Canonicalize(raw)
}
