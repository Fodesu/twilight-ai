// Package agent implements the Twilight AI agent runtime core: the Machine
// (Decide/Evolve/Next), the Loop, the Runtime contract with EvaluateCommit,
// and the canonical encoding that gives commands and facts stable identity.
//
// See docs/design/agent-runtime-refactor.md for the governing spec.
package agent

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// canonicalJSON transforms arbitrary JSON into its RFC 8785 (JCS) canonical
// form: object keys sorted by UTF-16 code units, numbers in ES6 shortest
// form, minimal string escaping, no insignificant whitespace.
//
// Canonical bytes are digest input (spec §5.5); the encoding for a published
// SchemaVersion is frozen forever.
func canonicalJSON(raw []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("agent: canonical: %w", err)
	}
	// Reject trailing content after the first JSON value.
	if dec.More() {
		return nil, errors.New("agent: canonical: trailing data after JSON value")
	}
	var b bytes.Buffer
	if err := writeCanonical(&b, v); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
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
		writeCanonicalString(b, x)
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
			writeCanonicalString(b, k)
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

// writeCanonicalString emits a JSON string with JCS minimal escaping:
// only `"`, `\` and control characters are escaped; control characters use
// their short forms where defined, \u00xx otherwise.
func writeCanonicalString(b *bytes.Buffer, s string) {
	b.WriteByte('"')
	for _, r := range s {
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
				// Invalid UTF-8 input cannot have a stable canonical form.
				b.WriteString(string(utf8.RuneError))
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
}

// canonicalNumber renders a JSON number per RFC 8785: the value is treated as
// an IEEE 754 double and serialized with the ES6 Number::toString algorithm.
func canonicalNumber(n json.Number) (string, error) {
	f, err := strconv.ParseFloat(n.String(), 64)
	if err != nil {
		return "", fmt.Errorf("agent: canonical: bad number %q: %w", n.String(), err)
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

// marshalCanonical marshals a Go value with encoding/json and canonicalizes
// the result. This is the single path from protocol values to digest input.
func marshalCanonical(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("agent: canonical: %w", err)
	}
	return canonicalJSON(raw)
}
