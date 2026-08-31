// Package run implements the Twilight Run Machine, persisted protocol,
// Runtime authority boundary, verified fold, and canonical identities.
//
// See docs/design/agent-run.md for the governing specification. The in-process
// execution interpreter and its model/tool ports are in agent/run/loop.
package run

import (
	"github.com/memohai/twilight/agent/es"
	"github.com/memohai/twilight/agent/jsonstable"
)

// CanonicalJSON is an immutable, agent-owned canonical JSON value. It can only
// be built by parsing external bytes through ParseCanonicalJSON or by
// marshaling a Go JSON-shaped value through CanonicalJSONFromValue.
type CanonicalJSON = jsonstable.Value

func ParseCanonicalJSON(raw []byte) (CanonicalJSON, error) {
	return jsonstable.Parse(raw)
}

func CanonicalJSONFromValue(v any) (CanonicalJSON, error) {
	return jsonstable.FromValue(v)
}

func MustParseCanonicalJSON(raw string) CanonicalJSON {
	return jsonstable.MustParse(raw)
}

func canonicalJSON(raw []byte) ([]byte, error) { return es.Canonicalize(raw) }

func marshalCanonical(v any) ([]byte, error) { return es.MarshalCanonical(v) }
