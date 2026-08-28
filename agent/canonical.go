// Package agent implements the Twilight AI agent runtime core: the Machine
// (Decide/Evolve/Next), the Loop, the Runtime contract with EvaluateCommit,
// and the canonical encoding that gives commands and facts stable identity.
//
// See docs/design/agent-runtime-refactor.md for the governing spec.
package agent

import (
	"github.com/memohai/twilight-ai/agent/es"
	"github.com/memohai/twilight-ai/agent/jsonstable"
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
