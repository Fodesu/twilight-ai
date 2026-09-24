package sdk

import (
	"encoding/json"
	"testing"
)

// TestApplyProviderOptions pins the contract both sides rely on: options are
// keyed by provider namespace, they override what the provider built, and a
// member the provider's wire request does not know is an error rather than a
// silent no-op.
func TestApplyProviderOptions(t *testing.T) {
	type wireRequest struct {
		Model       string  `json:"model"`
		Temperature float64 `json:"temperature"`
	}
	build := func() *wireRequest { return &wireRequest{Model: "m-1"} }

	// No options, and options addressed to someone else, are both a no-op.
	for name, options := range map[string]map[string]json.RawMessage{
		"none":  nil,
		"other": {"other": json.RawMessage(`{"temperature":0.9}`)},
	} {
		wire := build()
		if err := ApplyProviderOptions("mine", options, wire); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if wire.Temperature != 0 {
			t.Fatalf("%s: another provider's options reached this wire request: %+v", name, wire)
		}
	}

	// A member overrides the field the provider set and leaves the rest alone.
	wire := build()
	options := map[string]json.RawMessage{"mine": json.RawMessage(`{"temperature":0.42}`)}
	if err := ApplyProviderOptions("mine", options, wire); err != nil {
		t.Fatal(err)
	}
	if wire.Temperature != 0.42 || wire.Model != "m-1" {
		t.Fatalf("wire = %+v", wire)
	}

	// A misspelled member is loud, because an option that is quietly dropped is
	// indistinguishable from one that was never set.
	options = map[string]json.RawMessage{"mine": json.RawMessage(`{"temperatur":0.42}`)}
	if err := ApplyProviderOptions("mine", options, build()); err == nil {
		t.Fatal("a misspelled provider option was silently ignored")
	}
}
