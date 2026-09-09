package turn

import (
	"testing"

	"github.com/memohai/twilight/agent/session"
)

// EXT-COD-1: every registered event type's current codec is canonical
// round-trip stable — Encode, Decode, Encode reproduces the bytes.
func TestEventCodecCanonicalRoundTrip(t *testing.T) {
	samples := map[session.EventType]any{
		TypeStarted:    StartedPayload{TurnID: "t1", InputIDs: nil, Profile: ProfileRef{ID: "b", Digest: "sha256:b"}, Companion: CompanionV1Version},
		TypeCompleted:  CompletedPayload{TurnID: "t1", RunID: "run-1"},
		TypeFailed:     FailedPayload{TurnID: "t1", RunID: "run-1", Settlement: SettlementFailed, FailureClass: "provider"},
		TypeSuperseded: SupersededPayload{TurnID: "t1", ReplacementTurnID: "t2"},
	}
	for _, def := range Module.Events {
		value, ok := samples[def.Type]
		if !ok {
			t.Fatalf("no sample for %s", def.Type)
		}
		codec := def.Codecs[def.Current]
		first, err := codec.Encode(value)
		if err != nil {
			t.Fatalf("%s: encode: %v", def.Type, err)
		}
		back, err := codec.Decode(first)
		if err != nil {
			t.Fatalf("%s: decode: %v", def.Type, err)
		}
		again, err := codec.Encode(back)
		if err != nil || !again.Equal(first) {
			t.Fatalf("%s: round trip changed bytes: %s vs %s (%v)", def.Type, first, again, err)
		}
	}
}
