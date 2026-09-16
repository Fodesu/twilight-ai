package extension_test

import (
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
	"testing"
)

type goldenPayload struct {
	B int `json:"b"`
}

// TestEncodeWireGolden freezes the framework's payload wire: the canonical
// bytes after the `v` field is injected (EXT-COD-2). A drift with a non-empty
// want is an intentional wire change or an accident — update the fixture and
// agent-session-extension.md only for the former.
func TestEncodeWireGolden(t *testing.T) {
	reg, err := extension.BuildRegistry(session.ProtocolVersion1, extension.ModuleDescriptor{
		Source: "goldsrc",
		ID:     "gold",
		Events: []extension.EventDefinition{{
			Type:    "goldsrc/gold/sample",
			Current: 1,
			Codecs:  map[extension.PayloadVersion]extension.PayloadCodec{1: extension.JSONCodec[goldenPayload]{}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	wire, v, err := reg.Encode("goldsrc/gold/sample", goldenPayload{B: 1})
	if err != nil || v != 1 {
		t.Fatalf("encode: %v v=%d", err, v)
	}
	if got, want := wire.String(), `{"b":1,"v":1}`; got != want {
		t.Fatalf("golden encoded payload drifted:\n got: %s\nwant: %s", got, want)
	}
}
