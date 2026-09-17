package extension

import (
	"errors"
	"strings"
	"testing"

	"github.com/felinics/twilight/agent/session"
)

type policyPayload struct {
	Note string `json:"note"`
}

func policyModule(stream StreamPolicy) ModuleDescriptor {
	return ModuleDescriptor{
		Source: "polsrc", ID: "pol",
		Events: []EventDefinition{{
			Type: "polsrc/pol/note", Current: 1, Stream: stream,
			Codecs: map[PayloadVersion]PayloadCodec{1: JSONCodec[policyPayload]{}},
		}},
	}
}

// TestBuildRegistryValidatesStreamPolicy: policy declarations are assembly
// errors, caught before any write (EXT-STR-1).
func TestBuildRegistryValidatesStreamPolicy(t *testing.T) {
	cases := map[string]struct {
		stream StreamPolicy
		detail string
	}{
		"valid session":            {stream: SessionStream},
		"valid run":                {stream: RunStream("runId")},
		"missing kind":             {stream: StreamPolicy{}, detail: "no stream policy"},
		"run without field":        {stream: StreamPolicy{Kind: session.StreamKindRun}, detail: "must declare its stream ID field"},
		"run with the version key": {stream: RunStream("v"), detail: "collides with the payload version key"},
		"session with field": {stream: StreamPolicy{Kind: session.StreamKindSession, IDField: "runId"},
			detail: "must not declare a stream ID field"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := BuildRegistry(session.ProtocolVersion1, policyModule(tc.stream))
			if tc.detail == "" {
				if err != nil {
					t.Fatalf("BuildRegistry = %v, want success", err)
				}
				return
			}
			var eerr *Error
			if !errors.As(err, &eerr) || !strings.Contains(eerr.Detail, tc.detail) {
				t.Fatalf("BuildRegistry = %v, want detail containing %q", err, tc.detail)
			}
		})
	}
}
