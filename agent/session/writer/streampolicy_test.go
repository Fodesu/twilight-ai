package writer

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
)

func mustValue(t *testing.T, raw string) jsonstable.Value {
	t.Helper()
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatal(err)
	}
	v, err := jsonstable.FromValue(decoded)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// TestCheckStreamAffinity: every branch of the writer-side stream attribution
// check (EXT-STR-1), table-driven.
func TestCheckStreamAffinity(t *testing.T) {
	keyed := extension.StreamDefinition{Domain: "run", IDField: "runId", Lineage: session.LineageSegment}
	singleton := extension.StreamDefinition{Domain: "chat", Lineage: session.LineageSession}
	runStream := keyed.Ref("r1")
	chatStream := singleton.Ref("")
	cases := map[string]struct {
		stream  session.StreamRef
		def     extension.StreamDefinition
		raw     string
		verdict string // "" means accepted
	}{
		"singleton event in its stream":     {chatStream, singleton, `{"note":"n"}`, ""},
		"keyed event in its stream":         {runStream, keyed, `{"runId":"r1"}`, ""},
		"singleton event in another domain": {runStream, singleton, `{"note":"n"}`, `belongs to stream domain "chat"`},
		"keyed event in another domain":     {chatStream, keyed, `{"runId":"r1"}`, `belongs to stream domain "run"`},
		"singleton batch with an ID":        {session.StreamRef{Domain: "chat", ID: "x"}, singleton, `{"note":"n"}`, "is a singleton"},
		"keyed batch without an ID":         {session.StreamRef{Domain: "run"}, keyed, `{"runId":"r1"}`, "names no stream ID"},
		"id mismatch":                       {runStream, keyed, `{"runId":"r9"}`, "does not match"},
		"id missing":                        {runStream, keyed, `{"note":"n"}`, "lacks the stream binding"},
		"id not a string":                   {runStream, keyed, `{"runId":7}`, "lacks the stream binding"},
		"payload not an object":             {runStream, keyed, `[1]`, "not an object"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := checkStreamAffinity(tc.stream, tc.def, mustValue(t, tc.raw))
			if tc.verdict == "" {
				if got != "" {
					t.Fatalf("verdict = %q, want accepted", got)
				}
				return
			}
			if got == "" || !strings.Contains(got, tc.verdict) {
				t.Fatalf("verdict = %q, want containing %q", got, tc.verdict)
			}
		})
	}
}
