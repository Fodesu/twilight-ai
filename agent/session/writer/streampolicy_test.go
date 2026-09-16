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
	runStream := session.StreamRef{Kind: session.StreamKindRun, ID: "r1"}
	sessionStream := session.StreamRef{Kind: session.StreamKindSession}
	cases := map[string]struct {
		stream  session.StreamRef
		policy  extension.StreamPolicy
		raw     string
		verdict string // "" means accepted
	}{
		"session event in session batch": {sessionStream, extension.SessionStream, `{"note":"n"}`, ""},
		"run event in run batch":         {runStream, extension.RunStream("runId"), `{"runId":"r1"}`, ""},
		"session event in run batch":     {runStream, extension.SessionStream, `{"note":"n"}`, "session-scoped"},
		"run event in session batch":     {sessionStream, extension.RunStream("runId"), `{"runId":"r1"}`, "run-scoped"},
		"run id mismatch":                {runStream, extension.RunStream("runId"), `{"runId":"r9"}`, "does not match"},
		"run id missing":                 {runStream, extension.RunStream("runId"), `{"note":"n"}`, `lacks the stream binding`},
		"run id not a string":            {runStream, extension.RunStream("runId"), `{"runId":7}`, `lacks the stream binding`},
		"payload not an object":          {runStream, extension.RunStream("runId"), `[1]`, "not an object"},
		"no policy":                      {sessionStream, extension.StreamPolicy{}, `{"note":"n"}`, "no stream policy"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := checkStreamAffinity(tc.stream, tc.policy, mustValue(t, tc.raw))
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
