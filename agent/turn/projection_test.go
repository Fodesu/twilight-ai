package turn

import (
	"strings"
	"testing"

	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session/extension"
)

// TRN-PRJ-1: attempt_ended settles the attempt attempt_started registered. A
// completed Run completes the Turn, any other end leaves it attempt_failed; an
// end naming a Run the Turn never started, or a second end of one attempt, is
// a fold error rather than a silent no-op.
func TestSurfaceSettlementNamesAnAttempt(t *testing.T) {
	started := StartedPayload{TurnID: "t1"}
	attempt := AttemptStartedPayload{TurnID: "t1", RunID: "r1", Attempt: 1}
	ended := func(runID run.RunID, end run.RunEnd) AttemptEndedPayload {
		return AttemptEndedPayload{TurnID: "t1", RunID: runID, Attempt: 1, End: run.RunEnded{End: end}}
	}
	completed, failed := run.RunCompletedEnd{}, run.RunFailedEnd{Reason: "provider"}
	cases := []struct {
		name       string
		events     []any
		wantStatus TurnStatus
		wantEnded  bool
		wantErr    string
	}{
		{"completed ends the active attempt", []any{started, attempt, ended("r1", completed)}, TurnCompleted, true, ""},
		{"failure ends the active attempt", []any{started, attempt, ended("r1", failed)}, TurnAttemptFailed, true, ""},
		{"an end for a run the turn never started", []any{started, attempt, ended("r9", completed)}, "", false, "has no attempt"},
		{"failure twice", []any{started, attempt, ended("r1", failed), ended("r1", failed)}, "", false, "ended twice"},
		{"completed after the attempt failed", []any{started, attempt, ended("r1", failed), ended("r1", completed)}, "", false, "ended twice"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state, err := SurfaceProjection.Initial()
			if err != nil {
				t.Fatal(err)
			}
			for i, ev := range tc.events {
				if state, err = applySurface(state, extension.DecodedEvent{Value: ev}); err != nil {
					if i != len(tc.events)-1 {
						t.Fatalf("event %d: %v", i, err)
					}
					break
				}
			}
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("fold error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			v := state.(TurnSurface).Turns["t1"]
			if v.Status != tc.wantStatus || len(v.Attempts) != 1 || (v.Attempts[0].End != nil) != tc.wantEnded || (v.ActiveRun == "") != tc.wantEnded {
				t.Fatalf("view = %+v, want status %s ended=%v", v, tc.wantStatus, tc.wantEnded)
			}
		})
	}
}
