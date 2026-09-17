package turn

import (
	"strings"
	"testing"

	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session/extension"
)

// TRN-PRJ-1: a settlement names the attempt it ends. The fold rejects a
// completed or attempt_failed whose RunID the Turn never started, and a second
// end for one attempt, instead of settling the Turn with no attempt carrying
// the result.
func TestSurfaceSettlementNamesAnAttempt(t *testing.T) {
	started := StartedPayload{TurnID: "t1"}
	attempt := AttemptStartedPayload{TurnID: "t1", RunID: "r1", Attempt: 1}
	completed := run.RunEnded{End: run.RunCompletedEnd{}}
	failed := run.RunEnded{End: run.RunFailedEnd{}}
	cases := []struct {
		name    string
		events  []any
		wantErr string
	}{
		{"completed ends the active attempt", []any{started, attempt,
			CompletedPayload{TurnID: "t1", RunID: "r1", End: completed}}, ""},
		{"attempt failure ends the active attempt", []any{started, attempt,
			AttemptFailedPayload{TurnID: "t1", RunID: "r1", End: failed}}, ""},
		{"completed for an unknown run", []any{started, attempt,
			CompletedPayload{TurnID: "t1", RunID: "r9", End: completed}}, "has no attempt r9"},
		{"attempt failure for an unknown run", []any{started, attempt,
			AttemptFailedPayload{TurnID: "t1", RunID: "r9", End: failed}}, "has no attempt r9"},
		{"attempt failure twice", []any{started, attempt,
			AttemptFailedPayload{TurnID: "t1", RunID: "r1", End: failed},
			AttemptFailedPayload{TurnID: "t1", RunID: "r1", End: failed}}, "ended twice"},
		{"completed after the attempt failed", []any{started, attempt,
			AttemptFailedPayload{TurnID: "t1", RunID: "r1", End: failed},
			CompletedPayload{TurnID: "t1", RunID: "r1", End: completed}}, "ended twice"},
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
			if len(v.Attempts) != 1 || v.Attempts[0].End == nil || v.ActiveRun != "" {
				t.Fatalf("attempt not ended: %+v", v)
			}
		})
	}
}
