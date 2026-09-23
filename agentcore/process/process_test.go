package process_test

import (
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/process"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
)

var key = effect.AssignmentKey{Session: "s1", RunID: "r1", Effect: "sha256:e1"}

func requested() process.Requested {
	return process.Requested{RunID: "r1", StepID: "st1", Effect: "sha256:e1", Kind: process.KindModel}
}

func commit(t *testing.T, seq process.CommitSeq, steps ...struct {
	typ     process.EventType
	payload any
}) process.Commit {
	t.Helper()
	c := process.Commit{Seq: seq, CommitID: process.CommitID("c" + string(rune('0'+seq)))}
	for _, s := range steps {
		ev, err := ledger.NewEvent(s.typ, 1, s.payload)
		if err != nil {
			t.Fatal(err)
		}
		c.Events = append(c.Events, ev)
	}
	return c
}

type step = struct {
	typ     process.EventType
	payload any
}

func TestFold(t *testing.T) {
	cases := []struct {
		name    string
		steps   []step
		want    process.Phase
		wantErr error
	}{
		{"request then dispatched then delivered then acknowledged",
			[]step{{process.EventDispatchRequested, requested()}, {process.EventDispatched, nil}, {process.EventOutcomeDelivered, nil}, {process.EventAcknowledged, nil}},
			process.PhaseAcknowledged, nil},
		{"settlement before the executor confirmed the dispatch",
			[]step{{process.EventDispatchRequested, requested()}, {process.EventOutcomeDelivered, nil}}, process.PhaseDelivered, nil},
		{"failed attempts count up then give up",
			[]step{{process.EventDispatchRequested, requested()}, {process.EventDispatchFailed, process.Failed{Attempt: 1, Reason: "busy"}},
				{process.EventDispatchFailed, process.Failed{Attempt: 2, Reason: "busy"}}, {process.EventGivenUp, process.GivenUp{Reason: "twice"}}},
			process.PhaseGivenUp, nil},
		{"first event must be the request", []step{{process.EventDispatched, nil}}, "", ledger.ErrStateConflict},
		{"requested twice", []step{{process.EventDispatchRequested, requested()}, {process.EventDispatchRequested, requested()}}, "", ledger.ErrStateConflict},
		{"attempt out of order", []step{{process.EventDispatchRequested, requested()}, {process.EventDispatchFailed, process.Failed{Attempt: 2}}}, "", ledger.ErrStateConflict},
		{"acknowledged before delivered", []step{{process.EventDispatchRequested, requested()}, {process.EventDispatched, nil}, {process.EventAcknowledged, nil}}, "", ledger.ErrStateConflict},
		{"nothing after terminal", []step{{process.EventDispatchRequested, requested()}, {process.EventGivenUp, process.GivenUp{}}, {process.EventDispatched, nil}}, "", ledger.ErrStateConflict},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var state process.State
			var err error
			for i, s := range tc.steps {
				c := commit(t, process.CommitSeq(i), s)
				if state, err = process.Fold(state, &c); err != nil {
					break
				}
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("fold = %v, want %v", err, tc.wantErr)
			}
			if err == nil && state.Phase != tc.want {
				t.Fatalf("phase = %s, want %s", state.Phase, tc.want)
			}
			if err == nil && state.Requested != requested() {
				t.Fatalf("requested = %+v", state.Requested)
			}
		})
	}
}

func TestCommitIDs(t *testing.T) {
	ids := map[string]process.CommitID{
		"request": process.RequestCommitID(key), "dispatched": process.DispatchedCommitID(key), "delivered": process.DeliveredCommitID(key),
		"acknowledged": process.AcknowledgedCommitID(key), "given up": process.GivenUpCommitID(key),
		"failed 1": process.FailedCommitID(key, 1), "failed 2": process.FailedCommitID(key, 2),
	}
	seen := map[process.CommitID]string{}
	for name, id := range ids {
		if id == "" {
			t.Fatalf("%s: empty id", name)
		}
		if other, dup := seen[id]; dup {
			t.Fatalf("%s and %s share %s", name, other, id)
		}
		seen[id] = name
	}
	other := key
	other.Effect = run.EffectID("sha256:e2")
	if process.RequestCommitID(other) == process.RequestCommitID(key) {
		t.Fatal("request ids of two effects agree")
	}
}
