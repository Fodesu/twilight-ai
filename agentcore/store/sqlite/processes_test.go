package sqlite_test

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/process"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/store/sqlite/sqlitetest"
)

func processCommit(t *testing.T, seq process.CommitSeq, id process.CommitID, typ process.EventType, payload any) process.Commit {
	t.Helper()
	ev, err := ledger.NewEvent(typ, 1, payload)
	if err != nil {
		t.Fatal(err)
	}
	return process.Commit{Seq: seq, CommitID: id, Events: []process.Event{ev}}
}

func TestProcessStore(t *testing.T) {
	ctx := context.Background()
	store := sqlitetest.Open(t).Processes()
	k1 := effect.AssignmentKey{Session: "s1", RunID: "r1", Effect: "sha256:e1"}
	k2 := effect.AssignmentKey{Session: "s1", RunID: "r1", Effect: "sha256:e2"}
	req := process.Requested{RunID: "r1", StepID: "st1", Effect: "sha256:e1", Kind: process.KindModel}
	if _, _, ok, err := store.Load(ctx, k1); err != nil || ok {
		t.Fatalf("load of an unknown key = ok:%v %v", ok, err)
	}
	steps := []struct {
		name    string
		epoch   ledger.Epoch
		key     effect.AssignmentKey
		commit  process.Commit
		wantErr error
	}{
		{"request opens the process", 1, k1, processCommit(t, 0, process.RequestCommitID(k1), process.EventDispatchRequested, req), nil},
		{"replayed request is already applied", 1, k1, processCommit(t, 0, process.RequestCommitID(k1), process.EventDispatchRequested, req), ledger.ErrAlreadyApplied},
		{"stale seq conflicts", 1, k1, processCommit(t, 0, process.DispatchedCommitID(k1), process.EventDispatched, nil), ledger.ErrConflict},
		{"illegal step is a state conflict", 1, k1, processCommit(t, 1, process.AcknowledgedCommitID(k1), process.EventAcknowledged, nil), ledger.ErrStateConflict},
		{"a later epoch commits", 2, k1, processCommit(t, 1, process.DispatchedCommitID(k1), process.EventDispatched, nil), nil},
		{"an earlier epoch is fenced", 1, k1, processCommit(t, 2, process.DeliveredCommitID(k1), process.EventOutcomeDelivered, nil), ledger.ErrFenced},
		{"second process in the scope", 2, k2, processCommit(t, 0, process.RequestCommitID(k2), process.EventDispatchRequested, process.Requested{RunID: "r1", StepID: "st2", Effect: "sha256:e2", Kind: process.KindTool}), nil},
	}
	for _, s := range steps {
		t.Run(s.name, func(t *testing.T) {
			if err := store.Append(ctx, s.epoch, s.key, s.commit); !errors.Is(err, s.wantErr) {
				t.Fatalf("append = %v, want %v", err, s.wantErr)
			}
		})
	}
	state, head, ok, err := store.Load(ctx, k1)
	if err != nil || !ok || head.Next != 2 || state.Phase != process.PhaseDispatched || state.Key != k1 || state.Requested != req {
		t.Fatalf("load = %+v head:%+v ok:%v %v", state, head, ok, err)
	}
	open, err := store.Open(ctx, "s1")
	if err != nil || len(open) != 2 {
		t.Fatalf("open = %v %v, want both processes", open, err)
	}
	for seq, c := range []process.Commit{
		processCommit(t, 2, process.DeliveredCommitID(k1), process.EventOutcomeDelivered, nil),
		processCommit(t, 3, process.AcknowledgedCommitID(k1), process.EventAcknowledged, nil),
	} {
		if err := store.Append(ctx, 2, k1, c); err != nil {
			t.Fatalf("seq %d: %v", seq+2, err)
		}
	}
	open, err = store.Open(ctx, "s1")
	if err != nil || len(open) != 1 || open[0] != k2 {
		t.Fatalf("open after k1 acknowledged = %v %v, want only k2", open, err)
	}
	commits, head, err := store.Read(ctx, k1, 2)
	if err != nil || len(commits) != 2 || head.Next != 4 || commits[0].Seq != 2 {
		t.Fatalf("read from 2 = %d commits head:%+v %v", len(commits), head, err)
	}
}
