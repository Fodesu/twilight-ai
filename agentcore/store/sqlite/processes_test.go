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
	if _, _, ok, err := store.Load(ctx, k1); err != nil || ok {
		t.Fatalf("load of an unknown key = ok:%v %v", ok, err)
	}
	steps := []struct {
		name    string
		epoch   ledger.Epoch
		commit  process.Commit
		wantErr error
	}{
		{"first attempt opens the ledger", 1, processCommit(t, 0, process.AttemptCommitID(k1, 1), process.EventDispatchAttempted, process.Attempted{Attempt: 1}), nil},
		{"replayed attempt is already applied", 1, processCommit(t, 0, process.AttemptCommitID(k1, 1), process.EventDispatchAttempted, process.Attempted{Attempt: 1}), ledger.ErrAlreadyApplied},
		{"stale seq conflicts", 1, processCommit(t, 0, process.AttemptCommitID(k1, 2), process.EventDispatchAttempted, process.Attempted{Attempt: 2}), ledger.ErrConflict},
		{"attempt out of order is a state conflict", 1, processCommit(t, 1, process.AttemptCommitID(k1, 3), process.EventDispatchAttempted, process.Attempted{Attempt: 3}), ledger.ErrStateConflict},
		{"a later epoch commits", 2, processCommit(t, 1, process.AttemptCommitID(k1, 2), process.EventDispatchAttempted, process.Attempted{Attempt: 2}), nil},
		{"an earlier epoch is fenced", 1, processCommit(t, 2, process.GivenUpCommitID(k1), process.EventGivenUp, process.GivenUp{Reason: "x"}), ledger.ErrFenced},
		{"given up", 2, processCommit(t, 2, process.GivenUpCommitID(k1), process.EventGivenUp, process.GivenUp{Reason: "budget"}), nil},
	}
	for _, s := range steps {
		t.Run(s.name, func(t *testing.T) {
			if err := store.Append(ctx, s.epoch, k1, s.commit); !errors.Is(err, s.wantErr) {
				t.Fatalf("append = %v, want %v", err, s.wantErr)
			}
		})
	}
	state, head, ok, err := store.Load(ctx, k1)
	if err != nil || !ok || head.Next != 3 || state.Attempts != 2 || !state.GivenUp || state.Key != k1 {
		t.Fatalf("load = %+v head:%+v ok:%v %v", state, head, ok, err)
	}
	commits, head, err := store.Read(ctx, k1, 1)
	if err != nil || len(commits) != 2 || head.Next != 3 || commits[0].Seq != 1 {
		t.Fatalf("read from 1 = %d commits head:%+v %v", len(commits), head, err)
	}
}
