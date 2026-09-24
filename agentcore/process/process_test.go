package process_test

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/process"
	"github.com/felinics/twilight/agentcore/run/effect"
)

var key = effect.AssignmentKey{Session: "s1", RunID: "r1", Effect: "sha256:e1"}

type step = struct {
	typ     process.EventType
	payload any
}

func commit(t *testing.T, seq process.CommitSeq, s step) process.Commit {
	t.Helper()
	ev, err := ledger.NewEvent(s.typ, 1, s.payload)
	if err != nil {
		t.Fatal(err)
	}
	return process.Commit{Seq: seq, CommitID: process.CommitID("c" + string(rune('0'+seq))), Events: []process.Event{ev}}
}

func TestFold(t *testing.T) {
	cases := []struct {
		name    string
		steps   []step
		want    process.State
		wantErr error
	}{
		{"attempts count up", []step{{process.EventDispatchAttempted, process.Attempted{Attempt: 1}}, {process.EventDispatchAttempted, process.Attempted{Attempt: 2}}},
			process.State{Attempts: 2}, nil},
		{"given up after attempts", []step{{process.EventDispatchAttempted, process.Attempted{Attempt: 1}}, {process.EventGivenUp, process.GivenUp{Reason: "budget"}}},
			process.State{Attempts: 1, GivenUp: true, Reason: "budget"}, nil},
		{"given up without an attempt", []step{{process.EventGivenUp, process.GivenUp{Reason: "rejected"}}}, process.State{GivenUp: true, Reason: "rejected"}, nil},
		{"attempt out of order", []step{{process.EventDispatchAttempted, process.Attempted{Attempt: 2}}}, process.State{}, ledger.ErrStateConflict},
		{"nothing after given up", []step{{process.EventGivenUp, process.GivenUp{}}, {process.EventDispatchAttempted, process.Attempted{Attempt: 1}}}, process.State{}, ledger.ErrStateConflict},
		{"unknown event", []step{{"other", nil}}, process.State{}, ledger.ErrStateConflict},
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
			if err == nil && state != tc.want {
				t.Fatalf("state = %+v, want %+v", state, tc.want)
			}
		})
	}
}

// memStore is an in-memory process.Store for the helper tests.
type memStore struct {
	commits map[effect.AssignmentKey][]process.Commit
	epoch   map[effect.AssignmentKey]process.Epoch
}

func newMemStore() *memStore {
	return &memStore{commits: map[effect.AssignmentKey][]process.Commit{}, epoch: map[effect.AssignmentKey]process.Epoch{}}
}

func (m *memStore) Load(_ context.Context, k effect.AssignmentKey) (process.State, process.Head, bool, error) {
	cs := m.commits[k]
	state := process.State{Key: k}
	for i := range cs {
		var err error
		if state, err = process.Fold(state, &cs[i]); err != nil {
			return process.State{}, process.Head{}, false, err
		}
	}
	return state, process.Head{Next: process.CommitSeq(len(cs))}, len(cs) > 0, nil
}

func (m *memStore) Read(_ context.Context, k effect.AssignmentKey, from process.CommitSeq) ([]process.Commit, process.Head, error) {
	cs := m.commits[k]
	if int(from) > len(cs) {
		from = process.CommitSeq(len(cs))
	}
	return cs[from:], process.Head{Next: process.CommitSeq(len(cs))}, nil
}

func (m *memStore) Append(ctx context.Context, epoch process.Epoch, k effect.AssignmentKey, c process.Commit) error {
	for _, have := range m.commits[k] {
		if have.CommitID == c.CommitID {
			return ledger.ErrAlreadyApplied
		}
	}
	if epoch < m.epoch[k] {
		return ledger.ErrFenced
	}
	state, head, _, err := m.Load(ctx, k)
	if err != nil {
		return err
	}
	if c.Seq != head.Next {
		return ledger.ErrConflict
	}
	if _, err := process.Fold(state, &c); err != nil {
		return err
	}
	m.epoch[k] = epoch
	m.commits[k] = append(m.commits[k], c)
	return nil
}

func TestAttemptAndGiveUp(t *testing.T) {
	ctx := context.Background()
	s := newMemStore()
	for want := 1; want <= 3; want++ {
		if n, err := process.Attempt(ctx, s, 1, key, 1); err != nil || n != want {
			t.Fatalf("attempt %d = %d %v", want, n, err)
		}
	}
	if err := process.GiveUp(ctx, s, 1, key, "budget", 1); err != nil {
		t.Fatal(err)
	}
	// Giving up twice is a no-op; a stale epoch is fenced.
	if err := process.GiveUp(ctx, s, 1, key, "again", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := process.Attempt(ctx, s, 0, effect.AssignmentKey{Session: "s1", RunID: "r1", Effect: "sha256:e2"}, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := process.Attempt(ctx, s, 0, key, 1); !errors.Is(err, ledger.ErrFenced) && !errors.Is(err, ledger.ErrStateConflict) {
		t.Fatalf("attempt under a stale epoch after given up = %v", err)
	}
	state, head, ok, err := s.Load(ctx, key)
	if err != nil || !ok || head.Next != 4 || state.Attempts != 3 || !state.GivenUp || state.Reason != "budget" {
		t.Fatalf("state = %+v head %+v ok:%v %v", state, head, ok, err)
	}
}
