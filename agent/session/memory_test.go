package session

import (
	"context"
	"testing"

	"github.com/memohai/twilight/agent/jsonstable"
)

func newSession(t *testing.T) (*MemoryStore, SessionID) {
	t.Helper()
	store := NewMemoryStore()
	if _, err := store.Create(context.Background(), CreateRequest{ProtocolVersion: ProtocolVersion1, SessionID: "s"}); err != nil {
		t.Fatal(err)
	}
	return store, "s"
}

func ev(id, typ, payload string) UncommittedEvent {
	return UncommittedEvent{EventID: EventID(id), Type: EventType(typ), Payload: jsonstable.MustParse(payload), RecordedAtUnixMilli: 1}
}

func TestCreateIsIdempotentAndConflicts(t *testing.T) {
	store, _ := newSession(t)
	ctx := context.Background()
	if _, err := store.Create(ctx, CreateRequest{ProtocolVersion: ProtocolVersion1, SessionID: "s"}); err != nil {
		t.Fatalf("identical create: %v", err)
	}
	if _, err := store.Create(ctx, CreateRequest{ProtocolVersion: ProtocolVersion1, SessionID: "s", CausationID: "other"}); !IsCode(err, ErrConflict) {
		t.Fatalf("different create: %v, want conflict", err)
	}
	if _, err := store.Create(ctx, CreateRequest{ProtocolVersion: 9, SessionID: "x"}); !IsCode(err, ErrUnsupportedProfile) {
		t.Fatalf("unsupported profile: %v", err)
	}
}

// Commit is CAS; a retry of the same CommitID is AlreadyApplied even with a
// different timestamp and a moved head; a same-ID different group conflicts;
// CommitIn produces an equivalent commit.
func TestCommitIdempotencyAndConflicts(t *testing.T) {
	store, sid := newSession(t)
	ctx := context.Background()
	head, _ := store.Head(ctx, sid)
	if head.Revision != 0 || head.Digest == "" {
		t.Fatalf("empty head = %+v", head)
	}
	first := AppendRequest{SessionID: sid, ExpectedHead: head, CommitID: "c1", Events: []UncommittedEvent{ev("e1", "twilight/x/a", `{"v":1}`)}}
	res, err := store.Commit(ctx, first)
	if err != nil || res.Disposition != AppendApplied || res.Commit.Revision != 1 {
		t.Fatalf("first commit = %+v, %v", res, err)
	}
	stale, _ := store.Commit(ctx, AppendRequest{SessionID: sid, ExpectedHead: head, CommitID: "c2", Events: []UncommittedEvent{ev("e2", "twilight/x/a", `{"v":1}`)}})
	if stale.Disposition != AppendHeadConflict || stale.ActualHead.Revision != 1 {
		t.Fatalf("stale commit = %+v", stale)
	}
	retry := first
	retry.Events = []UncommittedEvent{ev("e1", "twilight/x/a", `{"v":1}`)}
	retry.Events[0].RecordedAtUnixMilli = 99
	again, _ := store.Commit(ctx, retry)
	if again.Disposition != AppendAlreadyApplied || again.Commit.Events[0].RecordedAtUnixMilli != 1 {
		t.Fatalf("retry = %+v", again)
	}
	conflict, _ := store.Commit(ctx, AppendRequest{SessionID: sid, ExpectedHead: res.ActualHead, CommitID: "c1", Events: []UncommittedEvent{ev("e9", "twilight/x/a", `{"v":2}`)}})
	if conflict.Disposition != AppendCommitConflict {
		t.Fatalf("same id different group = %+v", conflict)
	}
	// CommitIn: KV write in the same transaction; fn nil still commits KV.
	inRes, err := store.CommitIn(ctx, sid, func(tx SessionTx) (*AppendRequest, error) {
		if err := tx.ControlPut("ns", "k", []byte("v"), 0); err != nil {
			return nil, err
		}
		return &AppendRequest{SessionID: sid, ExpectedHead: tx.Head(), CommitID: "c3", Events: []UncommittedEvent{ev("e3", "twilight/y/b", `{"v":1}`)}}, nil
	})
	if err != nil || inRes.Disposition != AppendApplied || inRes.Commit.Revision != 2 {
		t.Fatalf("commit in = %+v, %v", inRes, err)
	}
	if entry, ok, _ := store.ControlGet(ctx, sid, "ns", "k"); !ok || string(entry.Value) != "v" {
		t.Fatal("KV write did not commit with the transaction")
	}
	noop, err := store.CommitIn(ctx, sid, func(tx SessionTx) (*AppendRequest, error) {
		return nil, tx.ControlDelete("ns", "k")
	})
	if err != nil || noop.Disposition != AppendApplied || noop.Commit != nil {
		t.Fatalf("noop = %+v, %v", noop, err)
	}
	if _, ok, _ := store.ControlGet(ctx, sid, "ns", "k"); ok {
		t.Fatal("nil fn did not commit its KV delete")
	}
	// Chain and replay filter.
	page, err := store.Replay(ctx, ReplayRequest{SessionID: sid})
	if err != nil || len(page.Commits) != 2 || page.Commits[1].PreviousDigest != page.Commits[0].CommitDigest {
		t.Fatalf("replay = %+v, %v", page, err)
	}
	filtered, _ := store.Replay(ctx, ReplayRequest{SessionID: sid, Types: []EventType{"twilight/y/"}})
	if len(filtered.Commits) != 1 || filtered.Commits[0].CommitID != "c3" {
		t.Fatalf("filtered = %+v", filtered.Commits)
	}
}

func TestControlCompareAndPutAndExpired(t *testing.T) {
	store, sid := newSession(t)
	ctx := context.Background()
	if ok, _ := store.ControlCompareAndPut(ctx, sid, "ns", "missing", nil, []byte("x"), 0); ok {
		t.Fatal("CAS wrote a missing entry")
	}
	_ = store.ControlPut(ctx, sid, "ns", "k", []byte("a"), 10)
	if ok, _ := store.ControlCompareAndPut(ctx, sid, "ns", "k", []byte("b"), []byte("c"), 20); ok {
		t.Fatal("CAS wrote with a stale expected value")
	}
	if ok, _ := store.ControlCompareAndPut(ctx, sid, "ns", "k", []byte("a"), []byte("a"), 20); !ok {
		t.Fatal("CAS refused a matching expected value")
	}
	_ = store.ControlPut(ctx, sid, "ns", "never", []byte("n"), 0)
	var seen []string
	_ = store.ControlExpired(ctx, "ns", 25, func(e ControlEntry) (bool, error) { seen = append(seen, e.Key); return true, nil })
	if len(seen) != 1 || seen[0] != "k" {
		t.Fatalf("expired = %v, want [k]", seen)
	}
	seen = nil
	_ = store.ControlExpired(ctx, "ns", 15, func(e ControlEntry) (bool, error) { seen = append(seen, e.Key); return true, nil })
	if len(seen) != 0 {
		t.Fatalf("renewed entry reported expired: %v", seen)
	}
}

func TestSnapshotThroughMustBeAPrefix(t *testing.T) {
	store, sid := newSession(t)
	ctx := context.Background()
	head, _ := store.Head(ctx, sid)
	res, _ := store.Commit(ctx, AppendRequest{SessionID: sid, ExpectedHead: head, CommitID: "c1", Events: []UncommittedEvent{ev("e1", "twilight/x/a", `{}`)}})
	snap := Snapshot{ProtocolVersion: ProtocolVersion1, SessionID: sid, ProjectionKey: "p", ProjectionVersion: 1, Through: res.ActualHead, State: jsonstable.MustParse(`{"n":1}`)}
	d, _ := ProfileV1().SnapshotDigest(snap)
	snap.SnapshotDigest = d
	if _, err := store.SaveSnapshot(ctx, SaveSnapshotRequest{Snapshot: snap}); err != nil {
		t.Fatal(err)
	}
	loaded, _ := store.LoadSnapshot(ctx, SnapshotRequest{SessionID: sid, ProjectionKey: "p", ProjectionVersion: 1})
	if !loaded.Found || loaded.Snapshot.Through != res.ActualHead {
		t.Fatalf("snapshot = %+v", loaded)
	}
	tampered := *loaded.Snapshot
	tampered.Through.Digest = "sha256:bogus"
	if err := ProfileV1().ValidateSnapshot(tampered); err == nil {
		t.Fatal("tampered snapshot validated")
	}
	if _, err := store.CommitIn(ctx, sid, func(tx SessionTx) (*AppendRequest, error) {
		_, err := tx.Tail(tampered.Through, nil)
		return nil, err
	}); !IsCode(err, ErrInvalid) {
		t.Fatalf("tail from a non-prefix head: %v, want invalid", err)
	}
}
