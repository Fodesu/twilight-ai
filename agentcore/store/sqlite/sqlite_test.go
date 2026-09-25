package sqlite_test

import (
	"context"
	"errors"
	"github.com/felinics/twilight/agentcore/executor/protocol"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	executionstore "github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/session/filestore"
	"github.com/felinics/twilight/agentcore/store/sqlite"
	"github.com/felinics/twilight/agentcore/store/sqlite/sqlitetest"
)

// The SQLite bindings and ledger run the artifact conformance suite next to
// the file-backed cas store.
func TestArtifactConformance(t *testing.T) {
	artifacttest.Run(t, func(t *testing.T) artifacttest.Fixture {
		var mu sync.Mutex
		now := time.Unix(1_000_000, 0)
		clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
		db := sqlitetest.Open(t)
		bindings := db.Bindings()
		root := t.TempDir()
		return artifacttest.Fixture{
			Bindings: bindings,
			Ledger:   db.Ledger(artifact.SetBuilder{Resolver: bindings}),
			NewContent: func(t *testing.T, authority artifact.Authority) artifact.ContentStore {
				store, err := filestore.NewContentStore(root, authority, filestore.ContentStoreOptions{Now: clock, EphemeralTTL: time.Hour})
				if err != nil {
					t.Fatal(err)
				}
				return store
			},
			Advance: func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() },
		}
	})
}

func assignment(id run.EffectID) effect.Assignment {
	return effect.Assignment{Session: "s", RunID: "r", StepID: "step", Effect: id,
		Body: effect.ModelAssignment{Model: "m", RequestDigest: "sha256:req"}}
}

func accept(t *testing.T, s executionstore.Store, a effect.Assignment) run.Digest {
	t.Helper()
	digest, err := a.Digest()
	if err != nil {
		t.Fatal(err)
	}
	ev, err := executionstore.NewEvent(executionstore.EventExecutionAccepted, 0, executionstore.Accepted{Assignment: a})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Append(context.Background(), executionstore.Lease{}, a.Key(), executionstore.Commit{CommitID: executionstore.AcceptCommitID(a.Key()), Events: []executionstore.Event{ev}}); err != nil {
		t.Fatal(err)
	}
	return digest
}

func step(s executionstore.Store, lease executionstore.Lease, seq executionstore.CommitSeq, typ executionstore.EventType, payload any) error {
	ev, err := executionstore.NewEvent(typ, 0, payload)
	if err != nil {
		return err
	}
	return s.Append(context.Background(), lease, lease.Key, executionstore.Commit{Seq: seq, CommitID: executionstore.DeriveCommitID(lease.Key, "test", string(typ)+"/"+strconv.FormatUint(uint64(seq), 10)), Events: []executionstore.Event{ev}})
}

// One execution ledger, two database handles: the second handle stands for a
// second process. Commits appended through one are read through the other,
// leases fence across them, and the state machine, identity and lease checks
// answer with the store's sentinel errors (RUN-EXE-3, RUN-EXE-6).
func TestExecutionStoreAcrossHandles(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(2_000_000, 0)
	clock := func() time.Time { return now }
	path := filepath.Join(t.TempDir(), "records.db")
	open := func() *sqlite.DB {
		db, err := sqlite.Open(path, sqlite.Options{Now: clock})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	a, b := open().Executions(), open().Executions()
	asg := assignment("e1")
	key := asg.Key()
	accept(t, a, asg)
	// A replayed acceptance is recognised by its identity and not written
	// again; the Worker tells two Assignments apart by reading the ledger.
	ev, _ := executionstore.NewEvent(executionstore.EventExecutionAccepted, 0, executionstore.Accepted{Assignment: asg})
	if err := b.Append(ctx, executionstore.Lease{}, key, executionstore.Commit{CommitID: executionstore.AcceptCommitID(key), Events: []executionstore.Event{ev}}); !errors.Is(err, executionstore.ErrAlreadyApplied) {
		t.Fatalf("replayed acceptance through the other handle = %v, want already applied", err)
	}
	if state, _, ok, err := b.Load(ctx, key); err != nil || !ok || state.Assignment.Key() != key {
		t.Fatalf("load after replay = %+v ok:%v %v", state, ok, err)
	}
	// worker-a acquires through handle a; worker-b cannot while the lease
	// lives, and a's fenced commits go through.
	held, ok, err := a.Acquire(ctx, key, "worker-a", time.Minute)
	if err != nil || !ok || held.Epoch != 1 {
		t.Fatalf("acquire = %+v ok:%v %v", held, ok, err)
	}
	if _, ok, err := b.Acquire(ctx, key, "worker-b", time.Minute); err != nil || ok {
		t.Fatalf("acquire under a live foreign lease = ok:%v %v", ok, err)
	}
	if err := step(a, held, 2, executionstore.EventExecutionStarted, nil); err != nil {
		t.Fatal(err)
	}
	if err := step(a, held, 2, executionstore.EventExecutionStarted, nil); !errors.Is(err, executionstore.ErrAlreadyApplied) {
		t.Fatalf("replayed commit = %v, want already applied", err)
	}
	if err := step(a, held, 2, executionstore.EventCancelRequested, nil); !errors.Is(err, executionstore.ErrConflict) {
		t.Fatalf("another commit at a taken seq = %v, want sequence conflict", err)
	}
	if err := step(a, held, 3, executionstore.EventExecutionStarted, nil); !errors.Is(err, executionstore.ErrStateConflict) {
		t.Fatalf("illegal step = %v, want state conflict", err)
	}
	foreign := executionstore.Lease{Key: key, Owner: "worker-b", Epoch: 1}
	if err := step(b, foreign, 3, executionstore.EventExecutionRunning, nil); !errors.Is(err, executionstore.ErrLeaseLost) {
		t.Fatalf("commit by a non-owner = %v, want lease lost", err)
	}
	if lease, ok, err := b.LeaseOf(ctx, key); err != nil || !ok || lease.Owner != "worker-a" || lease.Epoch != 1 {
		t.Fatalf("lease read through the other handle = %+v %v %v", lease, ok, err)
	}
	// The lease expires: worker-b takes over with a higher Epoch, fencing a.
	now = now.Add(2 * time.Minute)
	taken, ok, err := b.Acquire(ctx, key, "worker-b", time.Minute)
	if err != nil || !ok || taken.Epoch != 2 || taken.Owner != "worker-b" {
		t.Fatalf("takeover = %+v ok:%v %v", taken, ok, err)
	}
	if err := a.Renew(ctx, held, time.Minute); !errors.Is(err, executionstore.ErrLeaseLost) {
		t.Fatalf("renew by the fenced owner = %v, want lease lost", err)
	}
	if err := step(a, held, 4, executionstore.EventExecutionRunning, nil); !errors.Is(err, executionstore.ErrLeaseLost) {
		t.Fatalf("fenced commit = %v, want lease lost", err)
	}
	if err := step(b, taken, 4, executionstore.EventExecutionRunning, nil); err != nil {
		t.Fatal(err)
	}
	got, head, ok, err := a.Load(ctx, key)
	if err != nil || !ok || got.State != effect.ExecutionRunning || got.Lease.Owner != "worker-b" || got.Lease.Epoch != 2 || head.Next != 5 {
		t.Fatalf("fold through the first handle = %+v head %+v ok:%v %v", got, head, ok, err)
	}
	// Settlement ends the execution: leases are refused, and only the
	// acknowledgement may follow.
	out := protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: key}
	if err := step(b, taken, 5, executionstore.EventExecutionSettled, executionstore.Settled{State: effect.ExecutionCompleted, Outcome: out}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := b.Acquire(ctx, key, "worker-b", time.Minute); err != nil || ok {
		t.Fatalf("acquire of a settled execution = ok:%v %v", ok, err)
	}
	if _, _, err := b.Acquire(ctx, assignment("nope").Key(), "worker-b", time.Minute); !errors.Is(err, effect.ErrExecutionNotFound) {
		t.Fatalf("acquire of an unknown key = %v, want not found", err)
	}
	if err := step(a, executionstore.Lease{Key: key}, 6, executionstore.EventOutcomeAcknowledged, nil); err != nil {
		t.Fatal(err)
	}
	got, _, _, err = b.Load(ctx, key)
	if err != nil || !got.Acknowledged || got.Outcome == nil || got.Assignment.Body == nil || got.State != effect.ExecutionCompleted {
		t.Fatalf("acknowledged fold = %+v %v", got, err)
	}
	// Read serves a catch-up from any position with the ledger's head.
	tail, head, err := a.Read(ctx, key, 4)
	if err != nil || len(tail) != 3 || tail[0].Seq != 4 || head.Next != 7 {
		t.Fatalf("read from 4 = %d commits head %+v %v", len(tail), head, err)
	}
	accept(t, b, assignment("e2"))
	list, err := a.List(ctx)
	if err != nil || len(list) != 2 {
		t.Fatalf("list = %d %v, want 2", len(list), err)
	}
	if owned, err := a.ListOwned(ctx, "nobody"); err != nil || len(owned) != 0 {
		t.Fatalf("ListOwned(nobody) = %d %v, want none", len(owned), err)
	}
	if owned, err := a.ListOwned(ctx, "worker-b"); err != nil || len(owned) != 1 || owned[0] != key {
		t.Fatalf("ListOwned(worker-b) = %+v %v, want the leased key", owned, err)
	}
}

// A seeded aborted key has the ledger a real Abort leaves (RUN-EXE-16): the
// tombstone alone at Seq 0 under the abort identity, so a Dispatch's
// acceptance is already applied against it and no lease row exists.
func TestSeedAbortedIsALoneTombstone(t *testing.T) {
	ctx := context.Background()
	s := sqlitetest.Open(t).Executions()
	asg := assignment("aborted")
	key := asg.Key()
	if err := s.Seed(ctx, executionstore.Execution{ExecutionState: executionstore.ExecutionState{Assignment: asg, State: effect.ExecutionAborted}, Lease: executionstore.Lease{Owner: "ignored", Epoch: 3}}); err != nil {
		t.Fatal(err)
	}
	state, head, ok, err := s.Load(ctx, key)
	if err != nil || !ok || !state.Aborted() || state.Outcome != nil || state.Assignment.Effect != "" || head.Next != 1 {
		t.Fatalf("seeded aborted key = %+v head=%+v ok:%v %v, want a lone tombstone", state, head, ok, err)
	}
	ev, _ := executionstore.NewEvent(executionstore.EventExecutionAccepted, 0, executionstore.Accepted{Assignment: asg})
	if err := s.Append(ctx, executionstore.Lease{}, key, executionstore.Commit{Seq: 0, CommitID: executionstore.AcceptCommitID(key), Events: []executionstore.Event{ev}}); !errors.Is(err, executionstore.ErrConflict) {
		t.Fatalf("acceptance against the tombstone = %v, want the Seq 0 conflict a live Abort produces", err)
	}
	if owned, err := s.ListOwned(ctx, "ignored"); err != nil || len(owned) != 0 {
		t.Fatalf("ListOwned for a tombstone = %+v %v, want none", owned, err)
	}
}
