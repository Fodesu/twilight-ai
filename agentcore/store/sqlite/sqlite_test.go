package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
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
	return effect.Assignment{Session: "s", RunID: "r", StepID: "step", Effect: id, Schema: 1,
		Body: effect.ModelAssignment{Model: "m", RequestDigest: "sha256:req"}}
}

func mustPut(t *testing.T, s executionstore.Store, r executionstore.Record) {
	t.Helper()
	digest, err := r.Assignment.Digest()
	if err != nil {
		t.Fatal(err)
	}
	r.AssignmentDigest = digest
	if err := s.Put(context.Background(), r); err != nil {
		t.Fatal(err)
	}
}

// One record store, two database handles: the second handle stands for a
// second process. Records written through one are read through the other,
// leases fence across them, and the state machine, digest and lease checks
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
	rec := executionstore.Record{Assignment: assignment("e1"), State: effect.ExecutionAccepted}
	digest, err := rec.Assignment.Digest()
	if err != nil {
		t.Fatal(err)
	}
	rec.AssignmentDigest = digest
	key := rec.Assignment.Key()
	if _, created, err := a.Create(ctx, rec); err != nil || !created {
		t.Fatalf("create = created:%v %v", created, err)
	}
	if again, created, err := b.Create(ctx, rec); err != nil || created || again.State != effect.ExecutionAccepted {
		t.Fatalf("replayed create through the other handle = %+v created:%v %v", again, created, err)
	}
	other := rec
	other.AssignmentDigest = "sha256:other"
	if _, _, err := b.Create(ctx, other); !errors.Is(err, executionstore.ErrAssignmentConflict) {
		t.Fatalf("create with another digest = %v, want conflict", err)
	}
	// worker-a acquires through handle a; worker-b cannot while the lease
	// lives, and a's owned writes go through.
	held, ok, err := a.Acquire(ctx, key, "worker-a", time.Minute)
	if err != nil || !ok || held.FencingEpoch != 1 {
		t.Fatalf("acquire = %+v ok:%v %v", held, ok, err)
	}
	if _, ok, err := b.Acquire(ctx, key, "worker-b", time.Minute); err != nil || ok {
		t.Fatalf("acquire under a live foreign lease = ok:%v %v", ok, err)
	}
	if err := a.TransitionOwned(ctx, key, "worker-a", 1, effect.ExecutionAccepted, effect.ExecutionDispatching); err != nil {
		t.Fatal(err)
	}
	if err := a.TransitionOwned(ctx, key, "worker-a", 1, effect.ExecutionAccepted, effect.ExecutionDispatching); !errors.Is(err, executionstore.ErrStateConflict) {
		t.Fatalf("repeated transition = %v, want state conflict", err)
	}
	if err := b.TransitionOwned(ctx, key, "worker-b", 1, effect.ExecutionDispatching, effect.ExecutionRunning); !errors.Is(err, executionstore.ErrLeaseLost) {
		t.Fatalf("transition by a non-owner = %v, want lease lost", err)
	}
	if owned, err := b.LeaseOwned(ctx, key, "worker-a", 1); err != nil || !owned {
		t.Fatalf("lease read through the other handle = %v %v", owned, err)
	}
	// The lease expires: worker-b takes over with a higher epoch, fencing a.
	now = now.Add(2 * time.Minute)
	taken, ok, err := b.Acquire(ctx, key, "worker-b", time.Minute)
	if err != nil || !ok || taken.FencingEpoch != 2 || taken.Owner != "worker-b" {
		t.Fatalf("takeover = %+v ok:%v %v", taken, ok, err)
	}
	if err := a.Renew(ctx, key, "worker-a", 1, time.Minute); !errors.Is(err, executionstore.ErrLeaseLost) {
		t.Fatalf("renew by the fenced owner = %v, want lease lost", err)
	}
	taken.State = effect.ExecutionRunning
	if err := a.PutOwned(ctx, taken, "worker-a", 1); !errors.Is(err, executionstore.ErrLeaseLost) {
		t.Fatalf("owned put by the fenced owner = %v, want lease lost", err)
	}
	if err := b.PutOwned(ctx, taken, "worker-b", 2); err != nil {
		t.Fatal(err)
	}
	got, ok, err := a.Get(ctx, key)
	if err != nil || !ok || got.State != effect.ExecutionRunning || got.Owner != "worker-b" {
		t.Fatalf("record through the first handle = %+v ok:%v %v", got, ok, err)
	}
	// Terminal records refuse leases; the missing key is not found.
	got.State = effect.ExecutionCompleted
	mustPut(t, a, got)
	if _, ok, err := b.Acquire(ctx, key, "worker-b", time.Minute); err != nil || ok {
		t.Fatalf("acquire of a terminal record = ok:%v %v", ok, err)
	}
	if _, _, err := b.Acquire(ctx, assignment("nope").Key(), "worker-b", time.Minute); !errors.Is(err, effect.ErrExecutionNotFound) {
		t.Fatalf("acquire of an unknown key = %v, want not found", err)
	}
	mustPut(t, b, executionstore.Record{Assignment: assignment("e2"), State: effect.ExecutionAccepted})
	list, err := a.List(ctx)
	if err != nil || len(list) != 2 {
		t.Fatalf("list = %d %v, want 2", len(list), err)
	}
	if owned, err := a.ListOwned(ctx, "nobody"); err != nil || len(owned) != 0 {
		t.Fatalf("ListOwned(nobody) = %d %v, want none", len(owned), err)
	}
	if owned, err := a.ListOwned(ctx, got.Owner); err != nil || len(owned) != 1 || owned[0].Assignment.Key() != key {
		t.Fatalf("ListOwned(%q) = %+v %v, want the leased record", got.Owner, owned, err)
	}
}
