package postgres_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/store/postgres"
	"github.com/felinics/twilight/agent/store/postgres/postgrestest"
	"github.com/felinics/twilight/agent/workspace"
	"github.com/felinics/twilight/agent/workspace/workspacetest"
	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	"github.com/felinics/twilight/agentcore/checkpoint"
	"github.com/felinics/twilight/agentcore/checkpoint/checkpointtest"
	"github.com/felinics/twilight/agentcore/executor/protocol"
	executionstore "github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/executor/store/storetest"
	"github.com/felinics/twilight/agentcore/inbox"
	"github.com/felinics/twilight/agentcore/inbox/inboxtest"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/redispatch"
	"github.com/felinics/twilight/agentcore/run/redispatch/redispatchtest"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	runstoretest "github.com/felinics/twilight/agentcore/run/sessionstore/storetest"
	"github.com/felinics/twilight/agentcore/runtime/runtimetest"
	"github.com/felinics/twilight/agentcore/session/sessiontest"
)

// clock is a settable store clock shared by the handles of one fixture.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// Two handles over one schema stand for two processes.
func TestExecutionStoreConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) storetest.Fixture {
		c := &clock{now: time.Unix(2_000_000, 0)}
		dsn := postgrestest.Schema(t)
		open := func(t *testing.T) executionstore.Store {
			return postgrestest.OpenDSN(t, dsn, postgres.Options{Now: c.Now}).Executions()
		}
		return storetest.Fixture{Store: open(t), Reopen: open, Advance: c.Advance}
	})
}

func TestExecutionSettlementFeedCrossHandle(t *testing.T) {
	dsn := postgrestest.Schema(t)
	dbA := postgrestest.OpenDSN(t, dsn)
	dbB := postgrestest.OpenDSN(t, dsn)
	storeA, storeB := dbA.Executions(), dbB.Executions()
	feed := storeB

	assignment := effect.Assignment{Session: "session", RunID: "run", StepID: "step", Effect: "effect",
		Body: effect.ModelAssignment{Model: "chat"}}
	key := assignment.Key()
	accepted, err := ledger.NewEvent(executionstore.EventExecutionAccepted, 0, executionstore.Accepted{Assignment: assignment})
	if err != nil {
		t.Fatal(err)
	}
	if err := storeA.Append(context.Background(), executionstore.Lease{}, key, ledger.Commit{CommitID: executionstore.AcceptCommitID(key), Batches: []ledger.EventBatch{{Events: []ledger.Event{{Type: accepted.Type, Payload: accepted.Payload}}}}}); err != nil {
		t.Fatal(err)
	}
	lease, acquired, err := storeA.Acquire(context.Background(), key, "worker-a", time.Minute)
	if err != nil || !acquired {
		t.Fatalf("acquire = %+v, %v", lease, err)
	}

	_, head, exists, err := storeA.Load(context.Background(), key)
	if err != nil || !exists {
		t.Fatalf("load = %v, %v", exists, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan struct{})
	events := make(chan effect.Settlement, 2)
	go func() {
		_ = feed.SettlementEvents(ctx, "", 0, func(event effect.Settlement) bool {
			if event.Key == (effect.AssignmentKey{}) {
				close(started)
			} else {
				events <- event
			}
			return true
		})
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("settlement subscription did not initialize")
	}

	outcome := protocol.EncodeOutcome(effect.Outcome{Key: key, Result: effect.Unknown{Message: "test"}})
	settled, err := ledger.NewEvent(executionstore.EventExecutionSettled, 0, executionstore.Settled{State: effect.ExecutionUnknown, Outcome: outcome})
	if err != nil {
		t.Fatal(err)
	}
	if err := storeA.Append(context.Background(), lease, key, ledger.Commit{Seq: head.Next, CommitID: executionstore.SettleCommitID(key), Batches: []ledger.EventBatch{{Events: []ledger.Event{{Type: settled.Type, Payload: settled.Payload}}}}}); err != nil {
		t.Fatal(err)
	}
	var event effect.Settlement
	select {
	case event = <-events:
		if event.Key != key || event.Sequence == 0 {
			t.Fatalf("settlement event = %+v", event)
		}
	case <-ctx.Done():
		t.Fatal("second database handle did not receive durable settlement")
	}

	replayCtx, replayCancel := context.WithTimeout(context.Background(), time.Second)
	defer replayCancel()
	replayed := make(chan effect.Settlement, 1)
	go func() {
		_ = feed.SettlementEvents(replayCtx, event.Epoch, 0, func(next effect.Settlement) bool {
			if next.Key != (effect.AssignmentKey{}) {
				replayed <- next
				return false
			}
			return true
		})
	}()
	select {
	case replay := <-replayed:
		if replay.Key != key || replay.Sequence != event.Sequence {
			t.Fatalf("replayed settlement = %+v, first event = %+v", replay, event)
		}
	case <-replayCtx.Done():
		t.Fatal("durable settlement was not replayed")
	}
}

func TestRedispatchStoreConformance(t *testing.T) {
	redispatchtest.Run(t, func(t *testing.T) redispatch.Store { return postgrestest.Open(t).Redispatches() })
}

func TestCheckpointStoreConformance(t *testing.T) {
	checkpointtest.Run(t, func(t *testing.T) checkpoint.Store { return postgrestest.Open(t).Checkpoints() })
}

func TestInboxStoreConformance(t *testing.T) {
	inboxtest.Run(t, func(t *testing.T) inbox.Store { return postgrestest.Open(t).Inbox() })
}

func TestWorkspaceStoreConformance(t *testing.T) {
	workspacetest.Run(t, func(t *testing.T) workspace.Store { return postgrestest.Open(t).Workspaces() })
}

// The bindings, the retention ledger and the cas content store all live in
// the database; the content store's expiry follows the DB clock.
func TestArtifactConformance(t *testing.T) {
	artifacttest.Run(t, func(t *testing.T) artifacttest.Fixture {
		c := &clock{now: time.Unix(1_000_000, 0)}
		db := postgrestest.Open(t, postgres.Options{Now: c.Now})
		bindings := db.Bindings()
		return artifacttest.Fixture{
			Bindings: bindings,
			Ledger:   db.Ledger(artifact.SetBuilder{Resolver: bindings}),
			NewContent: func(t *testing.T, authority artifact.Authority) artifact.ContentStore {
				store, err := db.Content(authority, postgres.ContentStoreOptions{EphemeralTTL: time.Hour})
				if err != nil {
					t.Fatal(err)
				}
				return store
			},
			Advance: c.Advance,
		}
	})
}

func TestContentPutLimit(t *testing.T) {
	db := postgrestest.Open(t)
	artifacttest.PutLimit(t, func(t *testing.T, maxBytes int64) artifact.ContentStore {
		store, err := db.Content(sessionstore.FrozenAuthority, postgres.ContentStoreOptions{MaxBytes: maxBytes})
		if err != nil {
			t.Fatal(err)
		}
		return store
	})
}

// The Session store runs the kernel, runtime and turn suites: every rule of
// the Session ledger over Postgres tables.
func TestSessionKernelConformance(t *testing.T) {
	sessiontest.Run(t, func(t *testing.T) sessiontest.Fixture {
		c := &clock{now: time.Unix(1_700_000_000, 0)}
		return sessiontest.Fixture{Store: postgrestest.Open(t, postgres.Options{Now: c.Now}).Sessions(), Now: c.Now, Advance: c.Advance}
	})
}

func TestSessionRunStoreConformance(t *testing.T) {
	runstoretest.Run(t, func(t testing.TB) runstoretest.Fixture {
		return runstoretest.Fixture{Store: postgrestest.Open(t).Sessions()}
	})
}

func TestSessionTurnConformance(t *testing.T) {
	runtimetest.Run(t, func(t testing.TB) runtimetest.Fixture {
		return runtimetest.Fixture{Store: postgrestest.Open(t).Sessions()}
	})
}
