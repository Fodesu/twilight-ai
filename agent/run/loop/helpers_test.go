package loop

import (
	"context"
	"testing"
	"time"

	. "github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/session"
	"github.com/memohai/twilight/agent/session/extension"
	runmod "github.com/memohai/twilight/agent/session/run"
)

const (
	testModel   ModelRef          = "m-1"
	testSession session.SessionID = "s-1"
)

func cj(raw string) CanonicalJSON { return MustParseCanonicalJSON(raw) }

// nopCompanion writes no conversation content; Loop tests exercise the Run
// facts only.
type nopCompanion struct{}

func (nopCompanion) Version() string                             { return "test/nop" }
func (nopCompanion) Map(CompanionRequest) ([]ModuleEvent, error) { return nil, nil }

// testStack is the minimal Session stack a Loop test drives: kernel Memory
// Store, the run module, and a Runtime with a no-op companion.
type testStack struct {
	store    *session.MemoryStore
	appender extension.SemanticAppender
	runtime  *runmod.Runtime
}

func newTestStack(t testing.TB, ttl time.Duration, now func() time.Time) *testStack {
	t.Helper()
	store := session.NewMemoryStore()
	registry, err := extension.BuildRegistry(session.ProfileV1(), runmod.Module)
	if err != nil {
		t.Fatal(err)
	}
	appender, err := extension.NewSemanticAppender(store, registry, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	rt, err := runmod.NewRuntime(runmod.Config{Store: store, Registry: registry, Appender: appender,
		Projections: extension.NewProjectionReader(store, registry), Companion: nopCompanion{}, LeaseTTL: ttl, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(context.Background(), session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: testSession}); err != nil {
		t.Fatal(err)
	}
	return &testStack{store: store, appender: appender, runtime: rt}
}

// createRun appends the Start group of one Run with a seed input (RUN-NEW-1).
func (s *testStack) createRun(t testing.TB, runID RunID, inputs ...AgentInput) {
	t.Helper()
	newRun, err := BuildNewRun(runID, "")
	if err != nil {
		t.Fatal(err)
	}
	facts, err := ProtocolV1().BuildCreateGroup(newRun, inputs)
	if err != nil {
		t.Fatal(err)
	}
	group := extension.SemanticGroup{CommitID: session.CommitID("create/" + string(runID))}
	for _, f := range facts {
		group.Events = append(group.Events, extension.TypedEvent{Type: runmod.EventType(f), Value: runmod.Event{RunID: runID, Fact: f}})
	}
	head, err := s.store.Head(context.Background(), testSession)
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.appender.AppendSemantic(context.Background(), extension.SemanticAppendRequest{SessionID: testSession, ExpectedHead: head, Group: group})
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != extension.SemanticApplied {
		t.Fatalf("create run: %s %s", res.Outcome, res.Detail)
	}
}

// newTestRuntime is a Runtime holding "run-1" seeded with one input, leases
// never expiring.
func newTestRuntime(t testing.TB) Runtime {
	t.Helper()
	stack := newTestStack(t, 0, nil)
	stack.createRun(t, "run-1", AgentInput{ID: "seed", Payload: cj(`{"q":"hi"}`)})
	return stack.runtime
}

func loopRuntime(t *testing.T) Runtime {
	t.Helper()
	return newTestRuntime(t)
}

// recordFacts returns every committed fact of runID in stream order.
func recordFacts(t testing.TB, rt Runtime, runID RunID) []Fact {
	t.Helper()
	record, err := rt.Record(context.Background(), testSession, runID)
	if err != nil {
		t.Fatal(err)
	}
	return record.Facts
}

func loadState(t testing.TB, rt Runtime, runID RunID) RuntimeSnapshot {
	t.Helper()
	snap, err := rt.Load(context.Background(), testSession, runID)
	if err != nil {
		t.Fatal(err)
	}
	return snap
}
