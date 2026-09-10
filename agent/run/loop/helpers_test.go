package loop

import (
	"context"
	. "github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
	"github.com/felinics/twilight/agent/session/extension/writer"
	runmod "github.com/felinics/twilight/agent/session/run"
	"testing"
	"time"
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
// Store, the run module, one owner process (Writers) and a Runtime with a
// no-op companion.
type testStack struct {
	store    *session.MemoryStore
	registry *extension.Registry
	writers  writer.Writers
	runtime  *runmod.Runtime
	now      func() time.Time
}

func newTestStack(t testing.TB, now func() time.Time) *testStack {
	t.Helper()
	if now == nil {
		now = time.Now
	}
	store := session.NewMemoryStore()
	registry, err := extension.BuildRegistry(session.ProtocolVersion1, runmod.Module)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(context.Background(), session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: testSession}); err != nil {
		t.Fatal(err)
	}
	s := &testStack{store: store, registry: registry, now: now}
	s.open(t)
	return s
}

// open starts a new owner process over the same store, superseding a previous
// one that is still open.
func (s *testStack) open(t testing.TB) {
	t.Helper()
	s.writers = writer.NewWriters(s.store, s.registry, writer.Admission{}, session.OpenOptions{Takeover: true}, writer.WritersConfig{})
	rt, err := runmod.NewRuntime(runmod.Config{Writers: s.writers, Registry: s.registry, Store: s.store, Companion: nopCompanion{}, Now: s.now})
	if err != nil {
		t.Fatal(err)
	}
	s.runtime = rt
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
	group := &writer.SemanticGroup{CommitID: session.CommitID("create/" + string(runID))}
	for _, f := range facts {
		group.Events = append(group.Events, writer.TypedEvent{Type: runmod.EventType(f), Value: runmod.Event{RunID: runID, Fact: f}})
	}
	w, err := s.writers.Writer(context.Background(), testSession)
	if err != nil {
		t.Fatal(err)
	}
	res, err := w.Commit(context.Background(), func(writer.View) (*writer.SemanticGroup, error) { return group, nil })
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != writer.CommitApplied {
		t.Fatalf("create run: %s %s", res.Outcome, res.Detail)
	}
}

// newTestRuntime is a Runtime holding "run-1" seeded with one input.
func newTestRuntime(t testing.TB) Runtime {
	t.Helper()
	stack := newTestStack(t, nil)
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
