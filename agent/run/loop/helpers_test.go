package loop

import (
	"context"
	"testing"

	. "github.com/memohai/twilight/agent/run"
)

const testModel ModelRef = "m-1"

func cj(raw string) CanonicalJSON { return MustParseCanonicalJSON(raw) }

func newTestRuntime(t *testing.T, _ RunConfig) *MemoryRuntime {
	t.Helper()
	rt := NewMemoryRuntime()
	newRun, err := BuildNewRun("run-1", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Create(context.Background(), newRun); err != nil {
		t.Fatal(err)
	}
	env, err := BuildEnvelope("run-1", DeriveInputCommandID("run-1", "seed"), AcceptInput{
		Input: AgentInput{ID: "seed", Payload: cj(`{"q":"hi"}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := rt.Load(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Commit(context.Background(), CommitRequest{BaseRevision: snap.Revision, Command: env}); err != nil {
		t.Fatal(err)
	}
	return rt
}

func recordEvents(t testing.TB, rt Runtime, runID RunID) []AgentEvent {
	t.Helper()
	record, err := rt.Record(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	var events []AgentEvent
	for _, transition := range record.Transitions {
		events = append(events, transition.Events...)
	}
	return events
}

type modelStepLimitRuntime struct{ Runtime }

func (r modelStepLimitRuntime) Load(ctx context.Context, runID RunID) (RuntimeSnapshot, error) {
	snapshot, err := r.Runtime.Load(ctx, runID)
	if err == nil {
		snapshot.State.ModelSteps = 1
	}
	return snapshot, err
}

func responseDecisionDigest(t *testing.T, kind ResponseKind, decision ResponseDecision, reason string) Digest {
	t.Helper()
	digest, err := DigestToolResponseDecision(kind, decision, reason)
	if err != nil {
		t.Fatal(err)
	}
	return digest
}
