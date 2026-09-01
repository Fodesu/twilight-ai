package run

import (
	"strings"
	"testing"

	"github.com/memohai/twilight/agent/es"
)

func TestBuildRunHeaderRoundTrip(t *testing.T) {
	h, err := BuildRunHeader("run-1", es.CausationID("session:entry-9"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateRunHeader(&h); err != nil {
		t.Fatalf("fresh header invalid: %v", err)
	}
	if h.InitialState.RunID != "run-1" || h.InitialState.Status != RunActive {
		t.Fatalf("initial state = %+v", h.InitialState)
	}
	if h.CausationID != "session:entry-9" {
		t.Fatal("causation id not carried")
	}
}

func TestValidateRunHeaderRejectsTampering(t *testing.T) {
	h, err := BuildRunHeader("run-1", "")
	if err != nil {
		t.Fatal(err)
	}
	// Tampered causation changes the header digest preimage.
	tampered := h
	tampered.CausationID = "forged"
	if err := ValidateRunHeader(&tampered); err == nil {
		t.Fatal("tampered causation accepted")
	}
	// A non-minimal initial state is rejected even with recomputed digests.
	fat := h
	fat.InitialState.ModelSteps = 3
	if err := ValidateRunHeader(&fat); err == nil || !strings.Contains(err.Error(), "minimal") {
		t.Fatalf("non-minimal initial state accepted: %v", err)
	}
	// RunID mismatch between header and state is rejected.
	cross := h
	cross.RunID = "run-2"
	if err := ValidateRunHeader(&cross); err == nil {
		t.Fatal("cross-run header accepted")
	}
}

func TestCommitRejectsCommandSchemaMismatch(t *testing.T) {
	rt := NewMemoryRuntime()
	newRun, err := BuildNewRun("run-1", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Create(t.Context(), newRun); err != nil {
		t.Fatal(err)
	}
	in := AgentInput{ID: "seed", Payload: MustParseCanonicalJSON(`{"q":"hi"}`)}
	env, err := BuildEnvelope("run-1", DeriveInputCommandID("run-1", in.ID), NextStep(in))
	if err != nil {
		t.Fatal(err)
	}
	env.SchemaVersion = 2
	if _, err := rt.Commit(t.Context(), CommitRequest{BaseRevision: 0, Command: env}); err == nil {
		t.Fatal("v2 envelope accepted on v1 run")
	}
}

func TestFoldRunFromHeaderMatchesRuntime(t *testing.T) {
	h, err := BuildRunHeader("run-1", "")
	if err != nil {
		t.Fatal(err)
	}
	rt := NewMemoryRuntime()
	newRun, err := BuildNewRun("run-1", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Create(t.Context(), newRun); err != nil {
		t.Fatal(err)
	}

	// Drive one transition: accept the seed input at Revision 1 (RUN-NEW-1 —
	// seed enters the log, not the header).
	in := AgentInput{ID: "seed", Payload: MustParseCanonicalJSON(`{"q":"hi"}`)}
	env, err := BuildEnvelope("run-1", DeriveInputCommandID("run-1", in.ID), NextStep(in))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Commit(t.Context(), CommitRequest{BaseRevision: 0, Command: env}); err != nil {
		t.Fatal(err)
	}

	record, err := rt.Record(t.Context(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	folded, rev, err := FoldRun(&h, record.Transitions)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 1 {
		t.Fatalf("rev = %d", rev)
	}
	live, err := rt.Load(t.Context(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if !statesEquivalent(&live.State, &folded) {
		t.Fatal("header fold diverged from live state")
	}
}
