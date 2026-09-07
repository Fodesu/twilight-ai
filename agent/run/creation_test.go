package run

import (
	"context"
	"testing"

	"github.com/memohai/twilight/agent/es"
)

func mustNewRun(t testing.TB, id RunID, cause es.CausationID) NewRun {
	t.Helper()
	run, err := BuildNewRun(id, cause)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func acceptInput(t testing.TB, rt Runtime, id RunID, input AgentInput) CommitResult {
	t.Helper()
	snapshot, err := rt.Load(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := ProtocolV1().BuildEnvelope(id, DeriveInputCommandID(id, input.ID), AcceptInput{Input: input})
	if err != nil {
		t.Fatal(err)
	}
	result, err := rt.Commit(context.Background(), CommitRequest{BaseRevision: snapshot.Revision, Command: envelope})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestNewRunValidationAndV1HeaderGolden(t *testing.T) {
	created := mustNewRun(t, "run-1", "session-1")
	if created.SchemaVersion != SchemaVersion1 {
		t.Fatalf("schema = %d", created.SchemaVersion)
	}
	for _, candidate := range []NewRun{
		{SchemaVersion: SchemaVersion1},
		{SchemaVersion: 99, RunID: "run-1"},
		{SchemaVersion: SchemaVersion1, RunID: RunID(string([]byte{0xff}))},
		{SchemaVersion: SchemaVersion1, RunID: "run-1", CausationID: es.CausationID(string([]byte{0xff}))},
	} {
		if err := ValidateNewRun(candidate); err == nil {
			t.Fatalf("invalid NewRun accepted: %+v", candidate)
		}
	}
	header, err := BuildRunHeaderFromNewRun(created)
	if err != nil {
		t.Fatal(err)
	}
	// Pre-release fixture; re-frozen when Owner/Attempt joined the state and
	// facts became digest-only (RUN-WIR-4).
	if header.InitialStateDigest != "sha256:42c13ce3c1d6f3e9ffe6300bfcfaf41098e8b77e330b7740642f36594c889eb6" ||
		header.HeaderDigest != "sha256:dc9be6579793c0014755bc5e49ef7e427f7803a731c162878143dd9cc6716e21" {
		t.Fatalf("v1 header changed: %+v", header)
	}
}
