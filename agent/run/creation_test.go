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
	// Pre-release fixture; re-frozen when lastClosedStep left the snapshot wire.
	if header.InitialStateDigest != "sha256:ca282c1b24d7281f8fde3b52e03a32f3c8dbd3231a802078437a7b2469e4f86a" ||
		header.HeaderDigest != "sha256:7a719c28596a23bdd2e71b17beae930377f6702b11168343bd781096b77d392c" {
		t.Fatalf("v1 header changed: %+v", header)
	}
}
