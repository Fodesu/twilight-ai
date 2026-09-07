package run

import (
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

func TestNewRunValidation(t *testing.T) {
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
}
