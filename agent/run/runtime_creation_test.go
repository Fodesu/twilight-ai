package run

import (
	"context"
	"errors"
	"fmt"
	"sync"
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

func mustCreateRun(t testing.TB, rt Runtime, id RunID) {
	t.Helper()
	if _, err := rt.Create(context.Background(), mustNewRun(t, id, "session-1")); err != nil {
		t.Fatal(err)
	}
}

func acceptInput(t testing.TB, rt Runtime, id RunID, input AgentInput) CommitResult {
	t.Helper()
	snapshot, err := rt.Load(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := BuildEnvelope(id, DeriveInputCommandID(id, input.ID), AcceptInput{Input: input})
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
	if header.InitialStateDigest != "sha256:6dd6e9f67d4b9e6c1d2c72d20a95276de2a8c18453a5cbea3f5d602087770468" ||
		header.HeaderDigest != "sha256:f51f1136690ef97ea4fef7385f76b48ec41933fc66f30283df673bafce64367d" {
		t.Fatalf("v1 header changed: %+v", header)
	}
}

func TestRuntimeCreateIdempotentAndConflict(t *testing.T) {
	rt := NewMemoryRuntime()
	first, err := rt.Create(context.Background(), mustNewRun(t, "run-1", "session-1"))
	if err != nil || !first.Created {
		t.Fatalf("first Create = %+v, %v", first, err)
	}
	first.Header.InitialState.RunID = "mutated"
	second, err := rt.Create(context.Background(), mustNewRun(t, "run-1", "session-1"))
	if err != nil || second.Created || second.Header.InitialState.RunID != "run-1" {
		t.Fatalf("idempotent Create = %+v, %v", second, err)
	}
	if _, err := rt.Create(context.Background(), mustNewRun(t, "run-1", "session-2")); !errors.Is(err, ErrCreateConflict) {
		t.Fatalf("conflict error = %v", err)
	}
}

func TestRuntimeMissingRun(t *testing.T) {
	rt := NewMemoryRuntime()
	if _, err := rt.Load(context.Background(), "missing"); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("Load error = %v", err)
	}
	if _, err := rt.Record(context.Background(), "missing"); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("Record error = %v", err)
	}
	env, err := BuildEnvelope("missing", "command-1", CancelRun{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Commit(context.Background(), CommitRequest{Command: env}); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("Commit error = %v", err)
	}
}

func TestRuntimeRecordFoldAndIsolation(t *testing.T) {
	rt := NewMemoryRuntime()
	mustCreateRun(t, rt, "run-1")
	zero, err := rt.Record(context.Background(), "run-1")
	if err != nil || zero.Snapshot.Revision != 0 || len(zero.Transitions) != 0 {
		t.Fatalf("revision-zero record = %+v, %v", zero, err)
	}
	acceptInput(t, rt, "run-1", AgentInput{ID: "in-1", Payload: MustParseCanonicalJSON(`{"q":"hi"}`)})
	record, err := rt.Record(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	folded, revision, err := FoldRun(&record.Header, record.Transitions)
	if err != nil || revision != record.Snapshot.Revision || !statesEquivalent(&folded, &record.Snapshot.State) {
		t.Fatalf("record fold = revision %d record %+v err %v", revision, record, err)
	}
	record.Header.InitialState.RunID = "mutated"
	record.Snapshot.State.RunID = "mutated"
	record.Transitions[0].Events[0].RunID = "mutated"
	fresh, err := rt.Record(context.Background(), "run-1")
	if err != nil || fresh.Header.InitialState.RunID != "run-1" || fresh.Snapshot.State.RunID != "run-1" || fresh.Transitions[0].Events[0].RunID != "run-1" {
		t.Fatalf("record aliases authority: %+v, %v", fresh, err)
	}
}

func TestRuntimeConcurrentCreateAndRunIsolation(t *testing.T) {
	rt := NewMemoryRuntime()
	newRun := mustNewRun(t, "run-1", "session-1")
	const callers = 32
	results := make(chan CreateResult, callers)
	errs := make(chan error, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := rt.Create(context.Background(), newRun)
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}()
	}
	group.Wait()
	close(results)
	close(errs)
	created := 0
	for err := range errs {
		t.Fatal(err)
	}
	for result := range results {
		if result.Created {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("created = %d", created)
	}
	mustCreateRun(t, rt, "run-2")
	acceptInput(t, rt, "run-1", AgentInput{ID: "one", Payload: MustParseCanonicalJSON(`1`)})
	acceptInput(t, rt, "run-2", AgentInput{ID: "two", Payload: MustParseCanonicalJSON(`2`)})
	one, _ := rt.Load(context.Background(), "run-1")
	two, _ := rt.Load(context.Background(), "run-2")
	if len(one.State.PendingInputs) != 1 || one.State.PendingInputs[0].ID != "one" || len(two.State.PendingInputs) != 1 || two.State.PendingInputs[0].ID != "two" {
		t.Fatalf("cross-run state leaked: one=%+v two=%+v", one.State.PendingInputs, two.State.PendingInputs)
	}
}

func TestRuntimeRecordConcurrentWithCommit(t *testing.T) {
	rt := NewMemoryRuntime()
	mustCreateRun(t, rt, "run-1")
	const commits = 64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < commits; i++ {
			input := AgentInput{ID: InputID(fmt.Sprintf("in-%d", i)), Payload: MustParseCanonicalJSON(`null`)}
			for {
				snapshot, err := rt.Load(context.Background(), "run-1")
				if err != nil {
					t.Errorf("Load: %v", err)
					return
				}
				env, _ := BuildEnvelope("run-1", DeriveInputCommandID("run-1", input.ID), AcceptInput{Input: input})
				_, err = rt.Commit(context.Background(), CommitRequest{BaseRevision: snapshot.Revision, Command: env})
				if errors.Is(err, ErrStaleRuntime) {
					continue
				}
				if err != nil {
					t.Errorf("Commit: %v", err)
				}
				break
			}
		}
	}()
	for {
		record, err := rt.Record(context.Background(), "run-1")
		if err != nil {
			t.Fatal(err)
		}
		folded, revision, err := FoldRun(&record.Header, record.Transitions)
		if err != nil || revision != record.Snapshot.Revision || !statesEquivalent(&folded, &record.Snapshot.State) {
			t.Fatalf("inconsistent record: rev=%d snapshot=%d err=%v", revision, record.Snapshot.Revision, err)
		}
		select {
		case <-done:
			return
		default:
		}
	}
}
