package run

import (
	"context"
	"testing"
	"time"
)

func TestExpiredLeaseAllowsGrantlessModelRecovery(t *testing.T) {
	clock := time.Unix(1000, 0)
	store := NewMemoryStore()
	rt := NewRuntimeWithOptions(store, RuntimeOptions{
		LeaseTTL: time.Second,
		Now:      func() time.Time { return clock },
	})
	ctx := context.Background()
	newRun, err := BuildNewRun("run-1", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Create(ctx, newRun); err != nil {
		t.Fatal(err)
	}
	in := AgentInput{ID: "seed", Payload: cj(`{"q":"hi"}`)}
	env, err := BuildEnvelope("run-1", DeriveInputCommandID("run-1", in.ID), AcceptInput{Input: in})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Commit(ctx, CommitRequest{Command: env}); err != nil {
		t.Fatal(err)
	}

	snap, err := rt.Load(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	prep, cmdID := buildPrepareFromSnap(t, snap, testRequest(), nil)
	if _, err := commitCmd(t, rt, cmdID, snap.Revision, "", prep); err != nil {
		t.Fatal(err)
	}
	claim := ExecutionClaim("claim-recover")
	startEnv, err := BuildEnvelope("run-1", "start-1", StartModelExecution{StepID: prep.StepID, Claim: claim})
	if err != nil {
		t.Fatal(err)
	}
	start, err := rt.Commit(ctx, CommitRequest{Command: startEnv})
	if err != nil {
		t.Fatal(err)
	}
	if start.Grant == "" {
		t.Fatal("start returned no grant")
	}

	clock = time.Unix(1002, 0)
	recoverEnv, err := BuildEnvelope("run-1", DeriveModelRecoveryCommandID("run-1", prep.StepID, claim), RecoverModelExecution{StepID: prep.StepID, Claim: claim})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Commit(ctx, CommitRequest{Command: recoverEnv}); err != nil {
		t.Fatal(err)
	}
	snap, err = rt.Load(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	ms, ok := snap.State.Current.(ModelStep)
	if !ok || ms.Status != ModelPrepared {
		t.Fatalf("after grantless recover: %+v", snap.State.Current)
	}
}

func TestZeroDeadlineRejectsGrantlessRecovery(t *testing.T) {
	rt := NewMemoryRuntime()
	ctx := context.Background()
	newRun, err := BuildNewRun("run-1", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Create(ctx, newRun); err != nil {
		t.Fatal(err)
	}
	in := AgentInput{ID: "seed", Payload: cj(`{"q":"hi"}`)}
	env, err := BuildEnvelope("run-1", DeriveInputCommandID("run-1", in.ID), AcceptInput{Input: in})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Commit(ctx, CommitRequest{Command: env}); err != nil {
		t.Fatal(err)
	}
	snap, _ := rt.Load(ctx, "run-1")
	prep, cmdID := buildPrepareFromSnap(t, snap, testRequest(), nil)
	if _, err := commitCmd(t, rt, cmdID, snap.Revision, "", prep); err != nil {
		t.Fatal(err)
	}
	claim := ExecutionClaim("claim-1")
	startEnv, _ := BuildEnvelope("run-1", "start-1", StartModelExecution{StepID: prep.StepID, Claim: claim})
	if _, err := rt.Commit(ctx, CommitRequest{Command: startEnv}); err != nil {
		t.Fatal(err)
	}
	recoverEnv, _ := BuildEnvelope("run-1", DeriveModelRecoveryCommandID("run-1", prep.StepID, claim), RecoverModelExecution{StepID: prep.StepID, Claim: claim})
	if _, err := rt.Commit(ctx, CommitRequest{Command: recoverEnv}); err == nil {
		t.Fatal("grantless recover accepted on non-expiring lease")
	}
}
