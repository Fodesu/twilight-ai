package run

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestRunExpiredRecoveryRejectsInvalidArgs(t *testing.T) {
	rt := NewRuntime(NewMemoryStore())
	if err := RunExpiredRecovery(context.Background(), nil, time.Second); err == nil {
		t.Fatal("nil runtime accepted")
	}
	if err := RunExpiredRecovery(context.Background(), rt, 0); err == nil {
		t.Fatal("zero interval accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := RunExpiredRecovery(ctx, rt, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ctx err = %v, want context.Canceled", err)
	}
}

func TestRunExpiredRecoveryRecoversExpiredModel(t *testing.T) {
	var mu sync.Mutex
	clock := time.Unix(1000, 0)
	now := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return clock
	}
	set := func(ts time.Time) {
		mu.Lock()
		defer mu.Unlock()
		clock = ts
	}
	rt := NewRuntimeWithOptions(NewMemoryStore(), RuntimeOptions{LeaseTTL: time.Second, Now: now})
	ctx := context.Background()
	newRun, err := BuildNewRun("run-1", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Create(ctx, newRun); err != nil {
		t.Fatal(err)
	}
	in := AgentInput{ID: "seed", Payload: cj(`{"q":"hi"}`)}
	env, err := ProtocolV1.BuildEnvelope("run-1", DeriveInputCommandID("run-1", in.ID), AcceptInput{Input: in})
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
	startEnv := startEnvelope(t, "run-1", StartModelExecution{StepID: prep.StepID, Claim: "claim-scan"})
	if _, err := rt.Commit(ctx, CommitRequest{Command: startEnv}); err != nil {
		t.Fatal(err)
	}

	loopCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() { errCh <- RunExpiredRecovery(loopCtx, rt, 10*time.Millisecond) }()
	set(time.Unix(1002, 0))

	deadline := time.Now().Add(2 * time.Second)
	for {
		snap, err := rt.Load(ctx, "run-1")
		if err != nil {
			t.Fatal(err)
		}
		if ms, ok := snap.State.Current.(ModelStep); ok && ms.Status == ModelPrepared {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("model still %+v", snap.State.Current)
		}
		select {
		case err := <-errCh:
			t.Fatalf("recovery loop exited: %v", err)
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("loop err = %v, want context.Canceled", err)
	}
}
