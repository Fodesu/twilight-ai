package runtimetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/sdk"
)

// RecoveryFactory constructs an empty Runtime whose leases use ttl and whose
// clock is now. Tests advance time by reassigning a captured time.Time.
type RecoveryFactory func(now func() time.Time, ttl time.Duration) run.Runtime

// RunRecoveryConformance covers lease expiry: grantless recover is rejected
// while a lease is live, RecoverExpired returns an Executing model to
// Prepared, and an expired Executing tool call is settled as Unknown without
// failing the Run. RunConformance stays on TTL 0.
func RunRecoveryConformance(t *testing.T, newRuntime RecoveryFactory) {
	t.Helper()
	t.Run("LiveLeaseRejectsGrantless", func(t *testing.T) { testRecoveryLiveLeaseRejectsGrantless(t, newRuntime) })
	t.Run("ZeroTTLRejectsGrantless", func(t *testing.T) { testRecoveryZeroTTLRejectsGrantless(t, newRuntime) })
	t.Run("ExpiredModelPrepared", func(t *testing.T) { testRecoveryExpiredModelPrepared(t, newRuntime) })
	t.Run("ExpiredToolUnknown", func(t *testing.T) { testRecoveryExpiredToolUnknown(t, newRuntime) })
	t.Run("ExpiredToolLeavesSiblingExecuting", func(t *testing.T) { testRecoveryExpiredToolLeavesSiblingExecuting(t, newRuntime) })
	t.Run("RecoverExpiredIdempotent", func(t *testing.T) { testRecoveryExpiredIdempotent(t, newRuntime) })
}

func recoveryPrepared(t testing.TB, newRuntime RecoveryFactory, ttl time.Duration, clock *time.Time, tools []sdk.ToolDefinition, specs []run.ToolSpec) (*conformanceCase, run.StepID, run.ExecutionGrant) {
	t.Helper()
	rt := newRuntime(func() time.Time { return *clock }, ttl)
	c := newCaseOnRuntime(t, rt, "run-1")
	snap := c.load()
	req := request(tools...)
	prep, cmdID := buildPrepareFromSnap(t, &snap, &req, specs)
	c.mustCommit(cmdID, snap.Revision, "", prep)
	start := c.mustCommit("start-1", 1, "", run.StartModelExecution{StepID: prep.StepID})
	if start.Grant == "" {
		t.Fatal("accepted start returned no grant")
	}
	return c, prep.StepID, start.Grant
}

func testRecoveryLiveLeaseRejectsGrantless(t *testing.T, newRuntime RecoveryFactory) {
	clock := time.Unix(1000, 0)
	c, stepID, _ := recoveryPrepared(t, newRuntime, time.Second, &clock, nil, nil)
	claim := startClaim("start-1")
	id := run.DeriveModelRecoveryCommandID(c.runID, stepID, claim)
	_, err := c.commit(id, 2, "", run.RecoverModelExecution{StepID: stepID, Claim: claim})
	if !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("grantless recover while live err = %v, want ErrStaleRuntime", err)
	}
	n, err := c.rt.RecoverExpired(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("RecoverExpired while live = %d, want 0", n)
	}
}

func testRecoveryZeroTTLRejectsGrantless(t *testing.T, newRuntime RecoveryFactory) {
	clock := time.Unix(1000, 0)
	c, stepID, _ := recoveryPrepared(t, newRuntime, 0, &clock, nil, nil)
	clock = time.Unix(1_000_000, 0)
	claim := startClaim("start-1")
	id := run.DeriveModelRecoveryCommandID(c.runID, stepID, claim)
	_, err := c.commit(id, 2, "", run.RecoverModelExecution{StepID: stepID, Claim: claim})
	if !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("grantless recover with zero TTL err = %v, want ErrStaleRuntime", err)
	}
	n, err := c.rt.RecoverExpired(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("RecoverExpired with zero TTL = %d, want 0", n)
	}
}

func testRecoveryExpiredModelPrepared(t *testing.T, newRuntime RecoveryFactory) {
	clock := time.Unix(1000, 0)
	c, _, _ := recoveryPrepared(t, newRuntime, time.Second, &clock, nil, nil)
	clock = time.Unix(1002, 0)
	n, err := c.rt.RecoverExpired(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("recovered = %d, want 1", n)
	}
	snap := c.load()
	if snap.State.RunID != c.runID {
		t.Fatalf("RunID = %q, want %q", snap.State.RunID, c.runID)
	}
	if snap.State.Status != run.RunActive {
		t.Fatalf("status = %v, want active", snap.State.Status)
	}
	ms, ok := snap.State.Current.(run.ModelStep)
	if !ok || ms.Status != run.ModelPrepared {
		t.Fatalf("after RecoverExpired: %+v", snap.State.Current)
	}
}

func testRecoveryExpiredToolUnknown(t *testing.T, newRuntime RecoveryFactory) {
	clock := time.Unix(1000, 0)
	def := toolDef("t")
	spec := makeSpec(t, def)
	c, stepID, grant := recoveryPrepared(t, newRuntime, time.Second, &clock, []sdk.ToolDefinition{def}, []run.ToolSpec{spec})
	b := makeBinding(t, "c1", &spec)
	opened := c.mustCommit("complete-1", 2, grant,
		run.SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []run.ToolCallBinding{b}})
	toolStep := openedToolStepID(t, &opened)
	c.mustCommit("start-c1", opened.Snapshot.Revision, "", run.StartToolCall{StepID: toolStep, CallID: "c1"})
	clock = time.Unix(1002, 0)
	n, err := c.rt.RecoverExpired(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("recovered = %d, want 1", n)
	}
	snap := c.load()
	if snap.State.Status != run.RunActive {
		t.Fatalf("status = %v, want active", snap.State.Status)
	}
	if _, ok := snap.State.Current.(run.Open); !ok {
		t.Fatalf("current = %+v, want Open", snap.State.Current)
	}
	if snap.State.LastToolStep == nil || len(snap.State.LastToolStep.Calls) != 1 {
		t.Fatalf("LastToolStep = %+v", snap.State.LastToolStep)
	}
	call := snap.State.LastToolStep.Calls[0]
	if call.Status != run.ToolFailed || call.Failure == nil || call.Failure.Outcome != run.ToolOutcomeUnknown || call.Failure.Failure.Class != run.FailureEffectUnknown {
		t.Fatalf("unknown call = %+v", call)
	}
}

func testRecoveryExpiredToolLeavesSiblingExecuting(t *testing.T, newRuntime RecoveryFactory) {
	clock := time.Unix(1000, 0)
	defA, defB := toolDef("a"), toolDef("b")
	specA := makeSpec(t, defA)
	specB := makeSpec(t, defB)
	c, stepID, grant := recoveryPrepared(t, newRuntime, time.Second, &clock,
		[]sdk.ToolDefinition{defA, defB}, []run.ToolSpec{specA, specB})
	bA := makeBinding(t, "cA", &specA)
	bB := makeBinding(t, "cB", &specB)
	r, err := run.FreezeModelResult(sdk.ModelResult{
		FinishReason: sdk.FinishReasonToolCalls,
		ToolCalls: []sdk.ToolCall{
			{ToolCallID: "cA", ToolName: "a", Input: `{}`},
			{ToolCallID: "cB", ToolName: "b", Input: `{}`},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	opened := c.mustCommit("complete-1", 2, grant,
		run.SubmitModelResult{StepID: stepID, Result: r, Calls: []run.ToolCallBinding{bA, bB}})
	toolStep := openedToolStepID(t, &opened)
	base := opened.Snapshot.Revision
	c.mustCommit("start-A", base, "", run.StartToolCall{StepID: toolStep, CallID: "cA"})
	clock = time.Unix(1000, 500_000_000)
	c.mustCommit("start-B", base, "", run.StartToolCall{StepID: toolStep, CallID: "cB"})
	clock = time.Unix(1001, 0)
	n, err := c.rt.RecoverExpired(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("recovered = %d, want 1", n)
	}
	snap := c.load()
	if snap.State.Status != run.RunActive {
		t.Fatalf("status = %v, want active", snap.State.Status)
	}
	ts, ok := snap.State.Current.(run.ToolStep)
	if !ok || len(ts.Calls) != 2 {
		t.Fatalf("current = %+v, want ToolStep with 2 calls", snap.State.Current)
	}
	if ts.Calls[0].Status != run.ToolFailed || ts.Calls[0].Failure == nil || ts.Calls[0].Failure.Outcome != run.ToolOutcomeUnknown {
		t.Fatalf("call A = %+v, want Unknown", ts.Calls[0])
	}
	if ts.Calls[1].Status != run.ToolExecuting {
		t.Fatalf("call B = %+v, want Executing", ts.Calls[1])
	}
}

func testRecoveryExpiredIdempotent(t *testing.T, newRuntime RecoveryFactory) {
	clock := time.Unix(1000, 0)
	c, _, _ := recoveryPrepared(t, newRuntime, time.Second, &clock, nil, nil)
	clock = time.Unix(1002, 0)
	n, err := c.rt.RecoverExpired(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("first recovered = %d, want 1", n)
	}
	n, err = c.rt.RecoverExpired(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("second recovered = %d, want 0", n)
	}
	snap := c.load()
	ms, ok := snap.State.Current.(run.ModelStep)
	if !ok || ms.Status != run.ModelPrepared {
		t.Fatalf("after second RecoverExpired: %+v", snap.State.Current)
	}
}

// RunLeaseRenewalConformance covers Runtime.RenewLease: a renewed lease is
// not recovered at its original deadline, a stale or foreign grant cannot
// renew, and renewal after settlement is rejected.
func RunLeaseRenewalConformance(t *testing.T, newRuntime RecoveryFactory) {
	t.Helper()
	t.Run("RenewExtendsDeadline", func(t *testing.T) { testRenewExtendsDeadline(t, newRuntime) })
	t.Run("RenewRejectsWrongGrant", func(t *testing.T) { testRenewRejectsWrongGrant(t, newRuntime) })
	t.Run("RenewAfterSettlementRejected", func(t *testing.T) { testRenewAfterSettlementRejected(t, newRuntime) })
}

func testRenewExtendsDeadline(t *testing.T, newRuntime RecoveryFactory) {
	clock := time.Unix(1000, 0)
	c, stepID, grant := recoveryPrepared(t, newRuntime, time.Second, &clock, nil, nil)
	ctx := context.Background()
	clock = time.Unix(1000, 800_000_000)
	if err := c.rt.RenewLease(ctx, c.runID, stepID, "", grant); err != nil {
		t.Fatalf("renew: %v", err)
	}
	// Past the original deadline, before the renewed one: nothing to recover.
	clock = time.Unix(1001, 500_000_000)
	n, err := c.rt.RecoverExpired(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("recovered = %d after renewal, want 0", n)
	}
	snap := c.load()
	if ms, ok := snap.State.Current.(run.ModelStep); !ok || ms.Status != run.ModelExecuting {
		t.Fatalf("current = %+v, want Executing", snap.State.Current)
	}
	// The owner's result is still accepted with the same grant.
	c.mustCommit("complete-1", snap.Revision, grant, run.SubmitModelResult{StepID: stepID, Result: modelResultWithCalls()})
	// Past the renewed deadline the lease is gone with the settlement.
	clock = time.Unix(1003, 0)
	n, err = c.rt.RecoverExpired(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("recovered = %d after settlement, want 0", n)
	}
}

func testRenewRejectsWrongGrant(t *testing.T, newRuntime RecoveryFactory) {
	clock := time.Unix(1000, 0)
	c, stepID, _ := recoveryPrepared(t, newRuntime, time.Second, &clock, nil, nil)
	ctx := context.Background()
	if err := c.rt.RenewLease(ctx, c.runID, stepID, "", "not-the-grant"); !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("wrong grant renew err = %v, want ErrStaleRuntime", err)
	}
	if err := c.rt.RenewLease(ctx, c.runID, stepID, "", ""); !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("empty grant renew err = %v, want ErrStaleRuntime", err)
	}
	if err := c.rt.RenewLease(ctx, "missing", stepID, "", "g"); !errors.Is(err, run.ErrRunNotFound) {
		t.Fatalf("missing run renew err = %v, want ErrRunNotFound", err)
	}
}

func testRenewAfterSettlementRejected(t *testing.T, newRuntime RecoveryFactory) {
	clock := time.Unix(1000, 0)
	c, stepID, grant := recoveryPrepared(t, newRuntime, time.Second, &clock, nil, nil)
	ctx := context.Background()
	snap := c.load()
	c.mustCommit("complete-1", snap.Revision, grant, run.SubmitModelResult{StepID: stepID, Result: modelResultWithCalls()})
	if err := c.rt.RenewLease(ctx, c.runID, stepID, "", grant); !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("renew after settlement err = %v, want ErrStaleRuntime", err)
	}
}
