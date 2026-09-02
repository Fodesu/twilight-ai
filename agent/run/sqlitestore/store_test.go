package sqlitestore_test

import (
	"context"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/run/runtimetest"
	"github.com/memohai/twilight/agent/run/sqlitestore"
	"github.com/memohai/twilight/sdk"
)

func TestOpenRejectsEmptyPath(t *testing.T) {
	if _, err := sqlitestore.Open(""); err == nil {
		t.Fatal("empty path accepted")
	}
}

func sqliteFactory(t *testing.T) runtimetest.Factory {
	t.Helper()
	var n atomic.Int64
	dir := t.TempDir()
	return func() run.Runtime {
		path := filepath.Join(dir, "runs-"+strconv.FormatInt(n.Add(1), 10)+".db")
		store, err := sqlitestore.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return run.NewRuntime(store)
	}
}

func sqliteRecoveryFactory(t *testing.T) runtimetest.RecoveryFactory {
	t.Helper()
	var n atomic.Int64
	dir := t.TempDir()
	return func(now func() time.Time, ttl time.Duration) run.Runtime {
		path := filepath.Join(dir, "recover-"+strconv.FormatInt(n.Add(1), 10)+".db")
		store, err := sqlitestore.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return run.NewRuntimeWithOptions(store, run.RuntimeOptions{LeaseTTL: ttl, Now: now})
	}
}

func TestSQLiteRuntimeConformance(t *testing.T) {
	runtimetest.RunConformance(t, sqliteFactory(t))
}

func TestSQLiteRuntimeRecovery(t *testing.T) {
	runtimetest.RunRecoveryConformance(t, sqliteRecoveryFactory(t))
}

func TestSQLiteRuntimeLeaseRenewal(t *testing.T) {
	runtimetest.RunLeaseRenewalConformance(t, sqliteRecoveryFactory(t))
}

// The stored snapshot is authoritative for Load: a reopened database serves
// Load without touching the transition log, and Record still verifies the
// snapshot against the full fold.
func TestSQLiteReopenLoadsSnapshotAndRecordVerifies(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.db")
	store, err := sqlitestore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	rt := run.NewRuntime(store)
	ctx := context.Background()
	newRun, err := run.BuildNewRun("run-1", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Create(ctx, newRun); err != nil {
		t.Fatal(err)
	}
	startExecutingTool(t, rt, "run-1")
	before, err := rt.Load(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sqlitestore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	rt2 := run.NewRuntime(reopened)
	after, err := rt2.Load(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	a, _ := run.ProtocolV1.EncodeMachineState(&before.State)
	b, _ := run.ProtocolV1.EncodeMachineState(&after.State)
	if string(a) != string(b) || before.Revision != after.Revision {
		t.Fatalf("reopened snapshot differs:\n before %s\n after  %s", a, b)
	}
	record, err := rt2.Record(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if record.Snapshot.Revision != after.Revision || len(record.Transitions) != int(after.Revision) {
		t.Fatalf("record = revision %d transitions %d", record.Snapshot.Revision, len(record.Transitions))
	}
	if diverged, err := run.Rebuild(ctx, reopened, "run-1"); err != nil || diverged {
		t.Fatalf("rebuild diverged=%v err=%v", diverged, err)
	}
}

func TestSQLiteCrashReopenRecoversExpiredModel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crash.db")
	clock := time.Unix(1000, 0)
	store, err := sqlitestore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	rt := run.NewRuntimeWithOptions(store, run.RuntimeOptions{
		LeaseTTL: time.Second,
		Now:      func() time.Time { return clock },
	})
	ctx := context.Background()
	newRun, err := run.BuildNewRun("run-1", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Create(ctx, newRun); err != nil {
		t.Fatal(err)
	}
	stepID := startExecutingModel(t, rt, "run-1")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := sqlitestore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	rt2 := run.NewRuntimeWithOptions(reopened, run.RuntimeOptions{
		LeaseTTL: time.Second,
		Now:      func() time.Time { return clock },
	})
	snap, err := rt2.Load(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	ms, ok := snap.State.Current.(run.ModelStep)
	if !ok || ms.Status != run.ModelExecuting || ms.RefValue.ID != stepID {
		t.Fatalf("after reopen: %+v", snap.State.Current)
	}

	clock = time.Unix(1002, 0)
	n, err := rt2.RecoverExpired(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("recovered = %d, want 1", n)
	}
	snap, err = rt2.Load(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if snap.State.RunID != "run-1" || snap.State.Status != run.RunActive {
		t.Fatalf("after recover: %+v", snap.State)
	}
	ms, ok = snap.State.Current.(run.ModelStep)
	if !ok || ms.Status != run.ModelPrepared {
		t.Fatalf("after recover current: %+v", snap.State.Current)
	}
}

func TestSQLiteCrashReopenSettlesExpiredToolUnknown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crash-tool.db")
	clock := time.Unix(1000, 0)
	store, err := sqlitestore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	rt := run.NewRuntimeWithOptions(store, run.RuntimeOptions{
		LeaseTTL: time.Second,
		Now:      func() time.Time { return clock },
	})
	ctx := context.Background()
	newRun, err := run.BuildNewRun("run-1", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Create(ctx, newRun); err != nil {
		t.Fatal(err)
	}
	startExecutingTool(t, rt, "run-1")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	clock = time.Unix(1002, 0)
	reopened, err := sqlitestore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	rt2 := run.NewRuntimeWithOptions(reopened, run.RuntimeOptions{
		LeaseTTL: time.Second,
		Now:      func() time.Time { return clock },
	})
	snap, err := rt2.Load(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if snap.State.Status != run.RunActive {
		t.Fatalf("status after reopen = %v", snap.State.Status)
	}
	if _, ok := snap.State.Current.(run.ToolStep); !ok {
		t.Fatalf("after reopen current = %+v, want ToolStep", snap.State.Current)
	}
	n, err := rt2.RecoverExpired(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("recovered = %d, want 1", n)
	}
	snap, err = rt2.Load(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if snap.State.RunID != "run-1" || snap.State.Status != run.RunActive {
		t.Fatalf("after recover: %+v", snap.State)
	}
	if _, ok := snap.State.Current.(run.Open); !ok {
		t.Fatalf("after recover current = %+v, want Open", snap.State.Current)
	}
	if snap.State.LastToolStep == nil || len(snap.State.LastToolStep.Calls) != 1 {
		t.Fatalf("LastToolStep = %+v", snap.State.LastToolStep)
	}
	call := snap.State.LastToolStep.Calls[0]
	if call.Status != run.ToolFailed || call.Failure == nil || call.Failure.Outcome != run.ToolOutcomeUnknown {
		t.Fatalf("unknown call = %+v", call)
	}
}

func startExecutingModel(t *testing.T, rt run.Runtime, runID run.RunID) run.StepID {
	t.Helper()
	ctx := context.Background()
	snap, err := rt.Load(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	req := sdk.Request{Model: "m-1", Messages: []sdk.Message{sdk.UserMessage("hi")}}
	prep, cmdID := prepareFromSnap(t, snap, req, nil)
	if _, err := commit(t, rt, runID, cmdID, snap.Revision, "", prep); err != nil {
		t.Fatal(err)
	}
	start, err := commit(t, rt, runID, "start-1", 1, "", run.StartModelExecution{StepID: prep.StepID, Claim: "claim-crash"})
	if err != nil {
		t.Fatal(err)
	}
	if start.Grant == "" {
		t.Fatal("start returned no grant")
	}
	return prep.StepID
}

func startExecutingTool(t *testing.T, rt run.Runtime, runID run.RunID) {
	t.Helper()
	def := sdk.ToolDefinition{Name: "t", Parameters: []byte(`{"type":"object"}`)}
	spec := freezeSpec(t, def)
	ctx := context.Background()
	snap, err := rt.Load(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	req := sdk.Request{Model: "m-1", Messages: []sdk.Message{sdk.UserMessage("hi")}, Tools: []sdk.ToolDefinition{def}}
	prep, cmdID := prepareFromSnap(t, snap, req, []run.ToolSpec{spec})
	if _, err := commit(t, rt, runID, cmdID, snap.Revision, "", prep); err != nil {
		t.Fatal(err)
	}
	start, err := commit(t, rt, runID, "start-1", 1, "", run.StartModelExecution{StepID: prep.StepID, Claim: "claim-crash"})
	if err != nil {
		t.Fatal(err)
	}
	args := run.MustParseCanonicalJSON(`{}`)
	bd, err := run.DigestToolCallBinding("c1", spec.DefinitionDigest, spec.Policy, args)
	if err != nil {
		t.Fatal(err)
	}
	binding := run.ToolCallBinding{
		CallID: "c1", ToolRef: spec.Ref, DefinitionDigest: spec.DefinitionDigest,
		BindingDigest: bd, Arguments: args, Policy: spec.Policy,
	}
	result := sdk.ModelResult{
		FinishReason: sdk.FinishReasonToolCalls,
		Usage:        sdk.Usage{InputTokens: 10, OutputTokens: 5, TotalTokens: 15},
		ToolCalls:    []sdk.ToolCall{{ToolCallID: "c1", ToolName: "t", Input: `{}`}},
	}
	frozen, err := run.FreezeModelResult(result)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := commit(t, rt, runID, "complete-1", 2, start.Grant, run.SubmitModelResult{
		StepID: prep.StepID, Result: frozen, Calls: []run.ToolCallBinding{binding},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(opened.Events) < 2 {
		t.Fatalf("events = %d, want ToolStepOpened", len(opened.Events))
	}
	openedFact, ok := opened.Events[1].Fact.(run.ToolStepOpened)
	if !ok {
		t.Fatalf("event[1] = %T, want ToolStepOpened", opened.Events[1].Fact)
	}
	if _, err := commit(t, rt, runID, "start-c1", opened.Snapshot.Revision, "", run.StartToolCall{
		StepID: openedFact.StepID, CallID: "c1", Claim: "claim-tool",
	}); err != nil {
		t.Fatal(err)
	}
}

func freezeSpec(t *testing.T, def sdk.ToolDefinition) run.ToolSpec {
	t.Helper()
	frozen, err := run.FreezeToolDefinition(def)
	if err != nil {
		t.Fatal(err)
	}
	d, err := run.ProtocolV1.DigestToolDefinition(frozen)
	if err != nil {
		t.Fatal(err)
	}
	return run.ToolSpec{Ref: run.ToolRef(def.Name), Definition: frozen, DefinitionDigest: d, Policy: run.DirectExecution}
}

func prepareFromSnap(t *testing.T, snap run.RuntimeSnapshot, req sdk.Request, specs []run.ToolSpec) (run.PrepareModelRequest, run.CommandID) {
	t.Helper()
	frozenReq, err := run.FreezeModelRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	reqDigest, err := run.ProtocolV1.DigestRequest(frozenReq)
	if err != nil {
		t.Fatal(err)
	}
	toolsDigest, err := run.ProtocolV1.DigestToolSpecs(specs)
	if err != nil {
		t.Fatal(err)
	}
	model := run.ModelRef(frozenReq.Model)
	binding, err := run.ProtocolV1.DigestModelStepBinding(model, reqDigest, toolsDigest)
	if err != nil {
		t.Fatal(err)
	}
	cmdID := run.DeriveModelRequestCommandID(snap.State.RunID, snap.Revision)
	stepID := run.DeriveModelStepID(snap.State.RunID, cmdID, binding)
	ids := make([]run.InputID, len(snap.State.PendingInputs))
	for i, in := range snap.State.PendingInputs {
		ids[i] = in.ID
	}
	return run.PrepareModelRequest{
		StepID: stepID, Model: model, Request: frozenReq,
		RequestDigest: reqDigest, InputIDs: ids, Tools: specs, ToolsDigest: toolsDigest,
	}, cmdID
}

func commit(t *testing.T, rt run.Runtime, runID run.RunID, id run.CommandID, base uint64, grant run.ExecutionGrant, cmd run.AgentCommand) (run.CommitResult, error) {
	t.Helper()
	env, err := run.ProtocolV1.BuildEnvelope(runID, id, cmd)
	if err != nil {
		t.Fatal(err)
	}
	return rt.Commit(context.Background(), run.CommitRequest{BaseRevision: base, Grant: grant, Command: env})
}
