package run

import (
	"context"
	"testing"

	"github.com/memohai/twilight/sdk"
)

// Shared helpers for package-local agent tests. Runtime conformance lives in
// agent/runtimetest so durable Runtime implementations can reuse it.

func newTestRuntime(t *testing.T) Runtime {
	t.Helper()
	rt := NewRuntime(NewMemoryStore())
	newRun, err := BuildNewRun("run-1", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Create(context.Background(), newRun); err != nil {
		t.Fatal(err)
	}
	// Seed inputs must enter through the public Runtime boundary so the
	// fixture has the same Revision-0 header and transition history as a Run.
	acceptInput(t, rt, "run-1", AgentInput{ID: "seed", Payload: cj(`{"q":"hi"}`)})
	return rt
}

func runtimeStore(t testing.TB, rt Runtime) Store {
	t.Helper()
	r, ok := rt.(*runtime)
	if !ok {
		t.Fatalf("runtime %T is not *runtime", rt)
	}
	return r.store
}

func rebuildRun(t testing.TB, rt Runtime, runID RunID) (bool, error) {
	t.Helper()
	return Rebuild(context.Background(), runtimeStore(t, rt), runID)
}

func recordEvents(t testing.TB, rt Runtime, runID RunID) []AgentEvent {
	t.Helper()
	record, err := rt.Record(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	return flattenTransitionRecords(record.Transitions)
}

func memoryEntry(t testing.TB, rt Runtime) *memoryRun {
	t.Helper()
	store, ok := runtimeStore(t, rt).(*MemoryStore)
	if !ok {
		t.Fatal("runtime is not backed by MemoryStore")
	}
	entry, err := store.entry("run-1")
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func commitCmd(t *testing.T, rt Runtime, id CommandID, base uint64, grant ExecutionGrant, cmd AgentCommand) (CommitResult, error) {
	t.Helper()
	cmd, id = withTestExecutionClaim("run-1", id, cmd)
	env, err := ProtocolV1().BuildEnvelope("run-1", id, cmd)
	if err != nil {
		t.Fatal(err)
	}
	return rt.Commit(context.Background(), CommitRequest{BaseRevision: base, Grant: grant, Command: env})
}

// withTestExecutionClaim treats id as the attempt label of a start command
// and derives claim and CommandID from it; see runtimetest.
func withTestExecutionClaim(runID RunID, id CommandID, cmd AgentCommand) (AgentCommand, CommandID) {
	claim := ExecutionClaim("test-claim/" + string(id))
	switch c := cmd.(type) {
	case StartModelExecution:
		if c.Claim == "" {
			c.Claim = claim
		}
		return c, DeriveStartCommandID(runID, c.StepID, "", c.Claim)
	case StartToolCall:
		if c.Claim == "" {
			c.Claim = claim
		}
		return c, DeriveStartCommandID(runID, c.StepID, c.CallID, c.Claim)
	default:
		return cmd, id
	}
}

// startEnvelope builds a start command's envelope under its derived id.
func startEnvelope(t testing.TB, runID RunID, cmd AgentCommand) CommandEnvelope {
	t.Helper()
	cmd, id := withTestExecutionClaim(runID, "start", cmd)
	env, err := ProtocolV1().BuildEnvelope(runID, id, cmd)
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func mustCommit(t *testing.T, rt Runtime, id CommandID, base uint64, grant ExecutionGrant, cmd AgentCommand) CommitResult {
	t.Helper()
	res, err := commitCmd(t, rt, id, base, grant, cmd)
	if err != nil {
		t.Fatalf("commit %T: %v", cmd, err)
	}
	return res
}

// preparedRuntime returns a runtime advanced to an Executing ModelStep, plus
// stepID and the model grant.
func preparedRuntime(t *testing.T, tools []sdk.ToolDefinition, specs []ToolSpec) (Runtime, StepID, ExecutionGrant) {
	t.Helper()
	rt := newTestRuntime(t)
	snap, _ := rt.Load(context.Background(), "run-1")
	req := testRequest(tools...)
	prep, cmdID := buildPrepareFromSnap(t, snap, req, specs)
	prepared := mustCommit(t, rt, cmdID, snap.Revision, "", prep)
	start := mustCommit(t, rt, "start-1", prepared.Snapshot.Revision, "", StartModelExecution{StepID: prep.StepID})
	if start.Grant == "" {
		t.Fatal("accepted start returned no grant")
	}
	return rt, prep.StepID, start.Grant
}

func buildPrepareFromSnap(t *testing.T, snap RuntimeSnapshot, req sdk.Request, specs []ToolSpec) (PrepareModelRequest, CommandID) {
	t.Helper()
	frozenReq, err := FreezeModelRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	reqDigest, err := ProtocolV1().DigestRequest(frozenReq)
	if err != nil {
		t.Fatal(err)
	}
	toolsDigest, err := ProtocolV1().DigestToolSpecs(specs)
	if err != nil {
		t.Fatal(err)
	}
	model := ModelRef(frozenReq.Model)
	binding, err := ProtocolV1().DigestModelStepBinding(model, reqDigest, toolsDigest)
	if err != nil {
		t.Fatal(err)
	}
	cmdID := DeriveModelRequestCommandID(snap.State.RunID, snap.Revision)
	stepID := DeriveModelStepID(snap.State.RunID, cmdID, binding)
	ids := make([]InputID, len(snap.State.PendingInputs))
	for i, in := range snap.State.PendingInputs {
		ids[i] = in.ID
	}
	return PrepareModelRequest{
		StepID: stepID, Model: model, Request: frozenReq,
		RequestDigest: reqDigest, InputIDs: ids, Tools: specs, ToolsDigest: toolsDigest,
	}, cmdID
}
