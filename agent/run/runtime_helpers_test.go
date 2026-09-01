package run

import (
	"context"
	"testing"

	"github.com/memohai/twilight/sdk"
)

// Shared helpers for package-local agent tests. Runtime conformance lives in
// agent/runtimetest so durable Runtime implementations can reuse it.

func newTestRuntime(t *testing.T) *MemoryRuntime {
	t.Helper()
	rt := NewMemoryRuntime()
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

func recordEvents(t testing.TB, rt Runtime, runID RunID) []AgentEvent {
	t.Helper()
	record, err := rt.Record(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	return flattenTransitionRecords(record.Transitions)
}

func memoryEntry(t testing.TB, rt *MemoryRuntime) *memoryRun {
	t.Helper()
	entry, err := rt.entry("run-1")
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func commitCmd(t *testing.T, rt Runtime, id CommandID, base uint64, grant ExecutionGrant, cmd AgentCommand) (CommitResult, error) {
	t.Helper()
	cmd = withTestExecutionClaim(id, cmd)
	env, err := BuildEnvelope("run-1", id, cmd)
	if err != nil {
		t.Fatal(err)
	}
	return rt.Commit(context.Background(), CommitRequest{BaseRevision: base, Grant: grant, Command: env})
}

func withTestExecutionClaim(id CommandID, cmd AgentCommand) AgentCommand {
	claim := ExecutionClaim("test-claim/" + string(id))
	switch c := cmd.(type) {
	case StartModelExecution:
		if c.Claim == "" {
			c.Claim = claim
		}
		return c
	case StartToolCall:
		if c.Claim == "" {
			c.Claim = claim
		}
		return c
	default:
		return cmd
	}
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
func preparedRuntime(t *testing.T, tools []sdk.ToolDefinition, specs []ToolSpec) (*MemoryRuntime, StepID, ExecutionGrant) {
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
	reqDigest, err := DigestRequest(SchemaVersion1, frozenReq)
	if err != nil {
		t.Fatal(err)
	}
	toolsDigest, err := DigestToolSpecs(SchemaVersion1, specs)
	if err != nil {
		t.Fatal(err)
	}
	model := ModelRef(frozenReq.Model)
	binding, err := DigestModelStepBinding(SchemaVersion1, model, reqDigest, toolsDigest)
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
