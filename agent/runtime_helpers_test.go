package agent

import (
	"context"
	"testing"

	"github.com/memohai/twilight-ai/sdk"
)

// Shared helpers for package-local agent tests. Runtime conformance lives in
// agent/runtimetest so durable Runtime implementations can reuse it.

func newTestRuntime(t *testing.T, cfg RunConfig) *MemoryRuntime {
	t.Helper()
	s, err := Initialize("run-1", cfg, NextRun(AgentInput{ID: "seed", Payload: cj(`{"q":"hi"}`)}))
	if err != nil {
		t.Fatal(err)
	}
	return NewMemoryRuntime(s)
}

func commitCmd(t *testing.T, rt Runtime, id CommandID, base uint64, grant ExecutionGrant, cmd AgentCommand) (CommitResult, error) {
	t.Helper()
	env, err := BuildEnvelope("run-1", id, cmd)
	if err != nil {
		t.Fatal(err)
	}
	return rt.Commit(context.Background(), CommitRequest{BaseRevision: base, Grant: grant, Command: env})
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
	rt := newTestRuntime(t, RunConfig{Model: "m-1", ModelRejectLimit: 2})
	snap, _ := rt.Load(context.Background())
	req := testRequest(tools...)
	prep, cmdID := buildPrepareFromSnap(t, snap, req, specs)
	mustCommit(t, rt, cmdID, snap.Revision, "", prep)
	start := mustCommit(t, rt, "start-1", 1, "", StartModelExecution{StepID: prep.StepID})
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
	reqDigest, err := DigestRequest(frozenReq)
	if err != nil {
		t.Fatal(err)
	}
	toolsDigest, err := DigestToolSpecs(specs)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := DigestModelStepBinding(snap.State.Config.Model, reqDigest, toolsDigest)
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
		StepID: stepID, Model: snap.State.Config.Model, Request: frozenReq,
		RequestDigest: reqDigest, InputIDs: ids, Tools: specs, ToolsDigest: toolsDigest,
	}, cmdID
}

func TestLoopPrepareRejectionDoesNotLivelock(t *testing.T) {
	// A planner that omits pending InputIDs produces a Prepare the authority
	// rejects at the same revision forever; the Loop must surface an error
	// instead of spinning (loop.go planAndPrepare guard).
	rt := newTestRuntime(t, RunConfig{Model: "m-1"})
	loop, err := NewLoop(fakeCatalog{&fakeInvoker{}}, fakeToolCatalog{},
		badPlanner{}, ExecutionPolicy{}, false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = loop.Run(context.Background(), rt, nil)
	if err == nil {
		t.Fatal("prepare livelock not surfaced")
	}
}

// badPlanner never consumes pending inputs, so its Prepare is always rejected.
type badPlanner struct{}

func (badPlanner) Plan(_ context.Context, hint PlanningHint) (RequestPlan, error) {
	return RequestPlan{Model: hint.Model, Request: sdk.Request{Model: string(hint.Model)}}, nil
}
