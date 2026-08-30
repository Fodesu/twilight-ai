// Package runtimetest contains the shared run.Runtime conformance suite.
// Durable Runtime implementations run this suite in their own tests instead
// of copying MemoryRuntime-specific assertions:
//
//	func TestMyRuntimeConformance(t *testing.T) {
//	    runtimetest.RunConformance(t, func() run.Runtime { return newMyRuntime() })
//	}
//
// The suite exercises only the public run API, so it holds for any Runtime
// that honors the contract (spec §14.3): command idempotency, revision/index
// assignment, grant lifecycle, call-local rebase, prepare hard-CAS, terminal
// arbitration, and replay-fold equivalence.
package runtimetest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/sdk"
)

// Factory constructs an empty Runtime under test.
type Factory func() run.Runtime

// RunConformance executes the shared Runtime conformance suite.
func RunConformance(t *testing.T, newRuntime Factory) {
	t.Helper()
	t.Run("CreateReturnsValidDetachedHeader", func(t *testing.T) { testCreateReturnsValidDetachedHeader(t, newRuntime) })
	t.Run("CreateRetryAndConflict", func(t *testing.T) { testCreateRetryAndConflict(t, newRuntime) })
	t.Run("MissingRunOperations", func(t *testing.T) { testMissingRunOperations(t, newRuntime) })
	t.Run("RecordRevisionZero", func(t *testing.T) { testRecordRevisionZero(t, newRuntime) })
	t.Run("RecordFoldsAcceptedTransition", func(t *testing.T) { testRecordFoldsAcceptedTransition(t, newRuntime) })
	t.Run("ReturnedValuesAreDetached", func(t *testing.T) { testReturnedValuesAreDetached(t, newRuntime) })
	t.Run("ConcurrentCreate", func(t *testing.T) { testConcurrentCreate(t, newRuntime) })
	t.Run("RunsAreIsolated", func(t *testing.T) { testRunsAreIsolated(t, newRuntime) })
	t.Run("CrossRunGrantIsRejected", func(t *testing.T) { testCrossRunGrantIsRejected(t, newRuntime) })
	t.Run("RecordConcurrentWithCommit", func(t *testing.T) { testRecordConcurrentWithCommit(t, newRuntime) })
	t.Run("IdempotentReplay", func(t *testing.T) { testIdempotentReplay(t, newRuntime) })
	t.Run("RevisionAndIndex", func(t *testing.T) { testRevisionAndIndex(t, newRuntime) })
	t.Run("StartGrantLifecycle", func(t *testing.T) { testStartGrantLifecycle(t, newRuntime) })
	t.Run("CallLocalRebase", func(t *testing.T) { testCallLocalRebase(t, newRuntime) })
	t.Run("PrepareDerivedIdentity", func(t *testing.T) { testPrepareDerivedIdentity(t, newRuntime) })
	t.Run("PrepareIsHardCAS", func(t *testing.T) { testPrepareIsHardCAS(t, newRuntime) })
	t.Run("CancelRebasesAndUnknownWins", func(t *testing.T) { testCancelRebasesAndUnknownWins(t, newRuntime) })
	t.Run("CancelOnStaleBase", func(t *testing.T) { testCancelOnStaleBase(t, newRuntime) })
	t.Run("ReplayFoldMatchesState", func(t *testing.T) { testReplayFoldMatchesState(t, newRuntime) })
	t.Run("AcceptInputByInputID", func(t *testing.T) { testAcceptInputByInputID(t, newRuntime) })
	t.Run("DerivedCommandIDEnforced", func(t *testing.T) { testDerivedCommandIDEnforced(t, newRuntime) })
}

func testCreateReturnsValidDetachedHeader(t *testing.T, newRuntime Factory) {
	rt := newRuntime()
	newRun, err := run.BuildNewRun("run-create", "cause-1")
	if err != nil {
		t.Fatal(err)
	}
	created, err := rt.Create(context.Background(), newRun)
	if err != nil || !created.Created {
		t.Fatalf("first Create = %+v, %v", created, err)
	}
	if err := run.ValidateRunHeader(&created.Header); err != nil {
		t.Fatalf("Create returned invalid Header: %v", err)
	}
	created.Header.InitialState.RunID = "mutated"
	record, err := rt.Record(context.Background(), "run-create")
	if err != nil {
		t.Fatal(err)
	}
	if record.Header.InitialState.RunID != "run-create" {
		t.Fatal("Create Header aliases runtime state")
	}
}

func testCreateRetryAndConflict(t *testing.T, newRuntime Factory) {
	rt := newRuntime()
	first, err := run.BuildNewRun("run-create", "cause-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Create(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	retry, err := rt.Create(context.Background(), first)
	if err != nil || retry.Created {
		t.Fatalf("retry Create = %+v, %v", retry, err)
	}
	retry.Header.InitialState.RunID = "mutated"
	record, err := rt.Record(context.Background(), first.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Header.InitialState.RunID != first.RunID {
		t.Fatal("retry Create Header aliases runtime state")
	}
	conflict, err := run.BuildNewRun("run-create", "cause-2")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Create(context.Background(), conflict); !errors.Is(err, run.ErrCreateConflict) {
		t.Fatalf("conflicting Create error = %v, want ErrCreateConflict", err)
	}
}

func testMissingRunOperations(t *testing.T, newRuntime Factory) {
	rt := newRuntime()
	if _, err := rt.Load(context.Background(), "missing"); !errors.Is(err, run.ErrRunNotFound) {
		t.Fatalf("Load error = %v, want ErrRunNotFound", err)
	}
	if _, err := rt.Record(context.Background(), "missing"); !errors.Is(err, run.ErrRunNotFound) {
		t.Fatalf("Record error = %v, want ErrRunNotFound", err)
	}
	env, err := run.BuildEnvelope("missing", "cancel", run.CancelRun{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Commit(context.Background(), run.CommitRequest{Command: env}); !errors.Is(err, run.ErrRunNotFound) {
		t.Fatalf("Commit error = %v, want ErrRunNotFound", err)
	}
}

func testRecordRevisionZero(t *testing.T, newRuntime Factory) {
	c := newCase(t, newRuntime)
	record, err := c.rt.Record(context.Background(), c.runID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Snapshot.Revision != 0 || len(record.Transitions) != 0 {
		t.Fatalf("revision-zero Record = %+v", record)
	}
}

func testRecordFoldsAcceptedTransition(t *testing.T, newRuntime Factory) {
	c := newCase(t, newRuntime)
	input := run.AgentInput{ID: "in-1", Payload: mustJSON(`{"q":"hi"}`)}
	res := c.mustCommit(run.DeriveInputCommandID(c.runID, input.ID), 0, "", run.AcceptInput{Input: input})
	if res.Status != run.CommitAccepted {
		t.Fatalf("accept status = %v", res.Status)
	}
	record, err := c.rt.Record(context.Background(), c.runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Transitions) != 1 {
		t.Fatalf("transitions = %d, want 1", len(record.Transitions))
	}
	folded, revision, err := run.FoldRun(&record.Header, record.Transitions)
	if err != nil {
		t.Fatal(err)
	}
	foldedJSON, _ := json.Marshal(stateComparable(&folded))
	snapshotJSON, _ := json.Marshal(stateComparable(&record.Snapshot.State))
	if revision != record.Snapshot.Revision || !bytes.Equal(foldedJSON, snapshotJSON) {
		t.Fatalf("FoldRun = revision %d state %s, Record = revision %d state %s", revision, foldedJSON, record.Snapshot.Revision, snapshotJSON)
	}
}

func testReturnedValuesAreDetached(t *testing.T, newRuntime Factory) {
	c := newCase(t, newRuntime)
	newRun, err := run.BuildNewRun(c.runID, "")
	if err != nil {
		t.Fatal(err)
	}
	created, err := c.rt.Create(context.Background(), newRun)
	if err != nil || created.Created {
		t.Fatalf("retry Create = %+v, %v", created, err)
	}
	created.Header.InitialState.RunID = "mutated"
	input := run.AgentInput{ID: "in-1", Payload: mustJSON(`1`)}
	res := c.mustCommit(run.DeriveInputCommandID(c.runID, input.ID), 0, "", run.AcceptInput{Input: input})
	res.Snapshot.State.RunID = "mutated"
	if len(res.Events) > 0 {
		res.Events[0].RunID = "mutated"
	}
	record, err := c.rt.Record(context.Background(), c.runID)
	if err != nil {
		t.Fatal(err)
	}
	record.Header.InitialState.RunID = "mutated"
	record.Snapshot.State.RunID = "mutated"
	record.Transitions[0].Events[0].RunID = "mutated"
	fresh, err := c.rt.Record(context.Background(), c.runID)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Header.InitialState.RunID != c.runID || fresh.Snapshot.State.RunID != c.runID || fresh.Transitions[0].Events[0].RunID != c.runID {
		t.Fatal("returned Header, Snapshot, or Transitions alias runtime state")
	}
}

func testConcurrentCreate(t *testing.T, newRuntime Factory) {
	rt := newRuntime()
	newRun, err := run.BuildNewRun("run-concurrent-create", "cause-1")
	if err != nil {
		t.Fatal(err)
	}
	const callers = 16
	results := make(chan run.CreateResult, callers)
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
	for err := range errs {
		t.Fatal(err)
	}
	created := 0
	for result := range results {
		if result.Created {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("Created=true results = %d, want 1", created)
	}
}

func testRunsAreIsolated(t *testing.T, newRuntime Factory) {
	rt := newRuntime()
	for _, id := range []run.RunID{"run-one", "run-two"} {
		newRun, err := run.BuildNewRun(id, "")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := rt.Create(context.Background(), newRun); err != nil {
			t.Fatal(err)
		}
		input := run.AgentInput{ID: "same-input", Payload: mustJSON(`{"run":"` + string(id) + `"}`)}
		env, err := run.BuildEnvelope(id, run.DeriveInputCommandID(id, input.ID), run.AcceptInput{Input: input})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := rt.Commit(context.Background(), run.CommitRequest{Command: env}); err != nil {
			t.Fatal(err)
		}
	}
	one, err := rt.Record(context.Background(), "run-one")
	if err != nil {
		t.Fatal(err)
	}
	two, err := rt.Record(context.Background(), "run-two")
	if err != nil {
		t.Fatal(err)
	}
	if len(one.Snapshot.State.PendingInputs) != 1 || len(two.Snapshot.State.PendingInputs) != 1 ||
		one.Snapshot.State.PendingInputs[0].Payload.String() == two.Snapshot.State.PendingInputs[0].Payload.String() {
		t.Fatalf("runs leaked state or commands: one=%+v two=%+v", one.Snapshot.State.PendingInputs, two.Snapshot.State.PendingInputs)
	}
	for _, record := range []run.RunRecord{one, two} {
		if len(record.Transitions) != 1 || record.Transitions[0].RunID != record.Header.RunID {
			t.Fatalf("run %q has foreign transition log: %+v", record.Header.RunID, record.Transitions)
		}
		for _, event := range record.Transitions[0].Events {
			if event.RunID != record.Header.RunID {
				t.Fatalf("run %q has foreign event %+v", record.Header.RunID, event)
			}
		}
	}
}

func testCrossRunGrantIsRejected(t *testing.T, newRuntime Factory) {
	rt := newRuntime()
	one := newCaseOnRuntime(t, rt, "grant-run-one")
	two := newCaseOnRuntime(t, rt, "grant-run-two")
	stepOne, grantOne := prepareAndStart(t, one)
	stepTwo, grantTwo := prepareAndStart(t, two)
	result, err := run.FreezeModelResult(sdk.ModelResult{Text: "ok"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := two.commit("foreign-grant", 2, grantOne, run.SubmitModelResult{StepID: stepTwo, Result: result}); !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("cross-run grant error = %v, want ErrStaleRuntime", err)
	}
	one.mustCommit("complete-one", 2, grantOne, run.SubmitModelResult{StepID: stepOne, Result: result})
	two.mustCommit("complete-two", 2, grantTwo, run.SubmitModelResult{StepID: stepTwo, Result: result})
}

func testRecordConcurrentWithCommit(t *testing.T, newRuntime Factory) {
	rt := newRuntime()
	c := newCaseOnRuntime(t, rt, "record-concurrent")
	const commits = 16
	for i := range commits {
		snapshot, err := rt.Load(context.Background(), c.runID)
		if err != nil {
			t.Fatal(err)
		}
		input := run.AgentInput{ID: run.InputID(fmt.Sprintf("in-%d", i)), Payload: mustJSON(`null`)}
		env, err := run.BuildEnvelope(c.runID, run.DeriveInputCommandID(c.runID, input.ID), run.AcceptInput{Input: input})
		if err != nil {
			t.Fatal(err)
		}

		start := make(chan struct{})
		var ready sync.WaitGroup
		ready.Add(2)
		commitCh := make(chan error, 1)
		recordCh := make(chan struct {
			record run.RunRecord
			err    error
		}, 1)
		go func() {
			ready.Done()
			<-start
			_, err := rt.Commit(context.Background(), run.CommitRequest{BaseRevision: snapshot.Revision, Command: env})
			commitCh <- err
		}()
		go func() {
			ready.Done()
			<-start
			record, err := rt.Record(context.Background(), c.runID)
			recordCh <- struct {
				record run.RunRecord
				err    error
			}{record: record, err: err}
		}()
		ready.Wait()
		close(start)

		if err := <-commitCh; err != nil {
			t.Fatal(err)
		}
		observed := <-recordCh
		if observed.err != nil {
			t.Fatal(observed.err)
		}
		folded, revision, err := run.FoldRun(&observed.record.Header, observed.record.Transitions)
		if err != nil {
			t.Fatal(err)
		}
		foldedJSON, _ := json.Marshal(stateComparable(&folded))
		snapshotJSON, _ := json.Marshal(stateComparable(&observed.record.Snapshot.State))
		if revision != observed.record.Snapshot.Revision || !bytes.Equal(foldedJSON, snapshotJSON) {
			t.Fatalf("inconsistent Record at revision %d", observed.record.Snapshot.Revision)
		}
		if wantBefore, wantAfter := snapshot.Revision, snapshot.Revision+1; revision != wantBefore && revision != wantAfter {
			t.Fatalf("concurrent Record revision = %d, want %d or %d", revision, wantBefore, wantAfter)
		}
	}
	final, err := rt.Record(context.Background(), c.runID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Snapshot.Revision != commits || len(final.Transitions) != commits {
		t.Fatalf("final Record = revision %d transitions %d, want %d", final.Snapshot.Revision, len(final.Transitions), commits)
	}
}

type conformanceCase struct {
	t       testing.TB
	runID   run.RunID
	initial run.MachineState
	rt      run.Runtime
	events  []run.AgentEvent
}

func newCase(t testing.TB, newRuntime Factory) *conformanceCase {
	t.Helper()
	return newCaseOnRuntime(t, newRuntime(), "run-1")
}

func newCaseOnRuntime(t testing.TB, rt run.Runtime, runID run.RunID) *conformanceCase {
	t.Helper()
	newRun, err := run.BuildNewRun(runID, "")
	if err != nil {
		t.Fatal(err)
	}
	created, err := rt.Create(context.Background(), newRun)
	if err != nil {
		t.Fatal(err)
	}
	return &conformanceCase{
		t: t, runID: runID, initial: created.Header.InitialState, rt: rt,
	}
}

func (c *conformanceCase) load() run.RuntimeSnapshot {
	c.t.Helper()
	snap, err := c.rt.Load(context.Background(), c.runID)
	if err != nil {
		c.t.Fatal(err)
	}
	return snap
}

func (c *conformanceCase) commit(id run.CommandID, base uint64, grant run.ExecutionGrant, cmd run.AgentCommand) (run.CommitResult, error) {
	c.t.Helper()
	env, err := run.BuildEnvelope(c.runID, id, cmd)
	if err != nil {
		c.t.Fatal(err)
	}
	res, err := c.rt.Commit(context.Background(), run.CommitRequest{BaseRevision: base, Grant: grant, Command: env})
	if err == nil && res.Status == run.CommitAccepted {
		c.events = append(c.events, res.Events...)
	}
	return res, err
}

func (c *conformanceCase) mustCommit(id run.CommandID, base uint64, grant run.ExecutionGrant, cmd run.AgentCommand) run.CommitResult {
	c.t.Helper()
	res, err := c.commit(id, base, grant, cmd)
	if err != nil {
		c.t.Fatalf("commit %T: %v", cmd, err)
	}
	return res
}

func preparedCase(t testing.TB, newRuntime Factory, tools []sdk.ToolDefinition, specs []run.ToolSpec) (*conformanceCase, run.StepID, run.ExecutionGrant) {
	t.Helper()
	c := newCase(t, newRuntime)
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

func prepareAndStart(t testing.TB, c *conformanceCase) (run.StepID, run.ExecutionGrant) {
	t.Helper()
	snap := c.load()
	req := request()
	prep, cmdID := buildPrepareFromSnap(t, &snap, &req, nil)
	c.mustCommit(cmdID, snap.Revision, "", prep)
	start := c.mustCommit(run.CommandID("start-"+string(c.runID)), snap.Revision+1, "", run.StartModelExecution{StepID: prep.StepID})
	if start.Grant == "" {
		t.Fatal("accepted start returned no grant")
	}
	return prep.StepID, start.Grant
}

func buildPrepareFromSnap(t testing.TB, snap *run.RuntimeSnapshot, req *sdk.Request, specs []run.ToolSpec) (run.PrepareModelRequest, run.CommandID) {
	t.Helper()
	frozenReq, err := run.FreezeModelRequest(*req)
	if err != nil {
		t.Fatal(err)
	}
	reqDigest, err := run.DigestRequest(frozenReq)
	if err != nil {
		t.Fatal(err)
	}
	toolsDigest, err := run.DigestToolSpecs(specs)
	if err != nil {
		t.Fatal(err)
	}
	model := run.ModelRef(frozenReq.Model)
	binding, err := run.DigestModelStepBinding(model, reqDigest, toolsDigest)
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

func request(tools ...sdk.ToolDefinition) sdk.Request {
	return sdk.Request{
		Model:    "m-1",
		Messages: []sdk.Message{sdk.UserMessage("hi")},
		Tools:    tools,
	}
}

func toolDef(name string) sdk.ToolDefinition {
	return sdk.ToolDefinition{Name: name, Parameters: json.RawMessage(`{"type":"object"}`)}
}

func mustJSON(raw string) run.CanonicalJSON { return run.MustParseCanonicalJSON(raw) }

func makeSpec(t testing.TB, def sdk.ToolDefinition) run.ToolSpec {
	t.Helper()
	frozen, err := run.FreezeToolDefinition(def)
	if err != nil {
		t.Fatal(err)
	}
	d, err := run.DigestToolDefinition(frozen)
	if err != nil {
		t.Fatal(err)
	}
	return run.ToolSpec{Ref: run.ToolRef(def.Name), Definition: frozen, DefinitionDigest: d, Policy: run.DirectExecution}
}

func makeBinding(t testing.TB, callID string, spec *run.ToolSpec) run.ToolCallBinding {
	t.Helper()
	parsedArgs := mustJSON(`{}`)
	bd, err := run.DigestToolCallBinding(run.CallID(callID), spec.DefinitionDigest, spec.Policy, parsedArgs)
	if err != nil {
		t.Fatal(err)
	}
	return run.ToolCallBinding{
		CallID:           run.CallID(callID),
		ToolRef:          spec.Ref,
		DefinitionDigest: spec.DefinitionDigest,
		BindingDigest:    bd,
		Arguments:        parsedArgs,
		Policy:           spec.Policy,
	}
}

func openedToolStepID(t testing.TB, res *run.CommitResult) run.StepID {
	t.Helper()
	if len(res.Events) < 2 {
		t.Fatalf("events = %d, want ToolStepOpened at index 1", len(res.Events))
	}
	opened, ok := res.Events[1].Fact.(run.ToolStepOpened)
	if !ok {
		t.Fatalf("event[1] fact = %T, want ToolStepOpened", res.Events[1].Fact)
	}
	return opened.StepID
}

func modelResultWithCalls(callIDs ...string) run.ModelResult {
	r := sdk.ModelResult{
		FinishReason: sdk.FinishReasonToolCalls,
		Usage:        sdk.Usage{InputTokens: 10, OutputTokens: 5, TotalTokens: 15},
	}
	for _, id := range callIDs {
		r.ToolCalls = append(r.ToolCalls, sdk.ToolCall{ToolCallID: id, ToolName: "t", Input: `{}`})
	}
	frozen, err := run.FreezeModelResult(r)
	if err != nil {
		panic(err)
	}
	return frozen
}

func testIdempotentReplay(t *testing.T, newRuntime Factory) {
	c := newCase(t, newRuntime)
	res1 := c.mustCommit("cancel-1", 0, "", run.CancelRun{})
	if res1.Status != run.CommitAccepted || len(res1.Events) != 1 {
		t.Fatalf("res1 = %+v", res1)
	}
	res2 := c.mustCommit("cancel-1", 0, "", run.CancelRun{})
	if res2.Status != run.CommitAlreadyApplied {
		t.Fatalf("status = %v", res2.Status)
	}
	if len(res2.Events) != 1 || res2.Events[0].Digest != res1.Events[0].Digest ||
		res2.Events[0].Revision != res1.Events[0].Revision {
		t.Fatal("replay did not return the original event group")
	}
	if res2.Snapshot.Revision != res1.Snapshot.Revision {
		t.Fatal("replay advanced the revision")
	}
	_, err := c.commit("cancel-1", 0, "", run.CancelRun{Reason: "other"})
	if !errors.Is(err, run.ErrCommandConflict) {
		t.Fatalf("err = %v, want ErrCommandConflict", err)
	}
}

func testRevisionAndIndex(t *testing.T, newRuntime Factory) {
	def := toolDef("t")
	spec := makeSpec(t, def)
	c, stepID, grant := preparedCase(t, newRuntime, []sdk.ToolDefinition{def}, []run.ToolSpec{spec})

	b := makeBinding(t, "c1", &spec)
	res := c.mustCommit("complete-1", 2, grant,
		run.SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []run.ToolCallBinding{b}})
	if len(res.Events) != 2 {
		t.Fatalf("events = %d", len(res.Events))
	}
	for i, e := range res.Events {
		if e.Revision != res.Snapshot.Revision {
			t.Fatalf("event revision %d != snapshot %d", e.Revision, res.Snapshot.Revision)
		}
		if int(e.Index) != i {
			t.Fatalf("index[%d] = %d", i, e.Index)
		}
		if e.CommandID != "complete-1" {
			t.Fatal("command id not stamped")
		}
	}
}

func testStartGrantLifecycle(t *testing.T, newRuntime Factory) {
	c, stepID, grant := preparedCase(t, newRuntime, nil, nil)

	_, err := c.commit("done-x", 2, "", run.SubmitModelResult{StepID: stepID, Result: run.ModelResult{}})
	if !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("grantless completion err = %v, want ErrStaleRuntime", err)
	}
	res := c.mustCommit("start-1", 1, "", run.StartModelExecution{StepID: stepID})
	if res.Status != run.CommitAlreadyApplied || res.Grant != "" {
		t.Fatalf("replayed start: %+v", res)
	}
	ok, err := run.FreezeModelResult(sdk.ModelResult{Text: "ok"})
	if err != nil {
		t.Fatal(err)
	}
	res = c.mustCommit("done-1", 2, grant, run.SubmitModelResult{StepID: stepID, Result: ok})
	if res.Status != run.CommitAccepted || res.Snapshot.State.Status != run.RunCompleted {
		t.Fatalf("completion: %+v", res.Snapshot.State.Status)
	}
}

func testCallLocalRebase(t *testing.T, newRuntime Factory) {
	defA, defB := toolDef("a"), toolDef("b")
	specA := makeSpec(t, defA)
	specB := makeSpec(t, defB)
	c, stepID, grant := preparedCase(t, newRuntime, []sdk.ToolDefinition{defA, defB}, []run.ToolSpec{specA, specB})

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
	res := c.mustCommit("complete-1", 2, grant,
		run.SubmitModelResult{StepID: stepID, Result: r, Calls: []run.ToolCallBinding{bA, bB}})
	toolStep := openedToolStepID(t, &res)
	base := res.Snapshot.Revision

	startA := c.mustCommit("start-A", base, "", run.StartToolCall{StepID: toolStep, CallID: "cA"})
	startB := c.mustCommit("start-B", base, "", run.StartToolCall{StepID: toolStep, CallID: "cB"})
	if startB.Status != run.CommitAccepted || startB.Grant == "" {
		t.Fatal("stale-base start of an untouched Pending call must rebase")
	}
	doneA := c.mustCommit("done-A", base, startA.Grant,
		run.SubmitToolResult{StepID: toolStep, CallID: "cA", Result: run.ToolExecutionResult{Output: mustJSON(`1`)}})
	if doneA.Status != run.CommitAccepted {
		t.Fatal("owner completion on stale base must rebase")
	}
	_, err = c.commit("start-A2", base, "", run.StartToolCall{StepID: toolStep, CallID: "cA"})
	if !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("restart of settled call err = %v, want ErrStaleRuntime", err)
	}
}

func testPrepareDerivedIdentity(t *testing.T, newRuntime Factory) {
	c := newCase(t, newRuntime)
	snap := c.load()
	req := request()
	prep, cmdID := buildPrepareFromSnap(t, &snap, &req, nil)

	if _, err := c.commit("wrong-prepare-id", snap.Revision, "", prep); err == nil {
		t.Fatal("PrepareModelRequest accepted a non-derived CommandID")
	}

	bad := prep
	bad.StepID = "wrong-step"
	_, err := c.commit(cmdID, snap.Revision, "", bad)
	if !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("bad prepare StepID err = %v, want ErrStaleRuntime", err)
	}
}

func testPrepareIsHardCAS(t *testing.T, newRuntime Factory) {
	c := newCase(t, newRuntime)
	snap := c.load()
	req := request()
	prep, cmdID := buildPrepareFromSnap(t, &snap, &req, nil)
	c.mustCommit(cmdID, snap.Revision, "", prep)

	otherReq := request()
	otherReq.System = "different"
	prep2, cmdID2 := buildPrepareFromSnap(t, &snap, &otherReq, nil)
	if cmdID2 != cmdID {
		t.Fatal("same revision must derive the same command id")
	}
	_, err := c.commit(cmdID2, snap.Revision, "", prep2)
	if !errors.Is(err, run.ErrCommandConflict) {
		t.Fatalf("err = %v, want ErrCommandConflict", err)
	}
	res := c.mustCommit(cmdID, snap.Revision, "", prep)
	if res.Status != run.CommitAlreadyApplied {
		t.Fatalf("status = %v", res.Status)
	}
}

func testCancelRebasesAndUnknownWins(t *testing.T, newRuntime Factory) {
	def := toolDef("t")
	spec := makeSpec(t, def)
	c, stepID, grant := preparedCase(t, newRuntime, []sdk.ToolDefinition{def}, []run.ToolSpec{spec})
	b := makeBinding(t, "c1", &spec)
	res := c.mustCommit("complete-1", 2, grant,
		run.SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []run.ToolCallBinding{b}})
	toolStep := openedToolStepID(t, &res)
	startRes := c.mustCommit("start-c1", res.Snapshot.Revision, "", run.StartToolCall{StepID: toolStep, CallID: "c1"})

	unknown := c.mustCommit("unk-1", startRes.Snapshot.Revision, startRes.Grant,
		run.SubmitToolFailure{StepID: toolStep, CallID: "c1", Outcome: run.ToolOutcomeUnknown})
	if unknown.Snapshot.State.Status != run.RunFailed {
		t.Fatal("unknown did not fail the run")
	}
	_, err := c.commit("cancel-late", 0, "", run.CancelRun{})
	if !errors.Is(err, run.ErrRunTerminal) {
		t.Fatalf("late cancel err = %v, want ErrRunTerminal", err)
	}
}

func testCancelOnStaleBase(t *testing.T, newRuntime Factory) {
	c, _, _ := preparedCase(t, newRuntime, nil, nil)
	res := c.mustCommit("cancel-1", 0, "", run.CancelRun{})
	if res.Status != run.CommitAccepted || res.Snapshot.State.Status != run.RunStopped {
		t.Fatalf("cancel: %+v", res.Snapshot.State.Status)
	}
}

func testReplayFoldMatchesState(t *testing.T, newRuntime Factory) {
	def := toolDef("t")
	spec := makeSpec(t, def)
	c, stepID, grant := preparedCase(t, newRuntime, []sdk.ToolDefinition{def}, []run.ToolSpec{spec})
	b := makeBinding(t, "c1", &spec)
	res := c.mustCommit("complete-1", 2, grant,
		run.SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []run.ToolCallBinding{b}})
	toolStep := openedToolStepID(t, &res)
	sRes := c.mustCommit("start-c1", res.Snapshot.Revision, "", run.StartToolCall{StepID: toolStep, CallID: "c1"})
	c.mustCommit("done-c1", sRes.Snapshot.Revision, sRes.Grant,
		run.SubmitToolResult{StepID: toolStep, CallID: "c1", Result: run.ToolExecutionResult{Output: mustJSON(`"ok"`)}})

	folded, lastRev, err := run.FoldEvents(c.initial, c.events)
	if err != nil {
		t.Fatalf("FoldEvents: %v", err)
	}
	live := c.load()
	a, _ := json.Marshal(stateComparable(&live.State))
	bts, _ := json.Marshal(stateComparable(&folded))
	if !bytes.Equal(a, bts) {
		t.Fatalf("replay diverged:\n live   %s\n replay %s", a, bts)
	}
	if live.Revision != lastRev {
		t.Fatalf("snapshot revision %d != last event revision %d", live.Revision, lastRev)
	}
}

func testAcceptInputByInputID(t *testing.T, newRuntime Factory) {
	c := newCase(t, newRuntime)
	in := run.AgentInput{ID: "in-9", Payload: mustJSON(`{"t":"x"}`)}
	id := run.DeriveInputCommandID("run-1", in.ID)
	res1 := c.mustCommit(id, 0, "", run.NextStep(in))
	if res1.Status != run.CommitAccepted {
		t.Fatal("first accept rejected")
	}
	res2 := c.mustCommit(id, 0, "", run.NextStep(in))
	if res2.Status != run.CommitAlreadyApplied {
		t.Fatalf("status = %v", res2.Status)
	}
	_, err := c.commit(id, 0, "", run.NextStep(run.AgentInput{ID: "in-9", Payload: mustJSON(`{"t":"y"}`)}))
	if !errors.Is(err, run.ErrCommandConflict) {
		t.Fatalf("err = %v, want ErrCommandConflict", err)
	}
}

func testDerivedCommandIDEnforced(t *testing.T, newRuntime Factory) {
	c := newCase(t, newRuntime)
	_, err := c.commit("random-id", 0, "", run.NextStep(run.AgentInput{ID: "in-1", Payload: mustJSON(`1`)}))
	if err == nil {
		t.Fatal("AcceptInput with non-derived CommandID accepted")
	}
	_, err = c.commit("random-id-2", 0, "", run.ApproveToolCall{StepID: "s", CallID: "c", ResponseID: "r"})
	if err == nil {
		t.Fatal("ApproveToolCall with non-derived CommandID accepted")
	}
}

func stateComparable(s *run.MachineState) map[string]any {
	m := map[string]any{
		"runId": s.RunID, "status": s.Status,
		"modelSteps": s.ModelSteps, "lastClosedStep": s.LastClosedStep,
		"usage": s.Usage, "pendingInputs": s.PendingInputs,
		"lastModelResult": s.LastModelResult, "result": s.Result,
	}
	switch cur := s.Current.(type) {
	case run.ModelStep:
		m["modelStep"] = cur
	case run.ToolStep:
		m["toolStep"] = cur
	}
	return m
}
