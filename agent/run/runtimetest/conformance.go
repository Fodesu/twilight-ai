// Package runtimetest contains the shared run.Runtime conformance suite.
// Durable Runtime implementations run this suite in their own tests instead
// of copying MemoryStore-backed assertions:
//
//	func TestMyRuntimeConformance(t *testing.T) {
//	    runtimetest.RunConformance(t, func() run.Runtime { return newMyRuntime() })
//	}
//
// The suite exercises only the public run API, so it holds for any Runtime
// that honors the contract (RUN-CMP-2). Eight groups cover Create, Record,
// Isolation, CommandIdentity, Prepare, Grant, CallLocalRebase, and Cancel.
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

// RunConformance executes the shared Runtime conformance suite (RUN-CMP-2).
// Group names are a directory; each check is a separate function with its own Runtime.
func RunConformance(t *testing.T, newRuntime Factory) {
	t.Helper()
	t.Run("Create", func(t *testing.T) {
		t.Run("RetryAndConflict", func(t *testing.T) { testCreateRetryAndConflict(t, newRuntime) })
		t.Run("Missing", func(t *testing.T) { testCreateMissing(t, newRuntime) })
		t.Run("Concurrent", func(t *testing.T) { testCreateConcurrent(t, newRuntime) })
	})
	t.Run("Record", func(t *testing.T) {
		t.Run("FoldAccepted", func(t *testing.T) { testRecordFoldAccepted(t, newRuntime) })
		t.Run("ReplayAndIndex", func(t *testing.T) { testRecordReplayAndIndex(t, newRuntime) })
		t.Run("ConcurrentWithCommit", func(t *testing.T) { testRecordConcurrentWithCommit(t, newRuntime) })
	})
	t.Run("Isolation", func(t *testing.T) {
		t.Run("DetachedReturns", func(t *testing.T) { testIsolationDetachedReturns(t, newRuntime) })
		t.Run("Runs", func(t *testing.T) { testIsolationRuns(t, newRuntime) })
		t.Run("CrossRunGrant", func(t *testing.T) { testIsolationCrossRunGrant(t, newRuntime) })
	})
	t.Run("CommandIdentity", func(t *testing.T) {
		t.Run("IdempotentReplay", func(t *testing.T) { testCommandIdempotentReplay(t, newRuntime) })
		t.Run("DerivedIDs", func(t *testing.T) { testCommandDerivedIDs(t, newRuntime) })
	})
	t.Run("Prepare", func(t *testing.T) {
		t.Run("HardCAS", func(t *testing.T) { testPrepareHardCAS(t, newRuntime) })
		t.Run("ReplayAfterProgress", func(t *testing.T) { testPrepareReplayAfterProgress(t, newRuntime) })
	})
	t.Run("Grant", func(t *testing.T) {
		t.Run("Lifecycle", func(t *testing.T) { testGrant(t, newRuntime) })
		t.Run("HolderRecover", func(t *testing.T) { testGrantHolderRecover(t, newRuntime) })
		t.Run("GrantlessRejectedWhileLive", func(t *testing.T) { testGrantGrantlessRejectedWhileLive(t, newRuntime) })
	})
	t.Run("CallLocalRebase", func(t *testing.T) { testCallLocalRebase(t, newRuntime) })
	t.Run("Cancel", func(t *testing.T) {
		t.Run("StaleBase", func(t *testing.T) { testCancelStaleBase(t, newRuntime) })
		t.Run("AfterUnknown", func(t *testing.T) { testCancelAfterUnknown(t, newRuntime) })
		t.Run("UnknownRemaining", func(t *testing.T) { testCancelUnknownRemaining(t, newRuntime) })
	})
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
	cmd, id = withTestExecutionClaim(c.runID, id, cmd)
	env, err := run.ProtocolV1().BuildEnvelope(c.runID, id, cmd)
	if err != nil {
		c.t.Fatal(err)
	}
	res, err := c.rt.Commit(context.Background(), run.CommitRequest{BaseRevision: base, Grant: grant, Command: env})
	if err == nil && res.Status == run.CommitAccepted {
		c.events = append(c.events, res.Events...)
	}
	return res, err
}

// withTestExecutionClaim treats id as the attempt label of a start command:
// the claim derives from the label and the CommandID derives from the claim
// (RUN-WIR-3), so a test that replays "start-1" hits the same identity.
// Non-start commands keep id verbatim.
func withTestExecutionClaim(runID run.RunID, id run.CommandID, cmd run.AgentCommand) (run.AgentCommand, run.CommandID) {
	switch c := cmd.(type) {
	case run.StartModelExecution:
		if c.Claim == "" {
			c.Claim = startClaim(id)
		}
		return c, run.DeriveStartCommandID(runID, c.StepID, "", c.Claim)
	case run.StartToolCall:
		if c.Claim == "" {
			c.Claim = startClaim(id)
		}
		return c, run.DeriveStartCommandID(runID, c.StepID, c.CallID, c.Claim)
	default:
		return cmd, id
	}
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
	reqDigest, err := run.ProtocolV1().DigestRequest(frozenReq)
	if err != nil {
		t.Fatal(err)
	}
	toolsDigest, err := run.ProtocolV1().DigestToolSpecs(specs)
	if err != nil {
		t.Fatal(err)
	}
	model := run.ModelRef(frozenReq.Model)
	binding, err := run.ProtocolV1().DigestModelStepBinding(model, reqDigest, toolsDigest)
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
	d, err := run.ProtocolV1().DigestToolDefinition(frozen)
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

func testCreateRetryAndConflict(t *testing.T, newRuntime Factory) {
	rt := newRuntime()
	first, err := run.BuildNewRun("run-create", "cause-1")
	if err != nil {
		t.Fatal(err)
	}
	created, err := rt.Create(context.Background(), first)
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
	retry, err := rt.Create(context.Background(), first)
	if err != nil || retry.Created {
		t.Fatalf("retry Create = %+v, %v", retry, err)
	}
	retry.Header.InitialState.RunID = "mutated"
	record, err = rt.Record(context.Background(), first.RunID)
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

func testCreateMissing(t *testing.T, newRuntime Factory) {
	rt := newRuntime()
	if _, err := rt.Load(context.Background(), "missing"); !errors.Is(err, run.ErrRunNotFound) {
		t.Fatalf("Load error = %v, want ErrRunNotFound", err)
	}
	if _, err := rt.Record(context.Background(), "missing"); !errors.Is(err, run.ErrRunNotFound) {
		t.Fatalf("Record error = %v, want ErrRunNotFound", err)
	}
	env, err := run.ProtocolV1().BuildEnvelope("missing", "cancel", run.CancelRun{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Commit(context.Background(), run.CommitRequest{Command: env}); !errors.Is(err, run.ErrRunNotFound) {
		t.Fatalf("Commit error = %v, want ErrRunNotFound", err)
	}
}

func testCreateConcurrent(t *testing.T, newRuntime Factory) {
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
	createdCount := 0
	for result := range results {
		if result.Created {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("Created=true results = %d, want 1", createdCount)
	}
}

func testRecordFoldAccepted(t *testing.T, newRuntime Factory) {
	c := newCase(t, newRuntime)
	record, err := c.rt.Record(context.Background(), c.runID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Snapshot.Revision != 0 || len(record.Transitions) != 0 {
		t.Fatalf("revision-zero Record = %+v", record)
	}
	input := run.AgentInput{ID: "in-1", Payload: mustJSON(`{"q":"hi"}`)}
	res := c.mustCommit(run.DeriveInputCommandID(c.runID, input.ID), 0, "", run.AcceptInput{Input: input})
	if res.Status != run.CommitAccepted {
		t.Fatalf("accept status = %v", res.Status)
	}
	record, err = c.rt.Record(context.Background(), c.runID)
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
	foldedJSON := encodeState(t, &folded)
	snapshotJSON := encodeState(t, &record.Snapshot.State)
	if revision != record.Snapshot.Revision || !bytes.Equal(foldedJSON, snapshotJSON) {
		t.Fatalf("FoldRun = revision %d state %s, Record = revision %d state %s", revision, foldedJSON, record.Snapshot.Revision, snapshotJSON)
	}
}

func testRecordReplayAndIndex(t *testing.T, newRuntime Factory) {
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
	toolStep := openedToolStepID(t, &res)
	sRes := c.mustCommit("start-c1", res.Snapshot.Revision, "", run.StartToolCall{StepID: toolStep, CallID: "c1"})
	c.mustCommit("done-c1", sRes.Snapshot.Revision, sRes.Grant,
		run.SubmitToolResult{StepID: toolStep, CallID: "c1", Result: run.ToolExecutionResult{Output: mustJSON(`"ok"`)}})
	foldedState, lastRev, err := run.FoldEvents(c.initial, c.events)
	if err != nil {
		t.Fatalf("FoldEvents: %v", err)
	}
	live := c.load()
	a := encodeState(t, &live.State)
	bts := encodeState(t, &foldedState)
	if !bytes.Equal(a, bts) {
		t.Fatalf("replay diverged:\n live   %s\n replay %s", a, bts)
	}
	if live.Revision != lastRev {
		t.Fatalf("snapshot revision %d != last event revision %d", live.Revision, lastRev)
	}
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
		in := run.AgentInput{ID: run.InputID(fmt.Sprintf("in-%d", i)), Payload: mustJSON(`null`)}
		env, err := run.ProtocolV1().BuildEnvelope(c.runID, run.DeriveInputCommandID(c.runID, in.ID), run.AcceptInput{Input: in})
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
			rec, err := rt.Record(context.Background(), c.runID)
			recordCh <- struct {
				record run.RunRecord
				err    error
			}{record: rec, err: err}
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
		foldedRun, rev, err := run.FoldRun(&observed.record.Header, observed.record.Transitions)
		if err != nil {
			t.Fatal(err)
		}
		foldedJSON := encodeState(t, &foldedRun)
		snapshotJSON := encodeState(t, &observed.record.Snapshot.State)
		if rev != observed.record.Snapshot.Revision || !bytes.Equal(foldedJSON, snapshotJSON) {
			t.Fatalf("inconsistent Record at revision %d", observed.record.Snapshot.Revision)
		}
		if wantBefore, wantAfter := snapshot.Revision, snapshot.Revision+1; rev != wantBefore && rev != wantAfter {
			t.Fatalf("concurrent Record revision = %d, want %d or %d", rev, wantBefore, wantAfter)
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

func testIsolationDetachedReturns(t *testing.T, newRuntime Factory) {
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

func testIsolationRuns(t *testing.T, newRuntime Factory) {
	rt := newRuntime()
	for _, id := range []run.RunID{"run-one", "run-two"} {
		nr, err := run.BuildNewRun(id, "")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := rt.Create(context.Background(), nr); err != nil {
			t.Fatal(err)
		}
		in := run.AgentInput{ID: "same-input", Payload: mustJSON(`{"run":"` + string(id) + `"}`)}
		env, err := run.ProtocolV1().BuildEnvelope(id, run.DeriveInputCommandID(id, in.ID), run.AcceptInput{Input: in})
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
	for _, rec := range []run.RunRecord{one, two} {
		if len(rec.Transitions) != 1 || rec.Transitions[0].RunID != rec.Header.RunID {
			t.Fatalf("run %q has foreign transition log: %+v", rec.Header.RunID, rec.Transitions)
		}
		for _, event := range rec.Transitions[0].Events {
			if event.RunID != rec.Header.RunID {
				t.Fatalf("run %q has foreign event %+v", rec.Header.RunID, event)
			}
		}
	}
}

func testIsolationCrossRunGrant(t *testing.T, newRuntime Factory) {
	rt := newRuntime()
	oneCase := newCaseOnRuntime(t, rt, "grant-run-one")
	twoCase := newCaseOnRuntime(t, rt, "grant-run-two")
	stepOne, grantOne := prepareAndStart(t, oneCase)
	stepTwo, grantTwo := prepareAndStart(t, twoCase)
	result, err := run.FreezeModelResult(sdk.ModelResult{Text: "ok"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := twoCase.commit("foreign-grant", 2, grantOne, run.SubmitModelResult{StepID: stepTwo, Result: result}); !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("cross-run grant error = %v, want ErrStaleRuntime", err)
	}
	oneCase.mustCommit("complete-one", 2, grantOne, run.SubmitModelResult{StepID: stepOne, Result: result})
	twoCase.mustCommit("complete-two", 2, grantTwo, run.SubmitModelResult{StepID: stepTwo, Result: result})
}

func testCommandIdempotentReplay(t *testing.T, newRuntime Factory) {
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

func testCommandDerivedIDs(t *testing.T, newRuntime Factory) {
	c := newCase(t, newRuntime)
	_, err := c.commit("random-id", 0, "", run.NextStep(run.AgentInput{ID: "in-1", Payload: mustJSON(`1`)}))
	if err == nil {
		t.Fatal("AcceptInput with non-derived CommandID accepted")
	}
	_, err = c.commit("random-id-2", 0, "", run.ApproveToolCall{StepID: "s", CallID: "c", ResponseID: "r"})
	if err == nil {
		t.Fatal("ApproveToolCall with non-derived CommandID accepted")
	}
	in := run.AgentInput{ID: "in-9", Payload: mustJSON(`{"t":"x"}`)}
	id := run.DeriveInputCommandID("run-1", in.ID)
	accepted := c.mustCommit(id, 0, "", run.NextStep(in))
	if accepted.Status != run.CommitAccepted {
		t.Fatal("first accept rejected")
	}
	replayed := c.mustCommit(id, 0, "", run.NextStep(in))
	if replayed.Status != run.CommitAlreadyApplied {
		t.Fatalf("status = %v", replayed.Status)
	}
	_, err = c.commit(id, 0, "", run.NextStep(run.AgentInput{ID: "in-9", Payload: mustJSON(`{"t":"y"}`)}))
	if !errors.Is(err, run.ErrCommandConflict) {
		t.Fatalf("err = %v, want ErrCommandConflict", err)
	}
}

func testPrepareHardCAS(t *testing.T, newRuntime Factory) {
	c := newCase(t, newRuntime)
	snap := c.load()
	req := request()
	prep, cmdID := buildPrepareFromSnap(t, &snap, &req, nil)
	if _, err := c.commit("wrong-prepare-id", snap.Revision, "", prep); !errors.Is(err, run.ErrCommandConflict) {
		t.Fatalf("wrong prepare id err = %v, want ErrCommandConflict", err)
	}
	bad := prep
	bad.StepID = "wrong-step"
	if _, err := c.commit(cmdID, snap.Revision, "", bad); !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("bad prepare StepID err = %v, want ErrStaleRuntime", err)
	}
	c.mustCommit(cmdID, snap.Revision, "", prep)
	otherReq := request()
	otherReq.System = "different"
	prep2, cmdID2 := buildPrepareFromSnap(t, &snap, &otherReq, nil)
	if cmdID2 != cmdID {
		t.Fatal("same revision must derive the same command id")
	}
	if _, err := c.commit(cmdID2, snap.Revision, "", prep2); !errors.Is(err, run.ErrCommandConflict) {
		t.Fatalf("err = %v, want ErrCommandConflict", err)
	}
	res := c.mustCommit(cmdID, snap.Revision, "", prep)
	if res.Status != run.CommitAlreadyApplied {
		t.Fatalf("status = %v", res.Status)
	}
}

func testPrepareReplayAfterProgress(t *testing.T, newRuntime Factory) {
	c := newCase(t, newRuntime)
	snap := c.load()
	input := run.AgentInput{ID: "input-1", Payload: mustJSON(`{"text":"hi"}`)}
	accepted := c.mustCommit(run.DeriveInputCommandID(c.runID, input.ID), snap.Revision, "", run.AcceptInput{Input: input})
	req := request()
	prep, cmdID := buildPrepareFromSnap(t, &accepted.Snapshot, &req, nil)
	prepared := c.mustCommit(cmdID, accepted.Snapshot.Revision, "", prep)
	start := c.mustCommit("start-derived-replay", prepared.Snapshot.Revision, "", run.StartModelExecution{StepID: prep.StepID})
	ok, err := run.FreezeModelResult(sdk.ModelResult{Text: "done"})
	if err != nil {
		t.Fatal(err)
	}
	c.mustCommit("finish-derived-replay", start.Snapshot.Revision, start.Grant, run.SubmitModelResult{StepID: prep.StepID, Result: ok})
	current := c.load()
	replayed, err := c.commit(cmdID, current.Revision, "", prep)
	if err != nil || replayed.Status != run.CommitAlreadyApplied {
		t.Fatalf("prepare replay after progress = %+v, %v", replayed, err)
	}
}

func testGrant(t *testing.T, newRuntime Factory) {
	c, stepID, grant := preparedCase(t, newRuntime, nil, nil)
	// The authority rejects an unbound start before it can mint ownership.
	empty, err := run.ProtocolV1().BuildEnvelope(c.runID, "empty-claim", run.StartModelExecution{StepID: stepID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.rt.Commit(context.Background(), run.CommitRequest{BaseRevision: 1, Command: empty}); !errors.Is(err, run.ErrCommandConflict) {
		t.Fatalf("empty start claim err = %v, want ErrCommandConflict", err)
	}

	_, err = c.commit("done-x", 2, "", run.SubmitModelResult{StepID: stepID, Result: run.ModelResult{}})
	if !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("grantless completion err = %v, want ErrStaleRuntime", err)
	}
	res := c.mustCommit("start-1", 1, "", run.StartModelExecution{StepID: stepID})
	if res.Status != run.CommitAlreadyApplied || res.Grant != grant {
		t.Fatalf("replayed start: %+v", res)
	}
	// A start under another claim is another attempt: its derived CommandID
	// differs, so it is not a replay; the target is Executing, so Decide
	// rejects it as stale. The live grant is untouched either way.
	if _, err := c.commit("start-other", 1, "", run.StartModelExecution{StepID: stepID}); !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("second start attempt err = %v, want ErrStaleRuntime", err)
	}
	// A start whose CommandID does not derive from its claim is rejected
	// before it can mint ownership.
	forged, err := run.ProtocolV1().BuildEnvelope(c.runID, "hand-minted", run.StartModelExecution{StepID: stepID, Claim: "different-claim"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.rt.Commit(context.Background(), run.CommitRequest{BaseRevision: 1, Command: forged}); !errors.Is(err, run.ErrCommandConflict) {
		t.Fatalf("non-derived start id err = %v, want ErrCommandConflict", err)
	}
	ok, err := run.FreezeModelResult(sdk.ModelResult{Text: "ok"})
	if err != nil {
		t.Fatal(err)
	}
	res = c.mustCommit("done-1", 2, grant, run.SubmitModelResult{StepID: stepID, Result: ok})
	if res.Status != run.CommitAccepted || res.Snapshot.State.Status != run.RunCompleted {
		t.Fatalf("completion: %+v", res.Snapshot.State.Status)
	}
	// The command remains idempotently replayable, but its consumed grant is
	// not returned after settlement.
	res = c.mustCommit("start-1", 1, "", run.StartModelExecution{StepID: stepID})
	if res.Status != run.CommitAlreadyApplied || res.Grant != "" {
		t.Fatalf("settled start replay: %+v", res)
	}
}

func startClaim(id run.CommandID) run.ExecutionClaim {
	return run.ExecutionClaim("test-claim/" + string(id))
}

func testGrantHolderRecover(t *testing.T, newRuntime Factory) {
	c, stepID, grant := preparedCase(t, newRuntime, nil, nil)
	claim := startClaim("start-1")
	id := run.DeriveModelRecoveryCommandID(c.runID, stepID, claim)
	res := c.mustCommit(id, 2, grant, run.RecoverModelExecution{StepID: stepID, Claim: claim})
	ms, ok := res.Snapshot.State.Current.(run.ModelStep)
	if !ok || ms.Status != run.ModelPrepared {
		t.Fatalf("after grant-holder recover: %+v", res.Snapshot.State.Current)
	}
}

func testGrantGrantlessRejectedWhileLive(t *testing.T, newRuntime Factory) {
	c, stepID, _ := preparedCase(t, newRuntime, nil, nil)
	claim := startClaim("start-1")
	id := run.DeriveModelRecoveryCommandID(c.runID, stepID, claim)
	_, err := c.commit(id, 2, "", run.RecoverModelExecution{StepID: stepID, Claim: claim})
	if !errors.Is(err, run.ErrStaleRuntime) {
		t.Fatalf("grantless recover while live err = %v, want ErrStaleRuntime", err)
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

func testCancelStaleBase(t *testing.T, newRuntime Factory) {
	c, _, _ := preparedCase(t, newRuntime, nil, nil)
	res := c.mustCommit("cancel-1", 0, "", run.CancelRun{})
	if res.Status != run.CommitAccepted || res.Snapshot.State.Status != run.RunStopped {
		t.Fatalf("cancel: %+v", res.Snapshot.State.Status)
	}
}

func testCancelAfterUnknown(t *testing.T, newRuntime Factory) {
	def := toolDef("t")
	spec := makeSpec(t, def)
	c, stepID, grant := preparedCase(t, newRuntime, []sdk.ToolDefinition{def}, []run.ToolSpec{spec})
	b := makeBinding(t, "c1", &spec)
	opened := c.mustCommit("complete-1", 2, grant,
		run.SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []run.ToolCallBinding{b}})
	toolStep := openedToolStepID(t, &opened)
	startRes := c.mustCommit("start-c1", opened.Snapshot.Revision, "", run.StartToolCall{StepID: toolStep, CallID: "c1"})
	unknown := c.mustCommit("unk-1", startRes.Snapshot.Revision, startRes.Grant,
		run.SubmitToolFailure{StepID: toolStep, CallID: "c1", Outcome: run.ToolOutcomeUnknown})
	if unknown.Snapshot.State.Status != run.RunActive {
		t.Fatalf("unknown status = %v, want active", unknown.Snapshot.State.Status)
	}
	if _, ok := unknown.Snapshot.State.Current.(run.Open); !ok {
		t.Fatalf("current = %+v, want Open", unknown.Snapshot.State.Current)
	}
	cancelled := c.mustCommit("cancel-1", unknown.Snapshot.Revision, "", run.CancelRun{})
	if cancelled.Snapshot.State.Status != run.RunStopped {
		t.Fatalf("cancel status = %v, want stopped", cancelled.Snapshot.State.Status)
	}
	if n := len(cancelled.Snapshot.State.Result.UncertainCalls); n != 0 {
		t.Fatalf("UncertainCalls = %v, want empty", cancelled.Snapshot.State.Result.UncertainCalls)
	}
}

func testCancelUnknownRemaining(t *testing.T, newRuntime Factory) {
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
	opened := c.mustCommit("complete-1", 2, grant,
		run.SubmitModelResult{StepID: stepID, Result: r, Calls: []run.ToolCallBinding{bA, bB}})
	toolStep := openedToolStepID(t, &opened)
	base := opened.Snapshot.Revision
	startA := c.mustCommit("start-A", base, "", run.StartToolCall{StepID: toolStep, CallID: "cA"})
	startB := c.mustCommit("start-B", base, "", run.StartToolCall{StepID: toolStep, CallID: "cB"})
	if startB.Status != run.CommitAccepted || startB.Grant == "" {
		t.Fatalf("start B: %+v", startB)
	}
	unknown := c.mustCommit("unk-A", startA.Snapshot.Revision, startA.Grant,
		run.SubmitToolFailure{StepID: toolStep, CallID: "cA", Outcome: run.ToolOutcomeUnknown})
	if unknown.Snapshot.State.Status != run.RunActive {
		t.Fatalf("unknown status = %v, want active", unknown.Snapshot.State.Status)
	}
	ts, ok := unknown.Snapshot.State.Current.(run.ToolStep)
	if !ok || len(ts.Calls) != 2 || ts.Calls[0].Status != run.ToolFailed || ts.Calls[1].Status != run.ToolExecuting {
		t.Fatalf("calls after unknown = %+v", unknown.Snapshot.State.Current)
	}
	cancelled := c.mustCommit("cancel-1", unknown.Snapshot.Revision, "", run.CancelRun{})
	if cancelled.Snapshot.State.Status != run.RunStopped {
		t.Fatalf("cancel status = %v, want stopped", cancelled.Snapshot.State.Status)
	}
	got := cancelled.Snapshot.State.Result.UncertainCalls
	if len(got) != 1 || got[0] != "cB" {
		t.Fatalf("UncertainCalls = %v, want [cB]", got)
	}
}

// encodeState renders the canonical snapshot bytes the protocol itself uses
// for state identity, so conformance compares states by the same rule as
// Runtime.Record.
func encodeState(t testing.TB, s *run.MachineState) []byte {
	t.Helper()
	raw, err := run.ProtocolV1().EncodeMachineState(s)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
