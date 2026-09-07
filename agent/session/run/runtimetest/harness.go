// Package runtimetest is the RUN-CMP-2 Runtime conformance suite. It takes a
// session.Store factory so the Memory store and every durable adapter run the
// same assertions; it asserts Run semantics only and leaves transaction
// atomicity, digest chains and snapshot equivalence to the kernel and
// Module Framework suites.
package runtimetest

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/memohai/twilight/agent/artifact"
	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/session"
	"github.com/memohai/twilight/agent/session/chatlog"
	"github.com/memohai/twilight/agent/session/extension"
	runmod "github.com/memohai/twilight/agent/session/run"
	"github.com/memohai/twilight/agent/turn"
	"github.com/memohai/twilight/sdk"
)

// Factory returns a fresh, empty Store for one test.
type Factory func(t testing.TB) session.Store

const sid session.SessionID = "conformance"

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// harness is one assembled stack over a Store.
type harness struct {
	t        testing.TB
	ctx      context.Context
	store    session.Store
	registry *extension.Registry
	appender extension.SemanticAppender
	reader   extension.ProjectionReader
	bindings *artifact.MemoryBindingStore
	frozen   *run.MemoryFrozenValues
	clock    *clock
	rt       *runmod.Runtime
	seq      int
}

func newHarness(t testing.TB, store session.Store, ttl time.Duration) *harness {
	t.Helper()
	registry, err := extension.BuildRegistry(session.ProfileV1(), chatlog.Module, runmod.Module, turn.Module)
	if err != nil {
		t.Fatal(err)
	}
	bindings := artifact.NewMemoryBindingStore()
	appender, err := extension.NewSemanticAppender(store, registry, artifact.SetBuilder{Resolver: bindings}, artifact.KVLedger{})
	if err != nil {
		t.Fatal(err)
	}
	reader := extension.NewProjectionReader(store, registry)
	h := &harness{t: t, ctx: context.Background(), store: store, registry: registry, appender: appender, reader: reader,
		bindings: bindings, frozen: run.NewMemoryFrozenValues(), clock: &clock{now: time.Unix(1_000_000, 0)}}
	h.rt, err = runmod.NewRuntime(runmod.Config{Store: store, Registry: registry, Appender: appender, Projections: reader,
		Frozen: h.frozen, Companion: turn.CompanionV1{}, LeaseTTL: ttl, Now: h.clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(h.ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: sid}); err != nil {
		t.Fatal(err)
	}
	return h
}

func (h *harness) fatal(args ...any) { h.t.Helper(); h.t.Fatal(args...) }

func (h *harness) head() session.Head {
	h.t.Helper()
	head, err := h.store.Head(h.ctx, sid)
	if err != nil {
		h.fatal(err)
	}
	return head
}

// appendGroup appends a typed group by CAS at the current head.
func (h *harness) appendGroup(group extension.SemanticGroup) extension.SemanticAppendResult {
	h.t.Helper()
	res, err := h.appender.AppendSemantic(h.ctx, extension.SemanticAppendRequest{SessionID: sid, ExpectedHead: h.head(), Group: group})
	if err != nil {
		h.fatal(err)
	}
	return res
}

func (h *harness) mustApply(group extension.SemanticGroup) session.SessionCommit {
	h.t.Helper()
	res := h.appendGroup(group)
	if res.Outcome != extension.SemanticApplied {
		h.fatal(fmt.Sprintf("append %s: %s %s", group.CommitID, res.Outcome, res.Detail))
	}
	return *res.Commit
}

func input(id string) run.AgentInput {
	return run.AgentInput{ID: run.InputID(id), Payload: run.MustParseCanonicalJSON(fmt.Sprintf(`{"text":%q}`, id))}
}

// submitInputs writes chatlog input_submitted for each input.
func (h *harness) submitInputs(inputs ...run.AgentInput) {
	h.t.Helper()
	for _, in := range inputs {
		h.seq++
		h.mustApply(extension.SemanticGroup{CommitID: session.CommitID(fmt.Sprintf("submitted/%s/%d", in.ID, h.seq)), Events: []extension.TypedEvent{{
			Type: chatlog.TypeInputSubmitted, RecordedAtUnixMilli: 1,
			Value: chatlog.InputSubmittedPayload{InputID: chatlog.InputID(in.ID), Content: in.Payload, SubmittedAtUnixMilli: 1}}}})
	}
}

// startGroup is TRN-STR-2 without a Coordinator: turn/started, input_delivered*,
// run/created, input_accepted*. Owner is the TurnID.
func (h *harness) startGroup(turnID turn.TurnID, runID run.RunID, attempt uint32, inputs ...run.AgentInput) extension.SemanticGroup {
	h.t.Helper()
	newRun, err := run.BuildNewRunFor(runID, run.OwnerID(turnID), attempt, "")
	if err != nil {
		h.fatal(err)
	}
	facts, err := run.ProtocolV1().BuildCreateGroup(newRun, inputs)
	if err != nil {
		h.fatal(err)
	}
	group := extension.SemanticGroup{CommitID: session.CommitID(fmt.Sprintf("start/%s/%d", turnID, attempt))}
	ids := make([]chatlog.InputID, len(inputs))
	for i, in := range inputs {
		ids[i] = chatlog.InputID(in.ID)
	}
	if attempt == 1 {
		group.Events = append(group.Events, extension.TypedEvent{Type: turn.TypeStarted, RecordedAtUnixMilli: 1,
			Value: turn.StartedPayload{TurnID: turnID, InputIDs: ids, ExecutionBinding: turn.ExecutionBindingRef{ID: "b", Digest: "sha256:b"},
				Companion: turn.CompanionV1Version, PlanDigest: "sha256:plan"}})
		for _, id := range ids {
			group.Events = append(group.Events, extension.TypedEvent{Type: chatlog.TypeInputDelivered, RecordedAtUnixMilli: 1,
				Value: chatlog.InputDeliveredPayload{InputID: id, TurnID: chatlog.TurnID(turnID)}})
		}
	}
	for _, f := range facts {
		group.Events = append(group.Events, extension.TypedEvent{Type: runmod.EventType(f), RecordedAtUnixMilli: 1, Value: runmod.Event{RunID: runID, Fact: f}})
	}
	return group
}

// startRun creates a Run under turnID with the given inputs and returns it.
func (h *harness) startRun(turnID turn.TurnID, runID run.RunID, inputs ...run.AgentInput) {
	h.t.Helper()
	h.submitInputs(inputs...)
	h.mustApply(h.startGroup(turnID, runID, 1, inputs...))
}

func (h *harness) load(runID run.RunID) run.RuntimeSnapshot {
	h.t.Helper()
	snap, err := h.rt.Load(h.ctx, sid, runID)
	if err != nil {
		h.fatal(err)
	}
	return snap
}

func (h *harness) record(runID run.RunID) run.RunRecord {
	h.t.Helper()
	rec, err := h.rt.Record(h.ctx, sid, runID)
	if err != nil {
		h.fatal(err)
	}
	return rec
}

func (h *harness) proto(runID run.RunID) run.Protocol {
	h.t.Helper()
	p, err := h.load(runID).Protocol()
	if err != nil {
		h.fatal(err)
	}
	return p
}

// commit builds the envelope and submits it; attach events follow the companion.
func (h *harness) commit(runID run.RunID, id run.CommandID, base run.RunPosition, grant run.ExecutionGrant, cmd run.AgentCommand, attach ...run.ModuleEvent) (run.CommitResult, error) {
	h.t.Helper()
	env, err := h.proto(runID).BuildEnvelope(sid, runID, id, cmd)
	if err != nil {
		h.fatal(err)
	}
	return h.rt.Commit(h.ctx, sid, run.CommitRequest{Base: base, Grant: grant, Command: env, Attach: attach})
}

func (h *harness) mustCommit(runID run.RunID, id run.CommandID, base run.RunPosition, grant run.ExecutionGrant, cmd run.AgentCommand, attach ...run.ModuleEvent) run.CommitResult {
	h.t.Helper()
	res, err := h.commit(runID, id, base, grant, cmd, attach...)
	if err != nil {
		h.fatal(fmt.Sprintf("commit %T: %v", cmd, err))
	}
	return res
}

func (h *harness) claim() run.ExecutionClaim {
	h.seq++
	return run.ExecutionClaim(fmt.Sprintf("claim-%d", h.seq))
}

// --- run building blocks ---------------------------------------------------------

var toolDef = sdk.ToolDefinition{Name: "echo", Parameters: json.RawMessage(`{"type":"object"}`)}

func (h *harness) spec() run.ToolSpec {
	h.t.Helper()
	frozen, err := run.FreezeToolDefinition(toolDef)
	if err != nil {
		h.fatal(err)
	}
	d, err := run.ProtocolV1().DigestToolDefinition(frozen)
	if err != nil {
		h.fatal(err)
	}
	return run.ToolSpec{Ref: "echo", Name: "echo", DefinitionDigest: d, Policy: run.DirectExecution}
}

// preparedCommand builds PrepareModelRequest against snap with the derived ids.
func (h *harness) preparedCommand(snap run.RuntimeSnapshot, withTool bool) (run.PrepareModelRequest, run.CommandID) {
	h.t.Helper()
	req := sdk.Request{Model: "m-1", Messages: []sdk.Message{sdk.UserMessage("go")}}
	var specs []run.ToolSpec
	if withTool {
		req.Tools = []sdk.ToolDefinition{toolDef}
		specs = []run.ToolSpec{h.spec()}
	}
	frozen, err := run.FreezeModelRequest(req)
	if err != nil {
		h.fatal(err)
	}
	proto, _ := snap.Protocol()
	reqDigest, err := proto.DigestRequest(frozen)
	if err != nil {
		h.fatal(err)
	}
	toolsDigest, err := proto.DigestToolSpecs(specs)
	if err != nil {
		h.fatal(err)
	}
	binding, err := proto.DigestModelStepBinding("m-1", reqDigest, toolsDigest)
	if err != nil {
		h.fatal(err)
	}
	cmdID := run.DeriveModelRequestCommandID(snap.State.RunID, snap.Position)
	ids := make([]run.InputID, len(snap.State.PendingInputs))
	for i, in := range snap.State.PendingInputs {
		ids[i] = in.ID
	}
	return run.PrepareModelRequest{StepID: run.DeriveModelStepID(snap.State.RunID, cmdID, binding), Model: "m-1", Request: frozen,
		RequestDigest: reqDigest, InputIDs: ids, Tools: specs, ToolsDigest: toolsDigest}, cmdID
}

// prepare commits a Prepare at the Run's current position and returns the step.
func (h *harness) prepare(runID run.RunID, withTool bool) run.StepID {
	h.t.Helper()
	snap := h.load(runID)
	cmd, id := h.preparedCommand(snap, withTool)
	h.mustCommit(runID, id, snap.Position, "", cmd)
	return cmd.StepID
}

// startModel commits StartModelExecution with a fresh claim.
func (h *harness) startModel(runID run.RunID, step run.StepID) (run.ExecutionGrant, run.ExecutionClaim) {
	h.t.Helper()
	claim := h.claim()
	res := h.mustCommit(runID, run.DeriveStartCommandID(runID, step, "", claim), h.load(runID).Position, "", run.StartModelExecution{StepID: step, Claim: claim})
	if res.Grant == "" {
		h.fatal("start returned no grant")
	}
	return res.Grant, claim
}

// executingModel drives a fresh Run to Model Executing.
func (h *harness) executingModel(runID run.RunID, withTool bool) (run.StepID, run.ExecutionGrant, run.ExecutionClaim) {
	h.t.Helper()
	step := h.prepare(runID, withTool)
	grant, claim := h.startModel(runID, step)
	return step, grant, claim
}

func textResult(text string) run.ModelResult {
	r, err := run.FreezeModelResult(sdk.ModelResult{Text: text, FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}})
	if err != nil {
		panic(err)
	}
	return r
}

// toolCallResult is a model result issuing n calls of the harness tool.
func (h *harness) toolCallResult(step run.StepID, n int) (run.ModelResult, []run.ToolCallBinding) {
	h.t.Helper()
	spec := h.spec()
	calls := make([]sdk.ToolCall, n)
	bindings := make([]run.ToolCallBinding, n)
	for i := range calls {
		args := run.MustParseCanonicalJSON(fmt.Sprintf(`{"i":%d}`, i))
		calls[i] = sdk.ToolCall{ToolCallID: fmt.Sprintf("c%d", i), ToolName: "echo", Input: args.String()}
		callID := run.DeriveCallID(step, i)
		bd, err := run.DigestToolCallBinding(callID, spec.DefinitionDigest, spec.Policy, args)
		if err != nil {
			h.fatal(err)
		}
		bindings[i] = run.ToolCallBinding{CallID: callID, ProviderCallID: calls[i].ToolCallID, ToolRef: spec.Ref, DefinitionDigest: spec.DefinitionDigest,
			BindingDigest: bd, Arguments: args, Policy: spec.Policy}
	}
	r, err := run.FreezeModelResult(sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls, Usage: sdk.Usage{TotalTokens: 2}, ToolCalls: calls})
	if err != nil {
		h.fatal(err)
	}
	return r, bindings
}

// openToolStep drives a fresh Run to a ToolStep with n Pending calls.
func (h *harness) openToolStep(runID run.RunID, n int) (run.StepID, []run.CallID) {
	h.t.Helper()
	step, grant, claim := h.executingModel(runID, true)
	result, bindings := h.toolCallResult(step, n)
	res := h.mustCommit(runID, run.DeriveSettlementCommandID(runID, step, "", claim), h.load(runID).Position, grant,
		run.SubmitModelResult{StepID: step, Result: result, Calls: bindings})
	ts, ok := res.Snapshot.State.Current.(run.ToolStep)
	if !ok {
		h.fatal(fmt.Sprintf("after model result current = %T", res.Snapshot.State.Current))
	}
	ids := make([]run.CallID, n)
	for i := range bindings {
		ids[i] = bindings[i].CallID
	}
	return ts.RefValue.ID, ids
}

func (h *harness) startTool(runID run.RunID, step run.StepID, call run.CallID) (run.ExecutionGrant, run.ExecutionClaim) {
	h.t.Helper()
	claim := h.claim()
	res := h.mustCommit(runID, run.DeriveStartCommandID(runID, step, call, claim), h.load(runID).Position, "", run.StartToolCall{StepID: step, CallID: call, Claim: claim})
	if res.Grant == "" {
		h.fatal("tool start returned no grant")
	}
	return res.Grant, claim
}

func (h *harness) lease(runID run.RunID, step run.StepID, call run.CallID) (extension.Lease, bool) {
	h.t.Helper()
	l, ok, err := extension.Leases{Store: h.store}.Lookup(h.ctx, sid, runmod.LeaseNamespace, run.LeaseKey(runID, step, call))
	if err != nil {
		h.fatal(err)
	}
	return l, ok
}

func (h *harness) machine() runmod.Machine {
	h.t.Helper()
	state, _, err := h.reader.Load(h.ctx, sid, runmod.MachineProjectionID, runmod.MachineProjection.Version)
	if err != nil {
		h.fatal(err)
	}
	return state.(runmod.Machine)
}

func eventTypes(c *session.SessionCommit) []session.EventType {
	out := make([]session.EventType, len(c.Events))
	for i := range c.Events {
		out[i] = c.Events[i].Type
	}
	return out
}
