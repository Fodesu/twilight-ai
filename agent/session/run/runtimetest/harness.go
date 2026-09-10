// Package runtimetest is the RUN-CMP-2 Runtime conformance suite. It takes a
// session.Store factory so the Memory store and every durable adapter run the
// same assertions; it asserts Run semantics only and leaves group atomicity,
// digest chains, ownership fencing and cache equivalence to the kernel and
// Module Framework suites.
package runtimetest

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/artifact"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/extension"
	runmod "github.com/felinics/twilight/agent/session/run"
	"github.com/felinics/twilight/agent/turn"
	"github.com/felinics/twilight/sdk"
)

// Fixture is one adapter under test.
type Fixture struct {
	Store session.Store
}

// Factory returns a fresh, empty Fixture for one test.
type Factory func(t testing.TB) Fixture

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

// harness is one owner process over a Store: registry, Writers, Runtime.
type harness struct {
	t        testing.TB
	ctx      context.Context
	fixture  Fixture
	store    session.Store
	registry *extension.Registry
	bindings *artifact.MemoryBindingStore
	ledger   *artifact.MemoryLedger
	frozen   *run.MemoryFrozenValues
	cache    *extension.MemoryProjectionCache
	clock    *clock
	writers  extension.Writers
	rt       *runmod.Runtime
	seq      int
}

func newHarness(t testing.TB, f Fixture) *harness {
	t.Helper()
	registry, err := extension.BuildRegistry(session.ProtocolVersion1, chatlog.Module, runmod.Module, turn.Module)
	if err != nil {
		t.Fatal(err)
	}
	bindings := artifact.NewMemoryBindingStore()
	h := &harness{t: t, ctx: context.Background(), fixture: f, store: f.Store, registry: registry, bindings: bindings,
		ledger: artifact.NewMemoryLedger(artifact.SetBuilder{Resolver: bindings}), frozen: run.NewMemoryFrozenValues(),
		cache: extension.NewMemoryProjectionCache(), clock: &clock{now: time.Unix(1_000_000, 0)}}
	if _, err := f.Store.Create(h.ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: sid}); err != nil {
		t.Fatal(err)
	}
	h.open()
	return h
}

// open starts an owner process: Writers over the shared store and a Runtime.
// Takeover lets it supersede the previous owner process, if any.
func (h *harness) open() {
	h.t.Helper()
	h.writers = extension.NewWriters(h.store, h.registry, extension.Admission{Bindings: h.bindings, Ledger: h.ledger}, session.OpenOptions{Takeover: true},
		extension.WritersConfig{Cache: h.cache, CachePolicy: runmod.WriterCachePolicy(0)})
	rt, err := runmod.NewRuntime(runmod.Config{Writers: h.writers, Registry: h.registry, Store: h.store,
		Frozen: h.frozen, Companion: turn.CompanionV1{}, Cache: h.cache, Now: h.clock.Now})
	if err != nil {
		h.fatal(err)
	}
	h.rt = rt
}

// takeover opens a new owner process over the same store; the previous
// Runtime stays usable so tests can observe its fencing.
func (h *harness) takeover() *runmod.Runtime {
	h.t.Helper()
	old := h.rt
	h.open()
	return old
}

func (h *harness) fatal(args ...any) { h.t.Helper(); h.t.Fatal(args...) }

func (h *harness) writer() extension.Writer {
	h.t.Helper()
	w, err := h.writers.Writer(h.ctx, sid)
	if err != nil {
		h.fatal(err)
	}
	return w
}

func (h *harness) head() session.Head {
	h.t.Helper()
	page, err := h.store.Read(h.ctx, session.ReadRequest{SessionID: sid, From: ^session.Seq(0) >> 1})
	if err != nil {
		h.fatal(err)
	}
	return page.Head
}

// mustApply commits a typed group through the Writer and returns its rows.
func (h *harness) mustApply(group extension.SemanticGroup) []session.SessionEvent {
	h.t.Helper()
	res, err := h.writer().Commit(h.ctx, func(extension.View) (*extension.SemanticGroup, error) { return &group, nil })
	if err != nil {
		h.fatal(err)
	}
	if res.Outcome != extension.CommitApplied {
		h.fatal(fmt.Sprintf("append %s: %s %s", group.CommitID, res.Outcome, res.Detail))
	}
	return res.Events
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
			Value: turn.StartedPayload{TurnID: turnID, InputIDs: ids, Profile: turn.ProfileRef{ID: "b", Digest: "sha256:b"},
				Companion: turn.CompanionV1Version}})
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

// startRun creates a Run under turnID with the given inputs.
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
func (h *harness) commit(runID run.RunID, id run.CommandID, base run.RunPosition, cmd run.AgentCommand, attach ...run.ModuleEvent) (run.CommitResult, error) {
	h.t.Helper()
	return h.commitWith(h.rt, runID, id, base, cmd, attach...)
}

func (h *harness) commitWith(rt *runmod.Runtime, runID run.RunID, id run.CommandID, base run.RunPosition, cmd run.AgentCommand, attach ...run.ModuleEvent) (run.CommitResult, error) {
	h.t.Helper()
	env, err := h.proto(runID).BuildEnvelope(sid, runID, id, cmd)
	if err != nil {
		h.fatal(err)
	}
	return rt.Commit(h.ctx, sid, run.CommitRequest{Base: base, Command: env, Attach: attach})
}

func (h *harness) mustCommit(runID run.RunID, id run.CommandID, base run.RunPosition, cmd run.AgentCommand, attach ...run.ModuleEvent) run.CommitResult {
	h.t.Helper()
	res, err := h.commit(runID, id, base, cmd, attach...)
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
	h.mustCommit(runID, id, snap.Position, cmd)
	return cmd.StepID
}

// startModel commits StartModelExecution with a fresh claim.
func (h *harness) startModel(runID run.RunID, step run.StepID) run.ExecutionClaim {
	h.t.Helper()
	claim := h.claim()
	res := h.mustCommit(runID, run.DeriveStartCommandID(runID, step, "", claim), 0, run.StartModelExecution{StepID: step, Claim: claim})
	if res.Status != run.CommitAccepted {
		h.fatal("start was not accepted")
	}
	return claim
}

// executingModel drives a fresh Run to Model Executing.
func (h *harness) executingModel(runID run.RunID, withTool bool) (run.StepID, run.ExecutionClaim) {
	h.t.Helper()
	step := h.prepare(runID, withTool)
	return step, h.startModel(runID, step)
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
	step, claim := h.executingModel(runID, true)
	result, bindings := h.toolCallResult(step, n)
	res := h.mustCommit(runID, run.DeriveSettlementCommandID(runID, step, "", claim), 0,
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

func (h *harness) startTool(runID run.RunID, step run.StepID, call run.CallID) run.ExecutionClaim {
	h.t.Helper()
	claim := h.claim()
	res := h.mustCommit(runID, run.DeriveStartCommandID(runID, step, call, claim), 0, run.StartToolCall{StepID: step, CallID: call, Claim: claim})
	if res.Status != run.CommitAccepted {
		h.fatal("tool start was not accepted")
	}
	return claim
}

func (h *harness) machine() runmod.Machine {
	h.t.Helper()
	state, _, err := h.writer().Projections().Load(h.ctx, sid, runmod.MachineProjectionID, runmod.MachineProjection.Version)
	if err != nil {
		h.fatal(err)
	}
	return state.(runmod.Machine)
}

func eventTypes(rows []session.SessionEvent) []session.EventType {
	out := make([]session.EventType, len(rows))
	for i := range rows {
		out[i] = rows[i].Type
	}
	return out
}
