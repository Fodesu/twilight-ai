// Package turntest is the Store-parameterized conformance suite of the Turn
// module (agent-turn.md section 8). The Coordinator is pure protocol, so the
// suite assembles Writers, a Runtime and a Coordinator over the Store under
// test and drives Runs step by step through Runtime commits: no Loop, driver,
// model or tool stub is involved.
package turntest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/extension"
	runmod "github.com/felinics/twilight/agent/session/run"
	"github.com/felinics/twilight/agent/session/writer"
	"github.com/felinics/twilight/agent/turn"
	"github.com/felinics/twilight/sdk"
)

// Fixture is one adapter under test.
type Fixture struct{ Store session.Store }

// Factory returns a fresh, empty Fixture for one test.
type Factory func(t testing.TB) Fixture

const sid session.SessionID = "turn-conformance"

var profile = turn.ProfileRef{ID: "p-1", Digest: "sha256:p-1"}

// harness is one owner process: Writers, Runtime and Coordinator over the
// Store. now is the clock every event is stamped with; tests move it to show
// that timestamps never take part in idempotency.
type harness struct {
	t        testing.TB
	ctx      context.Context
	store    session.Store
	registry *extension.Registry
	frozen   *run.MemoryFrozenValues
	now      int64
	seq      int
	writers  writer.Writers
	rt       *runmod.Runtime
	c        *turn.Coordinator
}

func newHarness(t testing.TB, f Fixture) *harness {
	t.Helper()
	registry, err := extension.BuildRegistry(session.ProtocolVersion1, chatlog.Module, runmod.Module, turn.Module)
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, ctx: context.Background(), store: f.Store, registry: registry, frozen: run.NewMemoryFrozenValues(), now: 1_000}
	if _, err := f.Store.Create(h.ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: sid, CreatedAtUnixMilli: 1}); err != nil {
		t.Fatal(err)
	}
	h.open()
	return h
}

// open starts an owner process over the Store; a second call is a takeover.
func (h *harness) open() {
	h.t.Helper()
	clock := func() time.Time { return time.UnixMilli(h.now) }
	h.writers = writer.NewWriters(h.store, h.registry, writer.Admission{}, session.OpenOptions{Takeover: true}, writer.WritersConfig{})
	rt, err := runmod.NewRuntime(runmod.Config{Writers: h.writers, Registry: h.registry, Store: h.store,
		Frozen: h.frozen, Companion: turn.CompanionV1{}, Now: clock})
	if err != nil {
		h.t.Fatal(err)
	}
	h.rt = rt
	h.c = &turn.Coordinator{Writers: h.writers, Runtime: rt, Now: clock}
}

// takeover opens a new owner process and returns the superseded Coordinator so
// a test can observe its fencing.
func (h *harness) takeover() *turn.Coordinator {
	h.t.Helper()
	old := h.c
	h.open()
	return old
}

func (h *harness) fatal(args ...any) { h.t.Helper(); h.t.Fatal(args...) }

func (h *harness) ref(turnID turn.TurnID) turn.TurnRef {
	return turn.TurnRef{SessionID: sid, TurnID: turnID}
}

func (h *harness) writer() writer.Writer {
	h.t.Helper()
	w, err := h.writers.Writer(h.ctx, sid)
	if err != nil {
		h.fatal(err)
	}
	return w
}

// commit writes one typed group through the Writer and returns the outcome.
func (h *harness) commit(group writer.SemanticGroup) writer.CommitResult {
	h.t.Helper()
	res, err := h.writer().Commit(h.ctx, func(writer.View) (*writer.SemanticGroup, error) { return &group, nil })
	if err != nil {
		h.fatal(err)
	}
	return res
}

func (h *harness) mustApply(group writer.SemanticGroup) []session.SessionEvent {
	h.t.Helper()
	res := h.commit(group)
	if res.Outcome != writer.CommitApplied {
		h.fatal(fmt.Sprintf("append %s: %s %s", group.CommitID, res.Outcome, res.Detail))
	}
	return res.Events
}

func input(id string) run.AgentInput {
	return run.AgentInput{ID: run.InputID(id), Payload: run.MustParseCanonicalJSON(fmt.Sprintf(`{"text":%q}`, id))}
}

// submit writes twilight/chatlog/input_submitted for each id and returns the
// AgentInputs a Start or Deliver hands to the Coordinator.
func (h *harness) submit(ids ...string) []run.AgentInput {
	h.t.Helper()
	out := make([]run.AgentInput, len(ids))
	for i, id := range ids {
		in := input(id)
		h.mustApply(writer.SemanticGroup{CommitID: session.CommitID("submitted/" + id), Events: []writer.TypedEvent{{
			Type: chatlog.TypeInputSubmitted, RecordedAtUnixMilli: h.now,
			Value: chatlog.InputSubmittedPayload{InputID: chatlog.InputID(id), Content: in.Payload, SubmittedAtUnixMilli: h.now}}}})
		out[i] = in
	}
	return out
}

func (h *harness) startRequest(turnID turn.TurnID, inputs ...run.AgentInput) turn.StartRequest {
	return turn.StartRequest{Ref: h.ref(turnID), Inputs: inputs, Profile: profile, Companion: turn.CompanionV1Version}
}

// start submits ids and starts turnID with them.
func (h *harness) start(turnID turn.TurnID, ids ...string) turn.TurnResponse {
	h.t.Helper()
	resp, err := h.c.Start(h.ctx, h.startRequest(turnID, h.submit(ids...)...))
	if err != nil {
		h.fatal(fmt.Sprintf("start %s: %v", turnID, err))
	}
	return resp
}

func (h *harness) status(turnID turn.TurnID) turn.TurnResponse {
	h.t.Helper()
	resp, err := h.c.Status(h.ctx, h.ref(turnID))
	if err != nil {
		h.fatal(fmt.Sprintf("status %s: %v", turnID, err))
	}
	return resp
}

func (h *harness) surface() turn.TurnSurface {
	h.t.Helper()
	state, _, err := h.writer().Projections().Load(h.ctx, sid, turn.SurfaceProjectionID, turn.SurfaceProjection.Version)
	if err != nil {
		h.fatal(err)
	}
	return state.(turn.TurnSurface)
}

func (h *harness) chat() chatlog.Surface {
	h.t.Helper()
	state, _, err := h.writer().Projections().Load(h.ctx, sid, chatlog.SurfaceProjectionID, chatlog.SurfaceProjection.Version)
	if err != nil {
		h.fatal(err)
	}
	return state.(chatlog.Surface)
}

func (h *harness) rows() []session.SessionEvent {
	h.t.Helper()
	page, err := h.store.Read(h.ctx, session.ReadRequest{SessionID: sid})
	if err != nil {
		h.fatal(err)
	}
	return page.Events
}

func (h *harness) head() session.Head {
	h.t.Helper()
	page, err := h.store.Read(h.ctx, session.ReadRequest{SessionID: sid, From: ^session.Seq(0) >> 1})
	if err != nil {
		h.fatal(err)
	}
	return page.Head
}

// group returns the rows sharing commitID.
func (h *harness) group(commitID session.CommitID) []session.SessionEvent {
	h.t.Helper()
	var out []session.SessionEvent
	for _, r := range h.rows() {
		if r.CommitID == commitID {
			out = append(out, r)
		}
	}
	return out
}

// groupContaining returns the whole group of the first row that satisfies
// match.
func (h *harness) groupContaining(match func(*session.SessionEvent) bool) []session.SessionEvent {
	h.t.Helper()
	for _, r := range h.rows() {
		if match(&r) {
			return h.group(r.CommitID)
		}
	}
	return nil
}

func eventTypes(rows []session.SessionEvent) []session.EventType {
	out := make([]session.EventType, len(rows))
	for i := range rows {
		out[i] = rows[i].Type
	}
	return out
}

func sameTypes(got []session.SessionEvent, want ...session.EventType) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i].Type != want[i] {
			return false
		}
	}
	return true
}

func decode[T any](t testing.TB, registry *extension.Registry, row *session.SessionEvent) T {
	t.Helper()
	d, err := registry.Decode(*row)
	if err != nil {
		t.Fatal(err)
	}
	v, ok := d.Value.(T)
	if !ok {
		var zero T
		t.Fatalf("row %d is %T, want %T", row.Seq, d.Value, zero)
	}
	return v
}

func (h *harness) load(runID run.RunID) run.RuntimeSnapshot {
	h.t.Helper()
	snap, err := h.rt.Load(h.ctx, sid, runID)
	if err != nil {
		h.fatal(err)
	}
	return snap
}

// --- driving a Run without a Loop ----------------------------------------------------

func (h *harness) claim() run.ExecutionClaim {
	h.seq++
	return run.ExecutionClaim(fmt.Sprintf("claim-%d", h.seq))
}

func (h *harness) runCommit(runID run.RunID, id run.CommandID, base run.RunPosition, cmd run.AgentCommand, attach ...run.ModuleEvent) (run.CommitResult, error) {
	h.t.Helper()
	env, err := run.ProtocolV1().BuildEnvelope(sid, runID, id, cmd)
	if err != nil {
		h.fatal(err)
	}
	return h.rt.Commit(h.ctx, sid, run.CommitRequest{Base: base, Command: env, Attach: attach})
}

func (h *harness) mustRunCommit(runID run.RunID, id run.CommandID, base run.RunPosition, cmd run.AgentCommand) run.CommitResult {
	h.t.Helper()
	res, err := h.runCommit(runID, id, base, cmd)
	if err != nil {
		h.fatal(fmt.Sprintf("commit %T: %v", cmd, err))
	}
	return res
}

var toolDef = sdk.ToolDefinition{Name: "ask", Parameters: json.RawMessage(`{"type":"object"}`)}

func (h *harness) spec(policy run.ResponsePolicy) run.ToolSpec {
	h.t.Helper()
	frozen, err := run.FreezeToolDefinition(toolDef)
	if err != nil {
		h.fatal(err)
	}
	d, err := run.ProtocolV1().DigestToolDefinition(frozen)
	if err != nil {
		h.fatal(err)
	}
	return run.ToolSpec{Ref: "ask", Name: "ask", DefinitionDigest: d, Policy: policy}
}

// prepare commits PrepareModelRequest at the Run's current position; specs is
// nil for a model step without tools.
func (h *harness) prepare(runID run.RunID, specs []run.ToolSpec) run.StepID {
	h.t.Helper()
	snap := h.load(runID)
	req := sdk.Request{Model: "m-1", Messages: []sdk.Message{sdk.UserMessage("go")}}
	if len(specs) > 0 {
		req.Tools = []sdk.ToolDefinition{toolDef}
	}
	frozen, err := run.FreezeModelRequest(req)
	if err != nil {
		h.fatal(err)
	}
	proto := run.ProtocolV1()
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
	cmdID := run.DeriveModelRequestCommandID(runID, snap.Position)
	ids := make([]run.InputID, len(snap.State.PendingInputs))
	for i, in := range snap.State.PendingInputs {
		ids[i] = in.ID
	}
	cmd := run.PrepareModelRequest{StepID: run.DeriveModelStepID(runID, cmdID, binding), Model: "m-1", Request: frozen,
		RequestDigest: reqDigest, InputIDs: ids, Tools: specs, ToolsDigest: toolsDigest}
	h.mustRunCommit(runID, cmdID, snap.Position, cmd)
	return cmd.StepID
}

// executingModel takes the Run to a model step that is Executing with no
// worker in this process: the state RecoverInterrupted acts on.
func (h *harness) executingModel(runID run.RunID) (run.StepID, run.ExecutionClaim) {
	h.t.Helper()
	step := h.prepare(runID, nil)
	claim := h.claim()
	res := h.mustRunCommit(runID, run.DeriveStartCommandID(runID, step, "", claim), 0, run.StartModelExecution{StepID: step, Claim: claim})
	if res.Status != run.CommitAccepted {
		h.fatal("start model was not accepted")
	}
	return step, claim
}

func textResult(text string) run.ModelResult {
	r, err := run.FreezeModelResult(sdk.ModelResult{Text: text, FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}})
	if err != nil {
		panic(err)
	}
	return r
}

// complete finishes the Run with a text result; the companion settles the Turn
// as completed in the same group.
func (h *harness) complete(runID run.RunID) run.CommitResult {
	h.t.Helper()
	step, claim := h.executingModel(runID)
	return h.mustRunCommit(runID, run.DeriveSettlementCommandID(runID, step, "", claim), 0, run.SubmitModelResult{StepID: step, Result: textResult("done")})
}

// waitingTool takes the Run to a ToolStep whose single call needs approval.
func (h *harness) waitingTool(runID run.RunID) {
	h.t.Helper()
	spec := h.spec(run.ApprovalRequired)
	step := h.prepare(runID, []run.ToolSpec{spec})
	claim := h.claim()
	if res := h.mustRunCommit(runID, run.DeriveStartCommandID(runID, step, "", claim), 0, run.StartModelExecution{StepID: step, Claim: claim}); res.Status != run.CommitAccepted {
		h.fatal("start model was not accepted")
	}
	args := run.MustParseCanonicalJSON(`{"q":1}`)
	callID := run.DeriveCallID(step, 0)
	bd, err := run.DigestToolCallBinding(callID, spec.DefinitionDigest, spec.Policy, args)
	if err != nil {
		h.fatal(err)
	}
	result, err := run.FreezeModelResult(sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls, Usage: sdk.Usage{TotalTokens: 2},
		ToolCalls: []sdk.ToolCall{{ToolCallID: "c0", ToolName: "ask", Input: args.String()}}})
	if err != nil {
		h.fatal(err)
	}
	binding := run.ToolCallBinding{CallID: callID, ProviderCallID: "c0", ToolRef: spec.Ref, DefinitionDigest: spec.DefinitionDigest,
		BindingDigest: bd, Arguments: args, Policy: spec.Policy}
	h.mustRunCommit(runID, run.DeriveSettlementCommandID(runID, step, "", claim), 0,
		run.SubmitModelResult{StepID: step, Result: result, Calls: []run.ToolCallBinding{binding}})
}

// appCancel is the Application's own CancelRun: no settlement is attached, so
// the Turn becomes attempt_failed (TRN-STP-1).
func (h *harness) appCancel(runID run.RunID) {
	h.t.Helper()
	h.seq++
	h.mustRunCommit(runID, run.CommandID(fmt.Sprintf("app-cancel-%d", h.seq)), 0, run.CancelRun{})
}
