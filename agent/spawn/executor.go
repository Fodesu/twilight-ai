package spawn

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/felinics/twilight/agent/authority"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/effect"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/writer"
	"github.com/felinics/twilight/agent/turn"
)

// Options configures the subagent effect (HST-SPN).
type Options struct {
	// Tool is the ToolRef the model calls; empty selects DefaultTool.
	Tool run.ToolRef
	// Presets resolves the optional preset name in the arguments to the
	// PresetRef the child runs under. Nil allows no name: the child runs
	// under the parent Turn's preset.
	Presets func(name string) (turn.PresetRef, error)
	// MaxDepth bounds nesting: a Session at this depth cannot spawn. Zero
	// selects DefaultDepth.
	MaxDepth int
}

// ToolRef is the tool the model calls.
func (o Options) ToolRef() run.ToolRef {
	if o.Tool == "" {
		return DefaultTool
	}
	return o.Tool
}

// ExecutableTool is the model-facing definition of the spawn tool for preset
// catalogs. Its Execute never runs: the Executor claims the tool's
// Assignments before the deployment's Executor sees them.
func (o Options) ExecutableTool() loop.ExecutableTool { return Tool(o.ToolRef()) }

func (o Options) depth() int {
	if o.MaxDepth <= 0 {
		return DefaultDepth
	}
	return o.MaxDepth
}

// Executor runs the spawn tool's Assignments as child Sessions this process
// drives (HST-SPN-1). The in-flight table is process-scoped like a local
// executor's; what outlives the process is the child Session.
type Executor struct {
	opts Options
	tool loop.ExecutableTool
	// a is the authority children are created in and driven through: Open
	// yields the child's ownership Handle and its Turns, Driver and Chatlog
	// commands run through that Handle's Writer.
	a *authority.Authority

	mu       sync.Mutex
	inflight map[effect.AssignmentKey]*spawnRun
	closed   bool
}

type spawnRun struct {
	child   session.SessionID
	cancel  context.CancelFunc
	done    chan struct{}
	outcome effect.Outcome
	closed  bool
}

// NewExecutor returns the spawn effect; Bind supplies the authority it
// drives children through once that authority exists (it is built over the
// Port that intercepts for this Executor).
func NewExecutor(opts Options) *Executor {
	return &Executor{opts: opts, tool: Tool(opts.ToolRef()), inflight: make(map[effect.AssignmentKey]*spawnRun)}
}

// Bind supplies the authority. It must precede the first Dispatch or Attach.
func (e *Executor) Bind(a *authority.Authority) { e.a = a }

// Intercept wraps inner so the spawn tool's Assignments reach e and every
// other Assignment reaches inner. Key-only operations go to e for keys it
// drives or whose derived child exists on record (HST-SPN-4), else to inner.
func Intercept(e *Executor, inner effect.Port) effect.Port { return &intercept{e: e, inner: inner} }

func (e *Executor) ours(a effect.Assignment) bool {
	return a.Kind == effect.AssignmentTool && a.Tool != nil && a.Tool.ToolRef == e.tool.Ref()
}

// owns reports a key this process drives or drove, or whose derived child
// Session exists on record.
func (e *Executor) owns(ctx context.Context, key effect.AssignmentKey) (bool, error) {
	if _, ok := e.local(key); ok {
		return true, nil
	}
	_, err := e.a.Store.Record(ctx, ChildID(key.Session, key.RunID, key.CallID))
	if err == nil {
		return true, nil
	}
	if session.IsCode(err, session.ErrNotFound) {
		return false, nil
	}
	return false, err
}

// Close cancels every child drive; their Turns stay active and resume on
// the next open of the parent (adoption through Attach).
func (e *Executor) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
	for _, r := range e.inflight {
		r.cancel()
	}
}

// Validate checks the spawn tool's binding and arguments, the nesting depth
// and, for a replayed call, that the child on record was created for the
// same arguments (RUN-EXE-3).
func (e *Executor) Validate(ctx context.Context, a effect.Assignment) (*run.ToolFailure, error) {
	proto, err := run.ProtocolFor(a.Schema)
	if err != nil {
		return nil, err
	}
	if failure, err := CheckDefinition(proto, e.tool, a.Tool); err != nil || failure != nil {
		return failure, err
	}
	args, err := DecodeArguments(a.Tool.Arguments)
	if err != nil {
		return &run.ToolFailure{Class: run.FailureInvalidArguments, Message: err.Error()}, nil
	}
	if args.Preset != "" {
		if e.opts.Presets == nil {
			return &run.ToolFailure{Class: run.FailureInvalidArguments, Message: "named presets are not configured"}, nil
		}
		if _, err := e.opts.Presets(args.Preset); err != nil {
			return &run.ToolFailure{Class: run.FailureInvalidArguments, Message: err.Error()}, nil
		}
	}
	depth, err := e.depthOf(ctx, a.Session)
	if err != nil {
		return nil, err
	}
	if DepthExceeded(depth, e.opts.depth()) {
		return &run.ToolFailure{Class: run.FailureExecution, Message: fmt.Sprintf("subagent depth %d reached", e.opts.depth())}, nil
	}
	child := ChildID(a.Session, a.RunID, a.CallID)
	if prov, ok, err := e.provenance(ctx, child); err != nil {
		return nil, err
	} else if ok && ArgumentsConflict(prov, args) {
		return &run.ToolFailure{Class: run.FailureExecution, Message: fmt.Sprintf("call %s already spawned %s with different arguments", a.CallID, child)}, nil
	}
	return nil, nil
}

// depthOf reads a Session's nesting depth from its segment metadata; a
// Session no spawn created is at depth 0.
func (e *Executor) depthOf(ctx context.Context, sid session.SessionID) (int, error) {
	prov, ok, err := e.provenance(ctx, sid)
	if err != nil || !ok {
		return 0, err
	}
	return prov.Depth, nil
}

// provenance reads the spawn record of a Session's tip segment; ok is false
// when the Session does not exist or was not spawned.
func (e *Executor) provenance(ctx context.Context, sid session.SessionID) (Provenance, bool, error) {
	header, err := e.a.Store.Header(ctx, sid)
	if err != nil {
		if session.IsCode(err, session.ErrNotFound) {
			return Provenance{}, false, nil
		}
		return Provenance{}, false, err
	}
	return ProvenanceFromHeader(header)
}

func (e *Executor) Dispatch(ctx context.Context, a effect.Assignment) error {
	args, err := DecodeArguments(a.Tool.Arguments)
	if err != nil {
		return fmt.Errorf("%w: %v", loop.ErrExecutorRejected, err)
	}
	e.start(a.Key(), &args)
	return nil
}

// start registers the call and begins driving its child; a key already in
// flight is an idempotent replay. args is nil when a takeover adopts the
// call from its child's provenance.
func (e *Executor) start(key effect.AssignmentKey, args *Arguments) {
	e.mu.Lock()
	if _, dup := e.inflight[key]; dup || e.closed {
		e.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &spawnRun{child: ChildID(key.Session, key.RunID, key.CallID), cancel: cancel, done: make(chan struct{})}
	e.inflight[key] = r
	e.mu.Unlock()
	go func() {
		out := e.drive(ctx, key, r.child, args)
		out.Key = key
		if ctx.Err() != nil {
			out.Cancelled = true
		}
		e.mu.Lock()
		r.outcome = out
		r.closed = true
		close(r.done)
		e.mu.Unlock()
		cancel()
	}()
}

// drive runs one spawn call to its Outcome: it makes sure the child exists,
// opens it, brings its Turn to settlement and reads the reply. Every step is
// idempotent against the child's durable state, so a process that died
// anywhere in the sequence is continued, not repeated.
func (e *Executor) drive(ctx context.Context, key effect.AssignmentKey, child session.SessionID, args *Arguments) effect.Outcome {
	fail := func(class, msg string) effect.Outcome {
		return effect.Outcome{Tool: effect.ToolExecutionFailed{Failure: run.ToolFailure{Class: class, Message: msg}}}
	}
	prov, exists, err := e.provenance(ctx, child)
	if err != nil {
		return fail(run.FailureExecution, err.Error())
	}
	if !exists {
		if args == nil {
			return fail(run.FailureExecution, fmt.Sprintf("child %s of call %s does not exist", child, key.CallID))
		}
		if prov, err = e.create(ctx, key, child, *args); err != nil {
			return fail(run.FailureExecution, fmt.Sprintf("create subagent: %v", err))
		}
	} else if args != nil && ArgumentsConflict(prov, *args) {
		return fail(run.FailureExecution, fmt.Sprintf("call %s already spawned %s with different arguments", key.CallID, child))
	}
	preset, err := e.childPreset(ctx, key, prov.Arguments)
	if err != nil {
		return fail(run.FailureExecution, err.Error())
	}
	h, err := e.a.Open(ctx, child)
	if err != nil {
		return fail(run.FailureExecution, fmt.Sprintf("open subagent: %v", err))
	}
	defer func() { _ = h.Close(context.WithoutCancel(ctx)) }()
	turnID, err := e.settle(ctx, h, preset, prov.Arguments.Task)
	if err != nil {
		if ctx.Err() != nil {
			return fail(run.FailureCancelled, "subagent cancelled")
		}
		return fail(run.FailureExecution, fmt.Sprintf("drive subagent: %v", err))
	}
	ref := turn.TurnRef{SessionID: child, TurnID: turnID}
	status, err := e.a.Turns.Status(ctx, ref)
	if err != nil {
		return fail(run.FailureExecution, err.Error())
	}
	if status.Status != turn.TurnCompleted {
		return fail(run.FailureExecution, fmt.Sprintf("subagent %s turn %s ended %s", child, turnID, status.Status))
	}
	reply, err := e.a.Reply(ctx, ref)
	if err != nil {
		return fail(run.FailureExecution, err.Error())
	}
	body, err := run.CanonicalJSONFromValue(Result{ChildSession: child, TurnID: turnID, Status: status.Status, Reply: reply})
	if err != nil {
		return fail(run.FailureExecution, err.Error())
	}
	return effect.Outcome{Tool: effect.ToolExecutionSucceeded{Result: run.ToolExecutionResult{Output: body}}}
}

// create makes the child Session with its provenance as segment metadata:
// empty for Empty, a fork of the parent's history before the calling Turn
// for Fork (HST-SPN-5).
func (e *Executor) create(ctx context.Context, key effect.AssignmentKey, child session.SessionID, args Arguments) (Provenance, error) {
	depth, err := e.depthOf(ctx, key.Session)
	if err != nil {
		return Provenance{}, err
	}
	prov := Provenance{ParentSession: key.Session, ParentRun: key.RunID, CallID: key.CallID, Depth: depth + 1, Arguments: args}
	meta, err := Metadata(prov)
	if err != nil {
		return Provenance{}, err
	}
	now := e.a.Clock().UnixMilli()
	switch args.Mode {
	case Fork:
		turnID, err := e.callingTurn(ctx, key)
		if err != nil {
			return Provenance{}, err
		}
		at, err := e.a.History.PrefixCommit(ctx, key.Session, turnID)
		if err != nil {
			return Provenance{}, err
		}
		_, err = writer.Fork(ctx, e.a.Store, e.a.Registry, e.a.Admission, writer.ForkRequest{
			Parent: key.Session, At: at, Child: child, CreatedAtUnixMilli: now, Metadata: meta})
		return prov, err
	default:
		_, err := e.a.Store.Create(ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: child, CreatedAtUnixMilli: now, Metadata: meta})
		return prov, err
	}
}

// callingTurn is the parent Turn that owns the Run the call belongs to.
func (e *Executor) callingTurn(ctx context.Context, key effect.AssignmentKey) (turn.TurnID, error) {
	surface, err := turn.ReadSurface(ctx, e.a.Projections, key.Session)
	if err != nil {
		return "", err
	}
	turnID, ok := surface.RunOwner[key.RunID]
	if !ok {
		return "", fmt.Errorf("run %s has no owning turn in %s", key.RunID, key.Session)
	}
	return turnID, nil
}

// childPreset is the named preset when the arguments name one, else the
// parent Turn's preset.
func (e *Executor) childPreset(ctx context.Context, key effect.AssignmentKey, args Arguments) (turn.PresetRef, error) {
	if args.Preset != "" {
		if e.opts.Presets == nil {
			return turn.PresetRef{}, errors.New("named presets are not configured")
		}
		return e.opts.Presets(args.Preset)
	}
	turnID, err := e.callingTurn(ctx, key)
	if err != nil {
		return turn.PresetRef{}, err
	}
	surface, err := turn.ReadSurface(ctx, e.a.Projections, key.Session)
	if err != nil {
		return turn.PresetRef{}, err
	}
	return surface.Turns[turnID].Preset, nil
}

// settle brings the child to a settled Turn for task and returns it. It
// starts from wherever the child's durable state is: nothing submitted yet,
// an input awaiting delivery, an active Turn, or a Turn that settled before
// the parent learned of it. A child runs exactly one Turn per task: the
// submitted backlog is the task itself, so there is no draining loop.
func (e *Executor) settle(ctx context.Context, h *authority.Handle, preset turn.PresetRef, task string) (turn.TurnID, error) {
	turns, err := turn.ReadSurface(ctx, e.a.Projections, h.ID())
	if err != nil {
		return "", err
	}
	if active, ok := turns.Active(); ok {
		return e.driveTurn(ctx, h, active.TurnID)
	}
	chat, err := chatlog.ReadSurface(ctx, e.a.Projections, h.ID())
	if err != nil {
		return "", err
	}
	if pending := chat.SubmittedInputs(); len(pending) > 0 {
		inputs := make([]run.AgentInput, len(pending))
		for i, in := range pending {
			inputs[i] = run.AgentInput{ID: run.InputID(in.ID), Payload: in.Content}
		}
		return e.startAndDrive(ctx, h, preset, inputs)
	}
	// No active Turn and nothing submitted: the task is done only when the
	// newest input is it. A fork-mode child's prefix holds just the
	// conversation before the calling Turn, so otherwise the task was never
	// submitted and is sent now.
	if last, found := newestInput(chat); found {
		var body struct {
			Text string `json:"text"`
		}
		if last.Input.Content.Decode(&body) == nil && body.Text == task && len(turns.Order) > 0 {
			return turns.Order[len(turns.Order)-1], nil
		}
	}
	in, err := e.a.Chatlog.Submit(ctx, h.Writer(), chatlog.NewInputID(), task)
	if err != nil {
		return "", err
	}
	return e.startAndDrive(ctx, h, preset, []run.AgentInput{in})
}

func (e *Executor) startAndDrive(ctx context.Context, h *authority.Handle, preset turn.PresetRef, inputs []run.AgentInput) (turn.TurnID, error) {
	ref := turn.TurnRef{SessionID: h.ID(), TurnID: turn.NewTurnID()}
	if _, err := e.a.Turns.Start(ctx, h.Writer(), turn.StartRequest{Ref: ref, Inputs: inputs, Preset: preset}); err != nil {
		return "", err
	}
	return e.driveTurn(ctx, h, ref.TurnID)
}

// driveTurn drives the Turn to settlement; a Turn another local driver
// already carries is an error, since the parent's call needs this drive's
// result.
func (e *Executor) driveTurn(ctx context.Context, h *authority.Handle, turnID turn.TurnID) (turn.TurnID, error) {
	resp, err := e.a.Driver.Drive(ctx, h.Writer(), turnID)
	if err != nil {
		return "", err
	}
	if resp.Disposition == authority.ResumeAlreadyDriving {
		return "", fmt.Errorf("subagent %s is driven elsewhere", h.ID())
	}
	return turnID, nil
}

// newestInput is the most recently submitted input of the chatlog surface.
func newestInput(chat chatlog.Surface) (chatlog.InputView, bool) {
	var best chatlog.InputView
	var found bool
	chat.Inputs.Range(func(_ chatlog.InputID, v chatlog.InputView) bool {
		if !found || v.Seq > best.Seq {
			best, found = v, true
		}
		return true
	})
	return best, found
}

// Attach answers for calls this process drives or drove; for a key whose
// derived child Session exists, it adopts the call and continues the child
// (RUN-CMT-7, HST-SPN-4).
func (e *Executor) Attach(ctx context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	if att, ok := e.local(key); ok {
		return att, nil
	}
	e.start(key, nil)
	if att, ok := e.local(key); ok {
		return att, nil
	}
	return effect.Attachment{State: effect.AttachmentMissing, Execution: effect.ExecutionNotFound}, nil
}

func (e *Executor) local(key effect.AssignmentKey) (effect.Attachment, bool) {
	e.mu.Lock()
	r, ok := e.inflight[key]
	if !ok {
		e.mu.Unlock()
		return effect.Attachment{}, false
	}
	closed, out := r.closed, r.outcome
	e.mu.Unlock()
	if !closed {
		return effect.Attachment{State: effect.AttachmentActive, Execution: effect.ExecutionRunning, BackendAttached: true}, true
	}
	return effect.Attachment{State: effect.AttachmentTerminal, Execution: status(out), BackendAttached: true}, true
}

func status(out effect.Outcome) effect.ExecutionStatus {
	switch {
	case out.Cancelled:
		return effect.ExecutionCancelled
	case out.Unknown:
		return effect.ExecutionUnknown
	}
	switch out.Tool.(type) {
	case effect.ToolExecutionUnknown:
		return effect.ExecutionUnknown
	case effect.ToolExecutionFailed:
		return effect.ExecutionFailed
	}
	return effect.ExecutionCompleted
}

func (e *Executor) GetStatus(ctx context.Context, key effect.AssignmentKey) (effect.ExecutionStatus, error) {
	if att, ok := e.local(key); ok {
		return att.Execution, nil
	}
	return effect.ExecutionNotFound, effect.ErrExecutionNotFound
}

func (e *Executor) GetOutcome(ctx context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
	e.mu.Lock()
	r, ok := e.inflight[key]
	e.mu.Unlock()
	if !ok {
		return effect.Outcome{}, effect.ErrExecutionNotFound
	}
	select {
	case <-r.done:
		e.mu.Lock()
		out := r.outcome
		e.mu.Unlock()
		return out, nil
	case <-ctx.Done():
		return effect.Outcome{}, ctx.Err()
	}
}

// Cancel stops driving the child; its Turn stays active for a later resume.
func (e *Executor) Cancel(ctx context.Context, key effect.AssignmentKey) error {
	e.mu.Lock()
	r, ok := e.inflight[key]
	e.mu.Unlock()
	if !ok {
		return effect.ErrExecutionNotFound
	}
	r.cancel()
	return nil
}

// PrepareBinding names the child Session as the durable execution reference
// a Worker records for the call (RUN-EXE-3, HST-SPN-2).
func (e *Executor) PrepareBinding(_ context.Context, a effect.Assignment) (effect.ExecutionBinding, error) {
	return effect.ExecutionBinding{Provider: Provider, ExecutionRef: string(ChildID(a.Session, a.RunID, a.CallID))}, nil
}

func (e *Executor) checkBinding(key effect.AssignmentKey, b effect.ExecutionBinding) error {
	if b.ExecutionRef != string(ChildID(key.Session, key.RunID, key.CallID)) {
		return fmt.Errorf("spawn: binding %s is not the child of call %s", b.ExecutionRef, key.CallID)
	}
	return nil
}

var _ effect.Port = (*Executor)(nil)

// --- interception ------------------------------------------------------------------

// intercept is the Port the Runs drive against when the spawn effect is
// enabled: the spawn tool's Assignments and the keys of children on record
// reach the Executor, everything else the inner Port.
type intercept struct {
	e     *Executor
	inner effect.Port
}

func (p *intercept) route(ctx context.Context, key effect.AssignmentKey) (effect.Port, error) {
	owns, err := p.e.owns(ctx, key)
	if err != nil {
		return nil, err
	}
	if owns {
		return p.e, nil
	}
	return p.inner, nil
}

func (p *intercept) Validate(ctx context.Context, a effect.Assignment) (*run.ToolFailure, error) {
	if p.e.ours(a) {
		return p.e.Validate(ctx, a)
	}
	return p.inner.Validate(ctx, a)
}

func (p *intercept) Dispatch(ctx context.Context, a effect.Assignment) error {
	if p.e.ours(a) {
		return p.e.Dispatch(ctx, a)
	}
	return p.inner.Dispatch(ctx, a)
}

func (p *intercept) Attach(ctx context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	port, err := p.route(ctx, key)
	if err != nil {
		return effect.Attachment{}, err
	}
	return port.Attach(ctx, key)
}

func (p *intercept) GetStatus(ctx context.Context, key effect.AssignmentKey) (effect.ExecutionStatus, error) {
	port, err := p.route(ctx, key)
	if err != nil {
		return effect.ExecutionNotFound, err
	}
	return port.GetStatus(ctx, key)
}

func (p *intercept) GetOutcome(ctx context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
	port, err := p.route(ctx, key)
	if err != nil {
		return effect.Outcome{}, err
	}
	return port.GetOutcome(ctx, key)
}

func (p *intercept) Cancel(ctx context.Context, key effect.AssignmentKey) error {
	port, err := p.route(ctx, key)
	if err != nil {
		return err
	}
	return port.Cancel(ctx, key)
}

func (p *intercept) innerBinding() (effect.BindingPort, error) {
	if bp, ok := p.inner.(effect.BindingPort); ok {
		return bp, nil
	}
	return nil, effect.ErrBindingUnsupported
}

func (p *intercept) PrepareBinding(ctx context.Context, a effect.Assignment) (effect.ExecutionBinding, error) {
	if p.e.ours(a) {
		return p.e.PrepareBinding(ctx, a)
	}
	bp, err := p.innerBinding()
	if err != nil {
		return effect.ExecutionBinding{}, err
	}
	return bp.PrepareBinding(ctx, a)
}

func (p *intercept) DispatchBound(ctx context.Context, a effect.Assignment, b effect.ExecutionBinding) error {
	if p.e.ours(a) {
		if err := p.e.checkBinding(a.Key(), b); err != nil {
			return err
		}
		return p.e.Dispatch(ctx, a)
	}
	bp, err := p.innerBinding()
	if err != nil {
		return err
	}
	return bp.DispatchBound(ctx, a, b)
}

func (p *intercept) boundRoute(ctx context.Context, key effect.AssignmentKey, b effect.ExecutionBinding) (ours bool, bp effect.BindingPort, err error) {
	if b.Provider == Provider {
		return true, nil, p.e.checkBinding(key, b)
	}
	bp, err = p.innerBinding()
	return false, bp, err
}

func (p *intercept) AttachBound(ctx context.Context, key effect.AssignmentKey, b effect.ExecutionBinding) (effect.Attachment, error) {
	ours, bp, err := p.boundRoute(ctx, key, b)
	if err != nil {
		return effect.Attachment{}, err
	}
	if ours {
		return p.e.Attach(ctx, key)
	}
	return bp.AttachBound(ctx, key, b)
}

func (p *intercept) GetStatusBound(ctx context.Context, key effect.AssignmentKey, b effect.ExecutionBinding) (effect.ExecutionStatus, error) {
	ours, bp, err := p.boundRoute(ctx, key, b)
	if err != nil {
		return effect.ExecutionNotFound, err
	}
	if ours {
		return p.e.GetStatus(ctx, key)
	}
	return bp.GetStatusBound(ctx, key, b)
}

func (p *intercept) GetOutcomeBound(ctx context.Context, key effect.AssignmentKey, b effect.ExecutionBinding) (effect.Outcome, error) {
	ours, bp, err := p.boundRoute(ctx, key, b)
	if err != nil {
		return effect.Outcome{}, err
	}
	if ours {
		return p.e.GetOutcome(ctx, key)
	}
	return bp.GetOutcomeBound(ctx, key, b)
}

func (p *intercept) CancelBound(ctx context.Context, key effect.AssignmentKey, b effect.ExecutionBinding) error {
	ours, bp, err := p.boundRoute(ctx, key, b)
	if err != nil {
		return err
	}
	if ours {
		return p.e.Cancel(ctx, key)
	}
	return bp.CancelBound(ctx, key, b)
}

var (
	_ effect.Port        = (*intercept)(nil)
	_ effect.BindingPort = (*intercept)(nil)
)
