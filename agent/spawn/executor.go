package spawn

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/felinics/twilight/agent/authority"
	"github.com/felinics/twilight/agent/executor"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/effect"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/writer"
	"github.com/felinics/twilight/agent/turn"
)

// Options configures the subagent effect (SPN).
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
// catalogs. Its Execute never runs: the Worker routes the tool's Assignments
// to the spawn Backend (SPN-1).
func (o Options) ExecutableTool() loop.ExecutableTool { return Tool(o.ToolRef()) }

func (o Options) depth() int {
	if o.MaxDepth <= 0 {
		return DefaultDepth
	}
	return o.MaxDepth
}

// Executor is the subagent Backend (SPN-1, RUN-EXE-9): the spawn tool's
// Assignments are executions whose Ref is the child SessionID. Start creates
// the child (or continues one on record) and drives it through the
// authority to a settled Turn; Attach adopts a child that exists while no
// drive of it runs here, which is how a takeover continues a call the dead
// owner left executing (SPN-4). The in-flight table is process-scoped like
// the colocated backend's; what outlives the process is the child Session
// and the Worker's record.
type Executor struct {
	opts Options
	tool loop.ExecutableTool
	// a is the authority children are created in and driven through: Open
	// yields the child's ownership Handle and its Turns, Driver and Chatlog
	// commands run through that Handle's Writer.
	a *authority.Authority

	mu       sync.Mutex
	inflight map[string]*spawnRun
	closed   bool
}

type spawnRun struct {
	cancel  context.CancelFunc
	done    chan struct{}
	outcome effect.Outcome
	closed  bool
}

// NewExecutor returns the spawn Backend; Bind supplies the authority it
// drives children through once that authority exists (it is built over the
// Worker that routes to this Backend).
func NewExecutor(opts Options) *Executor {
	return &Executor{opts: opts, tool: Tool(opts.ToolRef()), inflight: make(map[string]*spawnRun)}
}

// Bind supplies the authority. It must precede the first Start or Attach.
func (e *Executor) Bind(a *authority.Authority) { e.a = a }

// Match reports the Assignments this Backend serves: the spawn tool's.
func (e *Executor) Match(a effect.Assignment) bool {
	tool, ok := a.Tool()
	return ok && tool.ToolRef == e.tool.Ref()
}

// Route is the Worker route that hands the spawn tool's Assignments to e
// under Provider (RUN-EXE-10).
func Route(e *Executor) executor.Route {
	return executor.Route{Provider: Provider, Match: e.Match, Backend: e}
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
	schema, err := run.SchemaFor(a.Schema)
	if err != nil {
		return nil, err
	}
	tool, ok := a.Tool()
	if !ok {
		return &run.ToolFailure{Class: run.FailureInvalidArguments, Message: "spawn assignment without a tool body"}, nil
	}
	if failure, err := CheckDefinition(schema, e.tool, &tool); err != nil || failure != nil {
		return failure, err
	}
	args, err := DecodeArguments(tool.Arguments)
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
	depth, err := e.depthOf(ctx, session.SessionID(a.Session))
	if err != nil {
		return nil, err
	}
	if DepthExceeded(depth, e.opts.depth()) {
		return &run.ToolFailure{Class: run.FailureExecution, Message: fmt.Sprintf("subagent depth %d reached", e.opts.depth())}, nil
	}
	child := ChildID(session.SessionID(a.Session), a.RunID, a.CallID)
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

// Prepare derives the Ref: the child SessionID, a function of the call's
// identity (SPN-2), so a replayed Prepare names the same child.
func (e *Executor) Prepare(_ context.Context, a effect.Assignment) (string, error) {
	return string(ChildID(session.SessionID(a.Session), a.RunID, a.CallID)), nil
}

// Restart keeps the Ref: the child Session is the durable execution and a
// takeover continues it rather than creating another child (SPN-4).
func (e *Executor) Restart(_ context.Context, previous string, _ effect.Assignment) (string, error) {
	return previous, nil
}

// Start begins driving the child ref names for the call a; a ref already
// driven here is a no-op.
func (e *Executor) Start(_ context.Context, ref string, a effect.Assignment) error {
	tool, ok := a.Tool()
	if !ok {
		return fmt.Errorf("%w: spawn assignment without a tool body", loop.ErrExecutorRejected)
	}
	args, err := DecodeArguments(tool.Arguments)
	if err != nil {
		return fmt.Errorf("%w: %w", loop.ErrExecutorRejected, err)
	}
	e.start(ref, a.Key(), &args)
	return nil
}

// start registers the drive of ref and begins it; a ref already in flight is
// an idempotent replay. args is nil when an adoption continues the call from
// the child's provenance.
func (e *Executor) start(ref string, key effect.AssignmentKey, args *Arguments) {
	e.mu.Lock()
	if _, dup := e.inflight[ref]; dup || e.closed {
		e.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &spawnRun{cancel: cancel, done: make(chan struct{})}
	e.inflight[ref] = r
	e.mu.Unlock()
	go func() {
		out := e.drive(ctx, key, session.SessionID(ref), args)
		out.Key = key
		if ctx.Err() != nil {
			if _, done := out.Result.(effect.ToolExecutionSucceeded); !done {
				out.Result = effect.Cancelled{Message: ctx.Err().Error()}
			}
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
		return effect.Outcome{Result: effect.ToolExecutionFailed{Failure: run.ToolFailure{Class: class, Message: msg}}}
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
	return effect.Outcome{Result: effect.ToolExecutionSucceeded{Result: run.ToolExecutionResult{Output: body}}}
}

// create makes the child Session with its provenance as segment metadata:
// empty for Empty, a fork of the parent's history before the calling Turn
// for Fork (SPN-5).
func (e *Executor) create(ctx context.Context, key effect.AssignmentKey, child session.SessionID, args Arguments) (Provenance, error) {
	depth, err := e.depthOf(ctx, session.SessionID(key.Session))
	if err != nil {
		return Provenance{}, err
	}
	prov := Provenance{ParentSession: session.SessionID(key.Session), ParentRun: key.RunID, CallID: key.CallID, Depth: depth + 1, Arguments: args}
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
		at, err := e.a.History.PrefixCommit(ctx, session.SessionID(key.Session), turnID)
		if err != nil {
			return Provenance{}, err
		}
		_, err = writer.Fork(ctx, e.a.Store, e.a.Registry, e.a.Admission, writer.ForkRequest{
			Parent: session.SessionID(key.Session), At: at, Child: child, CreatedAtUnixMilli: now, Metadata: meta})
		return prov, err
	default:
		_, err := e.a.Store.Create(ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: child, CreatedAtUnixMilli: now, Metadata: meta})
		return prov, err
	}
}

// callingTurn is the parent Turn that owns the Run the call belongs to.
func (e *Executor) callingTurn(ctx context.Context, key effect.AssignmentKey) (turn.TurnID, error) {
	surface, err := turn.ReadSurface(ctx, e.a.Projections, session.SessionID(key.Session))
	if err != nil {
		return "", err
	}
	turnID, ok := surface.OwnerOf(key.RunID)
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
	surface, err := turn.ReadSurface(ctx, e.a.Projections, session.SessionID(key.Session))
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
			inputs[i] = run.AgentInput{ID: run.InputID(in.ID), Digest: in.Digest}
		}
		return e.startAndDrive(ctx, h, preset, inputs)
	}
	// No active Turn and nothing submitted: the task is done only when the
	// newest input is it. A fork-mode child's prefix holds just the
	// conversation before the calling Turn, so otherwise the task was never
	// submitted and is sent now.
	if last, found := newestInput(&chat); found {
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
// result. A drive that quiesces waiting for recovery -- the child's own
// execution records belong to a dead owner and await the control plane's
// takeover (RUN-EXE-6) -- is not the end of the Turn: the adopted Outcome is
// delivered and the Run driven on by the recovery lifetime, so this waits for
// the Turn to move and drives again.
func (e *Executor) driveTurn(ctx context.Context, h *authority.Handle, turnID turn.TurnID) (turn.TurnID, error) {
	ref := turn.TurnRef{SessionID: h.ID(), TurnID: turnID}
	for {
		resp, err := e.a.Driver.Drive(ctx, h.Writer(), turnID)
		if err != nil {
			return "", err
		}
		if resp.AlreadyDriving {
			return "", fmt.Errorf("subagent %s is driven elsewhere", h.ID())
		}
		switch resp.Disposition {
		case turn.ResumeWaitingForRecovery:
			if err := e.awaitRecovery(ctx, ref); err != nil {
				return "", err
			}
		default:
			return turnID, nil
		}
	}
}

// awaitRecovery waits until the Turn leaves waiting_for_recovery: its
// deferred executions were adopted and settled, or the Turn settled.
func (e *Executor) awaitRecovery(ctx context.Context, ref turn.TurnRef) error {
	delay := 10 * time.Millisecond
	for {
		resp, err := e.a.Turns.Status(ctx, ref)
		if err != nil {
			return err
		}
		if resp.Status != turn.TurnActive || resp.Disposition != turn.ResumeWaitingForRecovery {
			return nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		delay = min(delay*2, 250*time.Millisecond)
	}
}

// newestInput is the most recently submitted input of the chatlog surface.
func newestInput(chat *chatlog.Surface) (chatlog.InputView, bool) {
	var best chatlog.InputView
	var found bool
	chat.Inputs.Range(func(_ chatlog.InputID, v chatlog.InputView) bool {
		if !found || best.Position.Less(v.Position) {
			best, found = v, true
		}
		return true
	})
	return best, found
}

// Attach answers for refs this process drives or drove. A ref no drive here
// knows whose child Session exists is adopted: its provenance names the call,
// and the drive continues from the child's durable state (SPN-4). A ref with
// no child is missing.
func (e *Executor) Attach(ctx context.Context, ref string) (effect.Attachment, error) {
	if att, ok := e.local(ref); ok {
		return att, nil
	}
	prov, exists, err := e.provenance(ctx, session.SessionID(ref))
	if err != nil {
		return effect.Attachment{}, err
	}
	if !exists {
		return effect.Attachment{State: effect.AttachmentMissing, Execution: effect.ExecutionNotFound}, nil
	}
	e.start(ref, effect.AssignmentKey{Session: run.Scope(prov.ParentSession), RunID: prov.ParentRun, CallID: prov.CallID}, nil)
	if att, ok := e.local(ref); ok {
		return att, nil
	}
	return effect.Attachment{State: effect.AttachmentMissing, Execution: effect.ExecutionNotFound}, nil
}

func (e *Executor) local(ref string) (effect.Attachment, bool) {
	e.mu.Lock()
	r, ok := e.inflight[ref]
	if !ok {
		e.mu.Unlock()
		return effect.Attachment{}, false
	}
	closed, out := r.closed, r.outcome
	e.mu.Unlock()
	if !closed {
		return effect.Attachment{State: effect.AttachmentActive, Execution: effect.ExecutionRunning, BackendAttached: true}, true
	}
	return effect.Attachment{State: effect.AttachmentTerminal, Execution: out.Status(), BackendAttached: true}, true
}

func (e *Executor) Status(_ context.Context, ref string) (effect.ExecutionStatus, error) {
	if att, ok := e.local(ref); ok {
		return att.Execution, nil
	}
	return effect.ExecutionNotFound, effect.ErrExecutionNotFound
}

func (e *Executor) Outcome(ctx context.Context, ref string) (effect.Outcome, error) {
	e.mu.Lock()
	r, ok := e.inflight[ref]
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
func (e *Executor) Cancel(_ context.Context, ref string) error {
	e.mu.Lock()
	r, ok := e.inflight[ref]
	e.mu.Unlock()
	if !ok {
		return effect.ErrExecutionNotFound
	}
	r.cancel()
	return nil
}

var _ executor.ExecutionBackend = (*Executor)(nil)
