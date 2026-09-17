package host

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/effect"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/writer"
	"github.com/felinics/twilight/agent/spawn"
	"github.com/felinics/twilight/agent/turn"
)

// SpawnOptions configures the subagent effect (Ports.Spawn, HST-SPN).
type SpawnOptions struct {
	// Tool is the ToolRef the model calls; empty selects spawn.DefaultTool.
	Tool run.ToolRef
	// Presets resolves the optional preset name in the arguments to the
	// PresetRef the child runs under. Nil allows no name: the child runs
	// under the parent Turn's preset.
	Presets func(name string) (turn.PresetRef, error)
	// MaxDepth bounds nesting: a Session at this depth cannot spawn. Zero
	// selects spawn.DefaultDepth.
	MaxDepth int
}

func (o SpawnOptions) tool() run.ToolRef {
	if o.Tool == "" {
		return spawn.DefaultTool
	}
	return o.Tool
}

func (o SpawnOptions) depth() int {
	if o.MaxDepth <= 0 {
		return spawn.DefaultDepth
	}
	return o.MaxDepth
}

// SpawnTool is the model-facing definition of the spawn tool for preset
// catalogs. Its Execute never runs: the Host intercepts the tool's
// Assignments (Ports.Spawn).
func SpawnTool(opts SpawnOptions) loop.ExecutableTool { return spawn.Tool(opts.tool()) }

// --- executor --------------------------------------------------------------------

// spawnExecutor is the Host's effect port: it runs the spawn tool's
// Assignments itself and forwards every other Assignment to the deployment's
// Executor. Its in-flight table is process-scoped like LocalExecutor's; what
// outlives the process is the child Session. The protocol primitives it
// applies live in agent/spawn.
type spawnExecutor struct {
	h     *Host
	inner loop.Executor
	opts  SpawnOptions
	tool  loop.ExecutableTool

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

func newSpawnExecutor(h *Host, inner loop.Executor, opts SpawnOptions) *spawnExecutor {
	return &spawnExecutor{h: h, inner: inner, opts: opts, tool: spawn.Tool(opts.tool()), inflight: make(map[effect.AssignmentKey]*spawnRun)}
}

func (e *spawnExecutor) ours(a effect.Assignment) bool {
	return a.Kind == effect.AssignmentTool && a.Tool != nil && a.Tool.ToolRef == e.tool.Ref()
}

// close cancels every child drive; their Turns stay active and resume on
// the next open of the parent (adoption through Attach).
func (e *spawnExecutor) close() {
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
func (e *spawnExecutor) Validate(ctx context.Context, a effect.Assignment) (*run.ToolFailure, error) {
	if !e.ours(a) {
		return e.inner.Validate(ctx, a)
	}
	proto, err := run.ProtocolFor(a.Schema)
	if err != nil {
		return nil, err
	}
	if failure, err := spawn.CheckDefinition(proto, e.tool, a.Tool); err != nil || failure != nil {
		return failure, err
	}
	args, err := spawn.DecodeArguments(a.Tool.Arguments)
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
	if spawn.DepthExceeded(depth, e.opts.depth()) {
		return &run.ToolFailure{Class: run.FailureExecution, Message: fmt.Sprintf("subagent depth %d reached", e.opts.depth())}, nil
	}
	child := spawn.ChildID(a.Session, a.RunID, a.CallID)
	if prov, ok, err := e.provenance(ctx, child); err != nil {
		return nil, err
	} else if ok && spawn.ArgumentsConflict(prov, args) {
		return &run.ToolFailure{Class: run.FailureExecution, Message: fmt.Sprintf("call %s already spawned %s with different arguments", a.CallID, child)}, nil
	}
	return nil, nil
}

// depthOf reads a Session's nesting depth from its segment metadata; a
// Session no spawn created is at depth 0.
func (e *spawnExecutor) depthOf(ctx context.Context, sid session.SessionID) (int, error) {
	prov, ok, err := e.provenance(ctx, sid)
	if err != nil || !ok {
		return 0, err
	}
	return prov.Depth, nil
}

// provenance reads the spawn record of a Session's tip segment; ok is false
// when the Session does not exist or was not spawned.
func (e *spawnExecutor) provenance(ctx context.Context, sid session.SessionID) (spawn.Provenance, bool, error) {
	header, err := e.h.Store.Header(ctx, sid)
	if err != nil {
		if session.IsCode(err, session.ErrNotFound) {
			return spawn.Provenance{}, false, nil
		}
		return spawn.Provenance{}, false, err
	}
	return spawn.ProvenanceFromHeader(header)
}

func (e *spawnExecutor) Dispatch(ctx context.Context, a effect.Assignment) error {
	if !e.ours(a) {
		return e.inner.Dispatch(ctx, a)
	}
	args, err := spawn.DecodeArguments(a.Tool.Arguments)
	if err != nil {
		return fmt.Errorf("%w: %v", loop.ErrExecutorRejected, err)
	}
	e.start(a.Key(), &args)
	return nil
}

// start registers the call and begins driving its child; a key already in
// flight is an idempotent replay. args is nil when a takeover adopts the call
// from its child's provenance.
func (e *spawnExecutor) start(key effect.AssignmentKey, args *spawn.Arguments) {
	e.mu.Lock()
	if _, dup := e.inflight[key]; dup || e.closed {
		e.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &spawnRun{child: spawn.ChildID(key.Session, key.RunID, key.CallID), cancel: cancel, done: make(chan struct{})}
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
func (e *spawnExecutor) drive(ctx context.Context, key effect.AssignmentKey, child session.SessionID, args *spawn.Arguments) effect.Outcome {
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
	} else if args != nil && spawn.ArgumentsConflict(prov, *args) {
		return fail(run.FailureExecution, fmt.Sprintf("call %s already spawned %s with different arguments", key.CallID, child))
	}
	preset, err := e.childPreset(ctx, key, prov.Arguments)
	if err != nil {
		return fail(run.FailureExecution, err.Error())
	}
	cs, err := e.h.OpenSession(ctx, child, SessionOptions{Preset: preset})
	if err != nil {
		return fail(run.FailureExecution, fmt.Sprintf("open subagent: %v", err))
	}
	defer func() { _ = cs.Close(context.WithoutCancel(ctx)) }()
	result, err := e.settle(ctx, cs, prov.Arguments.Task)
	if err != nil {
		if ctx.Err() != nil {
			return fail(run.FailureCancelled, "subagent cancelled")
		}
		return fail(run.FailureExecution, fmt.Sprintf("drive subagent: %v", err))
	}
	out := spawn.Result{ChildSession: child, TurnID: result.TurnID, Status: result.Status, Reply: result.Reply}
	if result.Status != turn.TurnCompleted {
		return fail(run.FailureExecution, fmt.Sprintf("subagent %s turn %s ended %s", child, result.TurnID, result.Status))
	}
	body, err := run.CanonicalJSONFromValue(out)
	if err != nil {
		return fail(run.FailureExecution, err.Error())
	}
	return effect.Outcome{Tool: effect.ToolExecutionSucceeded{Result: run.ToolExecutionResult{Output: body}}}
}

// create makes the child Session with its provenance as segment metadata:
// empty for spawn.Empty, a fork of the parent's history before the calling
// Turn for spawn.Fork.
func (e *spawnExecutor) create(ctx context.Context, key effect.AssignmentKey, child session.SessionID, args spawn.Arguments) (spawn.Provenance, error) {
	depth, err := e.depthOf(ctx, key.Session)
	if err != nil {
		return spawn.Provenance{}, err
	}
	prov := spawn.Provenance{ParentSession: key.Session, ParentRun: key.RunID, CallID: key.CallID, Depth: depth + 1, Arguments: args}
	meta, err := spawn.Metadata(prov)
	if err != nil {
		return spawn.Provenance{}, err
	}
	now := e.h.now().UnixMilli()
	switch args.Mode {
	case spawn.Fork:
		turnID, err := e.callingTurn(ctx, key)
		if err != nil {
			return spawn.Provenance{}, err
		}
		at, err := e.h.turnPrefixCommit(ctx, key.Session, turnID)
		if err != nil {
			return spawn.Provenance{}, err
		}
		_, err = writer.Fork(ctx, e.h.Store, e.h.registry, e.h.admission, writer.ForkRequest{
			Parent: key.Session, At: at, Child: child, CreatedAtUnixMilli: now, Metadata: meta})
		return prov, err
	default:
		_, err := e.h.Store.Create(ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: child, CreatedAtUnixMilli: now, Metadata: meta})
		return prov, err
	}
}

// callingTurn is the parent Turn that owns the Run the call belongs to.
func (e *spawnExecutor) callingTurn(ctx context.Context, key effect.AssignmentKey) (turn.TurnID, error) {
	surface, err := e.h.TurnSurface(ctx, key.Session)
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
func (e *spawnExecutor) childPreset(ctx context.Context, key effect.AssignmentKey, args spawn.Arguments) (turn.PresetRef, error) {
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
	surface, err := e.h.TurnSurface(ctx, key.Session)
	if err != nil {
		return turn.PresetRef{}, err
	}
	return surface.Turns[turnID].Preset, nil
}

// settle brings the child to a settled Turn for task, from wherever its
// durable state is: nothing submitted yet, an input awaiting delivery, an
// active Turn, or a Turn that already settled before the parent learned of
// it.
func (e *spawnExecutor) settle(ctx context.Context, cs *Session, task string) (Result, error) {
	chat, err := e.h.ChatlogSurface(ctx, cs.sid)
	if err != nil {
		return Result{}, err
	}
	status, err := cs.Status(ctx)
	if err != nil {
		return Result{}, err
	}
	var results []Result
	switch {
	case chat.Inputs.Len() == 0:
		results, err = cs.Send(ctx, task)
	case status.Active != "":
		results, _, err = cs.Resume(ctx)
	case len(chat.SubmittedInputs()) > 0:
		var resp turn.TurnResponse
		if resp, _, err = cs.Drain(ctx); err == nil {
			results, err = cs.settled(ctx, resp)
		}
	default:
		// No active Turn and no submitted input: the task was delivered and
		// settled only when the newest input is it. A fork-mode child's
		// prefix holds just the conversation before the calling Turn, so
		// otherwise the task was never submitted and must be sent now.
		last, ok := newestInput(chat)
		var body struct {
			Text string `json:"text"`
		}
		if !ok || last.Input.Content.Decode(&body) != nil || body.Text != task {
			results, err = cs.Send(ctx, task)
		} else {
			return e.lastResult(ctx, cs)
		}
	}
	if err != nil {
		return Result{}, err
	}
	if len(results) == 0 {
		return e.lastResult(ctx, cs)
	}
	last := results[len(results)-1]
	if last.Disposition == ResumeAlreadyDriving {
		return Result{}, fmt.Errorf("subagent %s is driven elsewhere", cs.sid)
	}
	return last, nil
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

// lastResult reads the child's most recent Turn and its reply from the
// projections.
func (e *spawnExecutor) lastResult(ctx context.Context, cs *Session) (Result, error) {
	surface, err := e.h.TurnSurface(ctx, cs.sid)
	if err != nil {
		return Result{}, err
	}
	if len(surface.Order) == 0 {
		return Result{}, fmt.Errorf("subagent %s has no turn", cs.sid)
	}
	turnID := surface.Order[len(surface.Order)-1]
	view := surface.Turns[turnID]
	r := Result{TurnID: turnID, Status: view.Status, Disposition: turn.ResumeFinished}
	chat, err := e.h.ChatlogSurface(ctx, cs.sid)
	if err != nil {
		return Result{}, err
	}
	r.Reply = cs.lastAssistantText(ctx, &chat, chatlog.TurnID(turnID))
	return r, nil
}

// Attach answers for calls this process drives or drove; for any other key
// whose derived child Session exists, it adopts the call and continues the
// child (RUN-CMT-7). Keys with no such child belong to the inner Executor.
func (e *spawnExecutor) Attach(ctx context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	if att, ok := e.local(key); ok {
		return att, nil
	}
	child := spawn.ChildID(key.Session, key.RunID, key.CallID)
	if _, err := e.h.Store.Record(ctx, child); err == nil {
		e.start(key, nil)
		if att, ok := e.local(key); ok {
			return att, nil
		}
		return effect.Attachment{State: effect.AttachmentMissing, Execution: effect.ExecutionNotFound}, nil
	} else if !session.IsCode(err, session.ErrNotFound) {
		return effect.Attachment{}, err
	}
	return e.inner.Attach(ctx, key)
}

func (e *spawnExecutor) local(key effect.AssignmentKey) (effect.Attachment, bool) {
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
	return effect.Attachment{State: effect.AttachmentTerminal, Execution: spawnStatus(out), BackendAttached: true}, true
}

func spawnStatus(out effect.Outcome) effect.ExecutionStatus {
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

func (e *spawnExecutor) GetStatus(ctx context.Context, key effect.AssignmentKey) (effect.ExecutionStatus, error) {
	if att, ok := e.local(key); ok {
		return att.Execution, nil
	}
	return e.inner.GetStatus(ctx, key)
}

func (e *spawnExecutor) GetOutcome(ctx context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
	e.mu.Lock()
	r, ok := e.inflight[key]
	e.mu.Unlock()
	if !ok {
		return e.inner.GetOutcome(ctx, key)
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
func (e *spawnExecutor) Cancel(ctx context.Context, key effect.AssignmentKey) error {
	e.mu.Lock()
	r, ok := e.inflight[key]
	e.mu.Unlock()
	if !ok {
		return e.inner.Cancel(ctx, key)
	}
	r.cancel()
	return nil
}

// --- BindingPort -------------------------------------------------------------------

// PrepareBinding names the child Session as the durable execution reference
// a Worker records for the call (RUN-EXE-3).
func (e *spawnExecutor) PrepareBinding(ctx context.Context, a effect.Assignment) (effect.ExecutionBinding, error) {
	if !e.ours(a) {
		if bp, ok := e.inner.(effect.BindingPort); ok {
			return bp.PrepareBinding(ctx, a)
		}
		return effect.ExecutionBinding{}, effect.ErrBindingUnsupported
	}
	return effect.ExecutionBinding{Provider: spawn.Provider, ExecutionRef: string(spawn.ChildID(a.Session, a.RunID, a.CallID))}, nil
}

func (e *spawnExecutor) checkBinding(key effect.AssignmentKey, b effect.ExecutionBinding) (bool, error) {
	if b.Provider != spawn.Provider {
		return false, nil
	}
	if b.ExecutionRef != string(spawn.ChildID(key.Session, key.RunID, key.CallID)) {
		return true, fmt.Errorf("host: spawn binding %s is not the child of call %s", b.ExecutionRef, key.CallID)
	}
	return true, nil
}

func (e *spawnExecutor) innerBinding() (effect.BindingPort, error) {
	if bp, ok := e.inner.(effect.BindingPort); ok {
		return bp, nil
	}
	return nil, effect.ErrBindingUnsupported
}

func (e *spawnExecutor) DispatchBound(ctx context.Context, a effect.Assignment, b effect.ExecutionBinding) error {
	if ours, err := e.checkBinding(a.Key(), b); err != nil {
		return err
	} else if ours {
		return e.Dispatch(ctx, a)
	}
	bp, err := e.innerBinding()
	if err != nil {
		return err
	}
	return bp.DispatchBound(ctx, a, b)
}

func (e *spawnExecutor) AttachBound(ctx context.Context, key effect.AssignmentKey, b effect.ExecutionBinding) (effect.Attachment, error) {
	if ours, err := e.checkBinding(key, b); err != nil {
		return effect.Attachment{}, err
	} else if ours {
		return e.Attach(ctx, key)
	}
	bp, err := e.innerBinding()
	if err != nil {
		return effect.Attachment{}, err
	}
	return bp.AttachBound(ctx, key, b)
}

func (e *spawnExecutor) GetStatusBound(ctx context.Context, key effect.AssignmentKey, b effect.ExecutionBinding) (effect.ExecutionStatus, error) {
	if ours, err := e.checkBinding(key, b); err != nil {
		return effect.ExecutionNotFound, err
	} else if ours {
		return e.GetStatus(ctx, key)
	}
	bp, err := e.innerBinding()
	if err != nil {
		return effect.ExecutionNotFound, err
	}
	return bp.GetStatusBound(ctx, key, b)
}

func (e *spawnExecutor) GetOutcomeBound(ctx context.Context, key effect.AssignmentKey, b effect.ExecutionBinding) (effect.Outcome, error) {
	if ours, err := e.checkBinding(key, b); err != nil {
		return effect.Outcome{}, err
	} else if ours {
		return e.GetOutcome(ctx, key)
	}
	bp, err := e.innerBinding()
	if err != nil {
		return effect.Outcome{}, err
	}
	return bp.GetOutcomeBound(ctx, key, b)
}

func (e *spawnExecutor) CancelBound(ctx context.Context, key effect.AssignmentKey, b effect.ExecutionBinding) error {
	if ours, err := e.checkBinding(key, b); err != nil {
		return err
	} else if ours {
		return e.Cancel(ctx, key)
	}
	bp, err := e.innerBinding()
	if err != nil {
		return err
	}
	return bp.CancelBound(ctx, key, b)
}

var (
	_ effect.Port        = (*spawnExecutor)(nil)
	_ effect.BindingPort = (*spawnExecutor)(nil)
)
