package orchestration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/effect"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/extension"
	"github.com/felinics/twilight/agent/session/writer"
	"github.com/felinics/twilight/agent/spawn"
	"github.com/felinics/twilight/agent/turn"
)

// SpawnOptions configures the subagent effect (HST-SPN).
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
// catalogs. Its Execute never runs: the spawn effect intercepts the tool's
// Assignments.
func SpawnTool(opts SpawnOptions) loop.ExecutableTool { return spawn.Tool(opts.tool()) }

// SpawnSurfaces are the projections the spawn effect drives children by.
type SpawnSurfaces interface {
	TurnSurface(ctx context.Context, sid session.SessionID) (turn.TurnSurface, error)
	ChatlogSurface(ctx context.Context, sid session.SessionID) (chatlog.Surface, error)
}

// SpawnPorts are the environment dependencies of the spawn effect (HST-SPN).
// The executor holds no Host: everything it needs arrives through these
// ports, and child Sessions drive through the same Driver as any Turn.
type SpawnPorts struct {
	// Store creates and looks up child Sessions.
	Store session.Store
	// Registry and Admission fork children at a parent prefix.
	Registry  *extension.Registry
	Admission writer.Admission
	// History finds the fork boundary of the calling Turn.
	History turn.History
	// Surfaces read turn and chatlog projections.
	Surfaces SpawnSurfaces
	// Chatlog submits the child's task input.
	Chatlog *chatlog.Service
	// Coordinator starts the child's Turns.
	Coordinator turn.Service
	// Driver opens children (takeover disposition) and drives their Turns.
	Driver *Driver
	// Writers releases the child's handle after a drive.
	Writers writer.Writers
	// Content renders the child's reply (CHT-MAT-1).
	Content chatlog.ContentResolver
	// Now stamps child creation.
	Now func() time.Time
	// NewTurnID and NewInputID mint the child's Turn and Input IDs.
	NewTurnID  func() turn.TurnID
	NewInputID func() run.InputID
	// Warn receives materialization failures; nil discards them.
	Warn func(error)
}

// NewSpawnExecutor wraps inner so the spawn tool's Assignments run as child
// Sessions this process drives (HST-SPN-1); every other Assignment forwards
// to inner. The in-flight table is process-scoped like a local executor's;
// what outlives the process is the child Session.
func NewSpawnExecutor(inner effect.Port, opts SpawnOptions, ports SpawnPorts) *SpawnExecutor {
	return &SpawnExecutor{inner: inner, opts: opts, tool: spawn.Tool(opts.tool()), ports: ports, inflight: make(map[effect.AssignmentKey]*spawnRun)}
}

// SpawnExecutor is the subagent effect port.
type SpawnExecutor struct {
	inner effect.Port
	opts  SpawnOptions
	tool  loop.ExecutableTool
	ports SpawnPorts

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

func (e *SpawnExecutor) ours(a effect.Assignment) bool {
	return a.Kind == effect.AssignmentTool && a.Tool != nil && a.Tool.ToolRef == e.tool.Ref()
}

// Close cancels every child drive; their Turns stay active and resume on
// the next open of the parent (adoption through Attach).
func (e *SpawnExecutor) Close() {
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
func (e *SpawnExecutor) Validate(ctx context.Context, a effect.Assignment) (*run.ToolFailure, error) {
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
func (e *SpawnExecutor) depthOf(ctx context.Context, sid session.SessionID) (int, error) {
	prov, ok, err := e.provenance(ctx, sid)
	if err != nil || !ok {
		return 0, err
	}
	return prov.Depth, nil
}

// provenance reads the spawn record of a Session's tip segment; ok is false
// when the Session does not exist or was not spawned.
func (e *SpawnExecutor) provenance(ctx context.Context, sid session.SessionID) (spawn.Provenance, bool, error) {
	header, err := e.ports.Store.Header(ctx, sid)
	if err != nil {
		if session.IsCode(err, session.ErrNotFound) {
			return spawn.Provenance{}, false, nil
		}
		return spawn.Provenance{}, false, err
	}
	return spawn.ProvenanceFromHeader(header)
}

func (e *SpawnExecutor) Dispatch(ctx context.Context, a effect.Assignment) error {
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
// flight is an idempotent replay. args is nil when a takeover adopts the
// call from its child's provenance.
func (e *SpawnExecutor) start(key effect.AssignmentKey, args *spawn.Arguments) {
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
func (e *SpawnExecutor) drive(ctx context.Context, key effect.AssignmentKey, child session.SessionID, args *spawn.Arguments) effect.Outcome {
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
	if _, err := e.ports.Driver.Open(ctx, child); err != nil {
		return fail(run.FailureExecution, fmt.Sprintf("open subagent: %v", err))
	}
	defer func() {
		e.ports.Driver.Stop(child)
		_ = writer.CloseWriter(context.WithoutCancel(ctx), e.ports.Writers, child)
	}()
	result, err := e.settle(ctx, child, preset, prov.Arguments.Task)
	if err != nil {
		if ctx.Err() != nil {
			return fail(run.FailureCancelled, "subagent cancelled")
		}
		return fail(run.FailureExecution, fmt.Sprintf("drive subagent: %v", err))
	}
	out := spawn.Result{ChildSession: child, TurnID: result.turnID, Status: result.status, Reply: result.reply}
	if result.status != turn.TurnCompleted {
		return fail(run.FailureExecution, fmt.Sprintf("subagent %s turn %s ended %s", child, result.turnID, result.status))
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
func (e *SpawnExecutor) create(ctx context.Context, key effect.AssignmentKey, child session.SessionID, args spawn.Arguments) (spawn.Provenance, error) {
	depth, err := e.depthOf(ctx, key.Session)
	if err != nil {
		return spawn.Provenance{}, err
	}
	prov := spawn.Provenance{ParentSession: key.Session, ParentRun: key.RunID, CallID: key.CallID, Depth: depth + 1, Arguments: args}
	meta, err := spawn.Metadata(prov)
	if err != nil {
		return spawn.Provenance{}, err
	}
	now := e.ports.Now().UnixMilli()
	switch args.Mode {
	case spawn.Fork:
		turnID, err := e.callingTurn(ctx, key)
		if err != nil {
			return spawn.Provenance{}, err
		}
		at, err := e.ports.History.PrefixCommit(ctx, key.Session, turnID)
		if err != nil {
			return spawn.Provenance{}, err
		}
		_, err = writer.Fork(ctx, e.ports.Store, e.ports.Registry, e.ports.Admission, writer.ForkRequest{
			Parent: key.Session, At: at, Child: child, CreatedAtUnixMilli: now, Metadata: meta})
		return prov, err
	default:
		_, err := e.ports.Store.Create(ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: child, CreatedAtUnixMilli: now, Metadata: meta})
		return prov, err
	}
}

// callingTurn is the parent Turn that owns the Run the call belongs to.
func (e *SpawnExecutor) callingTurn(ctx context.Context, key effect.AssignmentKey) (turn.TurnID, error) {
	surface, err := e.ports.Surfaces.TurnSurface(ctx, key.Session)
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
func (e *SpawnExecutor) childPreset(ctx context.Context, key effect.AssignmentKey, args spawn.Arguments) (turn.PresetRef, error) {
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
	surface, err := e.ports.Surfaces.TurnSurface(ctx, key.Session)
	if err != nil {
		return turn.PresetRef{}, err
	}
	return surface.Turns[turnID].Preset, nil
}

// childResult is one settled child Turn the drive observed.
type childResult struct {
	turnID      turn.TurnID
	status      turn.TurnStatus
	disposition turn.ResumeDisposition
	reply       string
}

// settle brings the child to a settled Turn for task, from wherever its
// durable state is: nothing submitted yet, an input awaiting delivery, an
// active Turn, or a Turn that already settled before the parent learned of
// it. The routing, driving and draining mirror the Session facade
// (HST-SES-2, HST-SES-3, HST-DRV-4) written directly against the Driver, so
// a child needs no facade of its own.
func (e *SpawnExecutor) settle(ctx context.Context, child session.SessionID, preset turn.PresetRef, task string) (childResult, error) {
	chat, err := e.ports.Surfaces.ChatlogSurface(ctx, child)
	if err != nil {
		return childResult{}, err
	}
	tsurf, err := e.ports.Surfaces.TurnSurface(ctx, child)
	if err != nil {
		return childResult{}, err
	}
	var results []childResult
	switch active, ok := tsurf.Active(); {
	case chat.Inputs.Len() == 0:
		results, err = e.send(ctx, child, preset, task)
	case ok:
		var resp turn.TurnResponse
		resp, err = e.ports.Driver.Drive(ctx, turn.TurnRef{SessionID: child, TurnID: active.TurnID})
		if err == nil {
			results, err = e.settled(ctx, child, preset, resp)
		}
	case len(chat.SubmittedInputs()) > 0:
		var resp turn.TurnResponse
		resp, _, err = e.drain(ctx, child, preset)
		if err == nil {
			results, err = e.settled(ctx, child, preset, resp)
		}
	default:
		// No active Turn and no submitted input: the task was delivered and
		// settled only when the newest input is it. A fork-mode child's
		// prefix holds just the conversation before the calling Turn, so
		// otherwise the task was never submitted and must be sent now.
		last, found := newestInput(chat)
		var body struct {
			Text string `json:"text"`
		}
		if !found || last.Input.Content.Decode(&body) != nil || body.Text != task {
			results, err = e.send(ctx, child, preset, task)
		} else {
			return e.lastResult(ctx, child, &chat)
		}
	}
	if err != nil {
		return childResult{}, err
	}
	if len(results) == 0 {
		return e.lastResult(ctx, child, &chat)
	}
	last := results[len(results)-1]
	if last.disposition == ResumeAlreadyDriving {
		return childResult{}, fmt.Errorf("subagent %s is driven elsewhere", child)
	}
	return last, nil
}

// send submits task text and drives the Turn it lands in to settlement,
// draining the backlog after it: the Session facade's Send (HST-SES-2).
func (e *SpawnExecutor) send(ctx context.Context, child session.SessionID, preset turn.PresetRef, task string) ([]childResult, error) {
	in, err := e.ports.Chatlog.SubmitInput(ctx, child, e.ports.NewInputID(), task)
	if err != nil {
		return nil, err
	}
	ref, absorbed, err := e.routeInput(ctx, child, preset, in)
	if err != nil {
		return nil, err
	}
	if absorbed != nil {
		return []childResult{*absorbed}, nil
	}
	resp, err := e.ports.Driver.Drive(ctx, ref)
	if err != nil {
		return nil, err
	}
	return e.settled(ctx, child, preset, resp)
}

// routeInput commits one input's route with the conflict retry of HST-SES-3.
// It returns the Turn to drive, or the already_driving result when another
// driver took the input first.
func (e *SpawnExecutor) routeInput(ctx context.Context, child session.SessionID, preset turn.PresetRef, in run.AgentInput) (turn.TurnRef, *childResult, error) {
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		ref, err := e.commitRoute(ctx, child, preset, []run.AgentInput{in})
		if err == nil {
			return ref, nil, nil
		}
		if !errors.Is(err, turn.ErrConflict) {
			return turn.TurnRef{}, nil, err
		}
		lastErr = err
		if r, taken := e.absorbed(ctx, child, in); taken {
			return turn.TurnRef{}, &r, nil
		}
	}
	return turn.TurnRef{}, nil, lastErr
}

// commitRoute is the commit half of routing: Deliver into the active Turn,
// or Start a new one, returning the Turn the inputs landed in.
func (e *SpawnExecutor) commitRoute(ctx context.Context, child session.SessionID, preset turn.PresetRef, inputs []run.AgentInput) (turn.TurnRef, error) {
	surface, err := e.ports.Surfaces.TurnSurface(ctx, child)
	if err != nil {
		return turn.TurnRef{}, err
	}
	if active, ok := surface.Active(); ok {
		ref := turn.TurnRef{SessionID: child, TurnID: active.TurnID}
		if _, err := e.ports.Coordinator.Deliver(ctx, turn.DeliverRequest{Ref: ref, Inputs: inputs}); err != nil {
			return turn.TurnRef{}, err
		}
		return ref, nil
	}
	for _, v := range surface.Turns {
		if v.Status == turn.TurnAttemptFailed {
			return turn.TurnRef{}, fmt.Errorf("%w: turn %s awaits Retry or Settle", turn.ErrConflict, v.TurnID)
		}
	}
	ref := turn.TurnRef{SessionID: child, TurnID: e.ports.NewTurnID()}
	if _, err := e.ports.Coordinator.Start(ctx, turn.StartRequest{Ref: ref, Inputs: inputs, Preset: preset}); err != nil {
		return turn.TurnRef{}, err
	}
	return ref, nil
}

// drain is HST-DRV-4: start the next Turn from the backlog of submitted,
// undelivered inputs; ok is false when there is none.
func (e *SpawnExecutor) drain(ctx context.Context, child session.SessionID, preset turn.PresetRef) (turn.TurnResponse, bool, error) {
	surface, err := e.ports.Surfaces.ChatlogSurface(ctx, child)
	if err != nil {
		return turn.TurnResponse{}, false, err
	}
	pending := surface.SubmittedInputs()
	if len(pending) == 0 {
		return turn.TurnResponse{}, false, nil
	}
	inputs := make([]run.AgentInput, len(pending))
	for i, in := range pending {
		inputs[i] = run.AgentInput{ID: run.InputID(in.ID), Payload: in.Content}
	}
	ref, err := e.commitRoute(ctx, child, preset, inputs)
	if err != nil {
		return turn.TurnResponse{}, false, err
	}
	resp, err := e.ports.Driver.Drive(ctx, ref)
	return resp, err == nil, err
}

// absorbed reports whether another driver already delivered the input; the
// Turn that took it settles and reports there.
func (e *SpawnExecutor) absorbed(ctx context.Context, child session.SessionID, in run.AgentInput) (childResult, bool) {
	chat, err := e.ports.Surfaces.ChatlogSurface(ctx, child)
	if err != nil {
		return childResult{}, false
	}
	v, ok := chat.Inputs.Get(chatlog.InputID(in.ID))
	if !ok || v.Status == chatlog.InputSubmitted {
		return childResult{}, false
	}
	r := childResult{turnID: turn.TurnID(v.Input.TurnID), disposition: ResumeAlreadyDriving}
	if surface, serr := e.ports.Surfaces.TurnSurface(ctx, child); serr == nil {
		r.status = surface.Turns[r.turnID].Status
	}
	return r, true
}

// settled turns a TurnResponse into results and drains the backlog: while a
// settlement leaves submitted, undelivered inputs, the next Turn starts from
// them (HST-DRV-4).
func (e *SpawnExecutor) settled(ctx context.Context, child session.SessionID, preset turn.PresetRef, resp turn.TurnResponse) ([]childResult, error) {
	out := []childResult{e.result(ctx, child, resp)}
	if resp.Disposition == ResumeAlreadyDriving {
		// The running driver settles the Turn and drains in its own call.
		return out, nil
	}
	for range [64]struct{}{} {
		next, ok, err := e.drain(ctx, child, preset)
		if err != nil {
			if errors.Is(err, turn.ErrConflict) {
				// A concurrent driver took the backlog; it reports there.
				return out, nil
			}
			return out, err
		}
		if !ok {
			return out, nil
		}
		out = append(out, e.result(ctx, child, next))
		if next.Disposition == ResumeAlreadyDriving {
			return out, nil
		}
	}
	return out, errors.New("orchestration: drain did not converge")
}

func (e *SpawnExecutor) result(ctx context.Context, child session.SessionID, resp turn.TurnResponse) childResult {
	r := childResult{turnID: resp.Ref.TurnID, status: resp.Status, disposition: resp.Disposition}
	if resp.Disposition == turn.ResumeFinished {
		if chat, err := e.ports.Surfaces.ChatlogSurface(ctx, child); err == nil {
			r.reply = e.lastAssistantText(ctx, &chat, chatlog.TurnID(resp.Ref.TurnID))
		}
	}
	return r
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
func (e *SpawnExecutor) lastResult(ctx context.Context, child session.SessionID, chat *chatlog.Surface) (childResult, error) {
	surface, err := e.ports.Surfaces.TurnSurface(ctx, child)
	if err != nil {
		return childResult{}, err
	}
	if len(surface.Order) == 0 {
		return childResult{}, fmt.Errorf("subagent %s has no turn", child)
	}
	turnID := surface.Order[len(surface.Order)-1]
	r := childResult{turnID: turnID, status: surface.Turns[turnID].Status, disposition: turn.ResumeFinished}
	r.reply = e.lastAssistantText(ctx, chat, chatlog.TurnID(turnID))
	return r, nil
}

// lastAssistantText materializes the text of the Turn's last assistant
// entry: the Surface names the frozen ModelResult by digest, the content
// store holds it (CHT-MAT-1).
func (e *SpawnExecutor) lastAssistantText(ctx context.Context, chat *chatlog.Surface, turnID chatlog.TurnID) string {
	for i := len(chat.EntryOrder) - 1; i >= 0; i-- {
		en := chat.EntryOrder[i]
		if en.Kind != chatlog.EntryAssistant {
			continue
		}
		a, ok := chat.Assistants.Get(chatlog.AssistantID(en.ID))
		if !ok || a.TurnID != turnID {
			continue
		}
		entry := chatlog.Entry{Kind: chatlog.EntryAssistant, ID: en.ID, Digest: a.Digest, Assistant: &a}
		m, err := chatlog.NewMaterializer(e.ports.Content).Entry(ctx, &entry)
		if err != nil {
			if e.ports.Warn != nil {
				e.ports.Warn(fmt.Errorf("orchestration: materialize reply of turn %s: %w", turnID, err))
			}
			return ""
		}
		return m.Text()
	}
	return ""
}

// Attach answers for calls this process drives or drove; for any other key
// whose derived child Session exists, it adopts the call and continues the
// child (RUN-CMT-7). Keys with no such child belong to the inner Executor.
func (e *SpawnExecutor) Attach(ctx context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	if att, ok := e.local(key); ok {
		return att, nil
	}
	child := spawn.ChildID(key.Session, key.RunID, key.CallID)
	if _, err := e.ports.Store.Record(ctx, child); err == nil {
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

func (e *SpawnExecutor) local(key effect.AssignmentKey) (effect.Attachment, bool) {
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

func (e *SpawnExecutor) GetStatus(ctx context.Context, key effect.AssignmentKey) (effect.ExecutionStatus, error) {
	if att, ok := e.local(key); ok {
		return att.Execution, nil
	}
	return e.inner.GetStatus(ctx, key)
}

func (e *SpawnExecutor) GetOutcome(ctx context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
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
func (e *SpawnExecutor) Cancel(ctx context.Context, key effect.AssignmentKey) error {
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
func (e *SpawnExecutor) PrepareBinding(ctx context.Context, a effect.Assignment) (effect.ExecutionBinding, error) {
	if !e.ours(a) {
		if bp, ok := e.inner.(effect.BindingPort); ok {
			return bp.PrepareBinding(ctx, a)
		}
		return effect.ExecutionBinding{}, effect.ErrBindingUnsupported
	}
	return effect.ExecutionBinding{Provider: spawn.Provider, ExecutionRef: string(spawn.ChildID(a.Session, a.RunID, a.CallID))}, nil
}

func (e *SpawnExecutor) checkBinding(key effect.AssignmentKey, b effect.ExecutionBinding) (bool, error) {
	if b.Provider != spawn.Provider {
		return false, nil
	}
	if b.ExecutionRef != string(spawn.ChildID(key.Session, key.RunID, key.CallID)) {
		return true, fmt.Errorf("orchestration: spawn binding %s is not the child of call %s", b.ExecutionRef, key.CallID)
	}
	return true, nil
}

func (e *SpawnExecutor) innerBinding() (effect.BindingPort, error) {
	if bp, ok := e.inner.(effect.BindingPort); ok {
		return bp, nil
	}
	return nil, effect.ErrBindingUnsupported
}

func (e *SpawnExecutor) DispatchBound(ctx context.Context, a effect.Assignment, b effect.ExecutionBinding) error {
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

func (e *SpawnExecutor) AttachBound(ctx context.Context, key effect.AssignmentKey, b effect.ExecutionBinding) (effect.Attachment, error) {
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

func (e *SpawnExecutor) GetStatusBound(ctx context.Context, key effect.AssignmentKey, b effect.ExecutionBinding) (effect.ExecutionStatus, error) {
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

func (e *SpawnExecutor) GetOutcomeBound(ctx context.Context, key effect.AssignmentKey, b effect.ExecutionBinding) (effect.Outcome, error) {
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

func (e *SpawnExecutor) CancelBound(ctx context.Context, key effect.AssignmentKey, b effect.ExecutionBinding) error {
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
	_ effect.Port        = (*SpawnExecutor)(nil)
	_ effect.BindingPort = (*SpawnExecutor)(nil)
)
