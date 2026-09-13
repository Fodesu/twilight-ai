package loop

import (
	"context"
	"errors"
	"fmt"
	"sync"

	run "github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
)

// Loop is the decision interpreter of one Run (RUN-LOP-2). It holds no
// authoritative state: every step starts from Runtime.Load, derives the next
// effect with run.Next, records the protocol transition and hands the effect
// to the Executor as an Assignment. Outcomes come back as data through
// Deliver and are settled under the attempt's Claim; the Loop never waits on
// an effect inside Advance.
type Loop struct {
	Executor  Executor
	Planner   RequestPlanner
	Execution ExecutionPolicy

	mu       sync.Mutex
	slots    map[run.RunID]*runSlot
	eventsMu sync.Mutex
}

// runSlot serializes one Run: step guards a single Advance or Deliver at a
// time; driving marks a blocking Run in progress so a second driver is
// reported instead of interleaved (RUN-CMT-6).
type runSlot struct {
	step    sync.Mutex
	driving bool
}

// New validates the execution policy (RUN-LOP-1) and binds the executor.
func New(exec Executor, planner RequestPlanner, policy ExecutionPolicy) (*Loop, error) {
	if exec == nil {
		return nil, errors.New("agent: loop: nil executor")
	}
	if planner == nil {
		return nil, errors.New("agent: loop: nil request planner")
	}
	if policy.ToolExecution != "" && policy.ToolExecution != ToolExecutionParallel && policy.ToolExecution != ToolExecutionSequential {
		return nil, fmt.Errorf("agent: loop: unknown ToolExecution mode %q", policy.ToolExecution)
	}
	if policy.MaxParallel < 0 {
		return nil, errors.New("agent: loop: negative MaxParallel")
	}
	return &Loop{Executor: exec, Planner: planner, Execution: policy, slots: make(map[run.RunID]*runSlot)}, nil
}

func (l *Loop) toolScheduling() run.ToolScheduling {
	mode := run.ToolScheduleMode(l.Execution.ToolExecution)
	if mode == "" {
		mode = run.ToolScheduleParallel
	}
	return run.ToolScheduling{Mode: mode, MaxParallel: l.Execution.MaxParallel}
}

func (l *Loop) slot(runID run.RunID) *runSlot {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.slots[runID]
	if !ok {
		s = &runSlot{}
		l.slots[runID] = s
	}
	return s
}

func (l *Loop) startDriving(runID run.RunID) error {
	s := l.slot(runID)
	l.mu.Lock()
	defer l.mu.Unlock()
	if s.driving {
		return ErrRunAlreadyRunning
	}
	s.driving = true
	return nil
}

func (l *Loop) stopDriving(runID run.RunID) {
	l.mu.Lock()
	if s, ok := l.slots[runID]; ok {
		s.driving = false
	}
	l.mu.Unlock()
}

func (l *Loop) isDriving(runID run.RunID) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.slots[runID]
	return ok && s.driving
}

func (l *Loop) checkArgs(ctx context.Context, rt run.Runtime, sid session.SessionID, runID run.RunID) error {
	if ctx == nil {
		return errors.New("agent: loop: nil context")
	}
	if rt == nil {
		return errors.New("agent: loop: nil runtime")
	}
	if sid == "" || runID == "" {
		return errors.New("agent: loop: empty SessionID or RunID")
	}
	return nil
}

func (l *Loop) wrapSink(events EventSink) EventSink {
	if events == nil {
		return nil
	}
	return &serializedEventSink{sink: events, mu: &l.eventsMu}
}

// Advance moves the Run to its next quiescent point without waiting on any
// effect (RUN-LOP-2): it records protocol transitions (prepare, withdraw,
// start barriers) and dispatches Assignments, then returns LoopDispatched,
// LoopWaiting or LoopFinished. A concurrent Advance or a blocking Run of the
// same Run is reported as ErrRunAlreadyRunning.
func (l *Loop) Advance(ctx context.Context, rt run.Runtime, sid session.SessionID, runID run.RunID, events EventSink) (LoopResult, error) {
	if err := l.checkArgs(ctx, rt, sid, runID); err != nil {
		return LoopResult{}, err
	}
	if l.isDriving(runID) {
		return LoopResult{}, ErrRunAlreadyRunning
	}
	s := l.slot(runID)
	if !s.step.TryLock() {
		return LoopResult{}, ErrRunAlreadyRunning
	}
	defer s.step.Unlock()
	return l.advance(ctx, boundRuntime{rt: rt, sid: sid}, runID, l.wrapSink(events), nil)
}

// advance is the body of Advance. deliver, when non-nil, is the callback
// dispatched assignments report to; the blocking Run passes its own so
// Outcomes reach its wait loop, while a host calling Advance directly gets
// Outcomes through the Executor it configured (see Deliver).
func (l *Loop) advance(ctx context.Context, runtime boundRuntime, runID run.RunID, events EventSink, deliver Deliver) (LoopResult, error) {
	for {
		if err := ctx.Err(); err != nil {
			return LoopResult{}, err
		}
		snapshot, err := runtime.Load(ctx, runID)
		if err != nil {
			return LoopResult{}, err
		}
		if snapshot.State.RunID != runID {
			return LoopResult{}, fmt.Errorf("agent: loop: runtime returned RunID %q for %q", snapshot.State.RunID, runID)
		}
		if snapshot.State.Status.Terminal() {
			return l.finish(ctx, events, runtime.sid, runID, snapshot.State.Result), nil
		}

		effect, err := run.Next(snapshot.State)
		if err != nil {
			return LoopResult{}, err
		}

		switch eff := effect.(type) {
		case run.NeedModelRequest:
			if err := l.planAndPrepare(ctx, runtime, events, &snapshot, eff.Hint); err != nil {
				return LoopResult{}, err
			}
		case run.WithdrawPrepared:
			// Inputs arrived after this step was frozen: discard the unsent
			// request and replan with them (RUN-LOP-8). A retriable rejection
			// means another actor moved the Run; the reload decides.
			proto, err := snapshot.Protocol()
			if err != nil {
				return LoopResult{}, err
			}
			res, err := l.commit(ctx, runtime, runID, run.DeriveWithdrawCommandID(runID, eff.StepID), snapshot.Position,
				run.WithdrawPreparedStep{StepID: eff.StepID}, proto)
			if err != nil && !retriable(err) {
				return LoopResult{}, err
			}
			if err == nil {
				l.emitCommitted(ctx, events, runtime.sid, runID, res.Events)
			}
		case run.StartModelCall:
			dispatched, err := l.startModelStep(ctx, runtime, events, &snapshot, eff.StepID, deliver)
			if err != nil {
				return LoopResult{}, err
			}
			if dispatched != nil {
				return LoopResult{Disposition: LoopDispatched, Dispatched: []AssignmentKey{*dispatched}}, nil
			}
		case run.StartToolCalls:
			dispatched, err := l.startToolCalls(ctx, runtime, events, &snapshot, eff, deliver)
			if err != nil {
				return LoopResult{}, err
			}
			if len(dispatched) > 0 {
				return LoopResult{Disposition: LoopDispatched, Dispatched: dispatched}, nil
			}
		case run.Idle:
			recovery := run.NeedsRecovery(snapshot.State)
			reason := WaitReason("")
			if recovery {
				reason = ExecutionRecovery
			}
			return LoopResult{Disposition: LoopWaiting, Reason: reason, ExecutionRecovery: recovery}, nil
		default:
			return LoopResult{}, fmt.Errorf("agent: loop: unknown effect %T", effect)
		}
	}
}

// finish is the single exit for a terminal Run, whether the terminal state
// was read by Load or returned by the settlement that produced it.
func (l *Loop) finish(ctx context.Context, events EventSink, sid session.SessionID, runID run.RunID, result *run.RunResult) LoopResult {
	if events != nil {
		_ = events.Emit(ctx, Event{Session: sid, RunID: runID, Kind: EventRunFinished, Durability: EventCommitted})
	}
	return LoopResult{Disposition: LoopFinished, Result: result}
}

// Deliver settles one Outcome (RUN-EXE-4). It finds the Executing target the
// Outcome's key names -- same step or call, same Claim -- and commits the
// attempt's settlement under the Claim-derived CommandID; an Outcome whose
// attempt is no longer Executing is dropped and nothing is written. It returns
// LoopFinished when the settlement terminated the Run, LoopDelivered when the
// host should Advance next, LoopDropped for a stale Outcome. Ownership loss
// is returned as is (RUN-LOP-5).
func (l *Loop) Deliver(ctx context.Context, rt run.Runtime, sid session.SessionID, out Outcome, events EventSink) (LoopResult, error) {
	if err := l.checkArgs(ctx, rt, sid, out.Key.RunID); err != nil {
		return LoopResult{}, err
	}
	s := l.slot(out.Key.RunID)
	s.step.Lock()
	defer s.step.Unlock()
	return l.deliver(ctx, boundRuntime{rt: rt, sid: sid}, out, l.wrapSink(events))
}

func (l *Loop) deliver(ctx context.Context, runtime boundRuntime, out Outcome, events EventSink) (LoopResult, error) {
	runID := out.Key.RunID
	snapshot, err := runtime.Load(ctx, runID)
	if err != nil {
		if errors.Is(err, run.ErrRunNotFound) {
			return LoopResult{Disposition: LoopDropped}, nil
		}
		return LoopResult{}, err
	}
	if snapshot.State.Status.Terminal() {
		return LoopResult{Disposition: LoopDropped}, nil
	}
	proto, err := snapshot.Protocol()
	if err != nil {
		return LoopResult{}, err
	}
	a := attempt{runID: runID, stepID: out.Key.StepID, callID: out.Key.CallID, claim: out.Key.Claim}

	var cmd run.AgentCommand
	var settleErr error
	if out.Key.CallID == "" {
		step, ok := snapshot.State.Current.(run.ModelStep)
		if !ok || step.RefValue.ID != out.Key.StepID || step.Status != run.ModelExecuting || step.Claim != out.Key.Claim {
			return LoopResult{Disposition: LoopDropped}, nil
		}
		cmd, settleErr = l.modelCompletion(&step, out)
	} else {
		call, ok := toolCallFromSnapshot(snapshot.State, out.Key.StepID, out.Key.CallID)
		if !ok || call.Status != run.ToolExecuting || call.Claim != out.Key.Claim {
			return LoopResult{Disposition: LoopDropped}, nil
		}
		cmd = toolCompletion(out.Key, out)
	}

	// Settlement uses a detached control context: a cancelled host request must
	// not discard an accepted effect's outcome (RUN-LOP-5).
	finished, err := l.settle(context.WithoutCancel(ctx), runtime, events, a, snapshot.Position, cmd, proto)
	if err != nil {
		return LoopResult{}, err
	}
	if out.Key.CallID != "" && events != nil {
		_ = events.Emit(ctx, Event{Session: runtime.sid, RunID: runID, StepID: out.Key.StepID, CallID: out.Key.CallID,
			Kind: EventToolCompleted, Durability: EventCommitted})
	}
	if settleErr != nil {
		// The settlement landed (the step is back to Prepared); the condition
		// itself is not retriable by this Loop.
		return LoopResult{Disposition: LoopDelivered}, settleErr
	}
	if finished != nil {
		return l.finish(ctx, events, runtime.sid, runID, finished), nil
	}
	return LoopResult{Disposition: LoopDelivered}, nil
}

// Run drives the Run until it finishes, has no executable effect, or the
// context is cancelled (RUN-LOP-2): Advance, wait for the Outcomes of what it
// dispatched, Deliver, repeat. It is the blocking form every colocated host
// uses; hosts that receive Outcomes from elsewhere call Advance and Deliver
// themselves. The caller context bounds the drive: on cancellation the
// in-flight assignments of the Run are cancelled and their Outcomes are still
// settled (RUN-LOP-5) before ctx.Err() is returned.
func (l *Loop) Run(ctx context.Context, rt run.Runtime, sid session.SessionID, runID run.RunID, events EventSink) (LoopResult, error) {
	if err := l.checkArgs(ctx, rt, sid, runID); err != nil {
		return LoopResult{}, err
	}
	if err := l.startDriving(runID); err != nil {
		return LoopResult{}, err
	}
	defer l.stopDriving(runID)
	events = l.wrapSink(events)
	runtime := boundRuntime{rt: rt, sid: sid}
	s := l.slot(runID)

	outcomes := make(chan Outcome, 64)
	deliver := func(out Outcome) { outcomes <- out }
	pending := map[AssignmentKey]struct{}{}
	cancelled := false
	settleCtx := context.WithoutCancel(ctx)

	// onOwnershipLost stops every in-flight effect: their Outcomes are not ours
	// to write any more (RUN-LOP-5). The cancelled effects still report, so the
	// pending Outcomes are drained -- never settled -- before returning; a tool
	// that ignores its context blocks here as it always would.
	onOwnershipLost := func(err error) (LoopResult, error) {
		_ = l.Executor.Cancel(settleCtx, runID)
		for len(pending) > 0 {
			out := <-outcomes
			delete(pending, out.Key)
		}
		return LoopResult{}, err
	}

	for {
		if len(pending) == 0 {
			if cancelled {
				return LoopResult{}, ctx.Err()
			}
			s.step.Lock()
			res, err := l.advance(ctx, runtime, runID, events, deliver)
			s.step.Unlock()
			if err != nil {
				if ownershipLost(err) {
					return onOwnershipLost(err)
				}
				return LoopResult{}, err
			}
			if res.Disposition != LoopDispatched {
				return res, nil
			}
			for _, k := range res.Dispatched {
				pending[k] = struct{}{}
			}
		}

		var out Outcome
		if cancelled {
			out = <-outcomes
		} else {
			select {
			case out = <-outcomes:
			case <-ctx.Done():
				// Stop what we started; each cancelled effect still reports an
				// Outcome, settled below under the detached context.
				cancelled = true
				_ = l.Executor.Cancel(settleCtx, runID)
				continue
			}
		}
		if _, ours := pending[out.Key]; !ours {
			continue // an Outcome of an attempt this drive did not dispatch
		}
		delete(pending, out.Key)

		s.step.Lock()
		res, err := l.deliver(settleCtx, runtime, out, events)
		s.step.Unlock()
		if err != nil {
			if ownershipLost(err) {
				return onOwnershipLost(err)
			}
			return res, err
		}
		if res.Disposition == LoopFinished {
			return res, nil
		}
	}
}

// commit builds the envelope via the sanctioned constructor and submits it.
// A non-sentinel commit failure is replayed once with the same CommandID
// (RUN-LOP-5): if the first attempt actually committed and only the response
// was lost, the replay returns AlreadyApplied instead of re-executing an
// expensive step. Ownership loss is never retried.
func (l *Loop) commit(ctx context.Context, runtime boundRuntime, runID run.RunID, id run.CommandID, base run.RunPosition, cmd run.AgentCommand, proto run.Protocol) (run.CommitResult, error) {
	if proto.Version() == 0 {
		return run.CommitResult{}, errors.New("agent: loop: uninitialized protocol")
	}
	env, err := proto.BuildEnvelope(runtime.sid, runID, id, cmd)
	if err != nil {
		return run.CommitResult{}, err
	}
	req := run.CommitRequest{Base: base, Command: env}
	res, err := runtime.Commit(ctx, req)
	if err != nil && !retriable(err) && !ownershipLost(err) {
		res, err = runtime.Commit(ctx, req)
	}
	return res, err
}

// retriable reports the commit errors that mean "reload and rederive".
func retriable(err error) bool {
	return errors.Is(err, run.ErrStaleRuntime) || errors.Is(err, run.ErrRunTerminal) || errors.Is(err, run.ErrCommandConflict)
}
