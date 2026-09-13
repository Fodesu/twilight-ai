package loop

import (
	"context"
	"fmt"

	run "github.com/felinics/twilight/agent/run"
)

func toolCallIndex(step run.ToolStep, callID run.CallID) int {
	for i := range step.Calls {
		if step.Calls[i].CallID == callID {
			return i
		}
	}
	return -1
}

// startToolCalls validates, starts and dispatches the Pending calls the
// frozen Scheduling allows (RUN-LOP-4). Validation happens before the start
// barrier through the Executor and settles as a Known failure without a
// claim; a validated call is started under a fresh attempt and handed to the
// Executor. It returns the dispatched keys; an empty list with no error means
// nothing is executing on this Loop's behalf and the reload decides.
func (l *Loop) startToolCalls(ctx context.Context, runtime boundRuntime, events EventSink, snapshot *run.RuntimeSnapshot, eff run.StartToolCalls, deliver Deliver) ([]AssignmentKey, error) {
	runID := snapshot.State.RunID
	proto, err := snapshot.Protocol()
	if err != nil {
		return nil, err
	}
	ts, ok := snapshot.State.Current.(run.ToolStep)
	if !ok || ts.RefValue.ID != eff.StepID {
		return nil, fmt.Errorf("agent: loop: tool step %q is not current", eff.StepID)
	}

	limit := len(eff.CallIDs)
	if ts.Scheduling.Mode == run.ToolScheduleSequential {
		limit = 1
	}
	if ts.Scheduling.MaxParallel > 0 && ts.Scheduling.MaxParallel < limit {
		limit = ts.Scheduling.MaxParallel
	}
	var dispatched []AssignmentKey
	for _, callID := range eff.CallIDs {
		if len(dispatched) >= limit {
			break
		}
		// Outer ctx cancelled: stop starting new calls; what was dispatched
		// settles through its Outcome.
		if ctx.Err() != nil {
			break
		}
		i := toolCallIndex(ts, callID)
		if i < 0 {
			continue
		}
		call := ts.Calls[i]
		if call.Status != run.ToolPending {
			// Executing calls belong to the attempt that started them or to the
			// owner's takeover disposition; never re-run (TRN-DUR-4).
			continue
		}
		binding := &ToolAssignment{ToolRef: call.ToolRef, DefinitionDigest: call.DefinitionDigest, Arguments: call.Arguments, Policy: call.Policy}
		probe := Assignment{Session: runtime.sid, RunID: runID, StepID: eff.StepID, CallID: callID, Schema: snapshot.SchemaVersion, Kind: AssignmentTool, Tool: binding}
		known, err := l.Executor.Validate(ctx, probe)
		if err != nil {
			return dispatched, err
		}
		if known != nil {
			// Known failure of a Pending call: no start barrier, no tool call,
			// no claim. Its identity derives from the call alone; a retry of
			// the same rejection is idempotent.
			res, err := l.commit(ctx, runtime, runID, run.DeriveSettlementCommandID(runID, eff.StepID, callID, ""), snapshot.Position,
				run.SubmitToolFailure{StepID: eff.StepID, CallID: callID, Failure: *known, Outcome: run.ToolOutcomeKnown}, proto)
			if err != nil {
				if retriable(err) {
					return dispatched, nil // another actor moved the call; reload decides
				}
				return dispatched, err
			}
			l.emitCommitted(ctx, events, runtime.sid, runID, res.Events)
			continue
		}

		a := newAttempt(runID, eff.StepID, callID)
		start, err := l.commit(ctx, runtime, runID, a.startID(), snapshot.Position,
			run.StartToolCall{StepID: eff.StepID, CallID: callID, Claim: a.claim}, proto)
		if err != nil {
			if retriable(err) {
				return dispatched, nil // another actor moved the call; reload decides
			}
			return dispatched, err
		}
		if startedCall, ok := toolCallFromSnapshot(start.Snapshot.State, eff.StepID, callID); !ok || startedCall.Status != run.ToolExecuting || startedCall.Claim != a.claim {
			// The one-shot replay may land after the call was settled, or the
			// call is Executing under another attempt. Never invoke an effect
			// for a call this attempt does not own.
			continue
		}
		l.emitCommitted(ctx, events, runtime.sid, runID, start.Events)
		if events != nil {
			_ = events.Emit(ctx, Event{Session: runtime.sid, RunID: runID, StepID: eff.StepID, CallID: callID,
				Kind: EventToolStarted, Durability: EventCommitted})
		}
		assignment := probe
		assignment.Claim = a.claim
		if err := l.Executor.Dispatch(ctx, assignment, l.deliverTo(runtime, events, deliver)); err != nil {
			// The effect never started: settle the attempt as a Known execution
			// failure so the call does not stay Executing.
			failure := run.ToolFailure{Class: run.FailureExecution, Message: "dispatch: " + err.Error()}
			if _, serr := l.settle(context.WithoutCancel(ctx), runtime, events, a, start.Snapshot.Position,
				run.SubmitToolFailure{StepID: eff.StepID, CallID: callID, Failure: failure, Outcome: run.ToolOutcomeKnown}, proto); serr != nil {
				return dispatched, serr
			}
			continue
		}
		dispatched = append(dispatched, assignment.Key())
	}
	return dispatched, nil
}

func toolCallFromSnapshot(state run.MachineState, stepID run.StepID, callID run.CallID) (run.ToolCallState, bool) {
	step, ok := state.Current.(run.ToolStep)
	if !ok || step.RefValue.ID != stepID {
		return run.ToolCallState{}, false
	}
	for _, call := range step.Calls {
		if call.CallID == callID {
			return call, true
		}
	}
	return run.ToolCallState{}, false
}

// toolCompletion maps a tool Outcome to the attempt's settlement command. A
// sealed outcome maps directly; a missing outcome or a transport error is
// Unknown, because the effect may have happened (RUN-LOP-5).
func toolCompletion(key AssignmentKey, out Outcome) run.AgentCommand {
	switch o := out.Tool.(type) {
	case ToolExecutionSucceeded:
		return run.SubmitToolResult{StepID: key.StepID, CallID: key.CallID, Result: o.Result}
	case ToolExecutionFailed:
		failure := o.Failure
		if failure.Class == "" || failure.Class == run.FailureEffectUnknown {
			failure.Class = run.FailureExecution
		}
		return run.SubmitToolFailure{StepID: key.StepID, CallID: key.CallID, Failure: failure, Outcome: run.ToolOutcomeKnown}
	case ToolExecutionUnknown:
		failure := o.Failure
		if failure.Class != "" && failure.Class != run.FailureEffectUnknown && failure.Message == "" {
			failure.Message = "tool reported " + failure.Class
		}
		failure.Class = run.FailureEffectUnknown
		return run.SubmitToolFailure{StepID: key.StepID, CallID: key.CallID, Failure: failure, Outcome: run.ToolOutcomeUnknown}
	}
	msg := "tool returned no outcome"
	if out.Err != nil {
		msg = "executor: " + out.Err.Error()
	}
	return run.SubmitToolFailure{StepID: key.StepID, CallID: key.CallID,
		Failure: run.ToolFailure{Class: run.FailureEffectUnknown, Message: msg}, Outcome: run.ToolOutcomeUnknown}
}
