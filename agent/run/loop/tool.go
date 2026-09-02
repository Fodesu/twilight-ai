package loop

import (
	"context"
	"errors"
	"fmt"
	"sync"

	run "github.com/memohai/twilight/agent/run"
)

type startedWorker struct {
	call  run.ToolCallState
	grant run.ExecutionGrant
	base  uint64
	tool  ExecutableTool
	key   startKey
}

func toolCallIndex(step run.ToolStep, callID run.CallID) int {
	for i := range step.Calls {
		if step.Calls[i].CallID == callID {
			return i
		}
	}
	return -1
}

func (l *Loop) resolveExecutableTool(proto run.Protocol, call run.ToolCallState) (ExecutableTool, *run.ToolFailure) {
	tool, resolveErr := l.Tools.Resolve(call.ToolRef)
	if resolveErr != nil {
		return nil, &run.ToolFailure{Class: run.FailureToolLookup, Message: resolveErr.Error()}
	}
	if tool == nil {
		return nil, &run.ToolFailure{Class: run.FailureToolLookup, Message: "tool catalog returned a nil tool"}
	}
	toolDef, freezeErr := run.FreezeToolDefinition(tool.Definition())
	if freezeErr != nil {
		return nil, &run.ToolFailure{Class: run.FailureDefinitionMismatch, Message: freezeErr.Error()}
	}
	defDigest, digestErr := proto.DigestToolDefinition(toolDef)
	if digestErr != nil {
		return nil, &run.ToolFailure{Class: run.FailureDefinitionMismatch, Message: digestErr.Error()}
	}
	switch {
	case tool.Ref() != call.ToolRef || defDigest != call.DefinitionDigest:
		return nil, &run.ToolFailure{Class: run.FailureDefinitionMismatch, Message: "tool definition digest mismatch"}
	case tool.ResponsePolicy() != call.Policy:
		return nil, &run.ToolFailure{Class: run.FailureDefinitionMismatch, Message: "response policy mismatch"}
	}
	if argErr := tool.ValidateArguments(call.Arguments); argErr != nil {
		return nil, &run.ToolFailure{Class: run.FailureInvalidArguments, Message: argErr.Error()}
	}
	return tool, nil
}

func (l *Loop) runToolCalls(ctx context.Context, runtime run.Runtime, events EventSink, snapshot *run.RuntimeSnapshot, eff run.StartToolCalls) error {
	runID := snapshot.State.RunID
	proto, err := snapshot.Protocol()
	if err != nil {
		return err
	}
	ts, ok := snapshot.State.Current.(run.ToolStep)
	if !ok || ts.RefValue.ID != eff.StepID {
		return fmt.Errorf("agent: loop: tool step %q is not current", eff.StepID)
	}

	limit := len(eff.CallIDs)
	if ts.Scheduling.Mode == run.ToolScheduleSequential {
		limit = 1
	}
	if ts.Scheduling.MaxParallel > 0 && ts.Scheduling.MaxParallel < limit {
		limit = ts.Scheduling.MaxParallel
	}
	var started []startedWorker
	for _, callID := range eff.CallIDs {
		if len(started) >= limit {
			break
		}
		// Outer ctx cancelled: stop starting new calls; settle what we own.
		if ctx.Err() != nil {
			break
		}
		i := toolCallIndex(ts, callID)
		if i < 0 {
			continue
		}
		call := ts.Calls[i]
		key := startKey{runID: runID, stepID: eff.StepID, callID: callID}
		resuming := false
		switch call.Status {
		case run.ToolPending:
		case run.ToolExecuting:
			if _, ok := l.lookupStart(key); !ok {
				continue
			}
			resuming = true
		default:
			continue
		}

		tool, known := l.resolveExecutableTool(proto, call)
		if known != nil && !resuming {
			// Known failure of a Pending call: no start barrier, no tool call.
			res, err := l.commit(ctx, runtime, runID, freshCommandID(), snapshot.Revision, "",
				run.SubmitToolFailure{StepID: eff.StepID, CallID: callID, Failure: *known, Outcome: run.ToolOutcomeKnown}, proto)
			if err != nil {
				settleErr := l.settleWorkers(ctx, runtime, events, runID, eff.StepID, started, proto)
				if !retriable(err) {
					return err
				}
				return settleErr
			}
			l.emitCommitted(ctx, events, runID, res.Events)
			continue
		}

		attempt := l.startFor(key)
		start, err := l.commit(ctx, runtime, runID, attempt.commandID, snapshot.Revision, "",
			run.StartToolCall{StepID: eff.StepID, CallID: callID, Claim: attempt.claim}, proto)
		if err != nil {
			settleErr := l.settleWorkers(ctx, runtime, events, runID, eff.StepID, started, proto)
			if retriable(err) {
				// A sentinel rejection proves this start did not acquire the
				// call. Drop the local claim so a later snapshot can create a
				// fresh start attempt or observe the other owner.
				l.forgetStart(key)
				return settleErr
			}
			return err
		}
		if start.Status == run.CommitAlreadyApplied && start.Grant == "" {
			l.forgetStart(key)
			continue // another attempt owns this call
		}
		if start.Grant == "" {
			l.forgetStart(key)
			return errors.New("agent: loop: start tool returned no execution grant")
		}
		if startedCall, ok := toolCallFromSnapshot(start.Snapshot.State, eff.StepID, callID); !ok || startedCall.Status != run.ToolExecuting {
			// A replay may arrive after another worker has settled this call.
			// Keep the original grant in the Runtime's replay record, but never
			// invoke an effect for a call that is no longer Executing.
			l.forgetStart(key)
			continue
		}
		l.emitCommitted(ctx, events, runID, start.Events)
		if events != nil {
			_ = events.Emit(ctx, Event{RunID: runID, StepID: eff.StepID, CallID: callID,
				Kind: EventToolStarted, Durability: EventCommitted})
		}
		if resuming && known != nil {
			failure := *known
			if failure.Class != run.FailureEffectUnknown {
				if failure.Message == "" {
					failure.Message = "tool reported " + failure.Class
				}
				failure.Class = run.FailureEffectUnknown
			}
			settlement := l.settlementFor(key, freshCommandID(), start.Snapshot.Revision, start.Grant,
				run.SubmitToolFailure{StepID: eff.StepID, CallID: callID, Failure: failure, Outcome: run.ToolOutcomeUnknown})
			res, err := l.commit(context.WithoutCancel(ctx), runtime, runID, settlement.commandID, settlement.base, settlement.grant, settlement.command, proto)
			if err != nil {
				settleErr := l.settleWorkers(ctx, runtime, events, runID, eff.StepID, started, proto)
				if retriable(err) {
					l.forgetSettlement(key)
					l.forgetStart(key)
					return settleErr
				}
				return err
			}
			l.forgetSettlement(key)
			l.forgetStart(key)
			l.emitCommitted(ctx, events, runID, res.Events)
			continue
		}
		started = append(started, startedWorker{call: call, grant: start.Grant, base: start.Snapshot.Revision, tool: tool, key: key})
	}

	return l.settleWorkers(ctx, runtime, events, runID, eff.StepID, started, proto)
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

// settleWorkers executes every started worker and commits its outcome. An
// accepted start is never abandoned (RUN-LOP-4). Tool workers receive outer
// context cancellation; settlement uses a detached control context so the
// resulting outcome can still reach Runtime (RUN-LOP-5). Unknown settles
// only that call. A non-sentinel commit error leaves the same command in the
// local settlement cache for the next Run invocation.
func (l *Loop) settleWorkers(ctx context.Context, runtime run.Runtime, events EventSink, runID run.RunID, stepID run.StepID, started []startedWorker, proto run.Protocol) error {
	if len(started) == 0 {
		return nil
	}
	controlCtx := context.WithoutCancel(ctx)

	var mu sync.Mutex
	var wg sync.WaitGroup
	var firstErr error
	for i := range started {
		wg.Add(1)
		w := started[i]
		go func(w startedWorker) {
			defer wg.Done()
			req := ToolExecutionRequest{
				RunID:            runID,
				StepID:           stepID,
				CallID:           w.call.CallID,
				ToolRef:          w.call.ToolRef,
				DefinitionDigest: w.call.DefinitionDigest,
				Arguments:        w.call.Arguments,
				Progress:         &progressSink{events: events, run: runID, step: stepID, call: w.call.CallID},
			}
			workerCtx, stopLease := l.keepLease(ctx, runtime, runID, stepID, w.call.CallID, w.grant)
			outcome := executeToolSafely(workerCtx, w.tool, &req)
			stopLease()

			var cmd run.AgentCommand
			switch o := outcome.(type) {
			case ToolExecutionSucceeded:
				cmd = run.SubmitToolResult{StepID: stepID, CallID: w.call.CallID, Result: o.Result}
			case ToolExecutionFailed:
				failure := o.Failure
				if failure.Class == "" || failure.Class == run.FailureEffectUnknown {
					failure.Class = run.FailureExecution
				}
				cmd = run.SubmitToolFailure{StepID: stepID, CallID: w.call.CallID, Failure: failure, Outcome: run.ToolOutcomeKnown}
			case ToolExecutionUnknown:
				failure := o.Failure
				if failure.Class != "" && failure.Class != run.FailureEffectUnknown && failure.Message == "" {
					failure.Message = "tool reported " + failure.Class
				}
				failure.Class = run.FailureEffectUnknown
				cmd = run.SubmitToolFailure{StepID: stepID, CallID: w.call.CallID, Failure: failure, Outcome: run.ToolOutcomeUnknown}
			default:
				cmd = run.SubmitToolFailure{StepID: stepID, CallID: w.call.CallID,
					Failure: run.ToolFailure{Class: run.FailureEffectUnknown, Message: "tool returned no outcome"}, Outcome: run.ToolOutcomeUnknown}
			}

			mu.Lock()
			defer mu.Unlock()
			// Commit with the worker's own grant on its start base; stale
			// bases rebase call-locally. Late results after terminal return
			// ErrRunTerminal and are dropped (audit is the adapter's job).
			// The one-shot same-CommandID replay lives inside l.commit.
			settlement := l.settlementFor(w.key, freshCommandID(), w.base, w.grant, cmd)
			res, err := l.commit(controlCtx, runtime, runID, settlement.commandID, settlement.base, settlement.grant, settlement.command, proto)
			switch {
			case err == nil:
				l.forgetSettlement(w.key)
				l.forgetStart(w.key)
				l.emitCommitted(ctx, events, runID, res.Events)
				if events != nil {
					_ = events.Emit(ctx, Event{RunID: runID, StepID: stepID, CallID: w.call.CallID,
						Kind: EventToolCompleted, Durability: EventCommitted})
				}
			case retriable(err):
				l.forgetSettlement(w.key)
				l.forgetStart(w.key)
				// Terminal/stale: the authority already settled this call or
				// the run; the result is intentionally dropped.
			default:
				if firstErr == nil {
					firstErr = fmt.Errorf("agent: loop: settling call %q: %w", w.call.CallID, err)
				}
			}
		}(w)
	}
	wg.Wait()
	return firstErr
}

// executeToolSafely runs an application tool and converts a panic into
// ToolExecutionUnknown: the effect may have happened before the panic, and a
// crashing tool must not take down every run in the process.
func executeToolSafely(ctx context.Context, tool ExecutableTool, req *ToolExecutionRequest) (outcome ToolExecutionOutcome) {
	defer func() {
		if r := recover(); r != nil {
			outcome = ToolExecutionUnknown{Failure: run.ToolFailure{
				Class:   run.FailureEffectUnknown,
				Message: fmt.Sprintf("tool panic: %v", r),
			}}
		}
	}()
	return tool.Execute(ctx, *req)
}
