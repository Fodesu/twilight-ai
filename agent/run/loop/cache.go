package loop

import (
	"context"

	run "github.com/memohai/twilight/agent/run"
)

type startKey struct {
	runID  run.RunID
	stepID run.StepID
	callID run.CallID
}

type startAttempt struct {
	commandID run.CommandID
	claim     run.ExecutionClaim
}

type settlementAttempt struct {
	commandID run.CommandID
	base      uint64
	grant     run.ExecutionGrant
	command   run.AgentCommand
}

func (l *Loop) startFor(key startKey) startAttempt {
	l.startsMu.Lock()
	defer l.startsMu.Unlock()
	if attempt, ok := l.starts[key]; ok {
		return attempt
	}
	attempt := startAttempt{commandID: freshCommandID(), claim: freshExecutionClaim()}
	l.starts[key] = attempt
	return attempt
}

func (l *Loop) forgetStart(key startKey) {
	l.startsMu.Lock()
	delete(l.starts, key)
	l.startsMu.Unlock()
}

func (l *Loop) lookupStart(key startKey) (startAttempt, bool) {
	l.startsMu.Lock()
	defer l.startsMu.Unlock()
	attempt, ok := l.starts[key]
	return attempt, ok
}

func (l *Loop) settlementFor(key startKey, commandID run.CommandID, base uint64, grant run.ExecutionGrant, command run.AgentCommand) settlementAttempt {
	l.settlementsMu.Lock()
	defer l.settlementsMu.Unlock()
	if attempt, ok := l.settlements[key]; ok {
		return attempt
	}
	attempt := settlementAttempt{commandID: commandID, base: base, grant: grant, command: command}
	l.settlements[key] = attempt
	return attempt
}

func (l *Loop) lookupSettlement(key startKey) (settlementAttempt, bool) {
	l.settlementsMu.Lock()
	defer l.settlementsMu.Unlock()
	attempt, ok := l.settlements[key]
	return attempt, ok
}

func (l *Loop) forgetSettlement(key startKey) {
	l.settlementsMu.Lock()
	delete(l.settlements, key)
	l.settlementsMu.Unlock()
}

func (l *Loop) forgetRunCaches(runID run.RunID) {
	l.startsMu.Lock()
	for key := range l.starts {
		if key.runID == runID {
			delete(l.starts, key)
		}
	}
	l.startsMu.Unlock()
	l.settlementsMu.Lock()
	for key := range l.settlements {
		if key.runID == runID {
			delete(l.settlements, key)
		}
	}
	l.settlementsMu.Unlock()
}

// resumeSettlement replays a result whose first commit may have succeeded
// while its response was lost. The same command identity makes the retry
// idempotent and avoids re-running the external effect.
func (l *Loop) resumeSettlement(ctx context.Context, runtime run.Runtime, events EventSink, snapshot *run.RuntimeSnapshot) (bool, error) {
	runID := snapshot.State.RunID
	l.settlementsMu.Lock()
	keys := make([]startKey, 0)
	for key := range l.settlements {
		if key.runID == runID {
			keys = append(keys, key)
		}
	}
	l.settlementsMu.Unlock()
	proto, err := snapshot.Protocol()
	if err != nil {
		return false, err
	}
	for _, key := range keys {
		attempt, ok := l.lookupSettlement(key)
		if !ok {
			continue
		}
		if !settlementTargetActive(snapshot.State, key) {
			l.forgetSettlement(key)
			l.forgetStart(key)
			continue
		}
		res, err := l.commit(context.WithoutCancel(ctx), runtime, runID, attempt.commandID, attempt.base, attempt.grant, attempt.command, proto)
		if err != nil {
			if retriable(err) {
				l.forgetSettlement(key)
				l.forgetStart(key)
				return true, nil
			}
			return false, err
		}
		l.forgetSettlement(key)
		l.forgetStart(key)
		l.emitCommitted(ctx, events, runID, res.Events)
		return true, nil
	}
	return false, nil
}

func settlementTargetActive(state run.MachineState, key startKey) bool {
	switch current := state.Current.(type) {
	case run.ModelStep:
		return key.callID == "" && current.RefValue.ID == key.stepID && current.Status == run.ModelExecuting
	case run.ToolStep:
		if current.RefValue.ID != key.stepID {
			return false
		}
		for _, call := range current.Calls {
			if call.CallID == key.callID {
				return call.Status == run.ToolExecuting
			}
		}
	}
	return false
}

// resumeCachedStart re-enters an accepted start after a Loop.Run returned
// before receiving its grant. The authority state is Executing, so Next alone
// would otherwise wait for recovery instead of replaying the known start.
func (l *Loop) resumeCachedStart(ctx context.Context, runtime run.Runtime, events EventSink, snapshot *run.RuntimeSnapshot) (bool, error) {
	runID := snapshot.State.RunID
	switch current := snapshot.State.Current.(type) {
	case run.ModelStep:
		if current.Status != run.ModelExecuting {
			return false, nil
		}
		if _, ok := l.lookupStart(startKey{runID: runID, stepID: current.RefValue.ID}); !ok {
			return false, nil
		}
		return true, l.runModelStep(ctx, runtime, events, snapshot, current.RefValue.ID)
	case run.ToolStep:
		var ids []run.CallID
		for _, call := range current.Calls {
			if call.Status != run.ToolExecuting {
				continue
			}
			if _, ok := l.lookupStart(startKey{runID: runID, stepID: current.RefValue.ID, callID: call.CallID}); ok {
				ids = append(ids, call.CallID)
			}
		}
		if len(ids) == 0 {
			return false, nil
		}
		return true, l.runToolCalls(ctx, runtime, events, snapshot, run.StartToolCalls{StepID: current.RefValue.ID, CallIDs: ids})
	default:
		return false, nil
	}
}
