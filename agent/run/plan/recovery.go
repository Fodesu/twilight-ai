package plan

import (
	"github.com/felinics/twilight/agent/run"
)

// RecoveryDisposition is one takeover disposition command with its derived identity.
type RecoveryDisposition struct {
	Command run.AgentCommand
	ID      run.CommandID
}

// RecoveryTarget is one Executing target a takeover has to decide about: the
// model step or tool call, and the Claim of the attempt that started it (from
// the started fact). The reconciler (agent/run/reconcile) asks the executor
// whether that attempt still exists; only if not is the recovery command
// issued (RUN-CMT-7).
type RecoveryTarget struct {
	RunID  run.RunID
	Schema uint16 // the Run's protocol version, for the executor's digest checks
	StepID run.StepID
	CallID run.CallID // empty for a model step
	Claim  run.ExecutionClaim
	Model  *run.ModelStep     // set for a model target
	Call   *run.ToolCallState // set for a tool target
}

// RecoveryTargets lists the Executing targets of state in the order RecoveryCommands
// disposes them.
func RecoveryTargets(state *run.MachineState) []RecoveryTarget {
	if state.Status.Terminal() {
		return nil
	}
	switch cur := state.Current.(type) {
	case run.ModelStep:
		if cur.Status != run.ModelExecuting {
			return nil
		}
		ms := cur
		return []RecoveryTarget{{RunID: state.RunID, StepID: cur.RefValue.ID, Claim: cur.Claim, Model: &ms}}
	case run.ToolStep:
		var out []RecoveryTarget
		for i := range cur.Calls {
			call := cur.Calls[i]
			if call.Status != run.ToolExecuting {
				continue
			}
			out = append(out, RecoveryTarget{RunID: state.RunID, StepID: cur.RefValue.ID, CallID: call.CallID, Claim: call.Claim, Call: &call})
		}
		return out
	default:
		return nil
	}
}

// RecoveryCommand is the disposition of one target under the takeover claim:
// an Executing model step is withdrawn to Open (the next Prepare plans again);
// an Executing tool call settles as Unknown. schema is the Run's.
func RecoveryCommand(id run.Identity, target RecoveryTarget, claim run.ExecutionClaim) RecoveryDisposition {
	if target.Call == nil {
		return RecoveryDisposition{
			Command: run.RecoverModelExecution{StepID: target.StepID, Claim: claim},
			ID:      id.DeriveModelRecoveryCommandID(target.RunID, target.StepID, claim),
		}
	}
	return RecoveryDisposition{
		Command: run.SubmitToolFailure{
			StepID:  target.StepID,
			CallID:  target.CallID,
			Failure: run.ToolFailure{Class: run.FailureEffectUnknown, Message: "owner process lost before settlement"},
			Outcome: run.ToolOutcomeUnknown,
		},
		ID: id.DeriveToolRecoveryCommandID(target.RunID, target.StepID, target.CallID, claim),
	}
}

// RecoveryCommands lists the takeover dispositions of every Executing target
// in state (RUN-CMT-7). Pending and Waiting calls are left alone. claim is the
// takeover claim of the new owner.
func RecoveryCommands(id run.Identity, state *run.MachineState, claim run.ExecutionClaim) []RecoveryDisposition {
	targets := RecoveryTargets(state)
	if len(targets) == 0 {
		return nil
	}
	out := make([]RecoveryDisposition, len(targets))
	for i, t := range targets {
		out[i] = RecoveryCommand(id, t, claim)
	}
	return out
}
