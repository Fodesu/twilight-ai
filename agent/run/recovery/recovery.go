package recovery

import (
	"github.com/felinics/twilight/agent/run"
)

// Disposition is one takeover disposition command with its derived identity.
type Disposition struct {
	Command run.AgentCommand
	ID      run.CommandID
}

// Target is one Executing target a takeover has to decide about: the
// model step or tool call, and the Claim of the attempt that started it (from
// the started fact). The reconciler (agent/run/reconcile) asks the executor
// whether that attempt still exists; only if not is the recovery command
// issued (RUN-CMT-7).
type Target struct {
	RunID  run.RunID
	Schema uint16 // the Run's protocol version, for the executor's digest checks
	StepID run.StepID
	CallID run.CallID // empty for a model step
	Claim  run.ExecutionClaim
	Model  *run.ModelStep     // set for a model target
	Call   *run.ToolCallState // set for a tool target
}

// Targets lists the Executing targets of state in the order Commands
// disposes them.
func Targets(state *run.MachineState) []Target {
	if state.Status.Terminal() {
		return nil
	}
	switch cur := state.Current.(type) {
	case run.ModelStep:
		if cur.Status != run.ModelExecuting {
			return nil
		}
		ms := cur
		return []Target{{RunID: state.RunID, StepID: cur.RefValue.ID, Claim: cur.Claim, Model: &ms}}
	case run.ToolStep:
		var out []Target
		for i := range cur.Calls {
			call := cur.Calls[i]
			if call.Status != run.ToolExecuting {
				continue
			}
			out = append(out, Target{RunID: state.RunID, StepID: cur.RefValue.ID, CallID: call.CallID, Claim: call.Claim, Call: &call})
		}
		return out
	default:
		return nil
	}
}

// Command is the disposition of one target under the takeover claim:
// an Executing model step is withdrawn to Open (the next Prepare plans again);
// an Executing tool call settles as Unknown. schema is the Run's.
func Command(id run.Identity, target Target, claim run.ExecutionClaim) Disposition {
	if target.Call == nil {
		return Disposition{
			Command: run.RecoverModelExecution{StepID: target.StepID, Claim: claim},
			ID:      id.DeriveModelRecoveryCommandID(target.RunID, target.StepID, claim),
		}
	}
	return Disposition{
		Command: run.SubmitToolFailure{
			StepID:  target.StepID,
			CallID:  target.CallID,
			Failure: run.ToolFailure{Class: run.FailureEffectUnknown, Message: "owner process lost before settlement"},
			Outcome: run.ToolOutcomeUnknown,
		},
		ID: id.DeriveToolRecoveryCommandID(target.RunID, target.StepID, target.CallID, claim),
	}
}

// Commands lists the takeover dispositions of every Executing target
// in state (RUN-CMT-7). Pending and Waiting calls are left alone. claim is the
// takeover claim of the new owner.
func Commands(id run.Identity, state *run.MachineState, claim run.ExecutionClaim) []Disposition {
	targets := Targets(state)
	if len(targets) == 0 {
		return nil
	}
	out := make([]Disposition, len(targets))
	for i, t := range targets {
		out[i] = Command(id, t, claim)
	}
	return out
}
