package run

import (
	"errors"
	"fmt"
)

type DecisionKind uint8

const (
	DecisionApply DecisionKind = iota
	DecisionConflict
	DecisionStale
	DecisionTerminal
)

// CommitDecision is EvaluateCommit's verdict. The Runtime maps rejections
// onto the sentinel errors: Conflict -> ErrCommandConflict, Stale ->
// ErrStaleRuntime, Terminal -> ErrRunTerminal.
type CommitDecision struct {
	Kind     DecisionKind
	NewState MachineState
	Facts    []Fact
	// Reject carries the precondition failure for Conflict/Stale/Terminal.
	Reject error
}

// ValidateEnvelope is step 1 of RUN-CMT-3: identity and schema. Envelopes are
// only built by Protocol.BuildEnvelope (RUN-WIR-3), so there is no per-commit
// self-verification of the command bytes.
func ValidateEnvelope(env *CommandEnvelope, proto Protocol) error {
	if env.SessionID == "" || env.RunID == "" || env.ID == "" {
		return errors.New("agent: commit: empty SessionID, RunID or CommandID")
	}
	if err := proto.ready(); err != nil {
		return err
	}
	if env.SchemaVersion != proto.Version() {
		return fmt.Errorf("agent: commit: command schema %d does not match run schema %d", env.SchemaVersion, proto.Version())
	}
	return nil
}

// EvaluateCommit is the pure evaluation every Runtime runs inside the Session
// Writer after the replay lookup (RUN-CMT-3 steps 4-8). Execution ownership is
// Session-level (RUN-CMT-6), so there is no per-target authorization: a
// command against a target whose state does not admit it is Stale.
//
//nolint:gocritic // hugeParam: public pure commit evaluator keeps state/request as value protocol inputs.
func EvaluateCommit(cur MachineState, position RunPosition, req CommitRequest, proto Protocol) (CommitDecision, error) {
	env := req.Command
	if env.RunID != cur.RunID {
		return CommitDecision{}, fmt.Errorf("agent: commit: command run %q does not match authority run %q", env.RunID, cur.RunID)
	}
	if err := ValidateEnvelope(&env, proto); err != nil {
		return CommitDecision{}, err
	}
	// A start claim is part of the command identity (RUN-WIR-1).
	switch cmd := env.Command.(type) {
	case StartModelExecution:
		if cmd.Claim == "" {
			return CommitDecision{Kind: DecisionConflict, Reject: errors.New("agent: commit: model start requires an execution claim")}, nil
		}
	case StartToolCall:
		if cmd.Claim == "" {
			return CommitDecision{Kind: DecisionConflict, Reject: errors.New("agent: commit: tool start requires an execution claim")}, nil
		}
	case RecoverModelExecution:
		if cmd.Claim == "" {
			return CommitDecision{Kind: DecisionConflict, Reject: errors.New("agent: commit: model recovery requires an execution claim")}, nil
		}
	}
	// Derived-identity families must use their derived CommandID (RUN-WIR-3):
	// the derivation is the idempotency index, so a caller-minted random ID
	// cannot bypass duplicate detection.
	if err := checkDerivedCommandID(&env, req.Base); err != nil {
		return CommitDecision{Kind: DecisionConflict, Reject: err}, nil
	}

	// Terminal absorbs non-duplicate commands (replay was handled before).
	if cur.Status.Terminal() {
		return CommitDecision{Kind: DecisionTerminal, Reject: ErrRunTerminal}, nil
	}

	// Base: PrepareModelRequest is the only hard-CAS command (RUN-CMT-4).
	if _, plan := env.Command.(PrepareModelRequest); plan && req.Base != position {
		return CommitDecision{Kind: DecisionStale, Reject: ErrStaleRuntime}, nil
	}

	// Step 7: Decide once, fold with Evolve.
	facts, err := proto.Decide(cur, env.Command)
	if err != nil {
		switch {
		case errors.Is(err, ErrRunTerminal):
			return CommitDecision{Kind: DecisionTerminal, Reject: err}, nil
		case errors.Is(err, ErrStaleRuntime):
			return CommitDecision{Kind: DecisionStale, Reject: err}, nil
		case errors.Is(err, ErrCommandConflict):
			return CommitDecision{Kind: DecisionConflict, Reject: err}, nil
		default:
			// Precondition failures against the current state are stale from
			// the caller's perspective: reload and rederive.
			return CommitDecision{Kind: DecisionStale, Reject: err}, nil
		}
	}
	if cmd, ok := env.Command.(PrepareModelRequest); ok {
		if len(facts) == 0 {
			return CommitDecision{}, errors.New("agent: commit: prepare produced no facts")
		}
		prepared, ok := facts[0].(ModelStepPrepared)
		if !ok {
			return CommitDecision{}, errors.New("agent: commit: prepare did not produce ModelStepPrepared")
		}
		wantStep := DeriveModelStepID(env.RunID, env.ID, prepared.BindingDigest)
		if cmd.StepID != wantStep {
			return CommitDecision{Kind: DecisionStale, Reject: fmt.Errorf("prepare: StepID %q does not match derived StepID %q", cmd.StepID, wantStep)}, nil
		}
	}

	state := cur
	detached := make([]Fact, len(facts))
	for i, f := range facts {
		// Detach every fact before it is folded: Decide forwards fields from
		// the caller's command and the decision must not carry caller-owned
		// mutable objects across the Runtime boundary.
		f, err = snapshotFact(f)
		if err != nil {
			return CommitDecision{}, err
		}
		state, err = proto.Evolve(state, f)
		if err != nil {
			return CommitDecision{}, err
		}
		detached[i] = f
	}
	return CommitDecision{Kind: DecisionApply, NewState: state, Facts: detached}, nil
}

// checkDerivedCommandID enforces the derived-identity rules of RUN-WIR-3.
func checkDerivedCommandID(env *CommandEnvelope, base RunPosition) error {
	var want CommandID
	switch cmd := env.Command.(type) {
	case PrepareModelRequest:
		want = DeriveModelRequestCommandID(env.RunID, base)
	case AcceptInput:
		want = DeriveInputCommandID(env.RunID, cmd.InputIDs()...)
	case WithdrawPreparedStep:
		want = DeriveWithdrawCommandID(env.RunID, cmd.StepID)
	case ApproveToolCall:
		want = DeriveResponseCommandID(env.RunID, cmd.StepID, cmd.CallID, cmd.ResponseID)
	case RejectToolCall:
		want = DeriveResponseCommandID(env.RunID, cmd.StepID, cmd.CallID, cmd.ResponseID)
	case SubmitToolResponse:
		want = DeriveResponseCommandID(env.RunID, cmd.StepID, cmd.CallID, cmd.ResponseID)
	case StartModelExecution:
		want = DeriveStartCommandID(env.RunID, cmd.StepID, "", cmd.Claim)
	case StartToolCall:
		want = DeriveStartCommandID(env.RunID, cmd.StepID, cmd.CallID, cmd.Claim)
	case RecoverModelExecution:
		want = DeriveModelRecoveryCommandID(env.RunID, cmd.StepID, cmd.Claim)
	default:
		return nil
	}
	if want != "" && env.ID != want {
		return fmt.Errorf("agent: commit: %s requires its derived CommandID", env.Type)
	}
	return nil
}

// IsStart reports whether c begins an execution attempt.
func IsStart(c AgentCommand) bool {
	switch c.(type) {
	case StartModelExecution, StartToolCall:
		return true
	}
	return false
}

// CommandClaim returns the ExecutionClaim a start or recovery command carries.
func CommandClaim(c AgentCommand) ExecutionClaim {
	switch cmd := c.(type) {
	case StartModelExecution:
		return cmd.Claim
	case StartToolCall:
		return cmd.Claim
	case RecoverModelExecution:
		return cmd.Claim
	}
	return ""
}

// Recovery is one takeover disposition command with its derived identity.
type Recovery struct {
	Command AgentCommand
	ID      CommandID
}

// RecoveryCommands lists the takeover dispositions of every Executing target
// in state (RUN-CMT-7): an Executing model step recovers to Prepared; each
// Executing tool call settles as Unknown. Pending and Waiting calls are left
// alone. claim is the takeover claim of the new owner.
func RecoveryCommands(state *MachineState, claim ExecutionClaim) []Recovery {
	if state.Status.Terminal() {
		return nil
	}
	switch cur := state.Current.(type) {
	case ModelStep:
		if cur.Status != ModelExecuting {
			return nil
		}
		return []Recovery{{
			Command: RecoverModelExecution{StepID: cur.RefValue.ID, Claim: claim},
			ID:      DeriveModelRecoveryCommandID(state.RunID, cur.RefValue.ID, claim),
		}}
	case ToolStep:
		var out []Recovery
		for _, call := range cur.Calls {
			if call.Status != ToolExecuting {
				continue
			}
			out = append(out, Recovery{
				Command: SubmitToolFailure{
					StepID:  cur.RefValue.ID,
					CallID:  call.CallID,
					Failure: ToolFailure{Class: FailureEffectUnknown, Message: "owner process lost before settlement"},
					Outcome: ToolOutcomeUnknown,
				},
				ID: DeriveToolRecoveryCommandID(state.RunID, cur.RefValue.ID, call.CallID, claim),
			})
		}
		return out
	default:
		return nil
	}
}
