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
// ErrStaleRuntime, Terminal -> ErrRunTerminal (RUN-CMT-7).
type CommitDecision struct {
	Kind     DecisionKind
	NewState MachineState
	Facts    []Fact
	// Reject carries the precondition failure for Conflict/Stale/Terminal.
	Reject error
}

// commandCategory classifies a command for Base handling (RUN-CMT-4).
// PrepareModelRequest is the only hard-CAS command.
type commandCategory uint8

const (
	catPlan commandCategory = iota
	catStart
	catOwnerSettle
	catIngress
	catRunControl
	catRecovery
)

func categorize(c AgentCommand) commandCategory {
	switch cmd := c.(type) {
	case PrepareModelRequest:
		return catPlan
	case StartModelExecution, StartToolCall:
		return catStart
	case SubmitModelResult, SubmitModelFailure, RejectModelResult, SubmitToolResult:
		return catOwnerSettle
	case SubmitToolFailure:
		if cmd.Outcome == ToolOutcomeUnknown {
			return catRecovery // scanner path when grantless; owner path with grant
		}
		return catIngress // known failure on Pending uses empty grant; Executing path checks grant below
	case ApproveToolCall, RejectToolCall, SubmitToolResponse, AcceptInput, WithdrawPreparedStep:
		return catIngress
	case CancelRun:
		return catRunControl
	case RecoverModelExecution:
		return catRecovery
	default:
		return catPlan
	}
}

// requiresGrant reports whether this command must carry the start grant of
// its target, given the current state (RUN-CMT-6).
func requiresGrant(s *MachineState, c AgentCommand) bool {
	switch cmd := c.(type) {
	case SubmitModelResult, SubmitModelFailure, RejectModelResult:
		return true
	case SubmitToolResult:
		return true
	case SubmitToolFailure:
		// Known failure on a Pending call uses an empty grant; anything
		// touching an Executing call needs the owner grant. Unknown from the
		// scanner is validated via recoveryValid instead.
		if ts, ok := s.Current.(ToolStep); ok {
			if i := ts.callIndex(cmd.CallID); i >= 0 {
				return ts.Calls[i].Status == ToolExecuting && cmd.Outcome == ToolOutcomeKnown
			}
		}
		return false
	case RecoverModelExecution:
		// Grant-holder release path; the grantless path is recovery-validated.
		return false
	default:
		return false
	}
}

// ValidateEnvelope is step 1 of RUN-CMT-3: identity, schema and digest. A
// digest that does not cover the command is a construction fault and is
// returned as a hard error, never as a retriable rejection.
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
	wantDigest, err := proto.DigestCommand(env.Type, env.Command)
	if err != nil {
		return err
	}
	if env.Digest != wantDigest {
		return errors.New("agent: commit: envelope digest mismatch")
	}
	return nil
}

// EvaluateCommit is the pure evaluation every Runtime runs inside the
// Session critical section after exact-replay lookup (RUN-CMT-3 steps 4-8).
// grantValid and recoveryValid are the control-plane verdicts the Runtime
// supplies: whether req.Grant is the live grant for the command's target, and
// whether a grantless recovery command matches an expired lease.
//
//nolint:gocritic // hugeParam: public pure commit evaluator keeps state/request as value protocol inputs.
func EvaluateCommit(
	cur MachineState, position RunPosition,
	req CommitRequest,
	grantValid bool,
	recoveryValid bool,
	proto Protocol,
) (CommitDecision, error) {
	env := req.Command
	if env.RunID != cur.RunID {
		return CommitDecision{}, fmt.Errorf("agent: commit: command run %q does not match authority run %q", env.RunID, cur.RunID)
	}
	if err := ValidateEnvelope(&env, proto); err != nil {
		return CommitDecision{}, err
	}
	// A start claim is part of the command identity. Rejecting an empty claim
	// here prevents an unbound worker from acquiring execution ownership.
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
	// the derivation is the idempotency index for inputs/responses/planning, so
	// a caller-minted random ID cannot bypass duplicate detection.
	if err := checkDerivedCommandID(&env, req.Base); err != nil {
		return CommitDecision{Kind: DecisionConflict, Reject: err}, nil
	}

	// Terminal absorbs non-duplicate commands (replay was handled before).
	if cur.Status.Terminal() {
		return CommitDecision{Kind: DecisionTerminal, Reject: ErrRunTerminal}, nil
	}

	// Base and authorization.
	cat := categorize(env.Command)
	if cat == catPlan && !samePosition(req.Base, position) {
		return CommitDecision{Kind: DecisionStale, Reject: ErrStaleRuntime}, nil
	}
	if requiresGrant(&cur, env.Command) && !grantValid {
		return CommitDecision{Kind: DecisionStale, Reject: ErrStaleRuntime}, nil
	}
	if cat == catRecovery && !grantValid && !recoveryValid {
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

func samePosition(a, b RunPosition) bool { return a.Revision == b.Revision && a.Index == b.Index }

// checkDerivedCommandID enforces the derived-identity rules of RUN-WIR-3.
func checkDerivedCommandID(env *CommandEnvelope, base RunPosition) error {
	var want CommandID
	switch cmd := env.Command.(type) {
	case PrepareModelRequest:
		want = DeriveModelRequestCommandID(env.RunID, base)
	case AcceptInput:
		want = DeriveInputCommandID(env.RunID, cmd.Input.ID)
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
		if cmd.Claim != "" {
			want = DeriveModelRecoveryCommandID(env.RunID, cmd.StepID, cmd.Claim)
		}
	default:
		return nil
	}
	if want != "" && env.ID != want {
		return fmt.Errorf("agent: commit: %s requires its derived CommandID", env.Type)
	}
	return nil
}

// LeaseKey addresses the execution target of a start inside the Run's lease
// namespace: <RunID>/model/<StepID> or <RunID>/call/<StepID>/<CallID> (RUN 5.1).
func LeaseKey(runID RunID, stepID StepID, callID CallID) string {
	switch {
	case stepID == "":
		return ""
	case callID == "":
		return string(runID) + "/model/" + string(stepID)
	default:
		return string(runID) + "/call/" + string(stepID) + "/" + string(callID)
	}
}

// GrantTarget returns the lease key a command's grant or recovery refers to.
func GrantTarget(runID RunID, c AgentCommand) string {
	switch cmd := c.(type) {
	case StartModelExecution:
		return LeaseKey(runID, cmd.StepID, "")
	case SubmitModelResult:
		return LeaseKey(runID, cmd.StepID, "")
	case SubmitModelFailure:
		return LeaseKey(runID, cmd.StepID, "")
	case RejectModelResult:
		return LeaseKey(runID, cmd.StepID, "")
	case RecoverModelExecution:
		return LeaseKey(runID, cmd.StepID, "")
	case StartToolCall:
		return LeaseKey(runID, cmd.StepID, cmd.CallID)
	case SubmitToolResult:
		return LeaseKey(runID, cmd.StepID, cmd.CallID)
	case SubmitToolFailure:
		return LeaseKey(runID, cmd.StepID, cmd.CallID)
	default:
		return ""
	}
}

// IsStart reports whether c acquires an execution lease.
func IsStart(c AgentCommand) bool {
	switch c.(type) {
	case StartModelExecution, StartToolCall:
		return true
	}
	return false
}

// IsSettlement reports whether c releases the lease of its target.
func IsSettlement(c AgentCommand) bool {
	switch c.(type) {
	case SubmitModelResult, SubmitModelFailure, RejectModelResult, RecoverModelExecution, SubmitToolResult, SubmitToolFailure:
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

// RecoveryCommand builds the grantless recovery command for an expired lease
// on target (RUN 5.1): an Executing model step recovers to Prepared; an
// Executing tool call settles as Unknown. ok is false when the target is no
// longer Executing.
func RecoveryCommand(state *MachineState, target string, claim ExecutionClaim) (AgentCommand, CommandID, bool) {
	switch cur := state.Current.(type) {
	case ModelStep:
		if cur.Status != ModelExecuting || target != LeaseKey(state.RunID, cur.RefValue.ID, "") {
			return nil, "", false
		}
		return RecoverModelExecution{StepID: cur.RefValue.ID, Claim: claim},
			DeriveModelRecoveryCommandID(state.RunID, cur.RefValue.ID, claim), true
	case ToolStep:
		prefix := LeaseKey(state.RunID, cur.RefValue.ID, "x")
		prefix = prefix[:len(prefix)-1]
		if len(target) <= len(prefix) || target[:len(prefix)] != prefix {
			return nil, "", false
		}
		callID := CallID(target[len(prefix):])
		if i := cur.callIndex(callID); i < 0 || cur.Calls[i].Status != ToolExecuting {
			return nil, "", false
		}
		return SubmitToolFailure{
			StepID:  cur.RefValue.ID,
			CallID:  callID,
			Failure: ToolFailure{Class: FailureEffectUnknown, Message: "lease expired"},
			Outcome: ToolOutcomeUnknown,
		}, DeriveToolRecoveryCommandID(state.RunID, cur.RefValue.ID, callID, claim), true
	default:
		return nil, "", false
	}
}
