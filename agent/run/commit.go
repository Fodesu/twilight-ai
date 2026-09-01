package run

import (
	"errors"
	"fmt"
)

type DecisionKind uint8

const (
	DecisionApply DecisionKind = iota
	DecisionAlreadyApplied
	DecisionConflict
	DecisionStale
	DecisionTerminal
)

// CommitDecision is EvaluateCommit's verdict. Runtime.Commit maps rejections
// onto the sentinel errors: Conflict -> ErrCommandConflict, Stale ->
// ErrStaleRuntime, Terminal -> ErrRunTerminal (RUN-CMT-6).
type CommitDecision struct {
	Kind       DecisionKind
	NewState   MachineState
	Events     []AgentEvent
	Transition TransitionRecord
	// Reject carries the precondition failure for Conflict/Stale/Terminal.
	Reject error
}

// commandCategory classifies a command for BaseRevision handling (RUN-CMT-3).
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
	case ApproveToolCall, RejectToolCall, SubmitToolResponse, AcceptInput:
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
// its target, given the current state (RUN-CMT-5).
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

// EvaluateCommit is the single, pure commit evaluation both runtimes call
// inside their own critical section (RUN-CMT-3). grantValid and recoveryValid
// are the control-plane verdicts the Runtime supplies: whether req.Grant is
// the live grant for the command's target, and whether a grantless recovery
// command matches the Runtime's own lease-expiry record.
//
//nolint:gocritic // hugeParam: public pure commit evaluator keeps state/request as value protocol inputs.
func EvaluateCommit(
	cur MachineState, curRevision uint64,
	prior *TransitionRecord,
	req CommitRequest,
	grantValid bool,
	recoveryValid bool,
	runSchemaVersion uint16,
) (CommitDecision, error) {
	env := req.Command

	// Step 1: envelope integrity.
	if env.RunID == "" || env.ID == "" {
		return CommitDecision{}, fmt.Errorf("agent: commit: empty RunID or CommandID")
	}
	if env.RunID != cur.RunID {
		return CommitDecision{}, fmt.Errorf("agent: commit: command run %q does not match authority run %q", env.RunID, cur.RunID)
	}
	if err := requireSchemaVersion(runSchemaVersion); err != nil {
		return CommitDecision{}, err
	}
	if env.SchemaVersion != runSchemaVersion {
		return CommitDecision{}, fmt.Errorf("agent: commit: command schema %d does not match run schema %d", env.SchemaVersion, runSchemaVersion)
	}
	wantDigest, err := DigestCommand(env.SchemaVersion, env.Type, env.Command)
	if err != nil {
		return CommitDecision{}, err
	}
	if env.Digest != wantDigest {
		return CommitDecision{Kind: DecisionConflict, Reject: fmt.Errorf("agent: commit: envelope digest mismatch")}, nil
	}
	// A start claim is part of the command identity. Rejecting an empty claim
	// here prevents an unbound worker from acquiring execution ownership.
	switch cmd := env.Command.(type) {
	case StartModelExecution:
		if cmd.Claim == "" {
			return CommitDecision{Kind: DecisionConflict, Reject: fmt.Errorf("agent: commit: model start requires an execution claim")}, nil
		}
	case StartToolCall:
		if cmd.Claim == "" {
			return CommitDecision{Kind: DecisionConflict, Reject: fmt.Errorf("agent: commit: tool start requires an execution claim")}, nil
		}
	case RecoverModelExecution:
		if cmd.Claim == "" {
			return CommitDecision{Kind: DecisionConflict, Reject: fmt.Errorf("agent: commit: model recovery requires an execution claim")}, nil
		}
	}
	// Steps 2-3: idempotent replay and identity conflict.
	if prior != nil {
		if prior.CommandDigest == env.Digest {
			return CommitDecision{Kind: DecisionAlreadyApplied, Events: prior.Events, Transition: cloneTransitionRecord(prior)}, nil
		}
		return CommitDecision{Kind: DecisionConflict, Reject: ErrCommandConflict}, nil
	}
	// Derived-identity families must use their derived CommandID (RUN-WIR-3):
	// the derivation is the idempotency index for inputs/responses/planning, so
	// a caller-minted random ID cannot bypass duplicate detection. This check is
	// deliberately after exact replay: BaseRevision is not part of the command
	// digest, and a retry may have reloaded a newer snapshot before replaying an
	// already accepted prepare command.
	if err := checkDerivedCommandID(&env, req.BaseRevision); err != nil {
		return CommitDecision{Kind: DecisionConflict, Reject: err}, nil
	}

	// Step 7 precheck: terminal absorbs non-duplicate commands.
	if cur.Status.Terminal() {
		return CommitDecision{Kind: DecisionTerminal, Reject: ErrRunTerminal}, nil
	}

	// Step 4: BaseRevision and authorization.
	cat := categorize(env.Command)
	if cat == catPlan && req.BaseRevision != curRevision {
		return CommitDecision{Kind: DecisionStale, Reject: ErrStaleRuntime}, nil
	}
	if requiresGrant(&cur, env.Command) && !grantValid {
		return CommitDecision{Kind: DecisionStale, Reject: ErrStaleRuntime}, nil
	}
	if cat == catRecovery && !grantValid && !recoveryValid {
		return CommitDecision{Kind: DecisionStale, Reject: ErrStaleRuntime}, nil
	}

	// Step 5: Decide once, fold with Evolve.
	facts, err := DecideVersion(env.SchemaVersion, cur, env.Command)
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
	newRevision := curRevision + 1
	state := cur
	events := make([]AgentEvent, len(facts))
	if cmd, ok := env.Command.(PrepareModelRequest); ok {
		if len(facts) == 0 {
			return CommitDecision{}, fmt.Errorf("agent: commit: prepare produced no facts")
		}
		prepared, ok := facts[0].(ModelStepPrepared)
		if !ok {
			return CommitDecision{}, fmt.Errorf("agent: commit: prepare did not produce ModelStepPrepared")
		}
		wantStep := DeriveModelStepID(env.RunID, env.ID, prepared.BindingDigest)
		if cmd.StepID != wantStep {
			return CommitDecision{Kind: DecisionStale, Reject: fmt.Errorf("prepare: StepID %q does not match derived StepID %q", cmd.StepID, wantStep)}, nil
		}
	}

	for i, f := range facts {
		// Detach every fact before it is folded or wrapped as an event. Decide
		// often forwards fields from the caller's command (ModelRequest,
		// ModelResult, CanonicalJSON payloads); the commit decision must not
		// carry caller-owned mutable objects across the Runtime boundary.
		f, err = snapshotFact(f)
		if err != nil {
			return CommitDecision{}, err
		}
		state, err = EvolveVersion(env.SchemaVersion, state, f)
		if err != nil {
			return CommitDecision{}, err
		}
		typ := factType(f)
		fd, err := DigestFact(env.SchemaVersion, typ, f)
		if err != nil {
			return CommitDecision{}, err
		}
		events[i] = AgentEvent{
			SchemaVersion: env.SchemaVersion,
			Type:          typ,
			RunID:         cur.RunID,
			Revision:      newRevision,
			Index:         uint16(i),
			CommandID:     env.ID,
			CommandDigest: env.Digest,
			Digest:        fd,
			Fact:          f,
		}
	}
	transition, err := BuildTransitionRecord(events)
	if err != nil {
		return CommitDecision{}, err
	}
	return CommitDecision{Kind: DecisionApply, NewState: state, Events: transition.Events, Transition: transition}, nil
}

// BuildEnvelope assembles a CommandEnvelope with its type discriminator and
// canonical digest. This is the only sanctioned construction path; callers
// never hand-assemble envelope fields (RUN-WIR-3).
func BuildEnvelope(run RunID, id CommandID, cmd AgentCommand) (CommandEnvelope, error) {
	return BuildEnvelopeVersion(currentSchemaVersion, run, id, cmd)
}

// BuildEnvelopeVersion stamps schemaVersion onto the envelope. Runtime.Commit
// accepts it only when it matches the Run header's SchemaVersion.
func BuildEnvelopeVersion(schemaVersion uint16, run RunID, id CommandID, cmd AgentCommand) (CommandEnvelope, error) {
	if err := requireSchemaVersion(schemaVersion); err != nil {
		return CommandEnvelope{}, err
	}
	typ := commandType(cmd)
	if typ == "" {
		return CommandEnvelope{}, fmt.Errorf("agent: envelope: unknown command variant %T", cmd)
	}
	d, err := DigestCommand(schemaVersion, typ, cmd)
	if err != nil {
		return CommandEnvelope{}, err
	}
	return CommandEnvelope{
		SchemaVersion: schemaVersion,
		Type:          typ,
		RunID:         run,
		ID:            id,
		Digest:        d,
		Command:       cmd,
	}, nil
}

// checkDerivedCommandID enforces the derived-identity rules of RUN-WIR-3.
// AcceptInput derives from (RunID, InputID); approval/rejection/answer derive
// from (RunID, StepID, CallID, ResponseID). Approve and reject of the same
// response share one identity by design, so a decision change surfaces as
// ErrCommandConflict instead of a second fact.
func checkDerivedCommandID(env *CommandEnvelope, baseRevision uint64) error {
	var want CommandID
	switch cmd := env.Command.(type) {
	case PrepareModelRequest:
		want = DeriveModelRequestCommandID(env.RunID, baseRevision)
	case AcceptInput:
		want = DeriveInputCommandID(env.RunID, cmd.Input.ID)
	case ApproveToolCall:
		want = DeriveResponseCommandID(env.RunID, cmd.StepID, cmd.CallID, cmd.ResponseID)
	case RejectToolCall:
		want = DeriveResponseCommandID(env.RunID, cmd.StepID, cmd.CallID, cmd.ResponseID)
	case SubmitToolResponse:
		want = DeriveResponseCommandID(env.RunID, cmd.StepID, cmd.CallID, cmd.ResponseID)
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
