package runtime

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/schema"
	"github.com/felinics/twilight/agent/run/wire"
)

type DecisionKind uint8

const (
	DecisionApply DecisionKind = iota
	DecisionConflict
	DecisionStale
	DecisionTerminal
)

// CommitDecision is EvaluateCommit's verdict. The RunStore maps rejections
// onto the sentinel errors: Conflict -> ErrCommandConflict, Stale ->
// ErrStaleRuntime, Terminal -> ErrRunTerminal.
type CommitDecision struct {
	Kind     DecisionKind
	NewState run.MachineState
	Facts    []run.Fact
	// Reject carries the precondition failure for Conflict/Stale/Terminal.
	Reject error
}

// ValidateEnvelope is step 1 of RUN-CMT-3: identity and schema. Envelopes are
// only built by wire.Codec.Envelope (RUN-WIR-3), so there is no per-commit
// self-verification of the command bytes.
func ValidateEnvelope(env *wire.CommandEnvelope, sch schema.Schema) error {
	if env.RunID == "" || env.ID == "" {
		return errors.New("agent: commit: empty RunID or CommandID")
	}
	if !sch.Valid() {
		return errors.New("agent: commit: unbound schema")
	}
	if env.SchemaVersion != sch.Version {
		return fmt.Errorf("agent: commit: command schema %d does not match run schema %d", env.SchemaVersion, sch.Version)
	}
	return nil
}

// EvaluateCommit is the pure evaluation every RunStore runs inside its
// store's critical section after the replay lookup (RUN-CMT-3 steps 4-8). Execution ownership is
// Session-level (RUN-CMT-6), so there is no per-target authorization: a
// command against a target whose state does not admit it is Stale.
//
//nolint:gocritic // hugeParam: public pure commit evaluator keeps state/request as value protocol inputs.
func EvaluateCommit(cur run.MachineState, position run.RunPosition, req CommitRequest, sch schema.Schema) (CommitDecision, error) {
	env := req.Command
	if env.RunID != cur.RunID {
		return CommitDecision{}, fmt.Errorf("agent: commit: command run %q does not match authority run %q", env.RunID, cur.RunID)
	}
	if err := ValidateEnvelope(&env, sch); err != nil {
		return CommitDecision{}, err
	}
	// The effect a start or recovery names is part of the command identity
	// (RUN-WIR-1).
	switch cmd := env.Command.(type) {
	case run.StartModelExecution:
		if cmd.Effect == "" {
			return CommitDecision{Kind: DecisionConflict, Reject: errors.New("agent: commit: model start requires its effect identity")}, nil
		}
	case run.StartToolCall:
		if cmd.Effect == "" {
			return CommitDecision{Kind: DecisionConflict, Reject: errors.New("agent: commit: tool start requires its effect identity")}, nil
		}
	case run.RecoverModelExecution:
		if cmd.Effect == "" {
			return CommitDecision{Kind: DecisionConflict, Reject: errors.New("agent: commit: model recovery requires its effect identity")}, nil
		}
	}
	// Derived-identity families must use their derived CommandID (RUN-WIR-3):
	// the derivation is the idempotency index, so a caller-minted random ID
	// cannot bypass duplicate detection.
	if err := checkDerivedCommandID(&env, req.Base, sch.Identity); err != nil {
		return CommitDecision{Kind: DecisionConflict, Reject: err}, nil
	}

	// Terminal absorbs non-duplicate commands (replay was handled before).
	if cur.Status.Terminal() {
		return CommitDecision{Kind: DecisionTerminal, Reject: run.ErrRunTerminal}, nil
	}

	// Base: PrepareModelRequest is the only hard-CAS command (RUN-CMT-4).
	if _, plan := env.Command.(run.PrepareModelRequest); plan && req.Base != position {
		return CommitDecision{Kind: DecisionStale, Reject: run.ErrStaleRuntime}, nil
	}

	// Step 7: Decide once, fold with Evolve.
	facts, err := sch.Machine.Decide(cur, env.Command)
	if err != nil {
		switch {
		case errors.Is(err, run.ErrRunTerminal):
			return CommitDecision{Kind: DecisionTerminal, Reject: err}, nil
		case errors.Is(err, run.ErrStaleRuntime):
			return CommitDecision{Kind: DecisionStale, Reject: err}, nil
		case errors.Is(err, run.ErrCommandConflict):
			return CommitDecision{Kind: DecisionConflict, Reject: err}, nil
		default:
			// Precondition failures against the current state are stale from
			// the caller's perspective: reload and rederive.
			return CommitDecision{Kind: DecisionStale, Reject: err}, nil
		}
	}
	if cmd, ok := env.Command.(run.PrepareModelRequest); ok {
		if len(facts) == 0 {
			return CommitDecision{}, errors.New("agent: commit: prepare produced no facts")
		}
		prepared, ok := facts[0].(run.ModelStepPrepared)
		if !ok {
			return CommitDecision{}, errors.New("agent: commit: prepare did not produce ModelStepPrepared")
		}
		wantStep := sch.Identity.DeriveModelStepID(env.RunID, env.ID, prepared.BindingDigest)
		if cmd.StepID != wantStep {
			return CommitDecision{Kind: DecisionStale, Reject: fmt.Errorf("prepare: StepID %q does not match derived StepID %q", cmd.StepID, wantStep)}, nil
		}
	}

	state := cur
	detached := make([]run.Fact, len(facts))
	for i, f := range facts {
		// Detach every fact before it is folded: Decide forwards fields from
		// the caller's command and the decision must not carry caller-owned
		// mutable objects across the Runtime boundary.
		f, err = run.SnapshotFact(f)
		if err != nil {
			return CommitDecision{}, err
		}
		state, err = sch.Machine.Evolve(state, f)
		if err != nil {
			return CommitDecision{}, err
		}
		detached[i] = f
	}
	return CommitDecision{Kind: DecisionApply, NewState: state, Facts: detached}, nil
}

// checkDerivedCommandID enforces the derived-identity rules of RUN-WIR-3.
func checkDerivedCommandID(env *wire.CommandEnvelope, base run.RunPosition, id run.Identity) error {
	var want run.CommandID
	switch cmd := env.Command.(type) {
	case run.PrepareModelRequest:
		want = id.DeriveModelRequestCommandID(env.RunID, base)
	case run.AcceptInput:
		want = id.DeriveInputCommandID(env.RunID, cmd.InputIDs()...)
	case run.WithdrawPreparedStep:
		want = id.DeriveWithdrawCommandID(env.RunID, cmd.StepID)
	case run.ApproveToolCall:
		want = id.DeriveResponseCommandID(env.RunID, cmd.StepID, cmd.CallID, cmd.ResponseID)
	case run.RejectToolCall:
		want = id.DeriveResponseCommandID(env.RunID, cmd.StepID, cmd.CallID, cmd.ResponseID)
	case run.SubmitToolResponse:
		want = id.DeriveResponseCommandID(env.RunID, cmd.StepID, cmd.CallID, cmd.ResponseID)
	case run.StartModelExecution:
		want = id.DeriveStartCommandID(cmd.Effect)
	case run.StartToolCall:
		want = id.DeriveStartCommandID(cmd.Effect)
	case run.RecoverModelExecution:
		want = id.DeriveRecoveryCommandID(cmd.Effect)
	default:
		return nil
	}
	if want != "" && env.ID != want {
		return fmt.Errorf("agent: commit: %s requires its derived CommandID", env.Type)
	}
	return nil
}
