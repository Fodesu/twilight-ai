package run

import (
	"errors"
	"fmt"
)

// Evolve folds one fact with the current schema version. Persisted replay uses
// EvolveVersion so future schema versions can keep their historical folding
// semantics while this convenience API remains value-based.
//
//nolint:gocritic // hugeParam: public fold boundary must stay value-based: Evolve(state, fact) -> new state.
func Evolve(s MachineState, f Fact) (MachineState, error) {
	return EvolveVersion(currentSchemaVersion, s, f)
}

// EvolveVersion folds one persisted fact using the Evolve semantics for its
// SchemaVersion. It is mechanical: no IO, no policy decisions, and no Decide.
//
//nolint:gocritic // hugeParam: public replay boundary must stay value-based: EvolveVersion(version, state, fact) -> new state.
func EvolveVersion(schemaVersion uint16, s MachineState, f Fact) (MachineState, error) {
	switch schemaVersion {
	case SchemaVersion1:
		return evolveV1(s, f)
	default:
		return s, fmt.Errorf("agent: evolve: unsupported schema version %d", schemaVersion)
	}
}

// evolveV1 is the current fold semantics for the pre-release SchemaVersion1.
//
//nolint:gocritic // hugeParam: v1 fold body intentionally preserves value-state semantics.
func evolveV1(s MachineState, f Fact) (MachineState, error) {
	if err := validateFactTransition(s, f); err != nil {
		return s, err
	}
	switch fact := f.(type) {
	case ModelStepPrepared:
		if s.Current != nil {
			return s, fmt.Errorf("agent: evolve: model step prepared while a step is current")
		}
		// v1 preparation is the atomic consumption boundary for pending inputs.
		// A persisted fact must name every pending input exactly once, in queue
		// order; accepting a subset or an invented ID would make replay diverge
		// from the command that created this frozen request.
		if len(fact.InputIDs) != len(s.PendingInputs) {
			return s, fmt.Errorf("agent: evolve: model step prepared input IDs do not completely consume pending inputs: got %d, want %d", len(fact.InputIDs), len(s.PendingInputs))
		}
		for i, input := range s.PendingInputs {
			if fact.InputIDs[i] != input.ID {
				return s, fmt.Errorf("agent: evolve: model step prepared input ID at position %d = %q, want pending input %q", i, fact.InputIDs[i], input.ID)
			}
		}
		s.Current = ModelStep{
			RefValue:      StepRef{RunID: s.RunID, ID: fact.StepID, Digest: fact.BindingDigest},
			Request:       fact.Request,
			RequestDigest: fact.RequestDigest,
			Model:         fact.Model,
			Tools:         fact.Tools,
			ToolsDigest:   fact.ToolsDigest,
			Status:        ModelPrepared,
		}
		s.ModelSteps++
		s.PendingInputs = nil
		return s, nil

	case ModelStepStarted:
		ms, err := evolveModelStep(&s, fact.StepID)
		if err != nil {
			return s, err
		}
		ms.Status = ModelExecuting
		s.Current = *ms
		return s, nil

	case ModelStepRecovered:
		ms, err := evolveModelStep(&s, fact.StepID)
		if err != nil {
			return s, err
		}
		ms.Status = ModelPrepared
		s.Current = *ms
		return s, nil

	case ModelStepRejected:
		ms, err := evolveModelStep(&s, fact.StepID)
		if err != nil {
			return s, err
		}
		ms.Rejects++
		ms.Status = ModelPrepared
		s.Current = *ms
		s.Usage = s.Usage.Add(fact.Usage)
		return s, nil

	case ModelStepCompleted:
		if _, err := evolveModelStep(&s, fact.StepID); err != nil {
			return s, err
		}
		result := fact.Result
		s.LastModelResult = &result
		s.Usage = s.Usage.Add(fact.Result.Usage)
		s.Current = nil
		return s, nil

	case ToolStepOpened:
		if s.Current != nil {
			return s, fmt.Errorf("agent: evolve: tool step opened while a step is current")
		}
		calls := make([]ToolCallState, len(fact.Calls))
		for i, b := range fact.Calls {
			status := ToolPending
			var waiting *ResponseRequest
			if b.Response != nil {
				status = ToolWaiting
				w := *b.Response
				waiting = &w
			}
			calls[i] = ToolCallState{
				CallID:           b.CallID,
				ToolRef:          b.ToolRef,
				DefinitionDigest: b.DefinitionDigest,
				BindingDigest:    b.BindingDigest,
				Arguments:        b.Arguments,
				Policy:           b.Policy,
				Status:           status,
				Waiting:          waiting,
			}
			if err := ValidateToolCallState(calls[i]); err != nil {
				return s, err
			}
		}
		s.Current = ToolStep{
			RefValue: StepRef{RunID: s.RunID, ID: fact.StepID, Digest: fact.BindingSetDigest},
			Source:   fact.Source,
			Calls:    calls,
		}
		return s, nil

	case ToolCallStarted:
		return evolveCall(&s, fact.StepID, fact.CallID, func(c *ToolCallState) {
			c.Status = ToolExecuting
		})

	case ToolCallApproved:
		return evolveCall(&s, fact.StepID, fact.CallID, func(c *ToolCallState) {
			c.Status = ToolPending
			c.Waiting = nil
		})

	case ToolCallCompleted:
		return evolveCall(&s, fact.StepID, fact.CallID, func(c *ToolCallState) {
			c.Status = ToolCompleted
			r := fact.Result
			c.Result = &r
			c.Waiting = nil
		})

	case ToolCallAnswered:
		return evolveCall(&s, fact.StepID, fact.CallID, func(c *ToolCallState) {
			c.Status = ToolCompleted
			c.Result = &ToolExecutionResult{Output: fact.Payload}
			c.Waiting = nil
		})

	case ToolCallFailed:
		return evolveCall(&s, fact.StepID, fact.CallID, func(c *ToolCallState) {
			c.Status = ToolFailed
			c.Failure = &ToolCallFailure{Failure: fact.Failure, Outcome: fact.Outcome}
			c.Waiting = nil
		})

	case InputAccepted:
		for _, in := range s.PendingInputs {
			if in.ID == fact.Input.ID {
				return s, nil // idempotent append per InputID
			}
		}
		s.PendingInputs = append(append([]AgentInput(nil), s.PendingInputs...), fact.Input)
		return s, nil

	case RunEnded:
		normalized, err := fact.normalized()
		if err != nil {
			return s, err
		}
		status, reason, failure := endProjection(normalized.End)
		s.Status = status
		s.Current = nil
		s.Result = &RunResult{
			Status:  status,
			Reason:  reason,
			Failure: failure,
			Model:   s.LastModelResult,
			Usage:   s.Usage,
		}
		return s, nil

	default:
		return s, fmt.Errorf("agent: evolve: unknown fact variant %T", f)
	}
}

// validateFactTransition protects replay from a structurally valid event that
// was not a legal Machine transition. Decide performs the same checks before
// commit, while fold/recovery must defend itself without access to commands.
func validateFactTransition(s MachineState, f Fact) error {
	switch fact := f.(type) {
	case ModelStepPrepared:
		if s.Status.Terminal() || s.Current != nil {
			return fmt.Errorf("agent: evolve: model step prepared while run is not at a model boundary")
		}
		if fact.StepID == "" || fact.Model == "" || fact.RequestDigest == "" || fact.ToolsDigest == "" || fact.BindingDigest == "" {
			return errors.New("agent: evolve: model step prepared is missing identity or digest")
		}
		if ModelRef(fact.Request.Model) != fact.Model {
			return errors.New("agent: evolve: model step prepared model mismatch")
		}
		requestDigest, err := DigestRequest(fact.Request)
		if err != nil || requestDigest != fact.RequestDigest {
			return errors.New("agent: evolve: model step prepared request digest mismatch")
		}
		toolsDigest, err := DigestToolSpecs(fact.Tools)
		if err != nil || toolsDigest != fact.ToolsDigest {
			return errors.New("agent: evolve: model step prepared tools digest mismatch")
		}
		binding, err := DigestModelStepBinding(fact.Model, fact.RequestDigest, fact.ToolsDigest)
		if err != nil || binding != fact.BindingDigest {
			return errors.New("agent: evolve: model step prepared binding digest mismatch")
		}
	case ModelStepStarted:
		ms, ok := s.Current.(ModelStep)
		if !ok || ms.RefValue.ID != fact.StepID || ms.Status != ModelPrepared {
			return fmt.Errorf("agent: evolve: model step %q is not Prepared", fact.StepID)
		}
	case ModelStepRecovered:
		ms, ok := s.Current.(ModelStep)
		if !ok || ms.RefValue.ID != fact.StepID || ms.Status != ModelExecuting {
			return fmt.Errorf("agent: evolve: model step %q is not Executing", fact.StepID)
		}
	case ModelStepRejected:
		ms, ok := s.Current.(ModelStep)
		if !ok || ms.RefValue.ID != fact.StepID || ms.Status != ModelExecuting {
			return fmt.Errorf("agent: evolve: model step %q is not Executing", fact.StepID)
		}
	case ModelStepCompleted:
		ms, ok := s.Current.(ModelStep)
		if !ok || ms.RefValue.ID != fact.StepID || ms.Status != ModelExecuting {
			return fmt.Errorf("agent: evolve: model step %q is not Executing", fact.StepID)
		}
	case ToolStepOpened:
		if s.Status.Terminal() || s.Current != nil || len(fact.Calls) == 0 {
			return fmt.Errorf("agent: evolve: tool step opened outside an empty active boundary")
		}
		if fact.StepID == "" || fact.Source == "" || fact.BindingSetDigest == "" {
			return errors.New("agent: evolve: tool step is missing identity or digest")
		}
		// BindingSetDigest is defined over the ordered call bindings before
		// derived response requests are attached. Recompute that exact input.
		baseBindings := make([]ToolCallBinding, len(fact.Calls))
		for i := range fact.Calls {
			baseBindings[i] = fact.Calls[i]
			baseBindings[i].Response = nil
		}
		setDigest, err := digestBindingSet(baseBindings)
		if err != nil || setDigest != fact.BindingSetDigest || DeriveToolStepID(fact.Source, fact.BindingSetDigest) != fact.StepID {
			return errors.New("agent: evolve: tool step binding digest mismatch")
		}
		seen := make(map[CallID]struct{}, len(fact.Calls))
		for _, call := range fact.Calls {
			if call.CallID == "" {
				return errors.New("agent: evolve: tool step contains empty CallID")
			}
			if _, exists := seen[call.CallID]; exists {
				return fmt.Errorf("agent: evolve: duplicate CallID %q", call.CallID)
			}
			seen[call.CallID] = struct{}{}
			if call.ToolRef == "" || call.BindingDigest == "" || call.Arguments.IsZero() {
				return fmt.Errorf("agent: evolve: tool call %q is missing binding data", call.CallID)
			}
			wantBinding, err := DigestToolCallBinding(call.CallID, call.DefinitionDigest, call.Policy, call.Arguments)
			if err != nil || wantBinding != call.BindingDigest {
				return fmt.Errorf("agent: evolve: tool call %q binding digest mismatch", call.CallID)
			}
			if call.Response != nil {
				kind := ResponseApproval
				if call.Policy == ExternalResponse {
					kind = ResponseExternal
				}
				if call.Policy != ApprovalRequired && call.Policy != ExternalResponse {
					return fmt.Errorf("agent: evolve: direct call %q cannot carry a response request", call.CallID)
				}
				requestDigest, err := DigestToolCallBinding(call.CallID, call.DefinitionDigest, call.Policy, call.Arguments)
				if err != nil {
					return err
				}
				if err := validateResponseRequest(*call.Response, s.RunID, fact.StepID, call.CallID, kind, call.Arguments, requestDigest); err != nil {
					return err
				}
			}
		}
	case ToolCallStarted:
		call, err := currentCallState(s, fact.StepID, fact.CallID)
		if err != nil || call.Status != ToolPending {
			return fmt.Errorf("agent: evolve: tool call %q is not Pending", fact.CallID)
		}
	case ToolCallApproved:
		call, err := currentCallState(s, fact.StepID, fact.CallID)
		if err != nil || call.Status != ToolWaiting || call.Waiting == nil || call.Waiting.Kind != ResponseApproval {
			return fmt.Errorf("agent: evolve: tool call %q is not waiting for approval", fact.CallID)
		}
		if call.Waiting.ID != fact.ResponseID {
			return fmt.Errorf("agent: evolve: tool call %q response ID mismatch", fact.CallID)
		}
		want, err := DigestToolResponseDecision(ResponseApproval, ResponseDecisionApproved, "")
		if err != nil || want != fact.ResponseDigest {
			return fmt.Errorf("agent: evolve: tool call %q approval digest mismatch", fact.CallID)
		}
	case ToolCallCompleted:
		call, err := currentCallState(s, fact.StepID, fact.CallID)
		if err != nil || call.Status != ToolExecuting {
			return fmt.Errorf("agent: evolve: tool call %q is not Executing", fact.CallID)
		}
	case ToolCallAnswered:
		call, err := currentCallState(s, fact.StepID, fact.CallID)
		if err != nil || call.Status != ToolWaiting || call.Waiting == nil || call.Waiting.Kind != ResponseExternal {
			return fmt.Errorf("agent: evolve: tool call %q is not waiting for an external response", fact.CallID)
		}
		if call.Waiting.ID != fact.ResponseID {
			return fmt.Errorf("agent: evolve: tool call %q response ID mismatch", fact.CallID)
		}
		want, err := DigestToolResponsePayload(fact.Payload)
		if err != nil || want != fact.ResponseDigest {
			return fmt.Errorf("agent: evolve: tool call %q response digest mismatch", fact.CallID)
		}
	case ToolCallFailed:
		call, err := currentCallState(s, fact.StepID, fact.CallID)
		if err != nil || (call.Status != ToolPending && call.Status != ToolExecuting && call.Status != ToolWaiting) {
			return fmt.Errorf("agent: evolve: tool call %q cannot fail from its current state", fact.CallID)
		}
		if fact.Failure.Class == "" {
			return fmt.Errorf("agent: evolve: tool call %q failure has no class", fact.CallID)
		}
		switch fact.Outcome {
		case ToolOutcomeKnown:
			if fact.Failure.Class == FailureEffectUnknown {
				return fmt.Errorf("agent: evolve: known outcome cannot use %s", FailureEffectUnknown)
			}
		case ToolOutcomeUnknown:
			if call.Status != ToolExecuting {
				return fmt.Errorf("agent: evolve: unknown outcome requires Executing call")
			}
			if fact.Failure.Class != FailureEffectUnknown {
				return fmt.Errorf("agent: evolve: unknown outcome must use %s", FailureEffectUnknown)
			}
		default:
			return fmt.Errorf("agent: evolve: unknown failure outcome %d", fact.Outcome)
		}
	case InputAccepted:
		if s.Status.Terminal() || s.Current != nil {
			return errors.New("agent: evolve: input accepted outside an empty active boundary")
		}
	case RunEnded:
		if s.Status.Terminal() {
			return errors.New("agent: evolve: duplicate terminal fact")
		}
		if _, err := fact.normalized(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("agent: evolve: unknown fact variant %T", f)
	}
	return nil
}

func currentCallState(s MachineState, step StepID, call CallID) (ToolCallState, error) {
	ts, ok := s.Current.(ToolStep)
	if !ok || ts.RefValue.ID != step {
		return ToolCallState{}, errors.New("not current ToolStep")
	}
	i := ts.callIndex(call)
	if i < 0 {
		return ToolCallState{}, errors.New("unknown CallID")
	}
	return ts.Calls[i], nil
}

func validateResponseRequest(req ResponseRequest, runID RunID, stepID StepID, callID CallID, kind ResponseKind, payload CanonicalJSON, requestDigest Digest) error {
	if req.RunID != runID || req.StepID != stepID || req.CallID != callID || req.Kind != kind {
		return fmt.Errorf("agent: evolve: response request identity mismatch for call %q", callID)
	}
	if req.ID == "" || req.ID != DeriveResponseID(runID, stepID, callID, kind) {
		return fmt.Errorf("agent: evolve: response request ID mismatch for call %q", callID)
	}
	if req.RequestDigest != requestDigest || !req.Payload.Equal(payload) {
		return fmt.Errorf("agent: evolve: response request payload mismatch for call %q", callID)
	}
	return nil
}

func evolveModelStep(s *MachineState, step StepID) (*ModelStep, error) {
	ms, ok := s.Current.(ModelStep)
	if !ok || ms.RefValue.ID != step {
		return nil, fmt.Errorf("agent: evolve: model step %q is not current", step)
	}
	return &ms, nil
}

func evolveCall(s *MachineState, step StepID, call CallID, apply func(*ToolCallState)) (MachineState, error) {
	ts, ok := s.Current.(ToolStep)
	if !ok || ts.RefValue.ID != step {
		return *s, fmt.Errorf("agent: evolve: tool step %q is not current", step)
	}
	i := ts.callIndex(call)
	if i < 0 {
		return *s, fmt.Errorf("agent: evolve: unknown call %q", call)
	}
	calls := append([]ToolCallState(nil), ts.Calls...)
	apply(&calls[i])
	// Spec §4.2: Evolve must reject illegal field combinations, e.g. an
	// unknown-outcome failure whose class is not effect_unknown.
	if err := ValidateToolCallState(calls[i]); err != nil {
		return *s, err
	}
	ts.Calls = calls
	if allToolCallsTerminal(calls) {
		closed := ts
		closed.Calls = calls
		s.LastToolStep = &closed
		s.Current = nil
		s.LastClosedStep = step
	} else {
		s.Current = ts
	}
	return *s, nil
}

func allToolCallsTerminal(calls []ToolCallState) bool {
	if len(calls) == 0 {
		return false
	}
	for _, call := range calls {
		if call.Status != ToolCompleted && call.Status != ToolFailed {
			return false
		}
	}
	return true
}
