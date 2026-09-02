package run

import (
	"errors"
	"fmt"
)

// evolveV1 is the fold semantics for the pre-release SchemaVersion1. It first
// checks that the fact is a legal transition from s (fold and recovery must
// defend themselves without access to commands), then applies it.
//
//nolint:gocritic // hugeParam: v1 fold body intentionally preserves value-state semantics.
func evolveV1(s MachineState, f Fact) (MachineState, error) {
	if err := guardFactV1(&s, f); err != nil {
		return s, err
	}
	switch fact := f.(type) {
	case ModelStepPrepared:
		return applyModelStepPrepared(s, &fact), nil
	case ModelStepStarted:
		return applyModelStatus(s, ModelExecuting, Usage{}, false), nil
	case ModelStepRecovered:
		return applyModelStatus(s, ModelPrepared, Usage{}, false), nil
	case ModelStepRejected:
		return applyModelStatus(s, ModelPrepared, fact.Usage, true), nil
	case ModelStepCompleted:
		return applyModelStepCompleted(s, &fact), nil
	case ToolStepOpened:
		return applyToolStepOpened(s, &fact), nil
	case ToolCallStarted:
		return applyCall(s, fact.CallID, func(c *ToolCallState) { c.Status = ToolExecuting }), nil
	case ToolCallApproved:
		return applyCall(s, fact.CallID, func(c *ToolCallState) { c.Status, c.Waiting = ToolPending, nil }), nil
	case ToolCallCompleted:
		return applyCall(s, fact.CallID, func(c *ToolCallState) {
			r := fact.Result
			c.Status, c.Result, c.Waiting = ToolCompleted, &r, nil
		}), nil
	case ToolCallAnswered:
		return applyCall(s, fact.CallID, func(c *ToolCallState) {
			c.Status, c.Result, c.Waiting = ToolCompleted, &ToolExecutionResult{Output: fact.Payload}, nil
		}), nil
	case ToolCallFailed:
		return applyCall(s, fact.CallID, func(c *ToolCallState) {
			c.Status, c.Failure, c.Waiting = ToolFailed, &ToolCallFailure{Failure: fact.Failure, Outcome: fact.Outcome}, nil
		}), nil
	case InputAccepted:
		return applyInputAccepted(s, &fact), nil
	case RunEnded:
		return applyRunEnded(s, &fact), nil
	default:
		return s, fmt.Errorf("agent: evolve: unknown fact variant %T", f)
	}
}

// --- apply: mechanical folds; guardFactV1 has established every precondition ---

func applyModelStepPrepared(s MachineState, fact *ModelStepPrepared) MachineState {
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
	return s
}

// applyModelStatus moves the current ModelStep to status, adding usage and
// counting a reject when the fact was a rejection.
func applyModelStatus(s MachineState, status ModelStepStatus, usage Usage, rejected bool) MachineState {
	ms := s.Current.(ModelStep) //nolint:errcheck // guard established Current is this ModelStep
	ms.Status = status
	if rejected {
		ms.Rejects++
	}
	s.Current = ms
	s.Usage = s.Usage.Add(usage)
	return s
}

func applyModelStepCompleted(s MachineState, fact *ModelStepCompleted) MachineState {
	result := fact.Result
	s.LastModelResult = &result
	s.Usage = s.Usage.Add(fact.Result.Usage)
	s.Current = Open{}
	return s
}

func applyToolStepOpened(s MachineState, fact *ToolStepOpened) MachineState {
	calls := make([]ToolCallState, len(fact.Calls))
	for i, b := range fact.Calls {
		calls[i] = ToolCallState{
			CallID:           b.CallID,
			ToolRef:          b.ToolRef,
			DefinitionDigest: b.DefinitionDigest,
			BindingDigest:    b.BindingDigest,
			Arguments:        b.Arguments,
			Policy:           b.Policy,
			Status:           ToolPending,
		}
		if b.Response != nil {
			w := *b.Response
			calls[i].Status, calls[i].Waiting = ToolWaiting, &w
		}
	}
	s.Current = ToolStep{
		RefValue:   StepRef{RunID: s.RunID, ID: fact.StepID, Digest: fact.BindingSetDigest},
		Source:     fact.Source,
		Calls:      calls,
		Scheduling: fact.Scheduling,
	}
	return s
}

// applyCall mutates one call of the current ToolStep and closes the step when
// every call has reached Completed or Failed.
func applyCall(s MachineState, callID CallID, mutate func(*ToolCallState)) MachineState {
	ts := s.Current.(ToolStep) //nolint:errcheck // guard established Current is this ToolStep
	calls := append([]ToolCallState(nil), ts.Calls...)
	mutate(&calls[ts.callIndex(callID)])
	ts.Calls = calls
	if allToolCallsTerminal(calls) {
		s.LastToolStep = &ts
		s.Current = Open{}
	} else {
		s.Current = ts
	}
	return s
}

func applyInputAccepted(s MachineState, fact *InputAccepted) MachineState {
	for _, in := range s.PendingInputs {
		if in.ID == fact.Input.ID {
			return s // idempotent append per InputID
		}
	}
	s.PendingInputs = append(append([]AgentInput(nil), s.PendingInputs...), fact.Input)
	return s
}

func applyRunEnded(s MachineState, fact *RunEnded) MachineState {
	status, reason, failure := endProjection(fact.End)
	s.Status = status
	s.Current = nil
	result := &RunResult{Status: status, Reason: reason, Failure: failure, Model: s.LastModelResult, Usage: s.Usage}
	if stopped, ok := fact.End.(RunStoppedEnd); ok {
		result.UncertainCalls = append([]CallID(nil), stopped.UncertainCalls...)
		result.UncertainModel = stopped.UncertainModel
	}
	s.Result = result
	return s
}

// --- guards: one per fact. Each names the legal source state and the
// self-consistency the fact must carry. ---

func guardFactV1(s *MachineState, f Fact) error {
	if s.Status.Terminal() {
		return errors.New("agent: evolve: fact after terminal state")
	}
	switch fact := f.(type) {
	case ModelStepPrepared:
		return guardModelStepPrepared(s, &fact)
	case ModelStepStarted:
		return requireModelStep(s, fact.StepID, ModelPrepared)
	case ModelStepRecovered:
		return requireModelStep(s, fact.StepID, ModelExecuting)
	case ModelStepRejected:
		return requireModelStep(s, fact.StepID, ModelExecuting)
	case ModelStepCompleted:
		return requireModelStep(s, fact.StepID, ModelExecuting)
	case ToolStepOpened:
		return guardToolStepOpened(s, &fact)
	case ToolCallStarted:
		_, err := requireCall(s, fact.StepID, fact.CallID, ToolPending)
		return err
	case ToolCallApproved:
		return guardToolCallApproved(s, &fact)
	case ToolCallCompleted:
		_, err := requireCall(s, fact.StepID, fact.CallID, ToolExecuting)
		return err
	case ToolCallAnswered:
		return guardToolCallAnswered(s, &fact)
	case ToolCallFailed:
		return guardToolCallFailed(s, &fact)
	case InputAccepted:
		return requireOpen(s, "input accepted")
	case RunEnded:
		return validateRunEnd(fact.End)
	default:
		return fmt.Errorf("agent: evolve: unknown fact variant %T", f)
	}
}

func requireOpen(s *MachineState, what string) error {
	if !atOpen(s.Current) {
		return fmt.Errorf("agent: evolve: %s while run is not at Open", what)
	}
	return nil
}

// requireModelStep checks that Current is ModelStep stepID in status.
func requireModelStep(s *MachineState, stepID StepID, status ModelStepStatus) error {
	ms, ok := s.Current.(ModelStep)
	if !ok || ms.RefValue.ID != stepID || ms.Status != status {
		return fmt.Errorf("agent: evolve: model step %q is not %s", stepID, status)
	}
	return nil
}

// requireCall returns the call when Current is ToolStep stepID and the call
// is in one of statuses.
func requireCall(s *MachineState, stepID StepID, callID CallID, statuses ...ToolCallStatus) (ToolCallState, error) {
	ts, ok := s.Current.(ToolStep)
	if !ok || ts.RefValue.ID != stepID {
		return ToolCallState{}, fmt.Errorf("agent: evolve: tool step %q is not current", stepID)
	}
	i := ts.callIndex(callID)
	if i < 0 {
		return ToolCallState{}, fmt.Errorf("agent: evolve: unknown call %q", callID)
	}
	call := ts.Calls[i]
	for _, want := range statuses {
		if call.Status == want {
			return call, nil
		}
	}
	return ToolCallState{}, fmt.Errorf("agent: evolve: tool call %q is %s", callID, call.Status)
}

func guardModelStepPrepared(s *MachineState, fact *ModelStepPrepared) error {
	if err := requireOpen(s, "model step prepared"); err != nil {
		return err
	}
	if fact.StepID == "" || fact.Model == "" || fact.RequestDigest == "" || fact.ToolsDigest == "" || fact.BindingDigest == "" {
		return errors.New("agent: evolve: model step prepared is missing identity or digest")
	}
	if ModelRef(fact.Request.Model) != fact.Model {
		return errors.New("agent: evolve: model step prepared model mismatch")
	}
	// v1 preparation is the atomic consumption boundary for pending inputs.
	// A persisted fact must name every pending input exactly once, in queue
	// order; accepting a subset or an invented ID would make replay diverge
	// from the command that created this frozen request.
	if len(fact.InputIDs) != len(s.PendingInputs) {
		return fmt.Errorf("agent: evolve: model step prepared input IDs do not completely consume pending inputs: got %d, want %d", len(fact.InputIDs), len(s.PendingInputs))
	}
	for i, input := range s.PendingInputs {
		if fact.InputIDs[i] != input.ID {
			return fmt.Errorf("agent: evolve: model step prepared input ID at position %d = %q, want pending input %q", i, fact.InputIDs[i], input.ID)
		}
	}
	if d, err := digestRequestV1(fact.Request); err != nil || d != fact.RequestDigest {
		return errors.New("agent: evolve: model step prepared request digest mismatch")
	}
	if d, err := digestToolSpecsV1(fact.Tools); err != nil || d != fact.ToolsDigest {
		return errors.New("agent: evolve: model step prepared tools digest mismatch")
	}
	if d, err := digestModelStepBindingV1(fact.Model, fact.RequestDigest, fact.ToolsDigest); err != nil || d != fact.BindingDigest {
		return errors.New("agent: evolve: model step prepared binding digest mismatch")
	}
	return nil
}

func guardToolStepOpened(s *MachineState, fact *ToolStepOpened) error {
	if err := requireOpen(s, "tool step opened"); err != nil {
		return err
	}
	if len(fact.Calls) == 0 {
		return errors.New("agent: evolve: tool step opened with no calls")
	}
	if fact.StepID == "" || fact.Source == "" || fact.BindingSetDigest == "" {
		return errors.New("agent: evolve: tool step is missing identity or digest")
	}
	if _, err := normalizeToolScheduling(fact.Scheduling); err != nil {
		return fmt.Errorf("agent: evolve: tool step scheduling: %w", err)
	}
	// BindingSetDigest is defined over the ordered call bindings before
	// derived response requests are attached. Recompute that exact input.
	base := make([]ToolCallBinding, len(fact.Calls))
	for i := range fact.Calls {
		base[i] = fact.Calls[i]
		base[i].Response = nil
	}
	if d, err := digestBindingSet(base); err != nil || d != fact.BindingSetDigest || DeriveToolStepID(fact.Source, fact.BindingSetDigest) != fact.StepID {
		return errors.New("agent: evolve: tool step binding digest mismatch")
	}
	seen := make(map[CallID]struct{}, len(fact.Calls))
	for i := range fact.Calls {
		if err := guardToolCallBinding(s.RunID, fact.StepID, &fact.Calls[i], seen); err != nil {
			return err
		}
	}
	return nil
}

func guardToolCallBinding(runID RunID, stepID StepID, call *ToolCallBinding, seen map[CallID]struct{}) error {
	if call.CallID == "" {
		return errors.New("agent: evolve: tool step contains empty CallID")
	}
	if _, dup := seen[call.CallID]; dup {
		return fmt.Errorf("agent: evolve: duplicate CallID %q", call.CallID)
	}
	seen[call.CallID] = struct{}{}
	if call.ToolRef == "" || call.BindingDigest == "" || call.Arguments.IsZero() {
		return fmt.Errorf("agent: evolve: tool call %q is missing binding data", call.CallID)
	}
	want, err := DigestToolCallBinding(call.CallID, call.DefinitionDigest, call.Policy, call.Arguments)
	if err != nil || want != call.BindingDigest {
		return fmt.Errorf("agent: evolve: tool call %q binding digest mismatch", call.CallID)
	}
	if call.Response == nil {
		return nil
	}
	kind, ok := responseKindForPolicy(call.Policy)
	if !ok {
		return fmt.Errorf("agent: evolve: direct call %q cannot carry a response request", call.CallID)
	}
	return validateResponseRequest(call.Response, runID, stepID, call.CallID, kind, call.Arguments, want)
}

func guardToolCallApproved(s *MachineState, fact *ToolCallApproved) error {
	call, err := requireCall(s, fact.StepID, fact.CallID, ToolWaiting)
	if err != nil {
		return err
	}
	if err := requireWaitingFor(&call, ResponseApproval, fact.ResponseID); err != nil {
		return err
	}
	if d, err := digestToolResponseDecisionV1(ResponseApproval, ResponseDecisionApproved, ""); err != nil || d != fact.ResponseDigest {
		return fmt.Errorf("agent: evolve: tool call %q approval digest mismatch", fact.CallID)
	}
	return nil
}

func guardToolCallAnswered(s *MachineState, fact *ToolCallAnswered) error {
	call, err := requireCall(s, fact.StepID, fact.CallID, ToolWaiting)
	if err != nil {
		return err
	}
	if err := requireWaitingFor(&call, ResponseExternal, fact.ResponseID); err != nil {
		return err
	}
	if d, err := digestToolResponsePayloadV1(fact.Payload); err != nil || d != fact.ResponseDigest {
		return fmt.Errorf("agent: evolve: tool call %q response digest mismatch", fact.CallID)
	}
	return nil
}

func requireWaitingFor(call *ToolCallState, kind ResponseKind, responseID ResponseID) error {
	if call.Waiting == nil || call.Waiting.Kind != kind {
		return fmt.Errorf("agent: evolve: tool call %q is not waiting for %s", call.CallID, kind)
	}
	if call.Waiting.ID != responseID {
		return fmt.Errorf("agent: evolve: tool call %q response ID mismatch", call.CallID)
	}
	return nil
}

func guardToolCallFailed(s *MachineState, fact *ToolCallFailed) error {
	call, err := requireCall(s, fact.StepID, fact.CallID, ToolPending, ToolExecuting, ToolWaiting)
	if err != nil {
		return err
	}
	if fact.Outcome == ToolOutcomeUnknown && call.Status != ToolExecuting {
		return fmt.Errorf("agent: evolve: unknown outcome requires Executing call, %q is %s", fact.CallID, call.Status)
	}
	// Class/outcome agreement is the rule ValidateToolCallState applies to
	// the folded state; check it here so the error names the fact.
	return ValidateToolCallState(ToolCallState{CallID: fact.CallID, Status: ToolFailed,
		Failure: &ToolCallFailure{Failure: fact.Failure, Outcome: fact.Outcome}})
}

func responseKindForPolicy(p ResponsePolicy) (ResponseKind, bool) {
	switch p {
	case ApprovalRequired:
		return ResponseApproval, true
	case ExternalResponse:
		return ResponseExternal, true
	default:
		return "", false
	}
}

func validateResponseRequest(req *ResponseRequest, runID RunID, stepID StepID, callID CallID, kind ResponseKind, payload CanonicalJSON, requestDigest Digest) error {
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
