package agent

import (
	"errors"
	"fmt"
)

// Machine errors. EvaluateCommit maps rejection reasons onto these; Decide
// returns them directly when a command's preconditions fail against the
// current state.
var (
	ErrCommandConflict = errors.New("agent: command identity conflict")
	ErrStaleRuntime    = errors.New("agent: stale runtime revision or grant")
	ErrRunTerminal     = errors.New("agent: run is terminal")
)

// rejectionf wraps a precondition failure that is not one of the sentinel
// errors; EvaluateCommit surfaces it as-is.
func rejectionf(format string, args ...any) error {
	return fmt.Errorf("agent: reject: "+format, args...)
}

// Decide validates one command against the current state and produces the
// complete fact sequence of its transition (spec §3.7.1). All decisions —
// acceptance, derived consequences, terminal transitions — happen here, run
// exactly once per accepted command; the output is frozen. Any precondition
// failure rejects the whole command with no partial facts.
func Decide(s MachineState, c AgentCommand) ([]Fact, error) {
	if s.Status.Terminal() {
		return nil, ErrRunTerminal
	}
	switch cmd := c.(type) {
	case PrepareModelRequest:
		return decidePrepareModelRequest(s, cmd)
	case StartModelExecution:
		return decideStartModelExecution(s, cmd)
	case RecoverModelExecution:
		return decideRecoverModelExecution(s, cmd)
	case SubmitModelResult:
		return decideSubmitModelResult(s, cmd)
	case SubmitModelFailure:
		return decideSubmitModelFailure(s, cmd)
	case RejectModelResult:
		return decideRejectModelResult(s, cmd)
	case StartToolCall:
		return decideStartToolCall(s, cmd)
	case SubmitToolResult:
		return decideSubmitToolResult(s, cmd)
	case SubmitToolFailure:
		return decideSubmitToolFailure(s, cmd)
	case ApproveToolCall:
		return decideApproveToolCall(s, cmd)
	case RejectToolCall:
		return decideRejectToolCall(s, cmd)
	case SubmitToolResponse:
		return decideSubmitToolResponse(s, cmd)
	case CancelRun:
		return decideCancelRun(s, cmd)
	case AcceptInput:
		return decideAcceptInput(s, cmd)
	default:
		return nil, rejectionf("unknown command variant %T", c)
	}
}

// --- rule 1: PrepareModelRequest ---

func decidePrepareModelRequest(s MachineState, cmd PrepareModelRequest) ([]Fact, error) {
	if s.Current != nil {
		return nil, rejectionf("prepare: run already has a current step")
	}
	if cmd.StepID == "" {
		return nil, rejectionf("prepare: empty StepID")
	}
	if cmd.Model != s.Config.Model {
		return nil, rejectionf("prepare: model %q does not match frozen RunConfig model %q", cmd.Model, s.Config.Model)
	}
	if s.Config.ModelStepLimit > 0 && s.ModelSteps >= s.Config.ModelStepLimit {
		// Unreachable when limits convert to terminal at the prior boundary,
		// but the authority still refuses rather than over-running.
		return nil, rejectionf("prepare: model-step limit reached")
	}
	// InputIDs must match PendingInputs completely and in current order.
	if len(cmd.InputIDs) != len(s.PendingInputs) {
		return nil, rejectionf("prepare: InputIDs must consume all %d pending inputs, got %d", len(s.PendingInputs), len(cmd.InputIDs))
	}
	for i, id := range cmd.InputIDs {
		if s.PendingInputs[i].ID != id {
			return nil, rejectionf("prepare: InputIDs[%d]=%q does not match pending input %q", i, id, s.PendingInputs[i].ID)
		}
	}
	// Tools must correspond one-to-one, in order, with the provider tool
	// definitions inside the frozen request.
	if len(cmd.Tools) != len(cmd.Request.Tools) {
		return nil, rejectionf("prepare: %d ToolSpecs for %d request tools", len(cmd.Tools), len(cmd.Request.Tools))
	}
	for i, spec := range cmd.Tools {
		if spec.Definition.Name != cmd.Request.Tools[i].Name {
			return nil, rejectionf("prepare: ToolSpec[%d] %q does not match request tool %q", i, spec.Definition.Name, cmd.Request.Tools[i].Name)
		}
		wantDigest, err := DigestToolDefinition(cmd.Request.Tools[i])
		if err != nil {
			return nil, err
		}
		if spec.DefinitionDigest != wantDigest {
			return nil, rejectionf("prepare: ToolSpec[%d] definition digest mismatch", i)
		}
	}
	wantReq, err := DigestRequest(cmd.Request)
	if err != nil {
		return nil, err
	}
	if cmd.RequestDigest != wantReq {
		return nil, rejectionf("prepare: request digest mismatch")
	}
	wantTools, err := DigestToolSpecs(cmd.Tools)
	if err != nil {
		return nil, err
	}
	if cmd.ToolsDigest != wantTools {
		return nil, rejectionf("prepare: tools digest mismatch")
	}
	return []Fact{ModelStepPrepared{
		StepID:        cmd.StepID,
		Model:         cmd.Model,
		Request:       cmd.Request,
		RequestDigest: cmd.RequestDigest,
		InputIDs:      cmd.InputIDs,
		Tools:         cmd.Tools,
		ToolsDigest:   cmd.ToolsDigest,
	}}, nil
}

// --- rule 2: StartModelExecution / RecoverModelExecution ---

func currentModelStep(s MachineState, step StepID) (*ModelStep, error) {
	ms, ok := s.Current.(ModelStep)
	if !ok {
		return nil, rejectionf("no current ModelStep")
	}
	if ms.RefValue.ID != step {
		return nil, rejectionf("step %q is not the current ModelStep %q", step, ms.RefValue.ID)
	}
	return &ms, nil
}

func decideStartModelExecution(s MachineState, cmd StartModelExecution) ([]Fact, error) {
	ms, err := currentModelStep(s, cmd.StepID)
	if err != nil {
		return nil, err
	}
	if ms.Status != ModelPrepared {
		return nil, rejectionf("start model: step is not Prepared")
	}
	return []Fact{ModelStepStarted{StepID: cmd.StepID}}, nil
}

func decideRecoverModelExecution(s MachineState, cmd RecoverModelExecution) ([]Fact, error) {
	ms, err := currentModelStep(s, cmd.StepID)
	if err != nil {
		return nil, err
	}
	if ms.Status != ModelExecuting {
		return nil, rejectionf("recover model: step is not Executing")
	}
	return []Fact{ModelStepRecovered{StepID: cmd.StepID}}, nil
}

// --- rule 3: SubmitModelResult ---

func decideSubmitModelResult(s MachineState, cmd SubmitModelResult) ([]Fact, error) {
	ms, err := currentModelStep(s, cmd.StepID)
	if err != nil {
		return nil, err
	}
	if ms.Status != ModelExecuting {
		return nil, rejectionf("model result: step is not Executing")
	}
	completed := ModelStepCompleted{StepID: cmd.StepID, Result: cmd.Result}

	if len(cmd.Calls) == 0 {
		return []Fact{completed, RunEnded{Status: RunCompleted}}, nil
	}

	// Validate bindings against the frozen ToolSpecs and the model result,
	// then freeze the full call set (Waiting requests included) here so both
	// runtimes derive identical ToolStepOpened facts.
	if len(cmd.Calls) != len(cmd.Result.ToolCalls) {
		return nil, rejectionf("model result: %d bindings for %d tool calls", len(cmd.Calls), len(cmd.Result.ToolCalls))
	}
	specByName := make(map[string]ToolSpec, len(ms.Tools))
	for _, spec := range ms.Tools {
		specByName[spec.Definition.Name] = spec
	}
	seen := make(map[CallID]bool, len(cmd.Calls))
	bindings := make([]ToolCallBinding, len(cmd.Calls))
	for i, b := range cmd.Calls {
		rc := cmd.Result.ToolCalls[i]
		if b.CallID == "" {
			return nil, rejectionf("model result: binding %d has empty CallID", i)
		}
		if string(b.CallID) != rc.ToolCallID {
			return nil, rejectionf("model result: binding %d CallID %q does not match result call %q", i, b.CallID, rc.ToolCallID)
		}
		if seen[b.CallID] {
			return nil, rejectionf("model result: duplicate CallID %q", b.CallID)
		}
		seen[b.CallID] = true

		if spec, known := specByName[string(b.ToolRef)]; known {
			if b.DefinitionDigest != spec.DefinitionDigest {
				return nil, rejectionf("model result: binding %q definition digest does not match frozen ToolSpec", b.CallID)
			}
			if b.Policy != spec.Policy {
				return nil, rejectionf("model result: binding %q policy does not match frozen ToolSpec", b.CallID)
			}
		} else {
			// Unknown ToolRef stays an unresolved DirectExecution binding with
			// an empty definition digest; StartToolCalls records the lookup
			// failure (spec §4.1).
			if b.Policy != DirectExecution || b.DefinitionDigest != "" {
				return nil, rejectionf("model result: unresolved binding %q must be DirectExecution with empty digest", b.CallID)
			}
		}
		wantBinding, err := digestToolCallBinding(b.CallID, b.DefinitionDigest, b.Policy, b.Arguments)
		if err != nil {
			return nil, err
		}
		if b.BindingDigest != wantBinding {
			return nil, rejectionf("model result: binding %q binding digest mismatch", b.CallID)
		}
		bindings[i] = b
		bindings[i].Response = nil // derived below; callers leave it empty
	}

	setDigest, err := digestBindingSet(bindings)
	if err != nil {
		return nil, err
	}
	toolStepID := DeriveToolStepID(cmd.StepID, setDigest)
	for i := range bindings {
		if bindings[i].Policy == ApprovalRequired || bindings[i].Policy == ExternalResponse {
			kind := ResponseApproval
			if bindings[i].Policy == ExternalResponse {
				kind = ResponseExternal
			}
			reqDigest, err := digestToolCallBinding(bindings[i].CallID, bindings[i].DefinitionDigest, bindings[i].Policy, bindings[i].Arguments)
			if err != nil {
				return nil, err
			}
			bindings[i].Response = &ResponseRequest{
				RunID:         s.RunID,
				StepID:        toolStepID,
				CallID:        bindings[i].CallID,
				ID:            DeriveResponseID(s.RunID, toolStepID, bindings[i].CallID, kind),
				Kind:          kind,
				Payload:       bindings[i].Arguments,
				RequestDigest: reqDigest,
			}
		}
	}
	return []Fact{completed, ToolStepOpened{
		StepID: toolStepID,
		Source: cmd.StepID,
		Calls:  bindings,
	}}, nil
}

// --- rule 4: SubmitModelFailure ---

func decideSubmitModelFailure(s MachineState, cmd SubmitModelFailure) ([]Fact, error) {
	ms, err := currentModelStep(s, cmd.StepID)
	if err != nil {
		return nil, err
	}
	if ms.Status != ModelExecuting {
		return nil, rejectionf("model failure: step is not Executing")
	}
	if cmd.Failure.Class == "" {
		return nil, rejectionf("model failure: empty failure class")
	}
	return []Fact{RunEnded{
		Status:  RunFailed,
		Reason:  ReasonProviderFailure,
		Failure: &RunFailure{Class: cmd.Failure.Class, Message: cmd.Failure.Message},
	}}, nil
}

// --- rule 5: RejectModelResult ---

func decideRejectModelResult(s MachineState, cmd RejectModelResult) ([]Fact, error) {
	ms, err := currentModelStep(s, cmd.StepID)
	if err != nil {
		return nil, err
	}
	if ms.Status != ModelExecuting {
		return nil, rejectionf("reject model result: step is not Executing")
	}
	rejected := ModelStepRejected{StepID: cmd.StepID, Usage: cmd.Usage, Failure: cmd.Failure}
	if ms.Rejects+1 > s.Config.ModelRejectLimit {
		return []Fact{rejected, RunEnded{
			Status:  RunFailed,
			Reason:  ReasonMalformedModel,
			Failure: &RunFailure{Class: FailureMalformedModel, Message: cmd.Failure.Message},
		}}, nil
	}
	return []Fact{rejected}, nil
}

// --- rules 6-8: tool call lifecycle ---

func currentToolStep(s MachineState, step StepID) (*ToolStep, error) {
	ts, ok := s.Current.(ToolStep)
	if !ok {
		return nil, rejectionf("no current ToolStep")
	}
	if ts.RefValue.ID != step {
		return nil, rejectionf("step %q is not the current ToolStep %q", step, ts.RefValue.ID)
	}
	return &ts, nil
}

func decideStartToolCall(s MachineState, cmd StartToolCall) ([]Fact, error) {
	ts, err := currentToolStep(s, cmd.StepID)
	if err != nil {
		return nil, err
	}
	i := ts.callIndex(cmd.CallID)
	if i < 0 {
		return nil, rejectionf("start tool: unknown call %q", cmd.CallID)
	}
	if ts.Calls[i].Status != ToolPending {
		return nil, rejectionf("start tool: call %q is not Pending", cmd.CallID)
	}
	return []Fact{ToolCallStarted{StepID: cmd.StepID, CallID: cmd.CallID}}, nil
}

func decideSubmitToolResult(s MachineState, cmd SubmitToolResult) ([]Fact, error) {
	ts, err := currentToolStep(s, cmd.StepID)
	if err != nil {
		return nil, err
	}
	i := ts.callIndex(cmd.CallID)
	if i < 0 {
		return nil, rejectionf("tool result: unknown call %q", cmd.CallID)
	}
	if ts.Calls[i].Status != ToolExecuting {
		return nil, rejectionf("tool result: call %q is not Executing", cmd.CallID)
	}
	facts := []Fact{ToolCallCompleted{StepID: cmd.StepID, CallID: cmd.CallID, Result: cmd.Result}}
	return appendCloseIfLast(s, *ts, i, facts)
}

func decideSubmitToolFailure(s MachineState, cmd SubmitToolFailure) ([]Fact, error) {
	ts, err := currentToolStep(s, cmd.StepID)
	if err != nil {
		return nil, err
	}
	i := ts.callIndex(cmd.CallID)
	if i < 0 {
		return nil, rejectionf("tool failure: unknown call %q", cmd.CallID)
	}
	call := ts.Calls[i]
	switch cmd.Outcome {
	case ToolOutcomeKnown:
		if call.Status != ToolPending && call.Status != ToolExecuting {
			return nil, rejectionf("tool failure: call %q is not Pending or Executing", cmd.CallID)
		}
		facts := []Fact{ToolCallFailed{StepID: cmd.StepID, CallID: cmd.CallID, Failure: cmd.Failure, Outcome: ToolOutcomeKnown}}
		return appendCloseIfLast(s, *ts, i, facts)
	case ToolOutcomeUnknown:
		if call.Status != ToolExecuting {
			return nil, rejectionf("tool failure: unknown outcome requires Executing call")
		}
		failure := cmd.Failure
		if failure.Class == "" {
			failure.Class = FailureEffectUnknown
		}
		if failure.Class != FailureEffectUnknown {
			return nil, rejectionf("tool failure: unknown outcome must use %s", FailureEffectUnknown)
		}
		return []Fact{
			ToolCallFailed{StepID: cmd.StepID, CallID: cmd.CallID, Failure: failure, Outcome: ToolOutcomeUnknown},
			RunEnded{
				Status:  RunFailed,
				Reason:  ReasonEffectUnknown,
				Failure: &RunFailure{Class: FailureEffectUnknown, Message: failure.Message, CallID: cmd.CallID},
			},
		}, nil
	default:
		return nil, rejectionf("tool failure: unknown outcome value %d", cmd.Outcome)
	}
}

// appendCloseIfLast appends ToolStepClosed when call i reaching a closable
// terminal state closes the step, plus RunEnded when the frozen model-step
// limit was reached (spec §3.7.1 rule 11).
func appendCloseIfLast(s MachineState, ts ToolStep, i int, facts []Fact) ([]Fact, error) {
	for j := range ts.Calls {
		if j == i {
			continue
		}
		switch ts.Calls[j].Status {
		case ToolCompleted, ToolFailed:
			// closable terminal
		default:
			return facts, nil
		}
	}
	facts = append(facts, ToolStepClosed{StepID: ts.RefValue.ID})
	if s.Config.ModelStepLimit > 0 && s.ModelSteps >= s.Config.ModelStepLimit {
		facts = append(facts, RunEnded{Status: RunStopped, Reason: ReasonStepLimit})
	}
	return facts, nil
}

// --- rules 9-10: responses ---

func waitingCall(s MachineState, step StepID, call CallID, kind ResponseKind, resp ResponseID) (*ToolCallState, error) {
	ts, err := currentToolStep(s, step)
	if err != nil {
		return nil, err
	}
	i := ts.callIndex(call)
	if i < 0 {
		return nil, rejectionf("response: unknown call %q", call)
	}
	c := ts.Calls[i]
	if c.Status != ToolWaiting || c.Waiting == nil {
		return nil, rejectionf("response: call %q is not Waiting", call)
	}
	if c.Waiting.Kind != kind {
		return nil, rejectionf("response: call %q expects kind %q, got %q", call, c.Waiting.Kind, kind)
	}
	if c.Waiting.ID != resp {
		return nil, rejectionf("response: call %q expects ResponseID %q, got %q", call, c.Waiting.ID, resp)
	}
	return &c, nil
}

func decideApproveToolCall(s MachineState, cmd ApproveToolCall) ([]Fact, error) {
	if _, err := waitingCall(s, cmd.StepID, cmd.CallID, ResponseApproval, cmd.ResponseID); err != nil {
		return nil, err
	}
	return []Fact{ToolCallApproved{StepID: cmd.StepID, CallID: cmd.CallID, ResponseID: cmd.ResponseID, ResponseDigest: cmd.ResponseDigest}}, nil
}

func decideRejectToolCall(s MachineState, cmd RejectToolCall) ([]Fact, error) {
	if _, err := waitingCall(s, cmd.StepID, cmd.CallID, ResponseApproval, cmd.ResponseID); err != nil {
		return nil, err
	}
	ts, _ := currentToolStep(s, cmd.StepID)
	i := ts.callIndex(cmd.CallID)
	facts := []Fact{ToolCallFailed{
		StepID:  cmd.StepID,
		CallID:  cmd.CallID,
		Failure: ToolFailure{Class: FailurePermissionDenied, Message: cmd.Reason},
		Outcome: ToolOutcomeKnown,
	}}
	return appendCloseIfLast(s, *ts, i, facts)
}

func decideSubmitToolResponse(s MachineState, cmd SubmitToolResponse) ([]Fact, error) {
	if _, err := waitingCall(s, cmd.StepID, cmd.CallID, ResponseExternal, cmd.ResponseID); err != nil {
		return nil, err
	}
	ts, _ := currentToolStep(s, cmd.StepID)
	i := ts.callIndex(cmd.CallID)
	facts := []Fact{ToolCallAnswered{
		StepID:         cmd.StepID,
		CallID:         cmd.CallID,
		ResponseID:     cmd.ResponseID,
		ResponseDigest: cmd.ResponseDigest,
		Payload:        cmd.Payload,
	}}
	return appendCloseIfLast(s, *ts, i, facts)
}

// --- rules 12-13: cancel and input ---

func decideCancelRun(s MachineState, cmd CancelRun) ([]Fact, error) {
	reason := cmd.Reason
	if reason == "" {
		reason = ReasonCancelled
	}
	return []Fact{RunEnded{Status: RunStopped, Reason: reason}}, nil
}

func decideAcceptInput(s MachineState, cmd AcceptInput) ([]Fact, error) {
	if s.Current != nil {
		return nil, rejectionf("accept input: run has a current step")
	}
	if cmd.Input.ID == "" {
		return nil, rejectionf("accept input: empty InputID")
	}
	return []Fact{InputAccepted{Input: cmd.Input}}, nil
}
