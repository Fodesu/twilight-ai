package run

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

//nolint:gocritic // hugeParam: v1 Decide is value-based.
func decideV1(s MachineState, c AgentCommand) ([]Fact, error) {
	if s.Status.Terminal() {
		return nil, ErrRunTerminal
	}
	switch cmd := c.(type) {
	case PrepareModelRequest:
		return decidePrepareModelRequest(&s, &cmd)
	case StartModelExecution:
		return decideStartModelExecution(&s, cmd)
	case RecoverModelExecution:
		return decideRecoverModelExecution(&s, cmd)
	case SubmitModelResult:
		return decideSubmitModelResult(&s, &cmd)
	case SubmitModelFailure:
		return decideSubmitModelFailure(&s, cmd)
	case RejectModelResult:
		return decideRejectModelResult(&s, &cmd)
	case StartToolCall:
		return decideStartToolCall(&s, cmd)
	case SubmitToolResult:
		return decideSubmitToolResult(&s, cmd)
	case SubmitToolFailure:
		return decideSubmitToolFailure(&s, cmd)
	case ApproveToolCall:
		return decideApproveToolCall(&s, cmd)
	case RejectToolCall:
		return decideRejectToolCall(&s, &cmd)
	case SubmitToolResponse:
		return decideSubmitToolResponse(&s, &cmd)
	case CancelRun:
		return decideCancelRun(&s, cmd)
	case AcceptInput:
		return decideAcceptInput(&s, cmd)
	default:
		return nil, rejectionf("unknown command variant %T", c)
	}
}

// --- rule 1: PrepareModelRequest ---

func decidePrepareModelRequest(s *MachineState, cmd *PrepareModelRequest) ([]Fact, error) {
	if !atOpen(s.Current) {
		return nil, rejectionf("prepare: run is not at Open")
	}
	if cmd.StepID == "" {
		return nil, rejectionf("prepare: empty StepID")
	}
	if cmd.Model == "" {
		return nil, rejectionf("prepare: empty model")
	}
	if ModelRef(cmd.Request.Model) != cmd.Model {
		return nil, rejectionf("prepare: request model %q does not match command model %q", cmd.Request.Model, cmd.Model)
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
		wantDigest, err := digestToolDefinitionV1(cmd.Request.Tools[i])
		if err != nil {
			return nil, err
		}
		if spec.DefinitionDigest != wantDigest {
			return nil, rejectionf("prepare: ToolSpec[%d] definition digest mismatch", i)
		}
	}
	wantReq, err := digestRequestV1(cmd.Request)
	if err != nil {
		return nil, err
	}
	if cmd.RequestDigest != wantReq {
		return nil, rejectionf("prepare: request digest mismatch")
	}
	wantTools, err := digestToolSpecsV1(cmd.Tools)
	if err != nil {
		return nil, err
	}
	if cmd.ToolsDigest != wantTools {
		return nil, rejectionf("prepare: tools digest mismatch")
	}
	binding, err := digestModelStepBindingV1(cmd.Model, cmd.RequestDigest, cmd.ToolsDigest)
	if err != nil {
		return nil, err
	}
	return []Fact{ModelStepPrepared{
		StepID:        cmd.StepID,
		Model:         cmd.Model,
		Request:       cmd.Request,
		RequestDigest: cmd.RequestDigest,
		InputIDs:      cmd.InputIDs,
		Tools:         cmd.Tools,
		ToolsDigest:   cmd.ToolsDigest,
		BindingDigest: binding,
	}}, nil
}

// --- rule 2: StartModelExecution / RecoverModelExecution ---

func currentModelStep(s *MachineState, step StepID) (*ModelStep, error) {
	ms, ok := s.Current.(ModelStep)
	if !ok {
		return nil, rejectionf("no current ModelStep")
	}
	if ms.RefValue.ID != step {
		return nil, rejectionf("step %q is not the current ModelStep %q", step, ms.RefValue.ID)
	}
	return &ms, nil
}

func decideStartModelExecution(s *MachineState, cmd StartModelExecution) ([]Fact, error) {
	ms, err := currentModelStep(s, cmd.StepID)
	if err != nil {
		return nil, err
	}
	if ms.Status != ModelPrepared {
		return nil, rejectionf("start model: step is not Prepared")
	}
	return []Fact{ModelStepStarted{StepID: cmd.StepID}}, nil
}

func decideRecoverModelExecution(s *MachineState, cmd RecoverModelExecution) ([]Fact, error) {
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

func decideSubmitModelResult(s *MachineState, cmd *SubmitModelResult) ([]Fact, error) {
	ms, err := currentModelStep(s, cmd.StepID)
	if err != nil {
		return nil, err
	}
	if ms.Status != ModelExecuting {
		return nil, rejectionf("model result: step is not Executing")
	}
	completed := ModelStepCompleted{StepID: cmd.StepID, Result: cmd.Result}

	// The result's own tool calls decide whether a ToolStep opens; gating on
	// the caller-supplied bindings would let zero bindings silently complete
	// a run whose model asked for tools.
	if len(cmd.Result.ToolCalls) == 0 {
		if len(cmd.Calls) != 0 {
			return nil, rejectionf("model result: %d bindings for a result with no tool calls", len(cmd.Calls))
		}
		return []Fact{completed, RunEnded{End: RunCompletedEnd{}}}, nil
	}
	bindings, err := checkToolCallBindings(ms, cmd)
	if err != nil {
		return nil, err
	}
	opened, err := openToolStep(s.RunID, cmd.StepID, bindings, cmd.Scheduling)
	if err != nil {
		return nil, err
	}
	return []Fact{completed, opened}, nil
}

// checkToolCallBindings validates the caller's bindings one-to-one against
// the model result and the frozen ToolSpecs (RUN-MCH-2) and returns them with
// Response cleared, ready for openToolStep to derive.
func checkToolCallBindings(ms *ModelStep, cmd *SubmitModelResult) ([]ToolCallBinding, error) {
	if len(cmd.Calls) != len(cmd.Result.ToolCalls) {
		return nil, rejectionf("model result: %d bindings for %d tool calls", len(cmd.Calls), len(cmd.Result.ToolCalls))
	}
	specByName := make(map[string]ToolSpec, len(ms.Tools))
	for _, spec := range ms.Tools {
		specByName[spec.Definition.Name] = spec
	}
	seen := make(map[CallID]bool, len(cmd.Calls))
	bindings := make([]ToolCallBinding, len(cmd.Calls))
	for i := range cmd.Calls {
		b := cmd.Calls[i]
		rc := &cmd.Result.ToolCalls[i]
		if want := DeriveCallID(cmd.StepID, i); b.CallID != want {
			return nil, rejectionf("model result: binding %d CallID %q is not the derived id %q", i, b.CallID, want)
		}
		if b.ProviderCallID != rc.ToolCallID {
			return nil, rejectionf("model result: binding %d ProviderCallID %q does not match result call %q", i, b.ProviderCallID, rc.ToolCallID)
		}
		if seen[b.CallID] {
			return nil, rejectionf("model result: duplicate CallID %q", b.CallID)
		}
		seen[b.CallID] = true
		if err := checkBindingAgainstResult(&b, rc, specByName); err != nil {
			return nil, err
		}
		b.Response = nil // derived by openToolStep; callers leave it empty
		bindings[i] = b
	}
	return bindings, nil
}

// checkBindingAgainstResult accepts only a binding for the tool the model
// actually named, with the arguments the model actually produced. A known
// tool must match its frozen ToolSpec; an unknown one stays an unresolved
// DirectExecution binding that StartToolCalls records as a lookup failure.
func checkBindingAgainstResult(b *ToolCallBinding, rc *ModelToolCall, specByName map[string]ToolSpec) error {
	if spec, known := specByName[rc.ToolName]; known {
		if b.ToolRef != spec.Ref {
			return rejectionf("model result: binding %q ToolRef %q does not match frozen spec ref %q for tool %q", b.CallID, b.ToolRef, spec.Ref, rc.ToolName)
		}
		if b.DefinitionDigest != spec.DefinitionDigest {
			return rejectionf("model result: binding %q definition digest does not match frozen ToolSpec", b.CallID)
		}
		if b.Policy != spec.Policy {
			return rejectionf("model result: binding %q policy does not match frozen ToolSpec", b.CallID)
		}
	} else {
		if string(b.ToolRef) != rc.ToolName {
			return rejectionf("model result: binding %q ToolRef %q does not match result tool %q", b.CallID, b.ToolRef, rc.ToolName)
		}
		if b.Policy != DirectExecution || b.DefinitionDigest != "" {
			return rejectionf("model result: unresolved binding %q must be DirectExecution with empty digest", b.CallID)
		}
	}
	wantArgs, argsCanonical := canonicalArgumentsForCompare(rc.Input)
	if !argsCanonical {
		return rejectionf("model result: call %q input is not frozen canonical JSON", b.CallID)
	}
	if !b.Arguments.Equal(wantArgs) {
		return rejectionf("model result: binding %q arguments do not match the model result", b.CallID)
	}
	wantBinding, err := digestToolCallBinding(b.CallID, b.DefinitionDigest, b.Policy, b.Arguments)
	if err != nil {
		return err
	}
	if b.BindingDigest != wantBinding {
		return rejectionf("model result: binding %q binding digest mismatch", b.CallID)
	}
	return nil
}

// openToolStep derives the ToolStep identity from the ordered binding set,
// attaches a ResponseRequest to every call whose policy waits, and freezes
// the scheduling (RUN-LOP-1).
func openToolStep(runID RunID, source StepID, bindings []ToolCallBinding, scheduling ToolScheduling) (ToolStepOpened, error) {
	setDigest, err := digestBindingSet(bindings)
	if err != nil {
		return ToolStepOpened{}, err
	}
	toolStepID := DeriveToolStepID(source, setDigest)
	for i := range bindings {
		kind, waits := responseKindForPolicy(bindings[i].Policy)
		if !waits {
			continue
		}
		reqDigest, err := digestToolCallBinding(bindings[i].CallID, bindings[i].DefinitionDigest, bindings[i].Policy, bindings[i].Arguments)
		if err != nil {
			return ToolStepOpened{}, err
		}
		bindings[i].Response = &ResponseRequest{
			RunID:         runID,
			StepID:        toolStepID,
			CallID:        bindings[i].CallID,
			ID:            DeriveResponseID(runID, toolStepID, bindings[i].CallID, kind),
			Kind:          kind,
			Payload:       bindings[i].Arguments,
			RequestDigest: reqDigest,
		}
	}
	normalized, err := normalizeToolScheduling(scheduling)
	if err != nil {
		return ToolStepOpened{}, rejectionf("model result: %v", err)
	}
	return ToolStepOpened{
		StepID:           toolStepID,
		Source:           source,
		BindingSetDigest: setDigest,
		Calls:            bindings,
		Scheduling:       normalized,
	}, nil
}

// canonicalArgumentsForCompare canonicalizes a model result's tool input for
// cross-checking a binding. The second return is false when the command did
// not carry a frozen JSON-stable tool input; Runtime commits reject that shape.
func canonicalArgumentsForCompare(input any) (CanonicalJSON, bool) {
	got, err := canonicalToolArguments(input)
	if err != nil {
		return CanonicalJSON{}, false
	}
	return got, true
}

// --- rule 4: SubmitModelFailure ---

func decideSubmitModelFailure(s *MachineState, cmd SubmitModelFailure) ([]Fact, error) {
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
	return []Fact{RunEnded{End: RunFailedEnd{
		Reason:  ReasonProviderFailure,
		Failure: RunFailure{Class: cmd.Failure.Class, Message: cmd.Failure.Message},
	}}}, nil
}

// --- rule 5: RejectModelResult ---

func decideRejectModelResult(s *MachineState, cmd *RejectModelResult) ([]Fact, error) {
	ms, err := currentModelStep(s, cmd.StepID)
	if err != nil {
		return nil, err
	}
	if ms.Status != ModelExecuting {
		return nil, rejectionf("reject model result: step is not Executing")
	}
	rejected := ModelStepRejected{StepID: cmd.StepID, Usage: cmd.Usage, Failure: cmd.Failure}
	switch cmd.Disposition {
	case ModelRejectRetry:
		return []Fact{rejected}, nil
	case ModelRejectFailRun:
		return []Fact{rejected, RunEnded{End: RunFailedEnd{
			Reason:  ReasonMalformedModel,
			Failure: RunFailure{Class: FailureMalformedModel, Message: cmd.Failure.Message},
		}}}, nil
	default:
		return nil, rejectionf("reject model result: unknown disposition %d", cmd.Disposition)
	}
}

// --- rules 6-8: tool call lifecycle ---

func currentToolStep(s *MachineState, step StepID) (*ToolStep, error) {
	ts, ok := s.Current.(ToolStep)
	if !ok {
		return nil, rejectionf("no current ToolStep")
	}
	if ts.RefValue.ID != step {
		return nil, rejectionf("step %q is not the current ToolStep %q", step, ts.RefValue.ID)
	}
	return &ts, nil
}

func decideStartToolCall(s *MachineState, cmd StartToolCall) ([]Fact, error) {
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

func decideSubmitToolResult(s *MachineState, cmd SubmitToolResult) ([]Fact, error) {
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
	facts := []Fact{ToolCallCompleted(cmd)}
	return facts, nil
}

func decideSubmitToolFailure(s *MachineState, cmd SubmitToolFailure) ([]Fact, error) {
	ts, err := currentToolStep(s, cmd.StepID)
	if err != nil {
		return nil, err
	}
	i := ts.callIndex(cmd.CallID)
	if i < 0 {
		return nil, rejectionf("tool failure: unknown call %q", cmd.CallID)
	}
	call := ts.Calls[i]
	if cmd.Failure.Class == "" {
		if cmd.Outcome == ToolOutcomeUnknown {
			cmd.Failure.Class = FailureEffectUnknown
		} else {
			return nil, rejectionf("tool failure: empty failure class")
		}
	}
	switch cmd.Outcome {
	case ToolOutcomeKnown:
		if call.Status != ToolPending && call.Status != ToolExecuting {
			return nil, rejectionf("tool failure: call %q is not Pending or Executing", cmd.CallID)
		}
		facts := []Fact{ToolCallFailed{StepID: cmd.StepID, CallID: cmd.CallID, Failure: cmd.Failure, Outcome: ToolOutcomeKnown}}
		return facts, nil
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
		return []Fact{ToolCallFailed{StepID: cmd.StepID, CallID: cmd.CallID, Failure: failure, Outcome: ToolOutcomeUnknown}}, nil
	default:
		return nil, rejectionf("tool failure: unknown outcome value %d", cmd.Outcome)
	}
}

// --- rules 9-10: responses ---

// waitingCall checks that call is Waiting on the current ToolStep for the
// given response kind and ID.
func waitingCall(s *MachineState, step StepID, call CallID, kind ResponseKind, resp ResponseID) error {
	ts, err := currentToolStep(s, step)
	if err != nil {
		return err
	}
	i := ts.callIndex(call)
	if i < 0 {
		return rejectionf("response: unknown call %q", call)
	}
	c := ts.Calls[i]
	if c.Status != ToolWaiting || c.Waiting == nil {
		return rejectionf("response: call %q is not Waiting", call)
	}
	if c.Waiting.Kind != kind {
		return rejectionf("response: call %q expects kind %q, got %q", call, c.Waiting.Kind, kind)
	}
	if c.Waiting.ID != resp {
		return rejectionf("response: call %q expects ResponseID %q, got %q", call, c.Waiting.ID, resp)
	}
	return nil
}

func decideApproveToolCall(s *MachineState, cmd ApproveToolCall) ([]Fact, error) {
	if err := waitingCall(s, cmd.StepID, cmd.CallID, ResponseApproval, cmd.ResponseID); err != nil {
		return nil, err
	}
	wantDigest, err := digestToolResponseDecisionV1(ResponseApproval, ResponseDecisionApproved, "")
	if err != nil {
		return nil, err
	}
	if cmd.ResponseDigest != wantDigest {
		return nil, rejectionf("response: approval digest mismatch")
	}
	return []Fact{ToolCallApproved(cmd)}, nil
}

func decideRejectToolCall(s *MachineState, cmd *RejectToolCall) ([]Fact, error) {
	// Reject closes a Waiting call of either kind as a Known failure:
	// approval rejection and external-response abandonment ("the answer is
	// never coming") share one exit. Waiting -> Failed(Known) is legal;
	// without this, an abandoned ask-user call would strand the run with
	// CancelRun as the only escape.
	ts, err := currentToolStep(s, cmd.StepID)
	if err != nil {
		return nil, err
	}
	i := ts.callIndex(cmd.CallID)
	if i < 0 {
		return nil, rejectionf("response: unknown call %q", cmd.CallID)
	}
	c := ts.Calls[i]
	if c.Status != ToolWaiting || c.Waiting == nil {
		return nil, rejectionf("response: call %q is not Waiting", cmd.CallID)
	}
	if c.Waiting.ID != cmd.ResponseID {
		return nil, rejectionf("response: call %q expects ResponseID %q, got %q", cmd.CallID, c.Waiting.ID, cmd.ResponseID)
	}
	wantDigest, err := digestToolResponseDecisionV1(c.Waiting.Kind, ResponseDecisionRejected, cmd.Reason)
	if err != nil {
		return nil, err
	}
	if cmd.ResponseDigest != wantDigest {
		return nil, rejectionf("response: rejection digest mismatch")
	}
	class := FailurePermissionDenied
	if c.Waiting.Kind == ResponseExternal {
		class = FailureResponseRejected
	}
	facts := []Fact{ToolCallFailed{
		StepID:  cmd.StepID,
		CallID:  cmd.CallID,
		Failure: ToolFailure{Class: class, Message: cmd.Reason},
		Outcome: ToolOutcomeKnown,
	}}
	return facts, nil
}

func decideSubmitToolResponse(s *MachineState, cmd *SubmitToolResponse) ([]Fact, error) {
	if err := waitingCall(s, cmd.StepID, cmd.CallID, ResponseExternal, cmd.ResponseID); err != nil {
		return nil, err
	}
	wantDigest, err := digestToolResponsePayloadV1(cmd.Payload)
	if err != nil {
		return nil, err
	}
	if cmd.ResponseDigest != wantDigest {
		return nil, rejectionf("response: answer payload digest mismatch")
	}
	facts := []Fact{ToolCallAnswered(*cmd)}
	return facts, nil
}

// --- rules 12-13: cancel and input ---

func decideCancelRun(s *MachineState, cmd CancelRun) ([]Fact, error) {
	// CancelRun always records RunStopped(cancelled).
	if cmd.Reason != "" && cmd.Reason != ReasonCancelled {
		return nil, rejectionf("cancel: reason must be empty or %q", ReasonCancelled)
	}
	facts := unknownExecutingCalls(s, ToolFailure{Class: FailureEffectUnknown, Message: "execution cancelled before settlement"})
	uncertain := make([]CallID, 0, len(facts))
	for _, f := range facts {
		if failed, ok := f.(ToolCallFailed); ok {
			uncertain = append(uncertain, failed.CallID)
		}
	}
	var uncertainModel StepID
	if ms, ok := s.Current.(ModelStep); ok && ms.Status == ModelExecuting {
		uncertainModel = ms.RefValue.ID
	}
	facts = append(facts, RunEnded{End: RunStoppedEnd{
		Reason:         ReasonCancelled,
		UncertainCalls: uncertain,
		UncertainModel: uncertainModel,
	}})
	return facts, nil
}

// unknownExecutingCalls records every Executing call that CancelRun is about
// to abandon. Waiting and already-settled calls are left unchanged.
func unknownExecutingCalls(s *MachineState, failure ToolFailure) []Fact {
	ts, ok := s.Current.(ToolStep)
	if !ok {
		return nil
	}
	facts := make([]Fact, 0, len(ts.Calls))
	for i := range ts.Calls {
		if ts.Calls[i].Status != ToolExecuting {
			continue
		}
		facts = append(facts, ToolCallFailed{StepID: ts.RefValue.ID, CallID: ts.Calls[i].CallID, Failure: failure, Outcome: ToolOutcomeUnknown})
	}
	return facts
}

func decideAcceptInput(s *MachineState, cmd AcceptInput) ([]Fact, error) {
	if !atOpen(s.Current) {
		return nil, rejectionf("accept input: run is not at Open")
	}
	if cmd.Input.ID == "" {
		return nil, rejectionf("accept input: empty InputID")
	}
	for _, in := range s.PendingInputs {
		if in.ID == cmd.Input.ID {
			return nil, ErrCommandConflict
		}
	}
	return []Fact{InputAccepted(cmd)}, nil
}
