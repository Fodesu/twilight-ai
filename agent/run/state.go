package run

import (
	"errors"
	"fmt"
)

type RunStatus uint8

const (
	RunActive RunStatus = iota
	RunCompleted
	RunStopped
	RunFailed
)

func (s RunStatus) Terminal() bool { return s != RunActive }

type RunReason string

const (
	ReasonCancelled       RunReason = "cancelled"
	ReasonProviderFailure RunReason = "provider_failure"
	ReasonMalformedModel  RunReason = "malformed_model_result"
	// ReasonEffectUnknown is unused as a RunEnded reason. Unknown tool
	// outcomes use FailureEffectUnknown on ToolCallFailed and leave the Run
	// active.
	ReasonEffectUnknown RunReason = "effect_unknown"
)

type RunFailure struct {
	Class   string `json:"class"`
	Message string `json:"message,omitempty"`
	CallID  CallID `json:"callId,omitempty"`
}

type RunResult struct {
	Status  RunStatus   `json:"status"`
	Reason  RunReason   `json:"reason,omitempty"`
	Failure *RunFailure `json:"failure,omitempty"`
	// UncertainCalls are tool calls left Executing when the Run stopped.
	UncertainCalls []CallID `json:"uncertainCalls,omitempty"`
	// UncertainModel is the ModelStep left Executing when the Run stopped.
	UncertainModel StepID       `json:"uncertainModel,omitempty"`
	Model          *ModelResult `json:"model,omitempty"`
	Usage          Usage        `json:"usage"`
}

type StepFailure struct {
	Class   string `json:"class"`
	Message string `json:"message,omitempty"`
}

const (
	FailurePermissionDenied   = "permission_denied"
	FailureResponseRejected   = "response_rejected"
	FailureToolLookup         = "tool_lookup_failed"
	FailureInvalidArguments   = "invalid_arguments"
	FailureMalformedModel     = "malformed_model_result"
	FailureDefinitionMismatch = "tool_definition_mismatch"
	FailureExecution          = "execution_failed"
	FailureEffectUnknown      = "effect_unknown"
	FailureProvider           = "provider_failure"
)

type ResponsePolicy uint8

const (
	DirectExecution ResponsePolicy = iota
	ApprovalRequired
	ExternalResponse
)

type ResponseKind string

const (
	ResponseApproval ResponseKind = "approval"
	ResponseExternal ResponseKind = "external_response"
)

type ResponseDecision string

const (
	ResponseDecisionApproved ResponseDecision = "approved"
	ResponseDecisionRejected ResponseDecision = "rejected"
)

// ResponseRequest is the stable, routable identity of one waiting call.
type ResponseRequest struct {
	RunID         RunID         `json:"runId"`
	StepID        StepID        `json:"stepId"`
	CallID        CallID        `json:"callId"`
	ID            ResponseID    `json:"id"`
	Kind          ResponseKind  `json:"kind"`
	Payload       CanonicalJSON `json:"payload,omitzero"`
	RequestDigest Digest        `json:"requestDigest"` // digest of the request payload
}

// ToolSpec is the agent-side sidecar for a provider-neutral ToolDefinition.
// ResponsePolicy is intentionally kept out of sdk to preserve package layering.
type ToolSpec struct {
	Ref              ToolRef        `json:"ref"`
	Definition       ToolDefinition `json:"definition"`
	DefinitionDigest Digest         `json:"definitionDigest"`
	Policy           ResponsePolicy `json:"policy"`
}

// ToolCallBinding is one frozen call inside ToolStepOpened.
type ToolCallBinding struct {
	CallID           CallID         `json:"callId"`
	ToolRef          ToolRef        `json:"toolRef"`
	DefinitionDigest Digest         `json:"definitionDigest"`
	BindingDigest    Digest         `json:"bindingDigest"` // definition, policy and canonical arguments
	Arguments        CanonicalJSON  `json:"arguments"`
	Policy           ResponsePolicy `json:"policy"` // unresolved ToolRef uses DirectExecution
	// Response is derived and filled by Decide inside ToolStepOpened; callers
	// leave it empty when submitting.
	Response *ResponseRequest `json:"response,omitempty"`
}

type StepRef struct {
	RunID  RunID  `json:"runId"`
	ID     StepID `json:"id"`
	Digest Digest `json:"digest"` // immutable step binding digest; progress is not included
}

// Step is sealed by the agent package: only ModelStep and ToolStep exist.
type Step interface {
	step()
	Ref() StepRef
}

// Current is the contents of an Active run. Open is the enterable interval:
// AcceptInput and Prepare are legal, and Next returns NeedModelRequest.
type Current interface{ current() }

// Open is Active with no ModelStep or ToolStep.
type Open struct{}

func (Open) current() {}

func atOpen(c Current) bool {
	_, ok := c.(Open)
	return ok
}

type ModelStepStatus uint8

const (
	ModelPrepared ModelStepStatus = iota
	ModelExecuting
)

func (s ModelStepStatus) String() string {
	switch s {
	case ModelPrepared:
		return "Prepared"
	case ModelExecuting:
		return "Executing"
	default:
		return fmt.Sprintf("ModelStepStatus(%d)", uint8(s))
	}
}

type ModelStep struct {
	RefValue      StepRef         `json:"ref"`
	Request       ModelRequest    `json:"request"`
	RequestDigest Digest          `json:"requestDigest"`
	Model         ModelRef        `json:"model"`
	Tools         []ToolSpec      `json:"tools,omitempty"`
	ToolsDigest   Digest          `json:"toolsDigest"`
	Status        ModelStepStatus `json:"status"`
	// Rejects counts accepted ModelStepRejected facts; progress, not part of
	// RefValue.Digest.
	Rejects int `json:"rejects,omitempty"`
}

func (ModelStep) step()    {}
func (ModelStep) current() {}

//nolint:gocritic // hugeParam: value receiver keeps ModelStep satisfying sealed Step as a value.
func (s ModelStep) Ref() StepRef { return s.RefValue }

type ToolCallStatus uint8

const (
	ToolPending ToolCallStatus = iota
	ToolExecuting
	ToolWaiting
	ToolCompleted
	ToolFailed
)

func (s ToolCallStatus) String() string {
	switch s {
	case ToolPending:
		return "Pending"
	case ToolExecuting:
		return "Executing"
	case ToolWaiting:
		return "Waiting"
	case ToolCompleted:
		return "Completed"
	case ToolFailed:
		return "Failed"
	default:
		return fmt.Sprintf("ToolCallStatus(%d)", uint8(s))
	}
}

// Terminal reports Completed or Failed.
func (s ToolCallStatus) Terminal() bool { return s == ToolCompleted || s == ToolFailed }

type ToolExecutionResult struct {
	Output CanonicalJSON `json:"output"`
}

type ToolFailure struct {
	Class   string `json:"class"`
	Message string `json:"message,omitempty"`
}

type ToolFailureOutcome uint8

const (
	ToolOutcomeKnown ToolFailureOutcome = iota
	ToolOutcomeUnknown
)

type ToolCallFailure struct {
	Failure ToolFailure        `json:"failure"`
	Outcome ToolFailureOutcome `json:"outcome"`
}

type ToolCallState struct {
	CallID           CallID               `json:"callId"`
	ToolRef          ToolRef              `json:"toolRef"`
	DefinitionDigest Digest               `json:"definitionDigest"`
	BindingDigest    Digest               `json:"bindingDigest"`
	Arguments        CanonicalJSON        `json:"arguments"`
	Policy           ResponsePolicy       `json:"policy"`
	Status           ToolCallStatus       `json:"status"`
	Result           *ToolExecutionResult `json:"result,omitempty"`
	Failure          *ToolCallFailure     `json:"failure,omitempty"`
	Waiting          *ResponseRequest     `json:"waiting,omitempty"`
}

// ValidateToolCallState rejects illegal field combinations (RUN-MCH-2).
//
//nolint:gocritic // hugeParam: public validator accepts the value stored in facts/state without mutating it.
func ValidateToolCallState(c ToolCallState) error {
	switch c.Status {
	case ToolPending, ToolExecuting:
		if c.Result != nil || c.Failure != nil || c.Waiting != nil {
			return fmt.Errorf("agent: call %s: pending/executing must have no result/failure/waiting", c.CallID)
		}
	case ToolWaiting:
		if c.Waiting == nil {
			return fmt.Errorf("agent: call %s: waiting requires a ResponseRequest", c.CallID)
		}
		if c.Policy != ApprovalRequired && c.Policy != ExternalResponse {
			return fmt.Errorf("agent: call %s: waiting requires approval or external-response policy", c.CallID)
		}
		if c.Result != nil || c.Failure != nil {
			return fmt.Errorf("agent: call %s: waiting must have no result/failure", c.CallID)
		}
	case ToolCompleted:
		if c.Result == nil {
			return fmt.Errorf("agent: call %s: completed requires a result", c.CallID)
		}
		if c.Failure != nil || c.Waiting != nil {
			return fmt.Errorf("agent: call %s: completed must have no failure/waiting", c.CallID)
		}
	case ToolFailed:
		if c.Failure == nil {
			return fmt.Errorf("agent: call %s: failed requires a failure", c.CallID)
		}
		if c.Result != nil || c.Waiting != nil {
			return fmt.Errorf("agent: call %s: failed must have no result/waiting", c.CallID)
		}
		if c.Failure.Failure.Class == "" {
			return fmt.Errorf("agent: call %s: failed requires a failure class", c.CallID)
		}
		switch c.Failure.Outcome {
		case ToolOutcomeKnown:
			if c.Failure.Failure.Class == FailureEffectUnknown {
				return fmt.Errorf("agent: call %s: known outcome cannot use %s", c.CallID, FailureEffectUnknown)
			}
		case ToolOutcomeUnknown:
			if c.Failure.Failure.Class != FailureEffectUnknown {
				return fmt.Errorf("agent: call %s: unknown outcome must use %s", c.CallID, FailureEffectUnknown)
			}
		default:
			return fmt.Errorf("agent: call %s: unknown failure outcome %d", c.CallID, c.Failure.Outcome)
		}
	default:
		return fmt.Errorf("agent: call %s: unknown status %d", c.CallID, c.Status)
	}
	return nil
}

// ToolScheduleMode is frozen onto a ToolStep at open. Resume must honor it.
type ToolScheduleMode string

const (
	ToolScheduleParallel   ToolScheduleMode = "parallel"
	ToolScheduleSequential ToolScheduleMode = "sequential"
)

// ToolScheduling is the durable dispatch constraint for one ToolStep.
// Empty Mode means parallel. MaxParallel 0 means every Pending call in the
// current Start batch may run; a positive value caps that batch.
type ToolScheduling struct {
	Mode        ToolScheduleMode `json:"mode,omitempty"`
	MaxParallel int              `json:"maxParallel,omitempty"`
}

func normalizeToolScheduling(s ToolScheduling) (ToolScheduling, error) {
	if s.Mode != "" && s.Mode != ToolScheduleParallel && s.Mode != ToolScheduleSequential {
		return ToolScheduling{}, fmt.Errorf("unknown mode %q", s.Mode)
	}
	if s.MaxParallel < 0 {
		return ToolScheduling{}, errors.New("negative MaxParallel")
	}
	return s, nil
}

type ToolStep struct {
	RefValue   StepRef         `json:"ref"`
	Source     StepID          `json:"source"`
	Calls      []ToolCallState `json:"calls"`
	Scheduling ToolScheduling  `json:"scheduling,omitzero"`
}

func (ToolStep) step()    {}
func (ToolStep) current() {}

//nolint:gocritic // hugeParam: value receiver keeps ToolStep satisfying sealed Step as a value.
func (s ToolStep) Ref() StepRef { return s.RefValue }

func (s *ToolStep) callIndex(id CallID) int {
	for i := range s.Calls {
		if s.Calls[i].CallID == id {
			return i
		}
	}
	return -1
}

// MachineState is the complete semantic state of one Run (RUN-MCH-1).
// Control metadata (owner, fence, lease, attempts, queue claims) never
// appears here.
type MachineState struct {
	RunID         RunID        `json:"runId"`
	Status        RunStatus    `json:"status"`
	Current       Current      `json:"-"`
	PendingInputs []AgentInput `json:"pendingInputs,omitempty"`
	ModelSteps    int          `json:"modelSteps"`
	// LastToolStep retains the most recently closed ToolStep so the planner can
	// include committed tool results in the next model request. Its RefValue.ID
	// is the SourceStep of the next PlanningHint.
	LastToolStep    *ToolStep    `json:"lastToolStep,omitempty"`
	Usage           Usage        `json:"usage"`
	LastModelResult *ModelResult `json:"lastModelResult,omitempty"`
	Result          *RunResult   `json:"result,omitempty"`
}

// ValidateMachineState checks the structural invariants required by Runtime
// snapshots. It does not inspect transition history; Record and Rebuild use
// FoldRun for that stronger verification.
func ValidateMachineState(s *MachineState) error {
	if s == nil {
		return errors.New("agent: state: nil state")
	}
	if s.RunID == "" {
		return errors.New("agent: state: empty RunID")
	}
	switch s.Status {
	case RunActive, RunCompleted, RunStopped, RunFailed:
	default:
		return fmt.Errorf("agent: state: unknown RunStatus %d", s.Status)
	}
	if s.ModelSteps < 0 {
		return errors.New("agent: state: negative model step count")
	}
	if err := validatePendingInputs(s.PendingInputs); err != nil {
		return err
	}
	if err := validateLastToolStep(s); err != nil {
		return err
	}
	if s.Status.Terminal() {
		if s.Current != nil {
			return errors.New("agent: state: terminal state has a current step")
		}
		if s.Result == nil || s.Result.Status != s.Status {
			return errors.New("agent: state: terminal state has no matching result")
		}
		return nil
	}
	if s.Result != nil {
		return errors.New("agent: state: active state has a result")
	}
	return validateCurrent(s)
}

func validatePendingInputs(inputs []AgentInput) error {
	seen := make(map[InputID]struct{}, len(inputs))
	for _, input := range inputs {
		if input.ID == "" {
			return errors.New("agent: state: pending input has empty InputID")
		}
		if _, dup := seen[input.ID]; dup {
			return fmt.Errorf("agent: state: duplicate pending InputID %q", input.ID)
		}
		seen[input.ID] = struct{}{}
	}
	return nil
}

func validateLastToolStep(s *MachineState) error {
	last := s.LastToolStep
	if last == nil {
		return nil
	}
	if last.RefValue.RunID != s.RunID || last.RefValue.ID == "" || last.RefValue.Digest == "" || last.Source == "" {
		return errors.New("agent: state: invalid LastToolStep projection")
	}
	if len(last.Calls) == 0 {
		return errors.New("agent: state: LastToolStep has no calls")
	}
	for i := range last.Calls {
		if err := ValidateToolCallState(last.Calls[i]); err != nil {
			return err
		}
		if !last.Calls[i].Status.Terminal() {
			return errors.New("agent: state: LastToolStep contains a live call")
		}
	}
	return nil
}

func validateCurrent(s *MachineState) error {
	switch current := s.Current.(type) {
	case Open:
		return nil
	case nil:
		return errors.New("agent: state: active state has no current")
	case ModelStep:
		if current.RefValue.RunID != s.RunID || current.RefValue.ID == "" || current.RefValue.Digest == "" || current.Model == "" {
			return errors.New("agent: state: invalid current ModelStep identity")
		}
		if current.Status != ModelPrepared && current.Status != ModelExecuting {
			return fmt.Errorf("agent: state: unknown ModelStep status %d", current.Status)
		}
		return nil
	case ToolStep:
		return validateCurrentToolStep(s.RunID, &current)
	default:
		return fmt.Errorf("agent: state: unknown current step %T", s.Current)
	}
}

func validateCurrentToolStep(runID RunID, ts *ToolStep) error {
	if ts.RefValue.RunID != runID || ts.RefValue.ID == "" || ts.RefValue.Digest == "" || ts.Source == "" || len(ts.Calls) == 0 {
		return errors.New("agent: state: invalid current ToolStep identity")
	}
	seen := make(map[CallID]struct{}, len(ts.Calls))
	live := false
	for i := range ts.Calls {
		call := &ts.Calls[i]
		if call.CallID == "" {
			return errors.New("agent: state: current ToolStep has empty CallID")
		}
		if _, dup := seen[call.CallID]; dup {
			return fmt.Errorf("agent: state: duplicate CallID %q", call.CallID)
		}
		seen[call.CallID] = struct{}{}
		if err := ValidateToolCallState(*call); err != nil {
			return err
		}
		if !call.Status.Terminal() {
			live = true
		}
	}
	if !live {
		return errors.New("agent: state: current ToolStep has no live calls")
	}
	return nil
}

// InitializeRun builds the minimal initial MachineState (Revision 0) for a
// new Run. It does not encode fixed-model policy, limits, or seed input; those
// belong to host policy and accepted transitions.
func InitializeRun(run RunID) (MachineState, error) {
	if run == "" {
		return MachineState{}, errors.New("agent: initialize: empty RunID")
	}
	return MachineState{RunID: run, Status: RunActive, Current: Open{}}, nil
}
