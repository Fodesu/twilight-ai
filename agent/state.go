package agent

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
	ReasonStepLimit       RunReason = "step_limit"
	ReasonProviderFailure RunReason = "provider_failure"
	ReasonMalformedModel  RunReason = "malformed_model_result"
	ReasonEffectUnknown   RunReason = "effect_unknown"
)

type RunFailure struct {
	Class   string `json:"class"`
	Message string `json:"message,omitempty"`
	CallID  CallID `json:"callId,omitempty"`
}

type RunResult struct {
	Status  RunStatus    `json:"status"`
	Reason  RunReason    `json:"reason,omitempty"`
	Failure *RunFailure  `json:"failure,omitempty"`
	Model   *ModelResult `json:"model,omitempty"`
	Usage   Usage        `json:"usage"`
}

// RunConfig is frozen at Run creation.
type RunConfig struct {
	Model ModelRef `json:"model"`
	// ModelStepLimit: zero means unlimited; a positive value caps ModelSteps.
	// Negative values are invalid.
	ModelStepLimit int `json:"modelStepLimit,omitempty"`
	// ModelRejectLimit: max accepted RejectModelResult per ModelStep before
	// the Run fails. Zero is normalized to DefaultModelRejectLimit by
	// Initialize; negative values are invalid. There is no unlimited value.
	ModelRejectLimit int `json:"modelRejectLimit,omitempty"`
}

// DefaultModelRejectLimit is the Initialize-time normalization of
// RunConfig.ModelRejectLimit == 0.
const DefaultModelRejectLimit = 2

type StepFailure struct {
	Class   string `json:"class"`
	Message string `json:"message,omitempty"`
}

const (
	FailurePermissionDenied   = "permission_denied"
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

type ModelStepStatus uint8

const (
	ModelPrepared ModelStepStatus = iota
	ModelExecuting
)

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

func (ModelStep) step()          {}
func (s ModelStep) Ref() StepRef { return s.RefValue }

type ToolCallStatus uint8

const (
	ToolPending ToolCallStatus = iota
	ToolExecuting
	ToolWaiting
	ToolCompleted
	ToolFailed
)

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

// ValidateToolCallState rejects illegal field combinations (spec §4.2).
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
		if c.Failure.Outcome == ToolOutcomeUnknown && c.Failure.Failure.Class != FailureEffectUnknown {
			return fmt.Errorf("agent: call %s: unknown outcome must use %s", c.CallID, FailureEffectUnknown)
		}
	default:
		return fmt.Errorf("agent: call %s: unknown status %d", c.CallID, c.Status)
	}
	return nil
}

type ToolStep struct {
	RefValue StepRef         `json:"ref"`
	Source   StepID          `json:"source"`
	Calls    []ToolCallState `json:"calls"`
}

func (ToolStep) step()          {}
func (s ToolStep) Ref() StepRef { return s.RefValue }

func (s ToolStep) callIndex(id CallID) int {
	for i := range s.Calls {
		if s.Calls[i].CallID == id {
			return i
		}
	}
	return -1
}

// MachineState is the complete semantic state of one Run (spec §3.3).
// Control metadata (owner, fence, lease, attempts, queue claims) never
// appears here.
type MachineState struct {
	RunID         RunID        `json:"runId"`
	Status        RunStatus    `json:"status"`
	Config        RunConfig    `json:"config"`
	Current       Step         `json:"-"` // serialized by adapters with their snapshot schema
	PendingInputs []AgentInput `json:"pendingInputs,omitempty"`
	ModelSteps    int          `json:"modelSteps"`
	// LastClosedStep is the most recently closed ToolStep; PlanningHint's
	// SourceStep is read from it at the next boundary.
	LastClosedStep  StepID       `json:"lastClosedStep,omitempty"`
	Usage           Usage        `json:"usage"`
	LastModelResult *ModelResult `json:"lastModelResult,omitempty"`
	Result          *RunResult   `json:"result,omitempty"`
}

// Initialize builds the initial MachineState (Revision 0) for a new Run from
// an admission seed (spec §3.7.1 rule 14). RunSeed never goes through
// Runtime.Commit.
func Initialize(run RunID, cfg RunConfig, seed RunSeed) (MachineState, error) {
	if run == "" {
		return MachineState{}, errors.New("agent: initialize: empty RunID")
	}
	if cfg.Model == "" {
		return MachineState{}, errors.New("agent: initialize: empty RunConfig.Model")
	}
	if cfg.ModelStepLimit < 0 {
		return MachineState{}, errors.New("agent: initialize: negative ModelStepLimit")
	}
	if cfg.ModelRejectLimit < 0 {
		return MachineState{}, errors.New("agent: initialize: negative ModelRejectLimit")
	}
	if cfg.ModelRejectLimit == 0 {
		cfg.ModelRejectLimit = DefaultModelRejectLimit
	}
	if seed.Input.ID == "" {
		return MachineState{}, errors.New("agent: initialize: seed input requires an InputID")
	}
	input, err := snapshotJSONStable(seed.Input)
	if err != nil {
		return MachineState{}, err
	}
	return MachineState{
		RunID:         run,
		Status:        RunActive,
		Config:        cfg,
		PendingInputs: []AgentInput{input},
	}, nil
}
