package agent

import (
	"encoding/json"

	"github.com/memohai/twilight-ai/sdk"
)

// Fact is one committed outcome produced by Machine.Decide. Facts are wrapped
// as AgentEvents; Machine.Evolve folds them mechanically (spec §3.6). The
// interface is sealed: only the fourteen variants below exist.
type Fact interface{ fact() }

// ModelStepPrepared establishes the frozen ModelStep and consumes the listed
// pending inputs.
type ModelStepPrepared struct {
	StepID        StepID        `json:"stepId"`
	Model         ModelRef      `json:"model"`
	Request       sdk.Request   `json:"request"`
	RequestDigest Digest        `json:"requestDigest"`
	InputIDs      []InputID     `json:"inputIds,omitempty"`
	Tools         []ToolSpec    `json:"tools,omitempty"`
	ToolsDigest   Digest        `json:"toolsDigest"`
}

func (ModelStepPrepared) fact() {}

// ModelStepStarted: Prepared -> Executing.
type ModelStepStarted struct {
	StepID StepID `json:"stepId"`
}

func (ModelStepStarted) fact() {}

// ModelStepRecovered: Executing -> Prepared, no accepted result.
type ModelStepRecovered struct {
	StepID StepID `json:"stepId"`
}

func (ModelStepRecovered) fact() {}

// ModelStepRejected records one structurally malformed result: usage is
// accumulated, Rejects is incremented, the step returns to Prepared.
type ModelStepRejected struct {
	StepID  StepID      `json:"stepId"`
	Usage   sdk.Usage   `json:"usage"`
	Failure StepFailure `json:"failure"`
}

func (ModelStepRejected) fact() {}

// ModelStepCompleted accepts one model result: usage is accumulated,
// LastModelResult is written, the current step is cleared.
type ModelStepCompleted struct {
	StepID StepID          `json:"stepId"`
	Result sdk.ModelResult `json:"result"`
}

func (ModelStepCompleted) fact() {}

// ToolStepOpened establishes the ToolStep with its full frozen call set.
// BindingSetDigest is computed by Decide over the ordered pre-Response
// binding set and carried in the fact: Evolve folds it verbatim, so replay
// never recomputes a digest with a different schema version, and
// DeriveToolStepID(Source, BindingSetDigest) == StepID always holds.
type ToolStepOpened struct {
	StepID           StepID            `json:"stepId"` // the new ToolStep
	Source           StepID            `json:"source"` // the completed ModelStep
	BindingSetDigest Digest            `json:"bindingSetDigest"`
	Calls            []ToolCallBinding `json:"calls"`
}

func (ToolStepOpened) fact() {}

// ToolCallStarted: Pending -> Executing.
type ToolCallStarted struct {
	StepID StepID `json:"stepId"`
	CallID CallID `json:"callId"`
}

func (ToolCallStarted) fact() {}

// ToolCallApproved: Waiting(Approval) -> Pending.
type ToolCallApproved struct {
	StepID         StepID     `json:"stepId"`
	CallID         CallID     `json:"callId"`
	ResponseID     ResponseID `json:"responseId"`
	ResponseDigest Digest     `json:"responseDigest"`
}

func (ToolCallApproved) fact() {}

// ToolCallCompleted: Executing -> Completed.
type ToolCallCompleted struct {
	StepID StepID              `json:"stepId"`
	CallID CallID              `json:"callId"`
	Result ToolExecutionResult `json:"result"`
}

func (ToolCallCompleted) fact() {}

// ToolCallAnswered: Waiting(ExternalResponse) -> Completed with the answer.
type ToolCallAnswered struct {
	StepID         StepID          `json:"stepId"`
	CallID         CallID          `json:"callId"`
	ResponseID     ResponseID      `json:"responseId"`
	ResponseDigest Digest          `json:"responseDigest"`
	Payload        json.RawMessage `json:"payload"`
}

func (ToolCallAnswered) fact() {}

// ToolCallFailed: Pending/Executing/Waiting -> Failed(Known/Unknown).
type ToolCallFailed struct {
	StepID  StepID             `json:"stepId"`
	CallID  CallID             `json:"callId"`
	Failure ToolFailure        `json:"failure"`
	Outcome ToolFailureOutcome `json:"outcome"`
}

func (ToolCallFailed) fact() {}

// ToolStepClosed: every call reached a closable terminal state; the current
// step is cleared.
type ToolStepClosed struct {
	StepID StepID `json:"stepId"`
}

func (ToolStepClosed) fact() {}

// InputAccepted appends one input to PendingInputs.
type InputAccepted struct {
	Input AgentInput `json:"input"`
}

func (InputAccepted) fact() {}

// RunEnded is the terminal fact. Always the last fact of its transition.
type RunEnded struct {
	Status  RunStatus  `json:"status"` // RunCompleted, RunStopped or RunFailed
	Reason  RunReason  `json:"reason,omitempty"`
	Failure *RunFailure `json:"failure,omitempty"`
}

func (RunEnded) fact() {}

// factType returns the wire discriminator for a sealed fact variant.
func factType(f Fact) string {
	switch f.(type) {
	case ModelStepPrepared:
		return "model_step_prepared"
	case ModelStepStarted:
		return "model_step_started"
	case ModelStepRecovered:
		return "model_step_recovered"
	case ModelStepRejected:
		return "model_step_rejected"
	case ModelStepCompleted:
		return "model_step_completed"
	case ToolStepOpened:
		return "tool_step_opened"
	case ToolCallStarted:
		return "tool_call_started"
	case ToolCallApproved:
		return "tool_call_approved"
	case ToolCallCompleted:
		return "tool_call_completed"
	case ToolCallAnswered:
		return "tool_call_answered"
	case ToolCallFailed:
		return "tool_call_failed"
	case ToolStepClosed:
		return "tool_step_closed"
	case InputAccepted:
		return "input_accepted"
	case RunEnded:
		return "run_ended"
	default:
		return ""
	}
}
