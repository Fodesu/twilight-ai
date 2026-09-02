package run

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Fact is one committed outcome produced by Machine.Decide. Facts are wrapped
// as AgentEvents; Machine.Evolve folds them mechanically (RUN-MCH-3). The
// interface is sealed: only the thirteen variants below exist.
type Fact interface{ fact() }

// ModelStepPrepared establishes the frozen ModelStep and consumes the listed
// pending inputs. BindingDigest (model + request + tools) is computed by
// Decide and carried in the fact: Evolve folds it verbatim, never recomputes
// (fact self-containment, RUN-MCH-3).
type ModelStepPrepared struct {
	StepID        StepID       `json:"stepId"`
	Model         ModelRef     `json:"model"`
	Request       ModelRequest `json:"request"`
	RequestDigest Digest       `json:"requestDigest"`
	InputIDs      []InputID    `json:"inputIds,omitempty"`
	Tools         []ToolSpec   `json:"tools,omitempty"`
	ToolsDigest   Digest       `json:"toolsDigest"`
	BindingDigest Digest       `json:"bindingDigest"`
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
	Usage   Usage       `json:"usage"`
	Failure StepFailure `json:"failure"`
}

func (ModelStepRejected) fact() {}

// ModelStepCompleted accepts one model result: usage is accumulated,
// LastModelResult is written, Current becomes Open. The same transition may
// then open a ToolStep or end the Run.
type ModelStepCompleted struct {
	StepID StepID      `json:"stepId"`
	Result ModelResult `json:"result"`
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
	Scheduling       ToolScheduling    `json:"scheduling,omitzero"`
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
	StepID         StepID        `json:"stepId"`
	CallID         CallID        `json:"callId"`
	ResponseID     ResponseID    `json:"responseId"`
	ResponseDigest Digest        `json:"responseDigest"`
	Payload        CanonicalJSON `json:"payload"`
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

// InputAccepted appends one input to PendingInputs.
type InputAccepted struct {
	Input AgentInput `json:"input"`
}

func (InputAccepted) fact() {}

// RunEnd is the closed set of terminal outcomes.
type RunEnd interface{ runEnd() }

type RunCompletedEnd struct{}
type RunStoppedEnd struct {
	Reason         RunReason
	UncertainCalls []CallID `json:"uncertainCalls,omitempty"`
	UncertainModel StepID   `json:"uncertainModel,omitempty"`
}
type RunFailedEnd struct {
	Reason  RunReason
	Failure RunFailure
}

func (RunCompletedEnd) runEnd() {}
func (RunStoppedEnd) runEnd()   {}
func (RunFailedEnd) runEnd()    {}

// RunEnded is the terminal fact. Always the last fact of its transition.
type RunEnded struct {
	End RunEnd `json:"-"`
}

func (RunEnded) fact() {}

func validateRunEnd(end RunEnd) error {
	switch e := end.(type) {
	case RunCompletedEnd:
		return nil
	case RunStoppedEnd:
		if e.Reason == "" {
			return errors.New("agent: run ended: stopped outcome requires a reason")
		}
		return nil
	case RunFailedEnd:
		if e.Reason == "" {
			return errors.New("agent: run ended: failed outcome requires a reason")
		}
		if e.Failure.Class == "" {
			return errors.New("agent: run ended: failed outcome requires a failure class")
		}
		return nil
	default:
		return fmt.Errorf("agent: run ended: unknown end variant %T", end)
	}
}

func endProjection(end RunEnd) (RunStatus, RunReason, *RunFailure) {
	switch e := end.(type) {
	case RunCompletedEnd:
		return RunCompleted, "", nil
	case RunStoppedEnd:
		return RunStopped, e.Reason, nil
	case RunFailedEnd:
		failure := e.Failure
		return RunFailed, e.Reason, &failure
	default:
		return RunActive, "", nil
	}
}

// runEndWire is the tagged-union wire of RunEnded: exactly one variant key is
// present. It mirrors the Go union so the wire cannot express an outcome the
// type system rejects.
type runEndWire struct {
	Completed *struct{}          `json:"completed,omitempty"`
	Stopped   *runStoppedEndWire `json:"stopped,omitempty"`
	Failed    *runFailedEndWire  `json:"failed,omitempty"`
}

type runStoppedEndWire struct {
	Reason         RunReason `json:"reason"`
	UncertainCalls []CallID  `json:"uncertainCalls,omitempty"`
	UncertainModel StepID    `json:"uncertainModel,omitempty"`
}

type runFailedEndWire struct {
	Reason  RunReason  `json:"reason"`
	Failure RunFailure `json:"failure"`
}

func (r RunEnded) MarshalJSON() ([]byte, error) {
	if err := validateRunEnd(r.End); err != nil {
		return nil, err
	}
	var w runEndWire
	switch e := r.End.(type) {
	case RunCompletedEnd:
		w.Completed = &struct{}{}
	case RunStoppedEnd:
		w.Stopped = &runStoppedEndWire{Reason: e.Reason, UncertainCalls: e.UncertainCalls, UncertainModel: e.UncertainModel}
	case RunFailedEnd:
		w.Failed = &runFailedEndWire{Reason: e.Reason, Failure: e.Failure}
	}
	return json.Marshal(w)
}

func (r *RunEnded) UnmarshalJSON(raw []byte) error {
	var w runEndWire
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&w); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return errors.New("agent: run ended: trailing JSON")
		}
		return err
	}
	variants := 0
	var end RunEnd
	if w.Completed != nil {
		variants++
		end = RunCompletedEnd{}
	}
	if w.Stopped != nil {
		variants++
		end = RunStoppedEnd{Reason: w.Stopped.Reason, UncertainCalls: w.Stopped.UncertainCalls, UncertainModel: w.Stopped.UncertainModel}
	}
	if w.Failed != nil {
		variants++
		end = RunFailedEnd{Reason: w.Failed.Reason, Failure: w.Failed.Failure}
	}
	if variants != 1 {
		return fmt.Errorf("agent: run ended: exactly one outcome required, got %d", variants)
	}
	if err := validateRunEnd(end); err != nil {
		return err
	}
	*r = RunEnded{End: end}
	return nil
}

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
	case InputAccepted:
		return "input_accepted"
	case RunEnded:
		return "run_ended"
	default:
		return ""
	}
}
