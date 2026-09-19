package run

import (
	"github.com/felinics/twilight/agent/run/model"
)

// AgentCommand is the intent submitted through Runtime.Commit for an existing
// Run. Accepting one command constitutes one transition (RUN-MCH-3). The
// interface is sealed: only the variants below exist. Commands may carry
// transient content bodies (frozen request, model result, tool output); the
// facts they produce keep only digests (RUN-WIR-4).
type AgentCommand interface{ agentCommand() }

// AgentInput is a queue-safe input: a stable ID plus an immutable payload.
// Queue item references, priority, order, claims and leases stay in the host.
// AgentInput is one input as the Run knows it: its identity and the digest of
// its content. The content itself lives with the module that owns inputs
// (the chatlog's input_submitted); the Run only records that this input
// entered this logical run (RUN-WIR-4), so an input has one body in the
// ledger and editing or forking it never touches a second copy.
type AgentInput struct {
	ID     InputID `json:"id"`
	Digest Digest  `json:"digest"`
}

// NextStep creates the AcceptInput command for one or more inputs.
func NextStep(inputs ...AgentInput) AcceptInput { return AcceptInput{Inputs: inputs} }

// PrepareModelRequest freezes the next model request. Its CommandID is
// derived from the loaded Revision, which is also its concurrency control.
// Request is the transient body; the fact keeps RequestDigest and the Runtime
// stores the body in the frozen.Store.
type PrepareModelRequest struct {
	StepID        StepID             `json:"stepId"`
	Model         ModelRef           `json:"model"`
	Request       model.ModelRequest `json:"request"`
	RequestDigest Digest             `json:"requestDigest"`
	InputIDs      []InputID          `json:"inputIds,omitempty"`
	PromptToken   PromptToken        `json:"promptToken,omitempty"`
	Tools         []ToolSpec         `json:"tools,omitempty"`
	ToolsDigest   Digest             `json:"toolsDigest"`
}

func (PrepareModelRequest) agentCommand() {}

// WithdrawPreparedStep discards a Prepared ModelStep whose frozen request
// predates inputs that have since been accepted; the Run returns to Open so
// the next Prepare includes them. Legal only while PendingInputs is non-empty.
type WithdrawPreparedStep struct {
	StepID StepID `json:"stepId"`
}

func (WithdrawPreparedStep) agentCommand() {}

// StartModelExecution takes execution ownership of a Prepared ModelStep.
type StartModelExecution struct {
	StepID StepID `json:"stepId"`
	// Effect is the model effect this start requests: the step's next model
	// effect as Identity.DeriveEffectID derives it. It is part of the command
	// digest and the preimage of the command's identity, so a transport retry
	// must retain it.
	Effect EffectID `json:"effect"`
}

func (StartModelExecution) agentCommand() {}

// RecoverModelExecution withdraws an Executing ModelStep whose model effect
// is lost: no provider result was accepted and none can be reached. The Run
// returns to Open and the next Prepare plans from the current state (the
// frozen request is a transfer copy for the Executor, not a replay target).
// Sources: the Loop when a dispatched effect ends without a result, or the
// recovery disposition of a new owner (RUN-CMT-7).
type RecoverModelExecution struct {
	StepID StepID `json:"stepId"`
	// Effect is the model effect being recovered: the one the step's
	// ModelStepStarted recorded. A recovery naming any other effect is stale.
	Effect EffectID `json:"effect"`
}

func (RecoverModelExecution) agentCommand() {}

// SubmitModelResult submits one complete model result with its tool-call
// bindings. Requires the model start grant.
type SubmitModelResult struct {
	StepID     StepID            `json:"stepId"`
	Result     model.ModelResult `json:"result"`
	Calls      []ToolCallBinding `json:"calls,omitempty"`
	Scheduling ToolScheduling    `json:"scheduling,omitzero"`
}

func (SubmitModelResult) agentCommand() {}

// SubmitModelFailure submits the final failure of one model call. Requires
// the model start grant.
type SubmitModelFailure struct {
	StepID  StepID      `json:"stepId"`
	Failure StepFailure `json:"failure"`
}

func (SubmitModelFailure) agentCommand() {}

type ModelRejectDisposition uint8

const (
	// ModelRejectRetry records the malformed result and returns the same frozen
	// ModelStep to Prepared for another execution attempt.
	ModelRejectRetry ModelRejectDisposition = iota
	// ModelRejectFailRun records the malformed result and fails the Run in the
	// same transition.
	ModelRejectFailRun
)

// RejectModelResult records a structurally malformed model result: usage is
// accumulated, the step's reject counter is incremented, and Disposition
// decides whether the same frozen request retries or the Run fails. Requires
// the model start grant.
type RejectModelResult struct {
	StepID      StepID                 `json:"stepId"`
	Usage       model.Usage            `json:"usage"`
	Failure     StepFailure            `json:"failure"`
	Disposition ModelRejectDisposition `json:"disposition,omitempty"`
}

func (RejectModelResult) agentCommand() {}

// StartToolCall takes execution ownership of one Pending tool call.
type StartToolCall struct {
	StepID StepID `json:"stepId"`
	CallID CallID `json:"callId"`
	// Effect is the tool effect this start requests (see StartModelExecution).
	Effect EffectID `json:"effect"`
}

func (StartToolCall) agentCommand() {}

// SubmitToolResult submits one successful tool execution. Requires that
// call's start grant.
type SubmitToolResult struct {
	StepID StepID              `json:"stepId"`
	CallID CallID              `json:"callId"`
	Result ToolExecutionResult `json:"result"`
}

func (SubmitToolResult) agentCommand() {}

// SubmitToolFailure submits a known or unknown tool failure. A known failure
// on a Pending call uses an empty grant; a failure on an Executing call
// requires that call's grant. Unknown outcome records ToolCallFailed for
// that Executing call and leaves the Run active.
type SubmitToolFailure struct {
	StepID  StepID             `json:"stepId"`
	CallID  CallID             `json:"callId"`
	Failure ToolFailure        `json:"failure"`
	Outcome ToolFailureOutcome `json:"outcome"`
}

func (SubmitToolFailure) agentCommand() {}

// ApproveToolCall approves a Waiting(Approval) call. ResponseDigest must be
// DigestToolResponseDecision(ResponseApproval, ResponseDecisionApproved, "").
type ApproveToolCall struct {
	StepID         StepID     `json:"stepId"`
	CallID         CallID     `json:"callId"`
	ResponseID     ResponseID `json:"responseId"`
	ResponseDigest Digest     `json:"responseDigest"`
}

func (ApproveToolCall) agentCommand() {}

// RejectToolCall rejects a Waiting(Approval or ExternalResponse) call.
// Approval rejection is ToolCallFailed{Known, permission_denied}.
// ExternalResponse rejection is ToolCallFailed{Known, response_rejected}.
// ResponseDigest must be DigestToolResponseDecision(waiting kind,
// ResponseDecisionRejected, Reason).
type RejectToolCall struct {
	StepID         StepID     `json:"stepId"`
	CallID         CallID     `json:"callId"`
	ResponseID     ResponseID `json:"responseId"`
	ResponseDigest Digest     `json:"responseDigest"`
	Reason         string     `json:"reason,omitempty"`
}

func (RejectToolCall) agentCommand() {}

// SubmitToolResponse completes a Waiting(ExternalResponse) call with the
// external answer. ResponseDigest must be DigestToolResponsePayload(Payload).
type SubmitToolResponse struct {
	StepID         StepID        `json:"stepId"`
	CallID         CallID        `json:"callId"`
	ResponseID     ResponseID    `json:"responseId"`
	ResponseDigest Digest        `json:"responseDigest"`
	Payload        CanonicalJSON `json:"payload"`
}

func (SubmitToolResponse) agentCommand() {}

// CancelRun stops a non-terminal Run as a business cancellation. Hosts must
// commit this before cancelling the Loop's context (RUN-LOP-5).
type CancelRun struct {
	Reason RunReason `json:"reason,omitempty"`
}

func (CancelRun) agentCommand() {}

// AcceptInput appends an ordered, non-empty list of inputs to PendingInputs as
// one command: every input is accepted or none is. Legal in every non-terminal
// state (Open, ModelStep, ToolStep, Waiting); the inputs are consumed by the
// next Prepare. Its CommandID derives from the ordered InputIDs
// (DeriveInputCommandID), so the same batch replays idempotently.
type AcceptInput struct {
	Inputs []AgentInput `json:"inputs"`
}

func (AcceptInput) agentCommand() {}

// InputIDs returns the ordered InputIDs of the batch.
func (c AcceptInput) InputIDs() []InputID {
	ids := make([]InputID, len(c.Inputs))
	for i, in := range c.Inputs {
		ids[i] = in.ID
	}
	return ids
}
