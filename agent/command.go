package agent

// AgentCommand is the intent submitted through Runtime.Commit for an existing
// Run. Accepting one command constitutes one transition (spec §3.6). The
// interface is sealed: only the fourteen variants below exist.
type AgentCommand interface{ agentCommand() }

// AgentInput is a queue-safe input: a stable ID plus an immutable payload.
// Queue item references, priority, order, claims and leases stay in the host.
type AgentInput struct {
	ID      InputID       `json:"id"`
	Payload CanonicalJSON `json:"payload"`
}

// NextStep creates the command consumed by an active Run at a safe boundary.
func NextStep(input AgentInput) AcceptInput { return AcceptInput{Input: input} }

// RunSeed is the admission seed for a new Run. It is not a command: it never
// goes through Runtime.Commit; application admission passes it to Initialize.
type RunSeed struct {
	Input AgentInput `json:"input"`
}

// NextRun creates the admission seed used by application admission. It does
// not allocate a RunID, claim a queue item, or mutate an existing Run.
func NextRun(input AgentInput) RunSeed { return RunSeed{Input: input} }

// PrepareModelRequest freezes the next model request. Its CommandID is
// derived from the loaded Revision, which is also its concurrency control.
type PrepareModelRequest struct {
	StepID        StepID        `json:"stepId"`
	Model         ModelRef      `json:"model"`
	Request       ModelRequest  `json:"request"`
	RequestDigest Digest        `json:"requestDigest"`
	InputIDs      []InputID     `json:"inputIds,omitempty"`
	PlanningToken PlanningToken `json:"planningToken,omitempty"`
	Tools         []ToolSpec    `json:"tools,omitempty"`
	ToolsDigest   Digest        `json:"toolsDigest"`
}

func (PrepareModelRequest) agentCommand() {}

// StartModelExecution takes execution ownership of a Prepared ModelStep.
type StartModelExecution struct {
	StepID StepID `json:"stepId"`
}

func (StartModelExecution) agentCommand() {}

// RecoverModelExecution releases or recovers model execution: no provider
// result was accepted, so the same frozen request may be prepared for another
// attempt. Legal sources: the current grant holder, or the Runtime's own
// lease-expiry recovery.
type RecoverModelExecution struct {
	StepID StepID `json:"stepId"`
}

func (RecoverModelExecution) agentCommand() {}

// SubmitModelResult submits one complete model result with its tool-call
// bindings. Requires the model start grant.
type SubmitModelResult struct {
	StepID StepID            `json:"stepId"`
	Result ModelResult       `json:"result"`
	Calls  []ToolCallBinding `json:"calls,omitempty"`
}

func (SubmitModelResult) agentCommand() {}

// SubmitModelFailure submits the final failure of one model call. Requires
// the model start grant.
type SubmitModelFailure struct {
	StepID  StepID      `json:"stepId"`
	Failure StepFailure `json:"failure"`
}

func (SubmitModelFailure) agentCommand() {}

// RejectModelResult records a structurally malformed model result: usage is
// accumulated, the step's reject counter is incremented, and the step returns
// to Prepared until RunConfig.ModelRejectLimit is exceeded. Requires the
// model start grant.
type RejectModelResult struct {
	StepID  StepID      `json:"stepId"`
	Usage   Usage       `json:"usage"`
	Failure StepFailure `json:"failure"`
}

func (RejectModelResult) agentCommand() {}

// StartToolCall takes execution ownership of one Pending tool call.
type StartToolCall struct {
	StepID StepID `json:"stepId"`
	CallID CallID `json:"callId"`
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
// requires that call's grant. Unknown outcome terminates the Run.
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

// RejectToolCall rejects a Waiting(Approval or ExternalResponse) call. Decide
// records the outcome as ToolCallFailed{Known, permission_denied}. ResponseDigest
// must be DigestToolResponseDecision(waiting kind, ResponseDecisionRejected, Reason).
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

// CancelRun stops a non-terminal Run. Hosts must commit this before
// cancelling the Loop's context (spec §6.6).
type CancelRun struct {
	Reason RunReason `json:"reason,omitempty"`
}

func (CancelRun) agentCommand() {}

// AcceptInput appends one queue-safe input to PendingInputs. Idempotent per
// (RunID, InputID) with identical payload.
type AcceptInput struct {
	Input AgentInput `json:"input"`
}

func (AcceptInput) agentCommand() {}

// commandType returns the wire discriminator for a sealed command variant.
func commandType(c AgentCommand) string {
	switch c.(type) {
	case PrepareModelRequest:
		return "prepare_model_request"
	case StartModelExecution:
		return "start_model_execution"
	case RecoverModelExecution:
		return "recover_model_execution"
	case SubmitModelResult:
		return "submit_model_result"
	case SubmitModelFailure:
		return "submit_model_failure"
	case RejectModelResult:
		return "reject_model_result"
	case StartToolCall:
		return "start_tool_call"
	case SubmitToolResult:
		return "submit_tool_result"
	case SubmitToolFailure:
		return "submit_tool_failure"
	case ApproveToolCall:
		return "approve_tool_call"
	case RejectToolCall:
		return "reject_tool_call"
	case SubmitToolResponse:
		return "submit_tool_response"
	case CancelRun:
		return "cancel_run"
	case AcceptInput:
		return "accept_input"
	default:
		return ""
	}
}
