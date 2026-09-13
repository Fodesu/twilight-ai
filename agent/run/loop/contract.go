package loop

import (
	"context"
	"encoding/json"
	"errors"

	run "github.com/felinics/twilight/agent/run"
	effect "github.com/felinics/twilight/agent/run/effect"
	"github.com/felinics/twilight/agent/session"

	"github.com/felinics/twilight/sdk"
)

// ErrRunAlreadyRunning identifies a second local driver for the same Run.
// A Loop permits concurrent execution of different Runs and serializes each
// Run locally so every Executing target has one in-process owner (RUN-CMT-6).
var ErrRunAlreadyRunning = errors.New("agent: loop: run already running")

// PromptBuilder is the decision-layer port the host resolves from the
// AgentPreset (DEC-PMT): it builds the next prompt from the Session's
// projections (RUN-LOP-2). Loop freezes the prompt into an agent-owned
// ModelRequest before crossing the Runtime boundary.
type PromptBuilder interface {
	Build(context.Context, run.PromptInput) (Prompt, error)
}

// Prompt is one built model input: the model to call, the provider request
// (messages and tool definitions), the inputs it consumed, the freshness
// token of the context it was built from, and the frozen tool specs.
type Prompt struct {
	Model    run.ModelRef
	Request  sdk.Request
	InputIDs []run.InputID
	Token    run.PromptToken
	Tools    []run.ToolSpec
}

// Settings are the execution parameters the Loop takes from the AgentPreset
// (RUN-LOP-1). Scheduling is frozen onto each ToolStep; MalformedRetries
// bounds the retries of one model step after malformed results.
type Settings struct {
	Scheduling       run.ToolScheduling
	MalformedRetries uint8
}

// ModelCatalog resolves a frozen run.ModelRef into an invoker at execution time;
// provider binding never enters the frozen request. The same ModelRef must
// resolve to equivalent execution semantics for the life of a Run (RUN-LOP-7).
type ModelCatalog interface {
	ResolveModel(run.ModelRef) (ModelInvoker, error)
}

type ModelInvoker interface {
	Generate(context.Context, sdk.Request) (sdk.ModelResult, error)
}

// StreamingModelInvoker is an optional optimization; it must produce the same
// final ModelResult as Generate.
type StreamingModelInvoker interface {
	Stream(context.Context, sdk.Request) (sdk.ModelStream, error)
}

type ToolCatalog interface {
	ResolveTool(run.ToolRef) (ExecutableTool, error)
}

type ToolExecutionRequest struct {
	RunID  run.RunID
	StepID run.StepID
	CallID run.CallID
	// Claim is the execution attempt the call runs under; it identifies the
	// Outcome the Executor returns through its message-shaped port (RUN-EXE-2).
	Claim            run.ExecutionClaim
	ToolRef          run.ToolRef
	DefinitionDigest run.Digest
	Arguments        run.CanonicalJSON
	// Workspace is the execution environment of the Turn's AgentPreset, passed
	// through for the tool; the Loop does not interpret it.
	Workspace run.WorkspaceRef
	Progress  ToolProgressSink
}

// ExecutableTool is the application-side execution contract (RUN-LOP-1).
type ExecutableTool interface {
	Ref() run.ToolRef
	Definition() sdk.ToolDefinition
	ResponsePolicy() run.ResponsePolicy
	// ValidateArguments runs before the start barrier and must not produce
	// external effects.
	ValidateArguments(run.CanonicalJSON) error
	Execute(context.Context, ToolExecutionRequest) ToolExecutionOutcome
}

// Tool outcomes belong to the process-independent effect protocol. Aliases
// keep the local tool implementation source-compatible.
type ToolExecutionOutcome = effect.ToolExecutionOutcome
type ToolExecutionSucceeded = effect.ToolExecutionSucceeded
type ToolExecutionFailed = effect.ToolExecutionFailed
type ToolExecutionUnknown = effect.ToolExecutionUnknown

type ToolProgressSink interface {
	Publish(context.Context, ToolProgress)
}

type ToolProgress struct {
	Payload json.RawMessage
}

// --- EventSink: realtime observation, never authority (RUN-LOP-6) ---

type EventSink interface {
	Emit(context.Context, Event) error
}

type EventDurability uint8

const (
	EventProvisional EventDurability = iota
	EventCommitted
)

type EventKind string

const (
	EventAgentCommitted      EventKind = "agent_committed"
	EventModelTextDelta      EventKind = "model_text_delta"
	EventModelReasoningDelta EventKind = "model_reasoning_delta"
	EventToolProgress        EventKind = "tool_progress"
	EventToolStarted         EventKind = "tool_started"
	EventToolCompleted       EventKind = "tool_completed"
	EventRunFinished         EventKind = "run_finished"
)

type Event struct {
	Session session.SessionID
	RunID   run.RunID
	StepID  run.StepID
	CallID  run.CallID
	// Sequence orders provisional observations within one stream. Committed
	// observations use the Session Seq for authority ordering.
	Sequence   uint64
	Kind       EventKind
	Durability EventDurability
	Payload    json.RawMessage
	// Committed is set for an EventAgentCommitted observation: the accepted
	// group (run facts, companion, attach); nil for provisional.
	Committed []session.SessionEvent
}

type LoopDisposition uint8

const (
	// LoopWaiting: no executable effect; the Run waits for a response, a
	// recovery, or an Outcome of an attempt this Loop did not dispatch.
	LoopWaiting LoopDisposition = iota
	// LoopFinished: the Run is terminal; Result is set.
	LoopFinished
	// LoopDispatched: Advance handed at least one Assignment to the Executor
	// and returned; Dispatched lists them. The Run moves again when their
	// Outcomes are read by key and then settled.
	LoopDispatched
	// LoopDelivered: Deliver settled an Outcome and the Run is not terminal;
	// the host advances it next.
	LoopDelivered
	// LoopDropped: Deliver found no Executing target under the Outcome's
	// key -- a late Outcome of a settled or disposed attempt -- and wrote
	// nothing.
	LoopDropped
)

type LoopResult struct {
	Disposition LoopDisposition
	// Reason is execution_recovery when ExecutionRecovery is true; otherwise empty.
	Reason WaitReason
	// ExecutionRecovery is true when NeedsRecovery(state) is true after this
	// Loop has no further executable effect: a ModelStep is Executing, or a
	// ToolStep has Executing calls and no Pending calls, and none of them was
	// dispatched by this Loop. Under Session-level ownership this only happens
	// before the owner's takeover disposition.
	ExecutionRecovery bool
	Result            *run.RunResult
	// Dispatched lists the assignments an Advance handed to the Executor.
	Dispatched []AssignmentKey
}

type WaitReason string

const (
	ExecutionRecovery WaitReason = "execution_recovery"
)
