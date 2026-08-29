package run

import (
	"context"
	"encoding/json"

	"github.com/memohai/twilight-ai/sdk"
)

// RequestPlanner is the port the application injects: it projects application
// context into the next boundary sdk.Request (spec §5.6). Loop freezes it into
// an agent-owned ModelRequest before crossing the Runtime boundary. Planning
// implementations never live in agent.
type RequestPlanner interface {
	Plan(context.Context, PlanningHint) (RequestPlan, error)
}

type RequestPlan struct {
	Model         ModelRef
	Request       sdk.Request
	InputIDs      []InputID
	PlanningToken PlanningToken
	Tools         []ToolSpec
}

// ModelCatalog resolves a frozen ModelRef into an invoker at execution time;
// provider binding never enters the frozen request.
type ModelCatalog interface {
	Resolve(ModelRef) (ModelInvoker, error)
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
	Resolve(ToolRef) (ExecutableTool, error)
}

type ToolExecutionRequest struct {
	RunID            RunID
	StepID           StepID
	CallID           CallID
	ToolRef          ToolRef
	DefinitionDigest Digest
	Arguments        CanonicalJSON
	Progress         ToolProgressSink
}

// ExecutableTool is the application-side execution contract (spec §7.1).
type ExecutableTool interface {
	Ref() ToolRef
	Definition() sdk.ToolDefinition
	ResponsePolicy() ResponsePolicy
	// ValidateArguments runs before the start barrier and must not produce
	// external effects.
	ValidateArguments(CanonicalJSON) error
	Execute(context.Context, ToolExecutionRequest) ToolExecutionOutcome
}

// ToolExecutionOutcome is sealed: succeeded, failed-known, or unknown.
type ToolExecutionOutcome interface{ toolExecutionOutcome() }

type ToolExecutionSucceeded struct{ Result ToolExecutionResult }

func (ToolExecutionSucceeded) toolExecutionOutcome() {}

// ToolExecutionFailed asserts the external effect did NOT complete.
type ToolExecutionFailed struct{ Failure ToolFailure }

func (ToolExecutionFailed) toolExecutionOutcome() {}

// ToolExecutionUnknown means the effect may or may not have happened.
type ToolExecutionUnknown struct{ Failure ToolFailure }

func (ToolExecutionUnknown) toolExecutionOutcome() {}

type ToolProgressSink interface {
	Publish(context.Context, ToolProgress)
}

type ToolProgress struct {
	Payload json.RawMessage
}

// --- EventSink: realtime observation, never authority (spec §9.2) ---

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
	RunID      RunID
	StepID     StepID
	CallID     CallID
	Sequence   uint64
	Kind       EventKind
	Durability EventDurability
	Payload    json.RawMessage
	// Canonical is set for a committed observation; nil for provisional.
	Canonical *AgentEvent
}

// ExecutionPolicy is host-owned loop policy. It is not persisted in
// MachineState or events. MaxParallel bounds only workers this Loop launches,
// never a global count (spec §4.3).
type ExecutionPolicy struct {
	// MaxParallel: 1 is sequential; n>1 is bounded parallel; 0 normalizes to 1;
	// negative is rejected.
	MaxParallel int
	// ModelStepLimit: 0 means unlimited; positive values make the Loop submit
	// StopRun(step_limit) before planning another ModelStep.
	ModelStepLimit int
	// MalformedModelResultLimit: 0 normalizes to
	// DefaultMalformedModelResultLimit; negative is rejected. The Loop chooses
	// RejectModelResult disposition from the current ModelStep reject count.
	MalformedModelResultLimit int
}

type LoopDisposition uint8

const (
	LoopWaiting LoopDisposition = iota
	LoopFinished
)

type WaitReason string

const (
	WaitingForResponse WaitReason = "waiting_for_response"
	ExecutionRecovery  WaitReason = "execution_recovery"
)

type LoopResult struct {
	Disposition LoopDisposition
	Reason      WaitReason
	Waiting     []ResponseRequest
	Result      *RunResult
}
