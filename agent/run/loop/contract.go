package loop

import (
	"context"
	"encoding/json"
	"errors"

	run "github.com/memohai/twilight/agent/run"

	"github.com/memohai/twilight/sdk"
)

// ErrRunAlreadyRunning identifies a second local driver for the same Run.
// A Loop permits concurrent execution of different Runs and serializes each
// Run locally so one execution grant has one in-process consumer.
var ErrRunAlreadyRunning = errors.New("agent: loop: run already running")

// RequestPlanner is the port the application injects: it projects application
// context into the next boundary sdk.Request (RUN-LOP-2). Loop freezes it into
// an agent-owned ModelRequest before crossing the Runtime boundary. Planning
// implementations never live in agent.
type RequestPlanner interface {
	Plan(context.Context, run.PlanningHint) (RequestPlan, error)
}

type RequestPlan struct {
	Model         run.ModelRef
	Request       sdk.Request
	InputIDs      []run.InputID
	PlanningToken run.PlanningToken
	Tools         []run.ToolSpec
}

// ModelCatalog resolves a frozen run.ModelRef into an invoker at execution time;
// provider binding never enters the frozen request. The same ModelRef must
// resolve to equivalent execution semantics for the life of a Run (RUN-LOP-7).
type ModelCatalog interface {
	Resolve(run.ModelRef) (ModelInvoker, error)
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
	Resolve(run.ToolRef) (ExecutableTool, error)
}

type ToolExecutionRequest struct {
	RunID            run.RunID
	StepID           run.StepID
	CallID           run.CallID
	ToolRef          run.ToolRef
	DefinitionDigest run.Digest
	Arguments        run.CanonicalJSON
	Progress         ToolProgressSink
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

// ToolExecutionOutcome is sealed: succeeded, failed-known, or unknown.
type ToolExecutionOutcome interface{ toolExecutionOutcome() }

type ToolExecutionSucceeded struct{ Result run.ToolExecutionResult }

func (ToolExecutionSucceeded) toolExecutionOutcome() {}

// ToolExecutionFailed asserts the external effect did NOT complete.
type ToolExecutionFailed struct{ Failure run.ToolFailure }

func (ToolExecutionFailed) toolExecutionOutcome() {}

// ToolExecutionUnknown means the effect may or may not have happened.
type ToolExecutionUnknown struct{ Failure run.ToolFailure }

func (ToolExecutionUnknown) toolExecutionOutcome() {}

type ToolProgressSink interface {
	Publish(context.Context, ToolProgress)
}

type ToolProgress struct {
	Payload json.RawMessage
}

// ToolExecutionMode is Loop-local until SubmitModelResult snapshots it onto
// ToolStep.Scheduling.
type ToolExecutionMode string

const (
	ToolExecutionParallel   ToolExecutionMode = "parallel"
	ToolExecutionSequential ToolExecutionMode = "sequential"
)

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
	RunID  run.RunID
	StepID run.StepID
	CallID run.CallID
	// Sequence orders provisional observations within one stream. Canonical
	// observations use AgentEvent.Revision/Index for authority ordering.
	Sequence   uint64
	Kind       EventKind
	Durability EventDurability
	Payload    json.RawMessage
	// Canonical is set for a committed observation; nil for provisional.
	Canonical *run.AgentEvent
}

// ExecutionPolicy is host-owned loop policy. ToolExecution and MaxParallel
// are snapshotted onto ToolStep at SubmitModelResult and then frozen.
// OnMalformedModelResult is not persisted.
type ExecutionPolicy struct {
	ToolExecution ToolExecutionMode
	// OnMalformedModelResult chooses the disposition recorded for a malformed
	// provider result. A nil handler fails the Run; retries must be explicit.
	OnMalformedModelResult func(run.ModelStep, run.StepFailure) run.ModelRejectDisposition
	// MaxParallel bounds local tool workers. Zero means all eligible calls in
	// the current batch may run concurrently.
	MaxParallel int
}

type LoopDisposition uint8

const (
	LoopWaiting LoopDisposition = iota
	LoopFinished
)

type LoopResult struct {
	Disposition LoopDisposition
	// Reason is retained for source compatibility. ExecutionRecovery is
	// the authoritative signal that a live execution needs recovery.
	Reason WaitReason
	// ExecutionRecovery is true when NeedsRecovery(state) is true after this
	// Loop has no further executable effect: a ModelStep is Executing, or a
	// ToolStep has Executing calls and no Pending calls.
	ExecutionRecovery bool
	Result            *run.RunResult
}

type WaitReason string

const (
	ExecutionRecovery WaitReason = "execution_recovery"
)
