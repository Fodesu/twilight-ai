package loop

import (
	"context"
	"errors"
	"fmt"
	"sync"

	run "github.com/felinics/twilight/agent/run"
	effect "github.com/felinics/twilight/agent/run/effect"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/sdk"
)

// The Loop package keeps aliases for the protocol types so existing Run/Loop
// call sites remain source-compatible. The definitions live in run/effect;
// this package owns only the local execution implementation and Loop helpers.
type AssignmentKind = effect.AssignmentKind

type AssignmentKey = effect.AssignmentKey

type ModelAssignment = effect.ModelAssignment

type ToolAssignment = effect.ToolAssignment

type Assignment = effect.Assignment

type Outcome = effect.Outcome

type ExecutionStatus = effect.ExecutionStatus

type Executor = effect.Port

const (
	AssignmentModel          = effect.AssignmentModel
	AssignmentTool           = effect.AssignmentTool
	ExecutionNotFound        = effect.ExecutionNotFound
	ExecutionAccepted        = effect.ExecutionAccepted
	ExecutionRunning         = effect.ExecutionRunning
	ExecutionCancelRequested = effect.ExecutionCancelRequested
	ExecutionCompleted       = effect.ExecutionCompleted
	ExecutionFailed          = effect.ExecutionFailed
	ExecutionCancelled       = effect.ExecutionCancelled
	ExecutionUnknown         = effect.ExecutionUnknown
)

var (
	ErrExecutionNotFound = effect.ErrExecutionNotFound
	ErrOutcomeNotReady   = effect.ErrOutcomeNotReady
)

// Deliver is an authority-local outcome sink. It is intentionally not part of
// Executor: the execution port is message-shaped, while this type is used only
// inside the authority to feed Loop.Deliver.
type Deliver func(Outcome)

// FrozenRequestReader is the compatibility read side of the frozen value
// store. New process-independent Assignments carry the model request inline;
// the reader remains for legacy digest-only local callers.
type FrozenRequestReader interface {
	FrozenRequest(context.Context, run.Digest) (run.ModelRequest, error)
}

// ErrExecutorRejected reports an assignment the executor would not start.
var ErrExecutorRejected = errors.New("agent: loop: executor rejected the assignment")

// ErrModelUnavailable reports a model assignment the executor cannot serve,
// found by Validate before the start barrier: the step stays Prepared.
var ErrModelUnavailable = errors.New("agent: loop: executor cannot serve the model")

// AssignmentFromTarget rebuilds the Assignment of an Executing target a
// takeover found in the projection, so the new owner can ask the Executor
// whether that attempt still runs (RUN-CMT-7). Workspace is not recorded in
// Run facts; a host that needs it resolves it from the Turn's AgentPreset.
func AssignmentFromTarget(sid session.SessionID, t run.RecoveryTarget) Assignment {
	a := Assignment{Session: sid, RunID: t.RunID, StepID: t.StepID, CallID: t.CallID, Claim: t.Claim, Schema: t.Schema}
	switch {
	case t.Call != nil:
		a.Kind = AssignmentTool
		a.Tool = &ToolAssignment{ToolRef: t.Call.ToolRef, DefinitionDigest: t.Call.DefinitionDigest, Arguments: t.Call.Arguments, Policy: t.Call.Policy}
	case t.Model != nil:
		a.Kind = AssignmentModel
		a.Model = &ModelAssignment{Model: t.Model.Model, RequestDigest: t.Model.RequestDigest}
	}
	return a
}

// Reattach adapts an Executor to run.Reattacher for one Session. The
// transport-facing Attach call only deals in the assignment key; the adapter
// waits for the result and feeds it to the Loop's internal Deliver path. The
// callback is therefore an authority-local concern, not part of Executor's
// process-independent interface.
func Reattach(exec Executor, sid session.SessionID, deliver Deliver) run.Reattacher {
	return reattacher{exec: exec, sid: sid, deliver: deliver}
}

type reattacher struct {
	exec    Executor
	sid     session.SessionID
	deliver Deliver
}

func (r reattacher) Attach(ctx context.Context, t run.RecoveryTarget) (bool, error) {
	if r.exec == nil || r.deliver == nil {
		return false, nil
	}
	a := AssignmentFromTarget(r.sid, t)
	attached, err := r.exec.Attach(ctx, a.Key())
	if err != nil || !attached {
		return attached, err
	}
	go func() {
		out, err := r.exec.GetOutcome(context.Background(), a.Key())
		if err != nil {
			out = Outcome{Key: a.Key(), Err: err}
		}
		r.deliver(out)
	}()
	return true, nil
}

// --- LocalExecutor ------------------------------------------------------------

// LocalExecutor runs effects in goroutines of one process. It is the compact
// in-process implementation of the message-shaped Executor port. Its records
// are process-scoped: after a process restart Attach cannot find an old
// assignment. Durable cross-worker recovery belongs to executor.Worker.
type LocalExecutor struct {
	models    ModelCatalog
	tools     ToolCatalog
	frozen    FrozenRequestReader
	sink      EventSink
	streaming bool

	mu       sync.Mutex
	inflight map[AssignmentKey]*inflight
}

type inflight struct {
	runID   run.RunID
	digest  run.Digest
	cancel  context.CancelFunc
	done    chan struct{}
	outcome Outcome
	closed  bool
}

// NewLocalExecutor builds the in-process executor. sink receives provisional
// observations (model deltas, tool progress); nil discards them. streaming
// selects StreamingModelInvoker when the invoker offers it.
func NewLocalExecutor(models ModelCatalog, tools ToolCatalog, frozen FrozenRequestReader, sink EventSink, streaming bool) (*LocalExecutor, error) {
	if models == nil {
		return nil, errors.New("agent: loop: nil model catalog")
	}
	if tools == nil {
		return nil, errors.New("agent: loop: nil tool catalog")
	}
	if frozen == nil {
		return nil, errors.New("agent: loop: nil frozen request reader")
	}
	return &LocalExecutor{models: models, tools: tools, frozen: frozen, sink: sink, streaming: streaming,
		inflight: make(map[AssignmentKey]*inflight)}, nil
}

// Validate is the pre-start check (RUN-EXE-5): tools per RUN-LOP-4, models
// by resolving the ModelRef in the catalog so a missing model fails before
// any start fact is written (RUN-LOP-3).
func (e *LocalExecutor) Validate(_ context.Context, a Assignment) (*run.ToolFailure, error) {
	switch a.Kind {
	case AssignmentModel:
		if a.Model == nil {
			return &run.ToolFailure{Class: run.FailureProvider, Message: "model assignment without body"}, nil
		}
		invoker, err := e.models.ResolveModel(a.Model.Model)
		if err != nil {
			return &run.ToolFailure{Class: run.FailureProvider, Message: err.Error()}, nil
		}
		if invoker == nil {
			return &run.ToolFailure{Class: run.FailureProvider, Message: "model catalog returned a nil invoker"}, nil
		}
		return nil, nil
	case AssignmentTool:
		if a.Tool == nil {
			return nil, nil
		}
		proto, err := run.ProtocolFor(a.Schema)
		if err != nil {
			return nil, err
		}
		_, failure := e.resolveTool(proto, a.Tool)
		return failure, nil
	default:
		return nil, nil
	}
}

func (e *LocalExecutor) resolveTool(proto run.Protocol, t *ToolAssignment) (ExecutableTool, *run.ToolFailure) {
	tool, resolveErr := e.tools.ResolveTool(t.ToolRef)
	if resolveErr != nil {
		return nil, &run.ToolFailure{Class: run.FailureToolLookup, Message: resolveErr.Error()}
	}
	if tool == nil {
		return nil, &run.ToolFailure{Class: run.FailureToolLookup, Message: "tool catalog returned a nil tool"}
	}
	toolDef, freezeErr := run.FreezeToolDefinition(tool.Definition())
	if freezeErr != nil {
		return nil, &run.ToolFailure{Class: run.FailureDefinitionMismatch, Message: freezeErr.Error()}
	}
	defDigest, digestErr := proto.DigestToolDefinition(toolDef)
	if digestErr != nil {
		return nil, &run.ToolFailure{Class: run.FailureDefinitionMismatch, Message: digestErr.Error()}
	}
	switch {
	case tool.Ref() != t.ToolRef || defDigest != t.DefinitionDigest:
		return nil, &run.ToolFailure{Class: run.FailureDefinitionMismatch, Message: "tool definition digest mismatch"}
	case tool.ResponsePolicy() != t.Policy:
		return nil, &run.ToolFailure{Class: run.FailureDefinitionMismatch, Message: "response policy mismatch"}
	}
	if argErr := tool.ValidateArguments(t.Arguments); argErr != nil {
		return nil, &run.ToolFailure{Class: run.FailureInvalidArguments, Message: argErr.Error()}
	}
	return tool, nil
}

// Dispatch starts the effect in a goroutine. The effect's context is derived
// from ctx's values but not its cancellation: the caller's request ends when
// Dispatch returns, while the effect ends by Outcome or Cancel. The outcome
// is retained in the execution record and is read through GetOutcome; it is
// not delivered through a process-local callback.
func (e *LocalExecutor) Dispatch(ctx context.Context, a Assignment) error {
	key := a.Key()
	digest, err := a.Digest()
	if err != nil {
		return fmt.Errorf("%w: assignment digest: %v", ErrExecutorRejected, err)
	}
	// Check before resolving catalogs or fetching frozen content: an
	// idempotent retry must not depend on transient execution dependencies.
	e.mu.Lock()
	if existing, ok := e.inflight[key]; ok {
		e.mu.Unlock()
		if existing.digest == digest {
			return nil
		}
		return fmt.Errorf("%w: assignment key reused with different content", ErrExecutorRejected)
	}
	e.mu.Unlock()

	var execute func(context.Context) Outcome
	switch a.Kind {
	case AssignmentModel:
		if a.Model == nil {
			return fmt.Errorf("%w: model assignment without body", ErrExecutorRejected)
		}
		invoker, err := e.models.ResolveModel(a.Model.Model)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrExecutorRejected, err)
		}
		if invoker == nil {
			return fmt.Errorf("%w: model catalog returned a nil invoker", ErrExecutorRejected)
		}
		// A process-independent assignment carries the immutable request when
		// available. The reader fallback keeps the compact local API compatible
		// with older callers that dispatch digest-only assignments.
		var frozenRequest run.ModelRequest
		if a.Model.Request != nil {
			frozenRequest = *a.Model.Request
			requestDigest, err := run.ProtocolFor(a.Schema)
			if err != nil {
				return err
			}
			got, err := requestDigest.DigestRequest(frozenRequest)
			if err != nil {
				return fmt.Errorf("%w: request digest: %v", ErrExecutorRejected, err)
			}
			if got != a.Model.RequestDigest || frozenRequest.Model != string(a.Model.Model) {
				return fmt.Errorf("%w: model request digest or model mismatch", ErrExecutorRejected)
			}
		} else {
			// The body is fetched before the effect starts: a missing transfer
			// copy is a Dispatch failure, not an Outcome that re-enters planning.
			frozenRequest, err = e.frozen.FrozenRequest(ctx, a.Model.RequestDigest)
			if err != nil {
				return fmt.Errorf("%w: %w", ErrExecutorRejected, err)
			}
		}
		execute = func(ctx context.Context) Outcome { return e.runModel(ctx, a, frozenRequest, invoker) }
	case AssignmentTool:
		if a.Tool == nil {
			return fmt.Errorf("%w: tool assignment without binding", ErrExecutorRejected)
		}
		proto, err := run.ProtocolFor(a.Schema)
		if err != nil {
			return err
		}
		tool, failure := e.resolveTool(proto, a.Tool)
		if failure != nil {
			return fmt.Errorf("%w: %s: %s", ErrExecutorRejected, failure.Class, failure.Message)
		}
		binding := *a.Tool
		execute = func(ctx context.Context) Outcome { return e.runTool(ctx, a, binding, tool) }
	default:
		return fmt.Errorf("%w: unknown assignment kind %q", ErrExecutorRejected, a.Kind)
	}

	effectCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	entry := &inflight{runID: a.RunID, digest: digest, cancel: cancel, done: make(chan struct{})}
	e.mu.Lock()
	if existing, dup := e.inflight[key]; dup {
		e.mu.Unlock()
		cancel()
		if existing.digest == digest {
			return nil // idempotent retry of the same accepted assignment
		}
		return fmt.Errorf("%w: assignment key reused with different content", ErrExecutorRejected)
	}
	e.inflight[key] = entry
	e.mu.Unlock()

	go func() {
		out := execute(effectCtx)
		out.Key = key
		if effectCtx.Err() != nil {
			out.Cancelled = true
		}
		e.mu.Lock()
		entry.outcome = out
		entry.closed = true
		close(entry.done)
		e.mu.Unlock()
		cancel()
	}()
	return nil
}

// Attach answers for attempts this process still runs or has completed. It
// only observes an existing record and never starts a second effect.
func (e *LocalExecutor) Attach(_ context.Context, key AssignmentKey) (bool, error) {
	e.mu.Lock()
	_, ok := e.inflight[key]
	e.mu.Unlock()
	return ok, nil
}

// GetStatus returns the current process-scoped execution status.
func (e *LocalExecutor) GetStatus(_ context.Context, key AssignmentKey) (ExecutionStatus, error) {
	e.mu.Lock()
	entry, ok := e.inflight[key]
	if !ok {
		e.mu.Unlock()
		return ExecutionNotFound, ErrExecutionNotFound
	}
	closed := entry.closed
	out := entry.outcome
	e.mu.Unlock()
	if !closed {
		return ExecutionRunning, nil
	}
	if out.Cancelled {
		return ExecutionCancelled, nil
	}
	if _, unknown := out.Tool.(ToolExecutionUnknown); unknown {
		return ExecutionUnknown, nil
	}
	if out.Err != nil {
		return ExecutionFailed, nil
	}
	return ExecutionCompleted, nil
}

// GetOutcome waits for and returns the stable outcome of an accepted
// assignment. The local record is intentionally retained for idempotent reads;
// a production implementation should apply an explicit retention policy.
func (e *LocalExecutor) GetOutcome(ctx context.Context, key AssignmentKey) (Outcome, error) {
	e.mu.Lock()
	entry, ok := e.inflight[key]
	e.mu.Unlock()
	if !ok {
		return Outcome{}, ErrExecutionNotFound
	}
	select {
	case <-entry.done:
		e.mu.Lock()
		out := entry.outcome
		e.mu.Unlock()
		return out, nil
	case <-ctx.Done():
		return Outcome{}, ctx.Err()
	}
}

// Cancel requests cancellation of one assignment; its Outcome remains
// observable through GetOutcome.
func (e *LocalExecutor) Cancel(_ context.Context, key AssignmentKey) error {
	e.mu.Lock()
	entry, ok := e.inflight[key]
	e.mu.Unlock()
	if ok {
		entry.cancel()
	}
	return nil
}

// InFlight reports the number of assignments still executing (tests).
func (e *LocalExecutor) InFlight() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for _, entry := range e.inflight {
		if !entry.closed {
			n++
		}
	}
	return n
}

func (e *LocalExecutor) runModel(ctx context.Context, a Assignment, frozenRequest run.ModelRequest, invoker ModelInvoker) Outcome {
	sdkRequest, err := frozenRequest.SDK()
	if err != nil {
		return Outcome{Err: fmt.Errorf("%w: %v", errMalformedFrozenRequest, err)}
	}
	result, err := e.invokeModel(ctx, invoker, &sdkRequest, a)
	if err != nil {
		return Outcome{Err: err}
	}
	return Outcome{Model: &result}
}

// errMalformedFrozenRequest marks a frozen body that decodes but cannot be
// materialized; the Loop settles it as a malformed-model rejection.
var errMalformedFrozenRequest = errors.New("agent: loop: frozen request cannot be materialized")

func (e *LocalExecutor) invokeModel(ctx context.Context, invoker ModelInvoker, req *sdk.Request, a Assignment) (sdk.ModelResult, error) {
	if e.streaming {
		if streamer, ok := invoker.(StreamingModelInvoker); ok {
			stream, err := streamer.Stream(ctx, *req)
			if err != nil {
				return sdk.ModelResult{}, err
			}
			// The range has an explicit ctx escape: a stream that stops
			// sending without closing Parts must not block cancellation. The
			// assembler behind Parts tolerates abandonment: it stops forwarding
			// once ctx is done and drains the provider.
			var sequence uint64
			emitDelta := func(kind EventKind, payload any) {
				if e.sink == nil {
					return
				}
				sequence++
				_ = e.sink.Emit(ctx, Event{Session: a.Session, RunID: a.RunID, StepID: a.StepID,
					Sequence: sequence, Kind: kind, Durability: EventProvisional,
					Payload: mustJSON(payload)})
			}
		consume:
			for {
				select {
				case part, open := <-stream.Parts:
					if !open {
						break consume
					}
					switch p := part.(type) {
					case *sdk.TextDeltaPart:
						emitDelta(EventModelTextDelta, p.Text)
					case *sdk.ReasoningDeltaPart:
						emitDelta(EventModelReasoningDelta, p.Text)
					}
				case <-ctx.Done():
					return sdk.ModelResult{}, ctx.Err()
				}
			}
			result, err := stream.Result()
			if err != nil {
				return sdk.ModelResult{}, err
			}
			if result == nil {
				return sdk.ModelResult{}, errors.New("agent: loop: stream returned no result")
			}
			return *result, nil
		}
	}
	return invoker.Generate(ctx, *req)
}

func (e *LocalExecutor) runTool(ctx context.Context, a Assignment, t ToolAssignment, tool ExecutableTool) Outcome {
	req := ToolExecutionRequest{
		RunID:            a.RunID,
		StepID:           a.StepID,
		CallID:           a.CallID,
		Claim:            a.Claim,
		ToolRef:          t.ToolRef,
		DefinitionDigest: t.DefinitionDigest,
		Arguments:        t.Arguments,
		Workspace:        t.Workspace,
		Progress:         &progressSink{events: e.sink, run: a.RunID, step: a.StepID, call: a.CallID},
	}
	return Outcome{Tool: executeToolSafely(ctx, tool, &req)}
}

// executeToolSafely runs an application tool and converts a panic into
// ToolExecutionUnknown: the effect may have happened before the panic, and a
// crashing tool must not take down every run in the process.
func executeToolSafely(ctx context.Context, tool ExecutableTool, req *ToolExecutionRequest) (outcome ToolExecutionOutcome) {
	defer func() {
		if r := recover(); r != nil {
			outcome = ToolExecutionUnknown{Failure: run.ToolFailure{
				Class:   run.FailureEffectUnknown,
				Message: fmt.Sprintf("tool panic: %v", r),
			}}
		}
	}()
	return tool.Execute(ctx, *req)
}

var _ Executor = (*LocalExecutor)(nil)
