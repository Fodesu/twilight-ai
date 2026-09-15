package loop

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

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
type AttachmentState = effect.AttachmentState
type Attachment = effect.Attachment

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

	AttachmentMissing  = effect.AttachmentMissing
	AttachmentActive   = effect.AttachmentActive
	AttachmentOrphaned = effect.AttachmentOrphaned
	AttachmentTerminal = effect.AttachmentTerminal
)

var (
	ErrExecutionNotFound = effect.ErrExecutionNotFound
	ErrOutcomeNotReady   = effect.ErrOutcomeNotReady
	ErrDispatchUnknown   = effect.ErrDispatchUnknown
)

// Deliver is an authority-local outcome sink. It is intentionally not part of
// Executor: the execution port is message-shaped, while this type is used only
// inside the authority to feed Loop.Deliver.
type Deliver func(Outcome)

// ErrExecutorRejected reports an assignment the executor would not start.
var ErrExecutorRejected = errors.New("agent: loop: executor rejected the assignment")

// ErrModelUnavailable reports a model assignment the executor cannot serve,
// found by Validate before the start barrier: the step stays Prepared.
var ErrModelUnavailable = errors.New("agent: loop: executor cannot serve the model")

// AssignmentFromTarget rebuilds the Assignment of an Executing target a
// takeover found in the projection, so the new owner can ask the Executor
// whether that attempt still runs (RUN-CMT-7). Resource semantics stay outside
// Run facts; an executor obtains any opaque TargetRef from its Assignment.
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

// RecoveryDispositionFromAttachment translates an executor observation into
// the recovery control-plane vocabulary. The two types stay separate on
// purpose: orphaned means the executor found a durable record without a live
// backend association; deferred means recovery must not dispose the Run yet.
func RecoveryDispositionFromAttachment(state AttachmentState) (run.RecoveryDisposition, error) {
	switch state {
	case effect.AttachmentActive:
		return run.RecoveryActive, nil
	case effect.AttachmentTerminal:
		return run.RecoveryTerminal, nil
	case effect.AttachmentOrphaned:
		return run.RecoveryDeferred, nil
	case effect.AttachmentMissing:
		return run.RecoveryMissing, nil
	default:
		return run.RecoveryMissing, fmt.Errorf("agent: loop: unknown attachment state %q", state)
	}
}

// Reattach adapts an Executor to run.Reattacher for one Session. The
// transport-facing Attach call only deals in the assignment key; the adapter
// waits for the result and feeds it to the Loop's internal Deliver path. The
// callback is therefore an authority-local concern, not part of Executor's
// process-independent interface. lifetime bounds background result reads;
// Attach's context bounds the initial attachment request.
func Reattach(lifetime context.Context, exec Executor, sid session.SessionID, deliver Deliver) run.Reattacher {
	return reattacher{lifetime: lifetime, exec: exec, sid: sid, deliver: deliver}
}

type reattacher struct {
	lifetime context.Context
	exec     Executor
	sid      session.SessionID
	deliver  Deliver
}

func (r reattacher) Attach(ctx context.Context, t run.RecoveryTarget) (run.RecoveryDisposition, error) {
	if r.lifetime == nil {
		return run.RecoveryMissing, errors.New("agent: loop: nil reattach lifetime")
	}
	if err := r.lifetime.Err(); err != nil {
		return run.RecoveryMissing, err
	}
	if r.exec == nil || r.deliver == nil {
		return run.RecoveryMissing, nil
	}
	a := AssignmentFromTarget(r.sid, t)
	attachment, err := r.exec.Attach(ctx, a.Key())
	if err != nil {
		return run.RecoveryMissing, err
	}
	disposition, err := RecoveryDispositionFromAttachment(attachment.State)
	if err != nil {
		return disposition, err
	}
	if disposition.PreservesExecution() {
		go func() {
			delay := 10 * time.Millisecond
			for {
				out, err := r.exec.GetOutcome(r.lifetime, a.Key())
				if err == nil {
					if r.lifetime.Err() == nil {
						r.deliver(out)
					}
					return
				}
				timer := time.NewTimer(delay)
				select {
				case <-r.lifetime.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
				if delay < time.Second {
					delay = min(delay*2, time.Second)
				}
			}
		}()
	}
	return disposition, nil
}

// --- LocalExecutor ------------------------------------------------------------

// LocalExecutor runs effects in goroutines of one process. It is the compact
// in-process implementation of the message-shaped Executor port. Its records
// are process-scoped: after a process restart Attach cannot find an old
// assignment. Durable cross-worker recovery belongs to executor.Worker.
type LocalExecutor struct {
	models    ModelCatalog
	tools     ToolCatalog
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
func NewLocalExecutor(models ModelCatalog, tools ToolCatalog, sink EventSink, streaming bool) (*LocalExecutor, error) {
	if models == nil {
		return nil, errors.New("agent: loop: nil model catalog")
	}
	if tools == nil {
		return nil, errors.New("agent: loop: nil tool catalog")
	}
	return &LocalExecutor{models: models, tools: tools, sink: sink, streaming: streaming,
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
		// Dispatch MUST carry the inline request payload (RUN-EXE-7): the
		// digest-only reconstruction of AssignmentFromTarget never enters
		// Dispatch, so a missing body is a definite rejection here.
		if a.Model.Request == nil {
			return fmt.Errorf("%w: model assignment without an inline request payload", ErrExecutorRejected)
		}
		frozenRequest := *a.Model.Request
		proto, err := run.ProtocolFor(a.Schema)
		if err != nil {
			return err
		}
		got, err := proto.DigestRequest(frozenRequest)
		if err != nil {
			return fmt.Errorf("%w: request digest: %v", ErrExecutorRejected, err)
		}
		if got != a.Model.RequestDigest || frozenRequest.Model != string(a.Model.Model) {
			return fmt.Errorf("%w: model request digest or model mismatch", ErrExecutorRejected)
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
func (e *LocalExecutor) Attach(_ context.Context, key AssignmentKey) (effect.Attachment, error) {
	e.mu.Lock()
	entry, ok := e.inflight[key]
	if !ok {
		e.mu.Unlock()
		return effect.Attachment{State: effect.AttachmentMissing, Execution: effect.ExecutionNotFound}, nil
	}
	closed := entry.closed
	out := entry.outcome
	e.mu.Unlock()
	if !closed {
		return effect.Attachment{State: effect.AttachmentActive, Execution: effect.ExecutionRunning, BackendAttached: true}, nil
	}
	return effect.Attachment{State: effect.AttachmentTerminal, Execution: localStatus(out), BackendAttached: true}, nil
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
	return localStatus(out), nil
}

func localStatus(out Outcome) ExecutionStatus {
	if out.Unknown {
		return ExecutionUnknown
	}
	if out.Cancelled {
		return ExecutionCancelled
	}
	if _, unknown := out.Tool.(ToolExecutionUnknown); unknown {
		return ExecutionUnknown
	}
	if out.Err != nil {
		return ExecutionFailed
	}
	return ExecutionCompleted
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
		Target:           cloneTarget(a.Target),
		Progress:         &progressSink{events: e.sink, run: a.RunID, step: a.StepID, call: a.CallID},
	}
	return Outcome{Tool: executeToolSafely(ctx, tool, &req)}
}

func cloneTarget(target *run.TargetRef) *run.TargetRef {
	if target == nil {
		return nil
	}
	copy := *target
	return &copy
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
