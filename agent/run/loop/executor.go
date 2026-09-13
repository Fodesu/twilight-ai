package loop

import (
	"context"
	"errors"
	"fmt"
	"sync"

	run "github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/sdk"
)

// AssignmentKind names the effect an Assignment asks for.
type AssignmentKind string

const (
	AssignmentModel AssignmentKind = "model"
	AssignmentTool  AssignmentKind = "tool"
)

// AssignmentKey identifies one execution attempt of one target. It is the
// preimage of the attempt's settlement CommandID (RUN-WIR-4), so an Outcome
// carrying the key of an attempt that is no longer Executing is stale and is
// dropped rather than settled.
type AssignmentKey struct {
	RunID  run.RunID
	StepID run.StepID
	CallID run.CallID // empty for a model step
	Claim  run.ExecutionClaim
}

// ModelAssignment is one model call. The request body is named by digest:
// the executor fetches it from the content-addressed frozen value store, so
// the assignment itself never carries the body (RUN-WIR-4).
type ModelAssignment struct {
	Model         run.ModelRef
	RequestDigest run.Digest
}

// ToolAssignment is one tool call under a frozen binding (RUN-MCH-2). The
// executor validates the binding against its implementation before the start
// barrier (Validate) and executes it after (Dispatch).
type ToolAssignment struct {
	ToolRef          run.ToolRef
	DefinitionDigest run.Digest
	Arguments        run.CanonicalJSON
	Policy           run.ResponsePolicy
	// Workspace is the execution environment recorded in the Turn's Profile;
	// the Loop passes it through and does not interpret it.
	Workspace run.WorkspaceRef
}

// Assignment is the unit of work the authority hands to an Executor
// (RUN-EXE-1): which effect, under which attempt, with what frozen inputs.
type Assignment struct {
	Session session.SessionID
	RunID   run.RunID
	StepID  run.StepID
	CallID  run.CallID // empty for a model step
	Claim   run.ExecutionClaim
	// Schema is the Run's protocol version; the executor uses it to pick the
	// digest profile it validates tool definitions with.
	Schema uint16
	Kind   AssignmentKind
	Model  *ModelAssignment
	Tool   *ToolAssignment
}

// Key returns the attempt identity of the assignment.
func (a Assignment) Key() AssignmentKey {
	return AssignmentKey{RunID: a.RunID, StepID: a.StepID, CallID: a.CallID, Claim: a.Claim}
}

// Outcome is what an executor returns for one Assignment (RUN-EXE-2). Exactly
// one of Model, Tool or Err is meaningful for the assignment's kind: a model
// call yields Model or Err (a provider failure, a missing frozen body, or a
// cancellation), a tool call yields a sealed ToolExecutionOutcome. Cancelled
// reports that the executor stopped the effect on request; a model call that
// was cancelled recovers to Prepared instead of failing the Run (RUN-LOP-3).
type Outcome struct {
	Key       AssignmentKey
	Model     *sdk.ModelResult
	Tool      ToolExecutionOutcome
	Err       error
	Cancelled bool
}

// Deliver receives an Outcome. An executor may call it from any goroutine at
// any time after Dispatch or Attach accepted the assignment; it calls it
// exactly once per accepted assignment.
type Deliver func(Outcome)

// Executor is the effect layer port (RUN-EXE-3). The Loop decides and
// records; the Executor performs effects and reports Outcomes. Whether the
// two live in one process is a deployment choice: LocalExecutor runs effects
// in goroutines, a remote implementation forwards Assignments over a
// transport and delivers the Outcomes it receives.
type Executor interface {
	// Validate checks a tool assignment against the implementation without
	// producing an effect: lookup, definition digest, response policy and
	// arguments (RUN-LOP-4). A non-nil failure is the Known failure the Loop
	// settles without crossing the start barrier. Model assignments validate
	// trivially. error reports the executor itself being unreachable.
	Validate(ctx context.Context, a Assignment) (*run.ToolFailure, error)
	// Dispatch accepts an assignment and returns at once; the Outcome arrives
	// through deliver. An error means the effect was not started.
	Dispatch(ctx context.Context, a Assignment, deliver Deliver) error
	// Attach asks, during a takeover, whether the attempt named by a.Key is
	// still running here. True registers deliver for its Outcome and leaves the
	// target Executing; false lets the takeover dispose it (RUN-CMT-7).
	Attach(ctx context.Context, a Assignment, deliver Deliver) (bool, error)
	// Cancel stops every in-flight assignment of the Run. Their Outcomes are
	// still delivered (as cancelled or unknown) unless ownership was lost, in
	// which case the Loop no longer settles them.
	Cancel(ctx context.Context, runID run.RunID) error
}

// FrozenRequestReader is the read side of the frozen value store an executor
// fetches model request bodies from (RUN-WIR-4). run.Runtime satisfies it.
type FrozenRequestReader interface {
	FrozenRequest(context.Context, run.Digest) (run.ModelRequest, error)
}

// ErrExecutorRejected reports an assignment the executor would not start.
var ErrExecutorRejected = errors.New("agent: loop: executor rejected the assignment")

// AssignmentFromTarget rebuilds the Assignment of an Executing target a
// takeover found in the projection, so the new owner can ask the Executor
// whether that attempt still runs (RUN-CMT-7). Workspace is not recorded in
// Run facts; a host that needs it resolves it from the Turn's Profile.
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

// Reattach adapts an Executor to run.Reattacher for one Session: every target
// the takeover asks about is turned into its Assignment and offered to the
// Executor; a reattached attempt delivers its Outcome to deliver.
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
	return r.exec.Attach(ctx, AssignmentFromTarget(r.sid, t), r.deliver)
}

// --- LocalExecutor ------------------------------------------------------------

// LocalExecutor runs effects in goroutines of the authority process: the
// reference Executor and the one every colocated host uses. It keeps an
// in-flight table so Attach can answer for attempts this process started;
// after a process restart the table is empty and every Attach is false.
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
	cancel  context.CancelFunc
	mu      sync.Mutex
	deliver Deliver
	done    bool
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

// Validate is RUN-LOP-4's pre-start check for tools.
func (e *LocalExecutor) Validate(_ context.Context, a Assignment) (*run.ToolFailure, error) {
	if a.Kind != AssignmentTool || a.Tool == nil {
		return nil, nil
	}
	proto, err := run.ProtocolFor(a.Schema)
	if err != nil {
		return nil, err
	}
	_, failure := e.resolveTool(proto, a.Tool)
	return failure, nil
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
// Dispatch returns, while the effect ends by Outcome or Cancel.
func (e *LocalExecutor) Dispatch(ctx context.Context, a Assignment, deliver Deliver) error {
	if deliver == nil {
		return errors.New("agent: loop: nil deliver")
	}
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
		model := *a.Model
		execute = func(ctx context.Context) Outcome { return e.runModel(ctx, a, model, invoker) }
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

	key := a.Key()
	effectCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	entry := &inflight{runID: a.RunID, cancel: cancel, deliver: deliver}
	e.mu.Lock()
	if _, dup := e.inflight[key]; dup {
		e.mu.Unlock()
		cancel()
		return fmt.Errorf("%w: assignment already in flight", ErrExecutorRejected)
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
		delete(e.inflight, key)
		e.mu.Unlock()
		entry.mu.Lock()
		entry.done = true
		d := entry.deliver
		entry.mu.Unlock()
		cancel()
		d(out)
	}()
	return nil
}

// Attach answers for attempts this process still runs.
func (e *LocalExecutor) Attach(_ context.Context, a Assignment, deliver Deliver) (bool, error) {
	if deliver == nil {
		return false, errors.New("agent: loop: nil deliver")
	}
	e.mu.Lock()
	entry, ok := e.inflight[a.Key()]
	e.mu.Unlock()
	if !ok {
		return false, nil
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.done {
		return false, nil
	}
	entry.deliver = deliver
	return true, nil
}

// Cancel stops every in-flight assignment of runID; their Outcomes still
// arrive, marked Cancelled.
func (e *LocalExecutor) Cancel(_ context.Context, runID run.RunID) error {
	e.mu.Lock()
	var cancels []context.CancelFunc
	for _, entry := range e.inflight {
		if entry.runID == runID {
			cancels = append(cancels, entry.cancel)
		}
	}
	e.mu.Unlock()
	for _, c := range cancels {
		c()
	}
	return nil
}

// InFlight reports the number of assignments still executing (tests).
func (e *LocalExecutor) InFlight() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.inflight)
}

func (e *LocalExecutor) runModel(ctx context.Context, a Assignment, m ModelAssignment, invoker ModelInvoker) Outcome {
	frozenRequest, err := e.frozen.FrozenRequest(ctx, m.RequestDigest)
	if err != nil {
		return Outcome{Err: err}
	}
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
