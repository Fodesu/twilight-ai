package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/memohai/twilight-ai/sdk"
)

// Loop is the in-process interpreter of one Run (spec §6). It holds no
// authoritative state; every iteration starts from Runtime.Load.
type Loop struct {
	Models    ModelCatalog
	Tools     ToolCatalog
	Planner   RequestPlanner
	Execution ExecutionPolicy
	Streaming bool
}

// NewLoop validates and normalizes the execution policy (spec §4.3).
func NewLoop(models ModelCatalog, tools ToolCatalog, planner RequestPlanner, policy ExecutionPolicy, streaming bool) (*Loop, error) {
	if policy.MaxParallel < 0 {
		return nil, errors.New("agent: loop: negative MaxParallel")
	}
	if policy.MaxParallel == 0 {
		policy.MaxParallel = 1
	}
	return &Loop{Models: models, Tools: tools, Planner: planner, Execution: policy, Streaming: streaming}, nil
}

// Run drives the Run until it finishes, must wait, or the context is
// cancelled (spec §6.2). controlCtx for reads/commits is derived from ctx via
// WithoutCancel so worker cancellation never blocks result submission.
func (l *Loop) Run(ctx context.Context, runtime Runtime, events EventSink) (LoopResult, error) {
	controlCtx := context.WithoutCancel(ctx)

	for {
		if err := ctx.Err(); err != nil {
			// Workers started by this Loop have already been settled by the
			// branches below before we reach this check.
			return LoopResult{}, err
		}
		snapshot, err := runtime.Load(controlCtx)
		if err != nil {
			return LoopResult{}, err
		}
		if snapshot.State.Status.Terminal() {
			if events != nil {
				_ = events.Emit(controlCtx, Event{
					RunID:      snapshot.State.RunID,
					Kind:       EventRunFinished,
					Durability: EventCommitted,
				})
			}
			return LoopResult{Disposition: LoopFinished, Result: snapshot.State.Result}, nil
		}

		effect, err := Next(snapshot.State)
		if err != nil {
			return LoopResult{}, err
		}

		switch eff := effect.(type) {
		case NeedModelRequest:
			if err := l.planAndPrepare(ctx, controlCtx, runtime, &snapshot, eff.Hint); err != nil {
				return LoopResult{}, err
			}
		case StartModelCall:
			if err := l.runModelStep(ctx, controlCtx, runtime, events, &snapshot, eff.StepID); err != nil {
				return LoopResult{}, err
			}
		case StartToolCalls:
			if err := l.runToolCalls(ctx, controlCtx, runtime, events, &snapshot, eff); err != nil {
				return LoopResult{}, err
			}
		case WaitForResponse:
			return LoopResult{Disposition: LoopWaiting, Reason: WaitingForResponse, Waiting: eff.Requests}, nil
		case WaitForExecutionRecovery:
			return LoopResult{Disposition: LoopWaiting, Reason: ExecutionRecovery}, nil
		default:
			return LoopResult{}, fmt.Errorf("agent: loop: unknown effect %T", effect)
		}
	}
}

// commit builds the envelope via the sanctioned constructor and submits it.
func (l *Loop) commit(ctx context.Context, runtime Runtime, run RunID, id CommandID, base uint64, grant ExecutionGrant, cmd AgentCommand) (CommitResult, error) {
	env, err := BuildEnvelope(run, id, cmd)
	if err != nil {
		return CommitResult{}, err
	}
	return runtime.Commit(ctx, CommitRequest{BaseRevision: base, Grant: grant, Command: env})
}

// retriable reports the commit errors that mean "reload and rederive".
func retriable(err error) bool {
	return errors.Is(err, ErrStaleRuntime) || errors.Is(err, ErrRunTerminal) || errors.Is(err, ErrCommandConflict)
}

func freshCommandID() CommandID {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("agent: loop: %v", err))
	}
	return CommandID(hex.EncodeToString(b[:]))
}

func (l *Loop) emitCommitted(ctx context.Context, events EventSink, run RunID, committed []AgentEvent) {
	if events == nil {
		return
	}
	for i := range committed {
		e := committed[i]
		_ = events.Emit(ctx, Event{
			RunID:      run,
			Kind:       EventAgentCommitted,
			Durability: EventCommitted,
			Canonical:  &e,
		})
	}
}

// --- NeedModelRequest ---

func (l *Loop) planAndPrepare(ctx, controlCtx context.Context, runtime Runtime, snapshot *RuntimeSnapshot, hint PlanningHint) error {
	plan, err := l.Planner.Plan(ctx, hint)
	if err != nil {
		return err
	}
	if plan.Model != hint.Model {
		return fmt.Errorf("agent: loop: plan model %q does not match hint model %q", plan.Model, hint.Model)
	}
	frozenRequest, err := FreezeModelRequest(plan.Request)
	if err != nil {
		return err
	}
	if ModelRef(frozenRequest.Model) != plan.Model {
		return fmt.Errorf("agent: loop: request model %q does not match plan model %q", frozenRequest.Model, plan.Model)
	}
	requestDigest, err := DigestRequest(frozenRequest)
	if err != nil {
		return err
	}
	toolsDigest, err := DigestToolSpecs(plan.Tools)
	if err != nil {
		return err
	}
	binding, err := DigestModelStepBinding(plan.Model, requestDigest, toolsDigest)
	if err != nil {
		return err
	}
	cmdID := DeriveModelRequestCommandID(snapshot.State.RunID, snapshot.Revision)
	stepID := DeriveModelStepID(snapshot.State.RunID, cmdID, binding)
	_, err = l.commit(controlCtx, runtime, snapshot.State.RunID, cmdID, snapshot.Revision, "", PrepareModelRequest{
		StepID:        stepID,
		Model:         plan.Model,
		Request:       frozenRequest,
		RequestDigest: requestDigest,
		InputIDs:      plan.InputIDs,
		PlanningToken: plan.PlanningToken,
		Tools:         plan.Tools,
		ToolsDigest:   toolsDigest,
	})
	if err == nil {
		return nil
	}
	if !retriable(err) {
		return err
	}
	// A retriable rejection with no authority progress means the rejection
	// was about THIS plan's content (InputIDs, digests), not concurrency:
	// retrying the same planner at the same revision would spin forever.
	after, loadErr := runtime.Load(controlCtx)
	if loadErr != nil {
		return loadErr
	}
	if after.Revision == snapshot.Revision {
		return fmt.Errorf("agent: loop: prepare rejected without authority progress: %w", err)
	}
	return nil // another actor advanced the run; reload decides the next action
}

// --- StartModelCall ---

func (l *Loop) runModelStep(ctx, controlCtx context.Context, runtime Runtime, events EventSink, snapshot *RuntimeSnapshot, stepID StepID) error {
	run := snapshot.State.RunID
	start, err := l.commit(controlCtx, runtime, run, freshCommandID(), snapshot.Revision, "", StartModelExecution{StepID: stepID})
	if err != nil {
		if retriable(err) {
			return nil
		}
		return err
	}
	if start.Status == CommitAlreadyApplied {
		return nil // another attempt owns it; reload
	}
	l.emitCommitted(controlCtx, events, run, start.Events)

	modelStep, ok := start.Snapshot.State.Current.(ModelStep)
	if !ok || modelStep.RefValue.ID != stepID {
		return fmt.Errorf("agent: loop: started step %q is not current", stepID)
	}

	var completion AgentCommand
	invoker, resolveErr := l.Models.Resolve(modelStep.Model)
	if resolveErr != nil {
		completion = SubmitModelFailure{StepID: stepID, Failure: StepFailure{Class: FailureProvider, Message: resolveErr.Error()}}
	} else {
		// Model workers derive from the outer ctx: cancelling a model call is
		// safe, the frozen request retries after recovery (spec §6.1).
		sdkRequest, err := modelStep.Request.SDK()
		if err != nil {
			completion = SubmitModelFailure{StepID: stepID, Failure: StepFailure{Class: FailureProvider, Message: err.Error()}}
		} else {
			result, invokeErr := l.invokeModel(ctx, invoker, &sdkRequest, run, stepID, events)
			switch {
			case invokeErr != nil && ctx.Err() != nil:
				completion = RecoverModelExecution{StepID: stepID}
			case invokeErr != nil:
				completion = SubmitModelFailure{StepID: stepID, Failure: StepFailure{Class: FailureProvider, Message: invokeErr.Error()}}
			default:
				bindings, bindErr := l.bindToolCalls(&result, &modelStep)
				if bindErr != nil {
					completion = RejectModelResult{StepID: stepID, Usage: UsageFromSDK(result.Usage),
						Failure: StepFailure{Class: FailureMalformedModel, Message: bindErr.Error()}}
				} else if frozenResult, freezeErr := FreezeModelResult(result); freezeErr != nil {
					completion = RejectModelResult{StepID: stepID, Usage: UsageFromSDK(result.Usage),
						Failure: StepFailure{Class: FailureMalformedModel, Message: freezeErr.Error()}}
				} else {
					completion = SubmitModelResult{StepID: stepID, Result: frozenResult, Calls: bindings}
				}
			}
		}
	}

	res, err := l.commit(controlCtx, runtime, run, freshCommandID(), start.Snapshot.Revision, start.Grant, completion)
	if err != nil {
		if retriable(err) {
			return nil
		}
		return err
	}
	l.emitCommitted(controlCtx, events, run, res.Events)
	return nil
}

func (l *Loop) invokeModel(ctx context.Context, invoker ModelInvoker, req *sdk.Request, run RunID, step StepID, events EventSink) (sdk.ModelResult, error) {
	if l.Streaming {
		if streamer, ok := invoker.(StreamingModelInvoker); ok {
			stream, err := streamer.Stream(ctx, *req)
			if err != nil {
				return sdk.ModelResult{}, err
			}
			// The range has an explicit ctx escape: a stream that stops
			// sending without closing Parts must not block cancellation and
			// the recovery path behind it.
		consume:
			for {
				select {
				case part, open := <-stream.Parts:
					if !open {
						break consume
					}
					if events == nil {
						continue
					}
					switch p := part.(type) {
					case *sdk.TextDeltaPart:
						_ = events.Emit(ctx, Event{
							RunID: run, StepID: step,
							Kind: EventModelTextDelta, Durability: EventProvisional,
							Payload: mustJSON(p.Text),
						})
					case *sdk.ReasoningDeltaPart:
						_ = events.Emit(ctx, Event{
							RunID: run, StepID: step,
							Kind: EventModelReasoningDelta, Durability: EventProvisional,
							Payload: mustJSON(p.Text),
						})
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

// bindToolCalls validates tool-call IDs/order/shape and produces bindings
// from the frozen ToolSpecs (spec §4.1). It never calls ExecutableTool.
func (l *Loop) bindToolCalls(result *sdk.ModelResult, step *ModelStep) ([]ToolCallBinding, error) {
	if len(result.ToolCalls) == 0 {
		return nil, nil
	}
	specByName := make(map[string]ToolSpec, len(step.Tools))
	for _, s := range step.Tools {
		specByName[s.Definition.Name] = s
	}
	seen := make(map[string]bool, len(result.ToolCalls))
	bindings := make([]ToolCallBinding, len(result.ToolCalls))
	for i, tc := range result.ToolCalls {
		if tc.ToolCallID == "" {
			return nil, fmt.Errorf("tool call %d has an empty id", i)
		}
		if seen[tc.ToolCallID] {
			return nil, fmt.Errorf("duplicate tool call id %q", tc.ToolCallID)
		}
		seen[tc.ToolCallID] = true
		args, err := canonicalToolArguments(tc.Input)
		if err != nil {
			// A single call with unparsable arguments is NOT structurally
			// malformed: bind it raw and let StartToolCalls close it as a
			// known invalid_arguments failure (spec §4.1).
			args = rawToolArguments(tc.Input)
		}
		b := ToolCallBinding{
			CallID:    CallID(tc.ToolCallID),
			ToolRef:   ToolRef(tc.ToolName),
			Arguments: args,
			Policy:    DirectExecution,
		}
		if spec, known := specByName[tc.ToolName]; known {
			// The binding's ToolRef is the frozen spec's Ref — the catalog
			// key — not the model-facing definition name; the two may differ
			// (aliased tools).
			b.ToolRef = spec.Ref
			b.DefinitionDigest = spec.DefinitionDigest
			b.Policy = spec.Policy
		}
		bd, err := digestToolCallBinding(b.CallID, b.DefinitionDigest, b.Policy, b.Arguments)
		if err != nil {
			return nil, err
		}
		b.BindingDigest = bd
		bindings[i] = b
	}
	return bindings, nil
}

// --- StartToolCalls ---

type startedWorker struct {
	call  ToolCallState
	grant ExecutionGrant
	base  uint64
	tool  ExecutableTool
}

func (l *Loop) runToolCalls(ctx, controlCtx context.Context, runtime Runtime, events EventSink, snapshot *RuntimeSnapshot, eff StartToolCalls) error {
	run := snapshot.State.RunID
	ts, ok := snapshot.State.Current.(ToolStep)
	if !ok || ts.RefValue.ID != eff.StepID {
		return fmt.Errorf("agent: loop: tool step %q is not current", eff.StepID)
	}

	limit := l.Execution.MaxParallel
	if limit <= 0 {
		limit = 1
	}
	var started []startedWorker
	for _, callID := range eff.CallIDs {
		if len(started) >= limit {
			break
		}
		// Outer ctx cancelled: stop starting new calls; settle what we own.
		if ctx.Err() != nil {
			break
		}
		i := ts.callIndex(callID)
		if i < 0 {
			continue
		}
		call := ts.Calls[i]

		tool, resolveErr := l.Tools.Resolve(call.ToolRef)
		var known *ToolFailure
		switch {
		case resolveErr != nil:
			known = &ToolFailure{Class: FailureToolLookup, Message: resolveErr.Error()}
		default:
			toolDef, err := FreezeToolDefinition(tool.Definition())
			if err != nil {
				return err
			}
			defDigest, err := DigestToolDefinition(toolDef)
			if err != nil {
				return err
			}
			switch {
			case tool.Ref() != call.ToolRef || defDigest != call.DefinitionDigest:
				known = &ToolFailure{Class: FailureDefinitionMismatch, Message: "tool definition digest mismatch"}
			case tool.ResponsePolicy() != call.Policy:
				known = &ToolFailure{Class: FailureDefinitionMismatch, Message: "response policy mismatch"}
			default:
				if argErr := tool.ValidateArguments(call.Arguments); argErr != nil {
					known = &ToolFailure{Class: FailureInvalidArguments, Message: argErr.Error()}
				}
			}
		}
		if known != nil {
			// Known failure of a Pending call: no start barrier, no tool call.
			res, err := l.commit(controlCtx, runtime, run, freshCommandID(), snapshot.Revision, "",
				SubmitToolFailure{StepID: eff.StepID, CallID: callID, Failure: *known, Outcome: ToolOutcomeKnown})
			if err != nil {
				settleErr := l.settleWorkers(ctx, controlCtx, runtime, events, run, eff.StepID, started)
				if !retriable(err) {
					return err
				}
				return settleErr
			}
			l.emitCommitted(controlCtx, events, run, res.Events)
			continue
		}

		start, err := l.commit(controlCtx, runtime, run, freshCommandID(), snapshot.Revision, "",
			StartToolCall{StepID: eff.StepID, CallID: callID})
		if err != nil {
			settleErr := l.settleWorkers(ctx, controlCtx, runtime, events, run, eff.StepID, started)
			if retriable(err) {
				return settleErr
			}
			return err
		}
		if start.Status == CommitAlreadyApplied {
			continue // another attempt owns this call
		}
		l.emitCommitted(controlCtx, events, run, start.Events)
		if events != nil {
			_ = events.Emit(controlCtx, Event{RunID: run, StepID: eff.StepID, CallID: callID,
				Kind: EventToolStarted, Durability: EventCommitted})
		}
		started = append(started, startedWorker{call: call, grant: start.Grant, base: start.Snapshot.Revision, tool: tool})
	}

	return l.settleWorkers(ctx, controlCtx, runtime, events, run, eff.StepID, started)
}

// settleWorkers executes every started worker and commits its outcome. An
// accepted start is never abandoned (spec §6.2). Tool workers do not inherit
// outer-ctx cancellation (spec §6.1); one Unknown cancels the rest. A commit
// that fails with a non-sentinel error is replayed once with the same
// CommandID (spec §6.6 "commit response unknown"); a still-failing commit is
// reported so the host does not mistake a wedged call for progress.
func (l *Loop) settleWorkers(ctx, controlCtx context.Context, runtime Runtime, events EventSink, run RunID, stepID StepID, started []startedWorker) error {
	if len(started) == 0 {
		return nil
	}
	execCtx, cancelAll := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelAll()

	var mu sync.Mutex
	var wg sync.WaitGroup
	var firstErr error
	for i := range started {
		wg.Add(1)
		w := started[i]
		go func(w startedWorker) {
			defer wg.Done()
			req := ToolExecutionRequest{
				RunID:            run,
				StepID:           stepID,
				CallID:           w.call.CallID,
				ToolRef:          w.call.ToolRef,
				DefinitionDigest: w.call.DefinitionDigest,
				Arguments:        w.call.Arguments,
				Progress:         &progressSink{events: events, run: run, step: stepID, call: w.call.CallID},
			}
			outcome := executeToolSafely(execCtx, w.tool, &req)

			var cmd AgentCommand
			unknown := false
			switch o := outcome.(type) {
			case ToolExecutionSucceeded:
				cmd = SubmitToolResult{StepID: stepID, CallID: w.call.CallID, Result: o.Result}
			case ToolExecutionFailed:
				cmd = SubmitToolFailure{StepID: stepID, CallID: w.call.CallID, Failure: o.Failure, Outcome: ToolOutcomeKnown}
			case ToolExecutionUnknown:
				failure := o.Failure
				if failure.Class == "" {
					failure.Class = FailureEffectUnknown
				}
				cmd = SubmitToolFailure{StepID: stepID, CallID: w.call.CallID, Failure: failure, Outcome: ToolOutcomeUnknown}
				unknown = true
			default:
				cmd = SubmitToolFailure{StepID: stepID, CallID: w.call.CallID,
					Failure: ToolFailure{Class: FailureEffectUnknown, Message: "tool returned no outcome"}, Outcome: ToolOutcomeUnknown}
				unknown = true
			}

			mu.Lock()
			defer mu.Unlock()
			// Commit with the worker's own grant on its start base; stale
			// bases rebase call-locally. Late results after terminal return
			// ErrRunTerminal and are dropped (audit is the adapter's job).
			cmdID := freshCommandID()
			res, err := l.commit(controlCtx, runtime, run, cmdID, w.base, w.grant, cmd)
			if err != nil && !retriable(err) {
				// One replay with the same CommandID and digest.
				res, err = l.commit(controlCtx, runtime, run, cmdID, w.base, w.grant, cmd)
			}
			switch {
			case err == nil:
				l.emitCommitted(controlCtx, events, run, res.Events)
				if events != nil {
					_ = events.Emit(controlCtx, Event{RunID: run, StepID: stepID, CallID: w.call.CallID,
						Kind: EventToolCompleted, Durability: EventCommitted})
				}
			case retriable(err):
				// Terminal/stale: the authority already settled this call or
				// the run; the result is intentionally dropped.
			default:
				if firstErr == nil {
					firstErr = fmt.Errorf("agent: loop: settling call %q: %w", w.call.CallID, err)
				}
			}
			if unknown {
				cancelAll() // one Unknown cancels sibling workers (spec §6.2)
			}
		}(w)
	}
	wg.Wait()
	return firstErr
}

// executeToolSafely runs an application tool and converts a panic into
// ToolExecutionUnknown: the effect may have happened before the panic, and a
// crashing tool must not take down every run in the process.
func executeToolSafely(ctx context.Context, tool ExecutableTool, req *ToolExecutionRequest) (outcome ToolExecutionOutcome) {
	defer func() {
		if r := recover(); r != nil {
			outcome = ToolExecutionUnknown{Failure: ToolFailure{
				Class:   FailureEffectUnknown,
				Message: fmt.Sprintf("tool panic: %v", r),
			}}
		}
	}()
	return tool.Execute(ctx, *req)
}

type progressSink struct {
	events EventSink
	run    RunID
	step   StepID
	call   CallID
	seq    uint64
	mu     sync.Mutex
}

func (p *progressSink) Publish(ctx context.Context, progress ToolProgress) {
	if p.events == nil {
		return
	}
	p.mu.Lock()
	p.seq++
	seq := p.seq
	p.mu.Unlock()
	_ = p.events.Emit(ctx, Event{
		RunID: p.run, StepID: p.step, CallID: p.call,
		Sequence: seq, Kind: EventToolProgress, Durability: EventProvisional,
		Payload: progress.Payload,
	})
}

func mustJSON(v any) []byte {
	b, err := jsonMarshal(v)
	if err != nil {
		return []byte("null")
	}
	return b
}
