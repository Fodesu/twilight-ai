package loop

import (
	"context"
	"errors"
	"fmt"

	run "github.com/memohai/twilight/agent/run"

	"github.com/memohai/twilight/sdk"
)

func (l *Loop) planAndPrepare(ctx context.Context, runtime run.Runtime, events EventSink, snapshot *run.RuntimeSnapshot, hint run.PlanningHint) error {
	plan, err := l.Planner.Plan(ctx, hint)
	if err != nil {
		return err
	}
	frozenRequest, err := run.FreezeModelRequest(plan.Request)
	if err != nil {
		return err
	}
	model := plan.Model
	if model == "" {
		model = run.ModelRef(frozenRequest.Model)
	}
	if model == "" {
		return fmt.Errorf("agent: loop: empty model")
	}
	if run.ModelRef(frozenRequest.Model) != model {
		return fmt.Errorf("agent: loop: request model %q does not match plan model %q", frozenRequest.Model, model)
	}
	proto, err := snapshot.Protocol()
	if err != nil {
		return err
	}
	requestDigest, err := proto.DigestRequest(frozenRequest)
	if err != nil {
		return err
	}
	toolsDigest, err := proto.DigestToolSpecs(plan.Tools)
	if err != nil {
		return err
	}
	binding, err := proto.DigestModelStepBinding(model, requestDigest, toolsDigest)
	if err != nil {
		return err
	}
	cmdID := run.DeriveModelRequestCommandID(snapshot.State.RunID, snapshot.Revision)
	stepID := run.DeriveModelStepID(snapshot.State.RunID, cmdID, binding)
	res, err := l.commit(ctx, runtime, snapshot.State.RunID, cmdID, snapshot.Revision, "", run.PrepareModelRequest{
		StepID:        stepID,
		Model:         model,
		Request:       frozenRequest,
		RequestDigest: requestDigest,
		InputIDs:      plan.InputIDs,
		PlanningToken: plan.PlanningToken,
		Tools:         plan.Tools,
		ToolsDigest:   toolsDigest,
	}, proto)
	if err == nil {
		// ModelStepPrepared carries the frozen request — the most informative
		// fact of the run; observers must see it like every other accepted
		// transition.
		l.emitCommitted(ctx, events, snapshot.State.RunID, res.Events)
		return nil
	}
	if !retriable(err) {
		return err
	}
	// A retriable rejection with no authority progress means the rejection
	// was about THIS plan's content (InputIDs, digests), not concurrency:
	// retrying the same planner at the same revision would spin forever.
	after, loadErr := runtime.Load(ctx, snapshot.State.RunID)
	if loadErr != nil {
		return loadErr
	}
	if after.Revision == snapshot.Revision {
		return fmt.Errorf("agent: loop: prepare rejected without authority progress: %w", err)
	}
	return nil // another actor advanced the run; reload decides the next action
}

// --- StartModelCall ---

func (l *Loop) runModelStep(ctx context.Context, runtime run.Runtime, events EventSink, snapshot *run.RuntimeSnapshot, stepID run.StepID) error {
	runID := snapshot.State.RunID
	proto, err := snapshot.Protocol()
	if err != nil {
		return err
	}
	key := startKey{runID: runID, stepID: stepID}
	attempt := l.startFor(key)
	start, err := l.commit(ctx, runtime, runID, attempt.commandID, snapshot.Revision, "", run.StartModelExecution{StepID: stepID, Claim: attempt.claim}, proto)
	if err != nil {
		if retriable(err) {
			l.forgetStart(key)
			return nil
		}
		// The start may have committed while its response was lost. Keep the
		// command identity so a later Run can replay it and recover the grant.
		return err
	}
	if start.Status == run.CommitAlreadyApplied && start.Grant == "" {
		l.forgetStart(key)
		return nil // another attempt owns it; reload
	}
	if start.Grant == "" {
		return errors.New("agent: loop: start model returned no execution grant")
	}
	l.emitCommitted(ctx, events, runID, start.Events)

	modelStep, ok := start.Snapshot.State.Current.(run.ModelStep)
	if !ok || modelStep.RefValue.ID != stepID || modelStep.Status != run.ModelExecuting {
		// An exact start replay can race with another owner that already
		// settled the step. The Runtime returns the original grant for replay,
		// but executing again would duplicate the provider effect; reload and
		// let the next machine state decide what to do.
		if start.Status == run.CommitAlreadyApplied {
			l.forgetStart(key)
			return nil
		}
		return fmt.Errorf("agent: loop: started step %q is not current", stepID)
	}

	var completion run.AgentCommand
	var catalogErr error
	invoker, resolveErr := l.Models.Resolve(modelStep.Model)
	switch {
	case resolveErr != nil:
		catalogErr = resolveErr
		completion = run.RecoverModelExecution{StepID: stepID, Claim: attempt.claim}
	case invoker == nil:
		catalogErr = errors.New("model catalog returned a nil invoker")
		completion = run.RecoverModelExecution{StepID: stepID, Claim: attempt.claim}
	default:
		// Model workers derive from the outer ctx: cancelling a model call is
		// safe, the frozen request retries after recovery (RUN-LOP-3).
		sdkRequest, err := modelStep.Request.SDK()
		if err != nil {
			failure := run.StepFailure{Class: run.FailureMalformedModel, Message: err.Error()}
			completion = run.RejectModelResult{StepID: stepID, Failure: failure, Disposition: l.modelRejectDisposition(modelStep, failure)}
		} else {
			result, invokeErr := l.invokeModel(ctx, invoker, &sdkRequest, runID, stepID, events)
			switch {
			case invokeErr != nil && ctx.Err() != nil:
				completion = run.RecoverModelExecution{StepID: stepID, Claim: attempt.claim}
			case invokeErr != nil:
				completion = run.SubmitModelFailure{StepID: stepID, Failure: run.StepFailure{Class: run.FailureProvider, Message: invokeErr.Error()}}
			default:
				bindings, bindErr := l.bindToolCalls(&result, &modelStep)
				if bindErr != nil {
					completion = run.RejectModelResult{StepID: stepID, Usage: run.UsageFromSDK(result.Usage),
						Failure:     run.StepFailure{Class: run.FailureMalformedModel, Message: bindErr.Error()},
						Disposition: l.modelRejectDisposition(modelStep, run.StepFailure{Class: run.FailureMalformedModel, Message: bindErr.Error()})}
				} else if frozenResult, freezeErr := run.FreezeModelResult(result); freezeErr != nil {
					completion = run.RejectModelResult{StepID: stepID, Usage: run.UsageFromSDK(result.Usage),
						Failure:     run.StepFailure{Class: run.FailureMalformedModel, Message: freezeErr.Error()},
						Disposition: l.modelRejectDisposition(modelStep, run.StepFailure{Class: run.FailureMalformedModel, Message: freezeErr.Error()})}
				} else {
					completion = run.SubmitModelResult{StepID: stepID, Result: frozenResult, Calls: bindings, Scheduling: l.toolScheduling()}
				}
			}
		}
	}

	completionID := freshCommandID()
	if _, recovering := completion.(run.RecoverModelExecution); recovering {
		completionID = run.DeriveModelRecoveryCommandID(runID, stepID, attempt.claim)
	}
	settlement := l.settlementFor(key, completionID, start.Snapshot.Revision, start.Grant, completion)
	settlementCtx := context.WithoutCancel(ctx)
	res, err := l.commit(settlementCtx, runtime, runID, settlement.commandID, settlement.base, settlement.grant, settlement.command, proto)
	if err != nil {
		if retriable(err) {
			l.forgetSettlement(key)
			l.forgetStart(key)
			return nil
		}
		return err
	}
	l.forgetSettlement(key)
	l.forgetStart(key)
	l.emitCommitted(ctx, events, runID, res.Events)
	if catalogErr != nil {
		return fmt.Errorf("agent: loop: model catalog: %w", catalogErr)
	}
	return nil
}

func (l *Loop) modelRejectDisposition(step run.ModelStep, failure run.StepFailure) run.ModelRejectDisposition {
	if l.Execution.OnMalformedModelResult != nil {
		disposition := l.Execution.OnMalformedModelResult(step, failure)
		if disposition == run.ModelRejectRetry || disposition == run.ModelRejectFailRun {
			return disposition
		}
		// Do not leave a model step Executing because a host callback returned
		// an unknown enum value; a malformed result must still settle.
		return run.ModelRejectFailRun
	}
	// A malformed result is never retried implicitly. Hosts that want a retry
	// must provide the handler and return ModelRejectRetry explicitly.
	return run.ModelRejectFailRun
}

func (l *Loop) invokeModel(ctx context.Context, invoker ModelInvoker, req *sdk.Request, runID run.RunID, step run.StepID, events EventSink) (sdk.ModelResult, error) {
	if l.Streaming {
		if streamer, ok := invoker.(StreamingModelInvoker); ok {
			stream, err := streamer.Stream(ctx, *req)
			if err != nil {
				return sdk.ModelResult{}, err
			}
			// The range has an explicit ctx escape: a stream that stops
			// sending without closing Parts must not block cancellation and
			// the recovery path behind it.
			var sequence uint64
			emitDelta := func(kind EventKind, payload any) {
				if events == nil {
					return
				}
				sequence++
				_ = events.Emit(ctx, Event{RunID: runID, StepID: step,
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
					if events == nil {
						continue
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

// bindToolCalls validates tool-call IDs/order/shape and produces bindings
// from the frozen ToolSpecs (RUN-MCH-2). It never calls ExecutableTool.
func (l *Loop) bindToolCalls(result *sdk.ModelResult, step *run.ModelStep) ([]run.ToolCallBinding, error) {
	if len(result.ToolCalls) == 0 {
		return nil, nil
	}
	specByName := make(map[string]run.ToolSpec, len(step.Tools))
	for _, s := range step.Tools {
		specByName[s.Definition.Name] = s
	}
	seen := make(map[string]bool, len(result.ToolCalls))
	bindings := make([]run.ToolCallBinding, len(result.ToolCalls))
	for i, tc := range result.ToolCalls {
		if tc.ToolCallID == "" {
			return nil, fmt.Errorf("tool call %d has an empty id", i)
		}
		if seen[tc.ToolCallID] {
			return nil, fmt.Errorf("duplicate tool call id %q", tc.ToolCallID)
		}
		seen[tc.ToolCallID] = true
		args, err := run.FreezeToolCallInput(tc.Input)
		if err != nil {
			return nil, fmt.Errorf("tool call %q input: %w", tc.ToolCallID, err)
		}
		b := run.ToolCallBinding{
			CallID:    run.CallID(tc.ToolCallID),
			ToolRef:   run.ToolRef(tc.ToolName),
			Arguments: args,
			Policy:    run.DirectExecution,
		}
		if spec, known := specByName[tc.ToolName]; known {
			// The binding's ToolRef is the frozen spec's Ref — the catalog
			// key — not the model-facing definition name; the two may differ
			// (aliased tools).
			b.ToolRef = spec.Ref
			b.DefinitionDigest = spec.DefinitionDigest
			b.Policy = spec.Policy
		}
		bd, err := run.DigestToolCallBinding(b.CallID, b.DefinitionDigest, b.Policy, b.Arguments)
		if err != nil {
			return nil, err
		}
		b.BindingDigest = bd
		bindings[i] = b
	}
	return bindings, nil
}
