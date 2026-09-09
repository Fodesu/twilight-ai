package loop

import (
	"context"
	"errors"
	"fmt"

	run "github.com/felinics/twilight/agent/run"

	"github.com/felinics/twilight/sdk"
)

func (l *Loop) planAndPrepare(ctx context.Context, runtime boundRuntime, events EventSink, snapshot *run.RuntimeSnapshot, hint run.PlanningHint) error {
	hint.Session = runtime.sid
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
	cmdID := run.DeriveModelRequestCommandID(snapshot.State.RunID, snapshot.Position)
	stepID := run.DeriveModelStepID(snapshot.State.RunID, cmdID, binding)
	res, err := l.commit(ctx, runtime, snapshot.State.RunID, cmdID, snapshot.Position, run.PrepareModelRequest{
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
		l.emitCommitted(ctx, events, runtime.sid, snapshot.State.RunID, res.Events)
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
	if after.Position == snapshot.Position {
		return fmt.Errorf("agent: loop: prepare rejected without authority progress: %w", err)
	}
	return nil // another actor advanced the run; reload decides the next action
}

// --- StartModelCall ---

// runModelStep owns one model execution attempt. It returns the terminal
// RunResult when its settlement ended the Run (RUN 7: no reload after a
// terminal settlement).
func (l *Loop) runModelStep(ctx context.Context, runtime boundRuntime, events EventSink, snapshot *run.RuntimeSnapshot, stepID run.StepID) (*run.RunResult, error) {
	runID := snapshot.State.RunID
	proto, err := snapshot.Protocol()
	if err != nil {
		return nil, err
	}
	a := newAttempt(runID, stepID, "")
	start, err := l.commit(ctx, runtime, runID, a.startID(), snapshot.Position, run.StartModelExecution{StepID: stepID, Claim: a.claim}, proto)
	if err != nil {
		if retriable(err) {
			return nil, nil // another actor moved the step; reload decides
		}
		return nil, err
	}
	l.emitCommitted(ctx, events, runtime.sid, runID, start.Events)

	modelStep, ok := start.Snapshot.State.Current.(run.ModelStep)
	if !ok || modelStep.RefValue.ID != stepID || modelStep.Status != run.ModelExecuting {
		// The start (or its one-shot replay) landed but the step is no longer
		// Executing: something settled it meanwhile. Reload decides.
		if start.Status == run.CommitAlreadyApplied {
			return nil, nil
		}
		return nil, fmt.Errorf("agent: loop: started step %q is not current", stepID)
	}

	var completion run.AgentCommand
	var catalogErr error
	invoker, resolveErr := l.Models.ResolveModel(modelStep.Model)
	switch {
	case resolveErr != nil:
		catalogErr = resolveErr
		completion = run.RecoverModelExecution{StepID: stepID, Claim: a.claim}
	case invoker == nil:
		catalogErr = errors.New("model catalog returned a nil invoker")
		completion = run.RecoverModelExecution{StepID: stepID, Claim: a.claim}
	default:
		// Model workers derive from the outer ctx: cancelling a model call is
		// safe, the frozen request retries after recovery (RUN-LOP-3). The body
		// is fetched by digest; a missing body cannot be retried by this Loop.
		frozenRequest, fetchErr := runtime.FrozenRequest(ctx, modelStep.RequestDigest)
		var sdkRequest sdk.Request
		if fetchErr == nil {
			sdkRequest, fetchErr = frozenRequest.SDK()
		}
		if fetchErr != nil {
			if errors.Is(fetchErr, run.ErrFrozenValueMissing) {
				// Release ownership so recovery or a fresh plan can proceed;
				// surface the condition to the host.
				if _, err := l.settle(ctx, runtime, events, a, start.Snapshot.Position, run.RecoverModelExecution{StepID: stepID, Claim: a.claim}, proto); err != nil {
					return nil, err
				}
				return nil, fetchErr
			}
			failure := run.StepFailure{Class: run.FailureMalformedModel, Message: fetchErr.Error()}
			completion = run.RejectModelResult{StepID: stepID, Failure: failure, Disposition: l.modelRejectDisposition(modelStep, failure)}
		} else {
			result, invokeErr := l.invokeModel(ctx, invoker, &sdkRequest, runID, stepID, events)
			switch {
			case invokeErr != nil && ctx.Err() != nil:
				completion = run.RecoverModelExecution{StepID: stepID, Claim: a.claim}
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

	finished, err := l.settle(ctx, runtime, events, a, start.Snapshot.Position, completion, proto)
	if err != nil {
		return nil, err
	}
	if catalogErr != nil {
		return nil, fmt.Errorf("agent: loop: model catalog: %w", catalogErr)
	}
	return finished, nil
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
		specByName[s.Name] = s
	}
	bindings := make([]run.ToolCallBinding, len(result.ToolCalls))
	for i, tc := range result.ToolCalls {
		args, err := run.FreezeToolCallInput(tc.Input)
		if err != nil {
			return nil, fmt.Errorf("tool call %d (%q) input: %w", i, tc.ToolCallID, err)
		}
		// The Run's CallID derives from the step and position; the provider's
		// id is carried for the round trip only, so a provider that repeats or
		// omits ids cannot break identity here.
		b := run.ToolCallBinding{
			CallID:         run.DeriveCallID(step.RefValue.ID, i),
			ProviderCallID: tc.ToolCallID,
			ToolRef:        run.ToolRef(tc.ToolName),
			Arguments:      args,
			Policy:         run.DirectExecution,
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
