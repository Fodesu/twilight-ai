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

// startModelStep commits the start barrier of one model attempt and hands the
// call to the Executor (RUN-LOP-3). It returns the dispatched key, or nil when
// the reload should decide (another actor moved the step). A model catalog
// that cannot serve the step releases it back to Prepared and reports the
// error: no model call has happened.
func (l *Loop) startModelStep(ctx context.Context, runtime boundRuntime, events EventSink, snapshot *run.RuntimeSnapshot, stepID run.StepID, deliver Deliver) (*AssignmentKey, error) {
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

	assignment := Assignment{Session: runtime.sid, RunID: runID, StepID: stepID, Claim: a.claim, Schema: snapshot.SchemaVersion,
		Kind: AssignmentModel, Model: &ModelAssignment{Model: modelStep.Model, RequestDigest: modelStep.RequestDigest}}
	if err := l.Executor.Dispatch(ctx, assignment, l.deliverTo(runtime, events, deliver)); err != nil {
		// Nothing was called: release the step to Prepared under this attempt's
		// recovery identity and surface the condition (RUN-LOP-3).
		if _, serr := l.settle(context.WithoutCancel(ctx), runtime, events, a, start.Snapshot.Position,
			run.RecoverModelExecution{StepID: stepID, Claim: a.claim}, proto); serr != nil {
			return nil, serr
		}
		return nil, fmt.Errorf("agent: loop: model dispatch: %w", err)
	}
	key := assignment.Key()
	return &key, nil
}

// deliverTo is the callback a dispatched assignment reports to. A blocking
// Run supplies its own wait-loop callback; a host calling Advance directly
// gets the Outcome settled here, on the executor's goroutine, through the
// Loop's Deliver.
func (l *Loop) deliverTo(runtime boundRuntime, events EventSink, deliver Deliver) Deliver {
	if deliver != nil {
		return deliver
	}
	return func(out Outcome) {
		_, _ = l.Deliver(context.Background(), runtime.rt, runtime.sid, out, events)
	}
}

// modelCompletion maps a model Outcome to the attempt's settlement command
// (RUN-LOP-3): a cancelled or unstartable call recovers the step to Prepared,
// a provider failure is SubmitModelFailure, a result that cannot be bound or
// frozen is RejectModelResult with the host's disposition, and a result binds
// its tool calls into SubmitModelResult. The returned error, when non-nil,
// accompanies a recovery settlement the Loop cannot retry itself (a missing
// frozen body).
func (l *Loop) modelCompletion(step *run.ModelStep, out Outcome) (run.AgentCommand, error) {
	stepID := step.RefValue.ID
	recover := run.RecoverModelExecution{StepID: stepID, Claim: out.Key.Claim}
	switch {
	case out.Err != nil && errors.Is(out.Err, run.ErrFrozenValueMissing):
		return recover, out.Err
	case out.Err != nil && errors.Is(out.Err, errMalformedFrozenRequest):
		failure := run.StepFailure{Class: run.FailureMalformedModel, Message: out.Err.Error()}
		return run.RejectModelResult{StepID: stepID, Failure: failure, Disposition: l.modelRejectDisposition(*step, failure)}, nil
	case out.Err != nil && (out.Cancelled || errors.Is(out.Err, context.Canceled) || errors.Is(out.Err, context.DeadlineExceeded)):
		return recover, nil
	case out.Err != nil:
		return run.SubmitModelFailure{StepID: stepID, Failure: run.StepFailure{Class: run.FailureProvider, Message: out.Err.Error()}}, nil
	case out.Model == nil:
		if out.Cancelled {
			return recover, nil
		}
		return run.SubmitModelFailure{StepID: stepID, Failure: run.StepFailure{Class: run.FailureProvider, Message: "executor delivered no result"}}, nil
	}
	result := *out.Model
	bindings, bindErr := l.bindToolCalls(&result, step)
	if bindErr != nil {
		failure := run.StepFailure{Class: run.FailureMalformedModel, Message: bindErr.Error()}
		return run.RejectModelResult{StepID: stepID, Usage: run.UsageFromSDK(result.Usage), Failure: failure,
			Disposition: l.modelRejectDisposition(*step, failure)}, nil
	}
	frozenResult, freezeErr := run.FreezeModelResult(result)
	if freezeErr != nil {
		failure := run.StepFailure{Class: run.FailureMalformedModel, Message: freezeErr.Error()}
		return run.RejectModelResult{StepID: stepID, Usage: run.UsageFromSDK(result.Usage), Failure: failure,
			Disposition: l.modelRejectDisposition(*step, failure)}, nil
	}
	return run.SubmitModelResult{StepID: stepID, Result: frozenResult, Calls: bindings, Scheduling: l.toolScheduling()}, nil
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
