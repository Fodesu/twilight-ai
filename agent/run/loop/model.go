package loop

import (
	"context"
	"errors"
	"fmt"

	run "github.com/felinics/twilight/agent/run"

	"github.com/felinics/twilight/sdk"
)

func (l *Loop) planAndPrepare(ctx context.Context, runtime boundRuntime, events EventSink, snapshot *run.RuntimeSnapshot, hint run.PromptInput) error {
	hint.Session = runtime.sid
	plan, err := l.Builder.Build(ctx, hint)
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
		PromptToken:   plan.Token,
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
	// retrying the same prompt builder at the same revision would spin forever.
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
// that cannot serve the step withdraws it to Open and reports the error: no
// model call has happened.
func (l *Loop) startModelStep(ctx context.Context, runtime boundRuntime, events EventSink, snapshot *run.RuntimeSnapshot, stepID run.StepID) (*AssignmentKey, error) {
	runID := snapshot.State.RunID
	proto, err := snapshot.Protocol()
	if err != nil {
		return nil, err
	}
	prepared, ok := snapshot.State.Current.(run.ModelStep)
	if !ok || prepared.RefValue.ID != stepID {
		return nil, fmt.Errorf("agent: loop: model step %q is not current", stepID)
	}
	a := newAttempt(runID, stepID, "")
	assignment := Assignment{Session: runtime.sid, RunID: runID, StepID: stepID, Claim: a.claim, Schema: snapshot.SchemaVersion,
		Kind: AssignmentModel, Model: &ModelAssignment{Model: prepared.Model, RequestDigest: prepared.RequestDigest}}
	// Pre-start check (RUN-EXE-5): an executor that cannot serve the model
	// fails here, with the step still Prepared and no start or recovery fact.
	unavailable, err := l.Executor.Validate(ctx, assignment)
	if err != nil {
		return nil, err
	}
	if unavailable != nil {
		return nil, fmt.Errorf("%w: %s: %s: %s", ErrModelUnavailable, prepared.Model, unavailable.Class, unavailable.Message)
	}
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

	request, err := runtime.FrozenRequest(ctx, prepared.RequestDigest)
	if err != nil {
		if _, serr := l.settle(context.WithoutCancel(ctx), runtime, events, a, start.Snapshot.Position,
			run.RecoverModelExecution{StepID: stepID, Claim: a.claim}, proto); serr != nil {
			return nil, serr
		}
		return nil, fmt.Errorf("agent: loop: load frozen model request: %w", err)
	}
	assignment.Model.Request = &request

	if err := l.Executor.Dispatch(ctx, assignment); err != nil {
		// Nothing was called: withdraw the step to Open under this attempt's
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

// modelCompletion maps a model Outcome to the attempt's settlement command
// (RUN-LOP-3): a cancelled call withdraws the step to Open (the next Advance
// plans again from the current state); a provider failure is
// SubmitModelFailure; a result that cannot be bound or frozen is
// RejectModelResult with the host's disposition; a result binds its tool
// calls into SubmitModelResult.
//
// A body the executor reports missing also withdraws the step, but the
// condition is returned as an error alongside the command: the settlement
// lands, and the drive stops instead of prompt building, freezing and dispatching
// again against the same missing store. Whether to try again is the host's
// decision, so a persistently unreadable store cannot spin the Run.
func (l *Loop) modelCompletion(step *run.ModelStep, out Outcome) (run.AgentCommand, error) {
	stepID := step.RefValue.ID
	recover := run.RecoverModelExecution{StepID: stepID, Claim: out.Key.Claim}
	switch {
	case out.Err != nil && errors.Is(out.Err, run.ErrFrozenValueMissing):
		return recover, fmt.Errorf("agent: loop: model dispatch: %w", out.Err)
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

// modelRejectDisposition applies Settings.MalformedRetries: the step's
// Rejects counts the malformed results already recorded, so the step is
// retried while that count is below the bound and fails the Run otherwise.
// Zero retries fails on the first malformed result.
func (l *Loop) modelRejectDisposition(step run.ModelStep, _ run.StepFailure) run.ModelRejectDisposition {
	if step.Rejects < int(l.Settings.MalformedRetries) {
		return run.ModelRejectRetry
	}
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
