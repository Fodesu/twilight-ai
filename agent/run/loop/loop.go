package loop

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	run "github.com/memohai/twilight/agent/run"

	"github.com/memohai/twilight/sdk"
)

// Loop is the in-process interpreter of one Run (RUN-LOP-2). It holds no
// authoritative state; every iteration starts from Runtime.Load.
type Loop struct {
	Models    ModelCatalog
	Tools     ToolCatalog
	Planner   RequestPlanner
	Execution ExecutionPolicy
	Streaming bool

	// starts retains the identity of an in-flight start command across a
	// transient Run return. A commit may have succeeded while its response was
	// lost; reusing the same command and claim lets the next Run recover the
	// original grant without issuing a second start.
	startsMu sync.Mutex
	starts   map[startKey]startAttempt

	settlementsMu sync.Mutex
	settlements   map[startKey]settlementAttempt

	runsMu   sync.Mutex
	runs     map[run.RunID]struct{}
	eventsMu sync.Mutex
}

type startKey struct {
	runID  run.RunID
	stepID run.StepID
	callID run.CallID
}

type startAttempt struct {
	commandID run.CommandID
	claim     run.ExecutionClaim
}

type settlementAttempt struct {
	commandID run.CommandID
	base      uint64
	grant     run.ExecutionGrant
	command   run.AgentCommand
}

// New validates and normalizes the execution policy (RUN-LOP-1).
func New(models ModelCatalog, tools ToolCatalog, planner RequestPlanner, policy ExecutionPolicy, streaming bool) (*Loop, error) {
	if models == nil {
		return nil, errors.New("agent: loop: nil model catalog")
	}
	if tools == nil {
		return nil, errors.New("agent: loop: nil tool catalog")
	}
	if planner == nil {
		return nil, errors.New("agent: loop: nil request planner")
	}
	if policy.ToolExecution != "" && policy.ToolExecution != ToolExecutionParallel && policy.ToolExecution != ToolExecutionSequential {
		return nil, fmt.Errorf("agent: loop: unknown ToolExecution mode %q", policy.ToolExecution)
	}
	if policy.MaxParallel < 0 {
		return nil, errors.New("agent: loop: negative MaxParallel")
	}
	if policy.ToolExecution == "" {
		policy.ToolExecution = ToolExecutionParallel
	}
	return &Loop{Models: models, Tools: tools, Planner: planner, Execution: policy, Streaming: streaming,
		starts: make(map[startKey]startAttempt), settlements: make(map[startKey]settlementAttempt), runs: make(map[run.RunID]struct{})}, nil
}

func (l *Loop) startFor(key startKey) startAttempt {
	l.startsMu.Lock()
	defer l.startsMu.Unlock()
	if attempt, ok := l.starts[key]; ok {
		return attempt
	}
	attempt := startAttempt{commandID: freshCommandID(), claim: freshExecutionClaim()}
	l.starts[key] = attempt
	return attempt
}

func (l *Loop) forgetStart(key startKey) {
	l.startsMu.Lock()
	delete(l.starts, key)
	l.startsMu.Unlock()
}

func (l *Loop) lookupStart(key startKey) (startAttempt, bool) {
	l.startsMu.Lock()
	defer l.startsMu.Unlock()
	attempt, ok := l.starts[key]
	return attempt, ok
}

func (l *Loop) acquireRun(runID run.RunID) error {
	l.runsMu.Lock()
	defer l.runsMu.Unlock()
	if _, ok := l.runs[runID]; ok {
		return ErrRunAlreadyRunning
	}
	l.runs[runID] = struct{}{}
	return nil
}

func (l *Loop) releaseRun(runID run.RunID) {
	l.runsMu.Lock()
	delete(l.runs, runID)
	l.runsMu.Unlock()
}

func (l *Loop) settlementFor(key startKey, commandID run.CommandID, base uint64, grant run.ExecutionGrant, command run.AgentCommand) settlementAttempt {
	l.settlementsMu.Lock()
	defer l.settlementsMu.Unlock()
	if attempt, ok := l.settlements[key]; ok {
		return attempt
	}
	attempt := settlementAttempt{commandID: commandID, base: base, grant: grant, command: command}
	l.settlements[key] = attempt
	return attempt
}

func (l *Loop) lookupSettlement(key startKey) (settlementAttempt, bool) {
	l.settlementsMu.Lock()
	defer l.settlementsMu.Unlock()
	attempt, ok := l.settlements[key]
	return attempt, ok
}

func (l *Loop) forgetSettlement(key startKey) {
	l.settlementsMu.Lock()
	delete(l.settlements, key)
	l.settlementsMu.Unlock()
}

func (l *Loop) forgetRunCaches(runID run.RunID) {
	l.startsMu.Lock()
	for key := range l.starts {
		if key.runID == runID {
			delete(l.starts, key)
		}
	}
	l.startsMu.Unlock()
	l.settlementsMu.Lock()
	for key := range l.settlements {
		if key.runID == runID {
			delete(l.settlements, key)
		}
	}
	l.settlementsMu.Unlock()
}

type serializedEventSink struct {
	sink EventSink
	mu   *sync.Mutex
}

func (s *serializedEventSink) Emit(ctx context.Context, event Event) error {
	if s == nil || s.sink == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sink.Emit(ctx, event)
}

// resumeSettlement replays a result whose first commit may have succeeded
// while its response was lost. The same command identity makes the retry
// idempotent and avoids re-running the external effect.
func (l *Loop) resumeSettlement(ctx context.Context, runtime run.Runtime, events EventSink, snapshot *run.RuntimeSnapshot) (bool, error) {
	runID := snapshot.State.RunID
	l.settlementsMu.Lock()
	keys := make([]startKey, 0)
	for key := range l.settlements {
		if key.runID == runID {
			keys = append(keys, key)
		}
	}
	l.settlementsMu.Unlock()
	proto, err := snapshot.Protocol()
	if err != nil {
		return false, err
	}
	for _, key := range keys {
		attempt, ok := l.lookupSettlement(key)
		if !ok {
			continue
		}
		if !settlementTargetActive(snapshot.State, key) {
			l.forgetSettlement(key)
			l.forgetStart(key)
			continue
		}
		res, err := l.commit(context.WithoutCancel(ctx), runtime, runID, attempt.commandID, attempt.base, attempt.grant, attempt.command, proto)
		if err != nil {
			if retriable(err) {
				l.forgetSettlement(key)
				l.forgetStart(key)
				return true, nil
			}
			return false, err
		}
		l.forgetSettlement(key)
		l.forgetStart(key)
		l.emitCommitted(ctx, events, runID, res.Events)
		return true, nil
	}
	return false, nil
}

func settlementTargetActive(state run.MachineState, key startKey) bool {
	switch current := state.Current.(type) {
	case run.ModelStep:
		return key.callID == "" && current.RefValue.ID == key.stepID && current.Status == run.ModelExecuting
	case run.ToolStep:
		if current.RefValue.ID != key.stepID {
			return false
		}
		for _, call := range current.Calls {
			if call.CallID == key.callID {
				return call.Status == run.ToolExecuting
			}
		}
	}
	return false
}

// resumeCachedStart re-enters an accepted start after a Loop.Run returned
// before receiving its grant. The authority state is Executing, so Next alone
// would otherwise wait for recovery instead of replaying the known start.
func (l *Loop) resumeCachedStart(ctx context.Context, runtime run.Runtime, events EventSink, snapshot *run.RuntimeSnapshot) (bool, error) {
	runID := snapshot.State.RunID
	switch current := snapshot.State.Current.(type) {
	case run.ModelStep:
		if current.Status != run.ModelExecuting {
			return false, nil
		}
		if _, ok := l.lookupStart(startKey{runID: runID, stepID: current.RefValue.ID}); !ok {
			return false, nil
		}
		return true, l.runModelStep(ctx, runtime, events, snapshot, current.RefValue.ID)
	case run.ToolStep:
		var ids []run.CallID
		for _, call := range current.Calls {
			if call.Status != run.ToolExecuting {
				continue
			}
			if _, ok := l.lookupStart(startKey{runID: runID, stepID: current.RefValue.ID, callID: call.CallID}); ok {
				ids = append(ids, call.CallID)
			}
		}
		if len(ids) == 0 {
			return false, nil
		}
		return true, l.runToolCalls(ctx, runtime, events, snapshot, run.StartToolCalls{StepID: current.RefValue.ID, CallIDs: ids})
	default:
		return false, nil
	}
}

// Run drives the Run until it finishes, has no executable effect, or the
// context is cancelled (RUN-LOP-2). The caller context remains active for
// reads and normal control commits. Accepted effect settlements use a
// detached control context so worker cancellation cannot discard their outcome.
func (l *Loop) Run(ctx context.Context, runtime run.Runtime, runID run.RunID, events EventSink) (LoopResult, error) {
	if ctx == nil {
		return LoopResult{}, errors.New("agent: loop: nil context")
	}
	if runtime == nil {
		return LoopResult{}, errors.New("agent: loop: nil runtime")
	}
	if runID == "" {
		return LoopResult{}, errors.New("agent: loop: empty RunID")
	}
	if err := l.acquireRun(runID); err != nil {
		return LoopResult{}, err
	}
	defer l.releaseRun(runID)
	if events != nil {
		events = &serializedEventSink{sink: events, mu: &l.eventsMu}
	}

	for {
		if err := ctx.Err(); err != nil {
			// Workers started by this Loop have already been settled by the
			// branches below before we reach this check.
			return LoopResult{}, err
		}
		snapshot, err := runtime.Load(ctx, runID)
		if err != nil {
			return LoopResult{}, err
		}
		if snapshot.State.RunID != runID {
			return LoopResult{}, fmt.Errorf("agent: loop: runtime returned RunID %q for %q", snapshot.State.RunID, runID)
		}
		if snapshot.State.Status.Terminal() {
			l.forgetRunCaches(runID)
			if events != nil {
				_ = events.Emit(ctx, Event{
					RunID:      snapshot.State.RunID,
					Kind:       EventRunFinished,
					Durability: EventCommitted,
				})
			}
			return LoopResult{Disposition: LoopFinished, Result: snapshot.State.Result}, nil
		}

		if handled, err := l.resumeSettlement(ctx, runtime, events, &snapshot); err != nil {
			return LoopResult{}, err
		} else if handled {
			continue
		}
		if handled, err := l.resumeCachedStart(ctx, runtime, events, &snapshot); err != nil {
			return LoopResult{}, err
		} else if handled {
			continue
		}

		effect, err := run.Next(snapshot.State)
		if err != nil {
			return LoopResult{}, err
		}

		switch eff := effect.(type) {
		case run.NeedModelRequest:
			if err := l.planAndPrepare(ctx, runtime, events, &snapshot, eff.Hint); err != nil {
				return LoopResult{}, err
			}
		case run.StartModelCall:
			if err := l.runModelStep(ctx, runtime, events, &snapshot, eff.StepID); err != nil {
				return LoopResult{}, err
			}
		case run.StartToolCalls:
			if err := l.runToolCalls(ctx, runtime, events, &snapshot, eff); err != nil {
				return LoopResult{}, err
			}
		case run.Idle:
			recovery := run.NeedsRecovery(snapshot.State)
			reason := WaitReason("")
			if recovery {
				reason = ExecutionRecovery
			}
			return LoopResult{Disposition: LoopWaiting, Reason: reason, ExecutionRecovery: recovery}, nil
		default:
			return LoopResult{}, fmt.Errorf("agent: loop: unknown effect %T", effect)
		}
	}
}

// commit builds the envelope via the sanctioned constructor and submits it.
// A non-sentinel commit failure is replayed once with the same CommandID and
// digest (RUN-LOP-5): if the first attempt actually
// committed and only the response was lost, the replay returns AlreadyApplied
// instead of abandoning a live grant or re-executing an expensive step.
func (l *Loop) commit(ctx context.Context, runtime run.Runtime, runID run.RunID, id run.CommandID, base uint64, grant run.ExecutionGrant, cmd run.AgentCommand, proto run.Protocol) (run.CommitResult, error) {
	if proto == nil {
		return run.CommitResult{}, errors.New("agent: loop: nil protocol")
	}
	env, err := proto.BuildEnvelope(runID, id, cmd)
	if err != nil {
		return run.CommitResult{}, err
	}
	req := run.CommitRequest{BaseRevision: base, Grant: grant, Command: env}
	res, err := runtime.Commit(ctx, req)
	if err != nil && !retriable(err) {
		res, err = runtime.Commit(ctx, req)
	}
	return res, err
}

// retriable reports the commit errors that mean "reload and rederive".
func retriable(err error) bool {
	return errors.Is(err, run.ErrStaleRuntime) || errors.Is(err, run.ErrRunTerminal) || errors.Is(err, run.ErrCommandConflict)
}

func freshCommandID() run.CommandID {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("agent: loop: %v", err))
	}
	return run.CommandID(hex.EncodeToString(b[:]))
}

func freshExecutionClaim() run.ExecutionClaim {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("agent: loop: %v", err))
	}
	return run.ExecutionClaim(hex.EncodeToString(b[:]))
}

func (l *Loop) emitCommitted(ctx context.Context, events EventSink, runID run.RunID, committed []run.AgentEvent) {
	if events == nil {
		return
	}
	for i := range committed {
		e := committed[i]
		_ = events.Emit(ctx, Event{
			RunID:      runID,
			Kind:       EventAgentCommitted,
			Durability: EventCommitted,
			Canonical:  &e,
		})
	}
}

// --- NeedModelRequest ---

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
	invoker, resolveErr := l.Models.Resolve(modelStep.Model)
	if resolveErr != nil {
		completion = run.SubmitModelFailure{StepID: stepID, Failure: run.StepFailure{Class: run.FailureProvider, Message: resolveErr.Error()}}
	} else {
		// Model workers derive from the outer ctx: cancelling a model call is
		// safe, the frozen request retries after recovery (RUN-LOP-3).
		sdkRequest, err := modelStep.Request.SDK()
		if err != nil {
			completion = run.SubmitModelFailure{StepID: stepID, Failure: run.StepFailure{Class: run.FailureProvider, Message: err.Error()}}
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
					completion = run.SubmitModelResult{StepID: stepID, Result: frozenResult, Calls: bindings}
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

// --- StartToolCalls ---

type startedWorker struct {
	call  run.ToolCallState
	grant run.ExecutionGrant
	base  uint64
	tool  ExecutableTool
	key   startKey
}

func toolCallIndex(step run.ToolStep, callID run.CallID) int {
	for i := range step.Calls {
		if step.Calls[i].CallID == callID {
			return i
		}
	}
	return -1
}

func (l *Loop) runToolCalls(ctx context.Context, runtime run.Runtime, events EventSink, snapshot *run.RuntimeSnapshot, eff run.StartToolCalls) error {
	runID := snapshot.State.RunID
	proto, err := snapshot.Protocol()
	if err != nil {
		return err
	}
	ts, ok := snapshot.State.Current.(run.ToolStep)
	if !ok || ts.RefValue.ID != eff.StepID {
		return fmt.Errorf("agent: loop: tool step %q is not current", eff.StepID)
	}

	limit := len(eff.CallIDs)
	if l.Execution.ToolExecution == ToolExecutionSequential {
		limit = 1
	}
	if l.Execution.MaxParallel > 0 && l.Execution.MaxParallel < limit {
		limit = l.Execution.MaxParallel
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
		i := toolCallIndex(ts, callID)
		if i < 0 {
			continue
		}
		call := ts.Calls[i]
		key := startKey{runID: runID, stepID: eff.StepID, callID: callID}
		if call.Status != run.ToolPending {
			if call.Status == run.ToolExecuting {
				if _, ok := l.lookupStart(key); !ok {
					continue
				}
			} else {
				continue
			}
		}

		tool, resolveErr := l.Tools.Resolve(call.ToolRef)
		var known *run.ToolFailure
		switch {
		case resolveErr != nil:
			known = &run.ToolFailure{Class: run.FailureToolLookup, Message: resolveErr.Error()}
		default:
			if tool == nil {
				known = &run.ToolFailure{Class: run.FailureToolLookup, Message: "tool catalog returned a nil tool"}
				break
			}
			toolDef, freezeErr := run.FreezeToolDefinition(tool.Definition())
			if freezeErr != nil {
				known = &run.ToolFailure{Class: run.FailureDefinitionMismatch, Message: freezeErr.Error()}
				break
			}
			defDigest, digestErr := proto.DigestToolDefinition(toolDef)
			if digestErr != nil {
				known = &run.ToolFailure{Class: run.FailureDefinitionMismatch, Message: digestErr.Error()}
				break
			}
			switch {
			case tool.Ref() != call.ToolRef || defDigest != call.DefinitionDigest:
				known = &run.ToolFailure{Class: run.FailureDefinitionMismatch, Message: "tool definition digest mismatch"}
			case tool.ResponsePolicy() != call.Policy:
				known = &run.ToolFailure{Class: run.FailureDefinitionMismatch, Message: "response policy mismatch"}
			default:
				if argErr := tool.ValidateArguments(call.Arguments); argErr != nil {
					known = &run.ToolFailure{Class: run.FailureInvalidArguments, Message: argErr.Error()}
				}
			}
		}
		if known != nil {
			// Known failure of a Pending call: no start barrier, no tool call.
			res, err := l.commit(ctx, runtime, runID, freshCommandID(), snapshot.Revision, "",
				run.SubmitToolFailure{StepID: eff.StepID, CallID: callID, Failure: *known, Outcome: run.ToolOutcomeKnown}, proto)
			if err != nil {
				settleErr := l.settleWorkers(ctx, runtime, events, runID, eff.StepID, started, proto)
				if !retriable(err) {
					return err
				}
				return settleErr
			}
			l.emitCommitted(ctx, events, runID, res.Events)
			continue
		}

		attempt := l.startFor(key)
		start, err := l.commit(ctx, runtime, runID, attempt.commandID, snapshot.Revision, "",
			run.StartToolCall{StepID: eff.StepID, CallID: callID, Claim: attempt.claim}, proto)
		if err != nil {
			settleErr := l.settleWorkers(ctx, runtime, events, runID, eff.StepID, started, proto)
			if retriable(err) {
				// A sentinel rejection proves this start did not acquire the
				// call. Drop the local claim so a later snapshot can create a
				// fresh start attempt or observe the other owner.
				l.forgetStart(key)
				return settleErr
			}
			return err
		}
		if start.Status == run.CommitAlreadyApplied && start.Grant == "" {
			l.forgetStart(key)
			continue // another attempt owns this call
		}
		if start.Grant == "" {
			l.forgetStart(key)
			return errors.New("agent: loop: start tool returned no execution grant")
		}
		if startedCall, ok := toolCallFromSnapshot(start.Snapshot.State, eff.StepID, callID); !ok || startedCall.Status != run.ToolExecuting {
			// A replay may arrive after another worker has settled this call.
			// Keep the original grant in the Runtime's replay record, but never
			// invoke an effect for a call that is no longer Executing.
			l.forgetStart(key)
			continue
		}
		l.emitCommitted(ctx, events, runID, start.Events)
		if events != nil {
			_ = events.Emit(ctx, Event{RunID: runID, StepID: eff.StepID, CallID: callID,
				Kind: EventToolStarted, Durability: EventCommitted})
		}
		started = append(started, startedWorker{call: call, grant: start.Grant, base: start.Snapshot.Revision, tool: tool, key: key})
	}

	return l.settleWorkers(ctx, runtime, events, runID, eff.StepID, started, proto)
}

func toolCallFromSnapshot(state run.MachineState, stepID run.StepID, callID run.CallID) (run.ToolCallState, bool) {
	step, ok := state.Current.(run.ToolStep)
	if !ok || step.RefValue.ID != stepID {
		return run.ToolCallState{}, false
	}
	for _, call := range step.Calls {
		if call.CallID == callID {
			return call, true
		}
	}
	return run.ToolCallState{}, false
}

// settleWorkers executes every started worker and commits its outcome. An
// accepted start is never abandoned (RUN-LOP-4). Tool workers receive outer
// context cancellation; settlement uses a detached control context so the
// resulting outcome can still reach Runtime (RUN-LOP-5). One Unknown cancels
// sibling workers. A non-sentinel commit error leaves the same command in the
// local settlement cache for the next Run invocation.
func (l *Loop) settleWorkers(ctx context.Context, runtime run.Runtime, events EventSink, runID run.RunID, stepID run.StepID, started []startedWorker, proto run.Protocol) error {
	if len(started) == 0 {
		return nil
	}
	// The tool sees cancellation so cooperative implementations can stop. The
	// settlement path uses controlCtx, which is independent of worker
	// cancellation and can record the resulting known/unknown outcome.
	execCtx, cancelAll := context.WithCancel(ctx)
	defer cancelAll()
	controlCtx := context.WithoutCancel(ctx)

	var mu sync.Mutex
	var wg sync.WaitGroup
	var firstErr error
	for i := range started {
		wg.Add(1)
		w := started[i]
		go func(w startedWorker) {
			defer wg.Done()
			req := ToolExecutionRequest{
				RunID:            runID,
				StepID:           stepID,
				CallID:           w.call.CallID,
				ToolRef:          w.call.ToolRef,
				DefinitionDigest: w.call.DefinitionDigest,
				Arguments:        w.call.Arguments,
				Progress:         &progressSink{events: events, run: runID, step: stepID, call: w.call.CallID},
			}
			outcome := executeToolSafely(execCtx, w.tool, &req)

			var cmd run.AgentCommand
			unknown := false
			switch o := outcome.(type) {
			case ToolExecutionSucceeded:
				cmd = run.SubmitToolResult{StepID: stepID, CallID: w.call.CallID, Result: o.Result}
			case ToolExecutionFailed:
				failure := o.Failure
				if failure.Class == "" || failure.Class == run.FailureEffectUnknown {
					failure.Class = run.FailureExecution
				}
				cmd = run.SubmitToolFailure{StepID: stepID, CallID: w.call.CallID, Failure: failure, Outcome: run.ToolOutcomeKnown}
			case ToolExecutionUnknown:
				failure := o.Failure
				if failure.Class != "" && failure.Class != run.FailureEffectUnknown && failure.Message == "" {
					failure.Message = "tool reported " + failure.Class
				}
				failure.Class = run.FailureEffectUnknown
				cmd = run.SubmitToolFailure{StepID: stepID, CallID: w.call.CallID, Failure: failure, Outcome: run.ToolOutcomeUnknown}
				unknown = true
			default:
				cmd = run.SubmitToolFailure{StepID: stepID, CallID: w.call.CallID,
					Failure: run.ToolFailure{Class: run.FailureEffectUnknown, Message: "tool returned no outcome"}, Outcome: run.ToolOutcomeUnknown}
				unknown = true
			}

			mu.Lock()
			defer mu.Unlock()
			// Commit with the worker's own grant on its start base; stale
			// bases rebase call-locally. Late results after terminal return
			// ErrRunTerminal and are dropped (audit is the adapter's job).
			// The one-shot same-CommandID replay lives inside l.commit.
			settlement := l.settlementFor(w.key, freshCommandID(), w.base, w.grant, cmd)
			res, err := l.commit(controlCtx, runtime, runID, settlement.commandID, settlement.base, settlement.grant, settlement.command, proto)
			switch {
			case err == nil:
				l.forgetSettlement(w.key)
				l.forgetStart(w.key)
				l.emitCommitted(ctx, events, runID, res.Events)
				if events != nil {
					_ = events.Emit(ctx, Event{RunID: runID, StepID: stepID, CallID: w.call.CallID,
						Kind: EventToolCompleted, Durability: EventCommitted})
				}
			case retriable(err):
				l.forgetSettlement(w.key)
				l.forgetStart(w.key)
				// Terminal/stale: the authority already settled this call or
				// the run; the result is intentionally dropped.
			default:
				if firstErr == nil {
					firstErr = fmt.Errorf("agent: loop: settling call %q: %w", w.call.CallID, err)
				}
			}
			if unknown {
				cancelAll() // one Unknown cancels sibling workers (RUN-LOP-4)
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
			outcome = ToolExecutionUnknown{Failure: run.ToolFailure{
				Class:   run.FailureEffectUnknown,
				Message: fmt.Sprintf("tool panic: %v", r),
			}}
		}
	}()
	return tool.Execute(ctx, *req)
}

type progressSink struct {
	events EventSink
	run    run.RunID
	step   run.StepID
	call   run.CallID
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
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("null")
	}
	return b
}
