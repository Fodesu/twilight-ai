package loop

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	. "github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/frozen"
	"github.com/felinics/twilight/agent/run/reconcile"
	"github.com/felinics/twilight/agent/run/runtime"
	"github.com/felinics/twilight/sdk"
)

// recordingExecutor captures Assignments instead of executing them, so a test
// can observe the Loop's dispatch and hand Outcomes back at will.
type recordingExecutor struct {
	mu          sync.Mutex
	dispatched  []Assignment
	outcomes    map[AssignmentKey]chan Outcome
	attached    []Assignment
	attachReply bool
	cancelled   []AssignmentKey
}

func newRecordingExecutor() *recordingExecutor {
	return &recordingExecutor{outcomes: map[AssignmentKey]chan Outcome{}}
}

func (e *recordingExecutor) Validate(context.Context, Assignment) (*ToolFailure, error) {
	return nil, nil
}

func (e *recordingExecutor) Dispatch(_ context.Context, a Assignment) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.dispatched = append(e.dispatched, a)
	e.outcomes[a.Key()] = make(chan Outcome, 1)
	return nil
}

func (e *recordingExecutor) Attach(_ context.Context, key AssignmentKey) (Attachment, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, a := range e.dispatched {
		if a.Key() == key {
			e.attached = append(e.attached, a)
			break
		}
	}
	if e.attachReply {
		return Attachment{State: AttachmentActive, Execution: ExecutionRunning, BackendAttached: true}, nil
	}
	return Attachment{State: AttachmentMissing, Execution: ExecutionNotFound}, nil
}

func (e *recordingExecutor) GetStatus(context.Context, AssignmentKey) (ExecutionStatus, error) {
	return ExecutionRunning, nil
}

func (e *recordingExecutor) GetOutcome(ctx context.Context, key AssignmentKey) (Outcome, error) {
	e.mu.Lock()
	ch, ok := e.outcomes[key]
	e.mu.Unlock()
	if !ok {
		return Outcome{}, ErrExecutionNotFound
	}
	select {
	case out := <-ch:
		return out, nil
	case <-ctx.Done():
		return Outcome{}, ctx.Err()
	}
}

func (e *recordingExecutor) Cancel(_ context.Context, key AssignmentKey) error {
	e.mu.Lock()
	e.cancelled = append(e.cancelled, key)
	e.mu.Unlock()
	return nil
}

func (e *recordingExecutor) last() Assignment {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.dispatched[len(e.dispatched)-1]
}

func (e *recordingExecutor) deliver(t *testing.T, key AssignmentKey, out Outcome) {
	t.Helper()
	e.mu.Lock()
	ch, ok := e.outcomes[key]
	e.mu.Unlock()
	if !ok {
		t.Fatalf("no outcome record for %+v", key)
	}
	out.Key = key
	ch <- out
}

type fixedTargetResolver struct{ target TargetRef }

func (r fixedTargetResolver) ResolveTarget(context.Context, Scope, RunID) (*TargetRef, error) {
	target := r.target
	return &target, nil
}

func TestAdvanceCopiesOpaqueTargetIntoAssignment(t *testing.T) {
	rt, w := loopRuntime(t)
	exec := newRecordingExecutor()
	l, err := New(exec, staticBuilder{}, Settings{TargetResolver: fixedTargetResolver{target: TargetRef{Kind: "workspace", ID: "ws-1"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Advance(context.Background(), rt.Bind(w), "run-1", nil); err != nil {
		t.Fatal(err)
	}
	assignment := exec.last()
	if assignment.Target == nil || *assignment.Target != (TargetRef{Kind: "workspace", ID: "ws-1"}) {
		t.Fatalf("target = %+v", assignment.Target)
	}
}

// Advance records the start barrier and hands the model call to the executor
// without waiting for it; Deliver settles the Outcome and the next Advance
// finishes the Run (RUN-EXE-3/4).
func TestAdvanceDispatchesAndDeliverSettles(t *testing.T) {
	rt, w := loopRuntime(t)
	exec := newRecordingExecutor()
	l, err := New(exec, staticBuilder{}, Settings{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	res, err := l.Advance(ctx, rt.Bind(w), "run-1", nil)
	if err != nil || res.Disposition != LoopDispatched || len(res.Dispatched) != 1 {
		t.Fatalf("advance = %+v %v", res, err)
	}
	a := exec.last()
	if model, ok := a.Model(); !ok || model.RequestDigest == "" || a.Claim == "" || a.Key() != res.Dispatched[0] {
		t.Fatalf("model assignment = %+v", a)
	}
	step := loadState(t, rt, w, "run-1").State.Current.(ModelStep)
	if step.Status != ModelExecuting || step.Claim != a.Claim {
		t.Fatalf("started step = %+v", step)
	}

	// Nothing moves while the effect is outstanding.
	again, err := l.Advance(ctx, rt.Bind(w), "run-1", nil)
	if err != nil || again.Disposition != LoopWaiting || !again.ExecutionRecovery {
		t.Fatalf("advance while executing = %+v %v", again, err)
	}

	result := textResult("done")
	delivered, err := l.Deliver(ctx, rt.Bind(w), Outcome{Key: a.Key(), Result: ModelSucceeded{Result: result}}, nil)
	if err != nil || delivered.Disposition != LoopFinished || delivered.Result == nil || delivered.Result.Status != RunCompleted {
		t.Fatalf("deliver = %+v %v", delivered, err)
	}
}

type failingOutcomeReader struct {
	*recordingExecutor
	readErr error
	failed  chan struct{}
	ready   chan struct{}
	once    sync.Once
}

func (e *failingOutcomeReader) GetOutcome(ctx context.Context, key AssignmentKey) (Outcome, error) {
	select {
	case <-e.ready:
		return e.recordingExecutor.GetOutcome(ctx, key)
	default:
		e.once.Do(func() { close(e.failed) })
		return Outcome{}, e.readErr
	}
}

func TestRunOutcomeReadErrorPreservesExecutingStep(t *testing.T) {
	rt, w := loopRuntime(t)
	exec := &failingOutcomeReader{recordingExecutor: newRecordingExecutor(), readErr: errors.New("temporary transport error"), failed: make(chan struct{}), ready: make(chan struct{})}
	l, err := New(exec, staticBuilder{}, Settings{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Run(context.Background(), rt.Bind(w), "run-1", nil); !errors.Is(err, exec.readErr) {
		t.Fatalf("Run error = %v, want read failure", err)
	}
	snapshot := loadState(t, rt, w, "run-1")
	step, ok := snapshot.State.Current.(ModelStep)
	if !ok || step.Status != ModelExecuting || snapshot.State.Status != RunActive {
		t.Fatalf("read error changed Run: %+v", snapshot.State)
	}
	result := textResult("eventual result")
	if _, err := l.Deliver(context.Background(), rt.Bind(w), Outcome{Key: exec.last().Key(), Result: ModelSucceeded{Result: result}}, nil); err != nil {
		t.Fatal(err)
	}
	if got := loadState(t, rt, w, "run-1").State.Status; got != RunCompleted {
		t.Fatalf("Run status after actual outcome = %v", got)
	}
}

// A late Outcome -- its attempt already settled or disposed -- is dropped:
// nothing is written and the Loop reports LoopDropped.
func TestDeliverDropsStaleOutcome(t *testing.T) {
	rt, w := loopRuntime(t)
	exec := newRecordingExecutor()
	l, err := New(exec, staticBuilder{}, Settings{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := l.Advance(ctx, rt.Bind(w), "run-1", nil); err != nil {
		t.Fatal(err)
	}
	key := exec.last().Key()
	// The new owner disposes the attempt (no executor to reattach).
	if n, err := rt.RecoverInterrupted(ctx, w, nil); err != nil || n != 1 {
		t.Fatalf("RecoverInterrupted = %d %v", n, err)
	}
	before := len(recordFacts(t, rt, "run-1"))
	result := textResult("late")
	res, err := l.Deliver(ctx, rt.Bind(w), Outcome{Key: key, Result: ModelSucceeded{Result: result}}, nil)
	if err != nil || res.Disposition != LoopDropped {
		t.Fatalf("late deliver = %+v %v", res, err)
	}
	if after := len(recordFacts(t, rt, "run-1")); after != before {
		t.Fatalf("stale outcome wrote %d fact(s)", after-before)
	}
	// A key with the wrong claim is stale too.
	if _, err := l.Advance(ctx, rt.Bind(w), "run-1", nil); err != nil {
		t.Fatal(err)
	}
	forged := exec.last().Key()
	forged.Claim = "someone-else"
	res, err = l.Deliver(ctx, rt.Bind(w), Outcome{Key: forged, Result: ModelSucceeded{Result: result}}, nil)
	if err != nil || res.Disposition != LoopDropped {
		t.Fatalf("forged deliver = %+v %v", res, err)
	}
}

// Takeover with a reachable executor: the new owner's RecoverInterrupted asks
// the executor, which still holds the attempt, so the step stays Executing
// with its original Claim and the attempt's Outcome settles it (RUN-CMT-7).
func TestTakeoverReattachesRunningAttempt(t *testing.T) {
	stack := newTestStack(t, nil)
	stack.createRun(t, "run-1", AgentInput{ID: "seed", Digest: inputDigest(`{}`)})
	exec := newRecordingExecutor()
	l, err := New(exec, staticBuilder{}, Settings{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := l.Advance(ctx, stack.runtime.Bind(stack.writer(t)), "run-1", nil); err != nil {
		t.Fatal(err)
	}
	a := exec.last()

	// Owner dies; a new owner opens the Session. Its executor still runs the
	// attempt (the same recording executor answers true).
	stack.open(t)
	exec.attachReply = true
	newLoop, err := New(exec, staticBuilder{}, Settings{})
	if err != nil {
		t.Fatal(err)
	}
	var reattached []Outcome
	var mu sync.Mutex
	deliverToNew := func(out Outcome) {
		if _, err := newLoop.Deliver(ctx, stack.runtime.Bind(stack.writer(t)), out, nil); err != nil {
			t.Errorf("reattached deliver: %v", err)
		}
		mu.Lock()
		reattached = append(reattached, out)
		mu.Unlock()
	}
	n, err := stack.runtime.RecoverInterrupted(ctx, stack.writer(t), &reconcile.Reconciler{Executions: exec, Lifetime: ctx, Deliver: deliverToNew})
	if err != nil || n != 0 {
		t.Fatalf("RecoverInterrupted with a reachable executor = %d %v, want 0 dispositions", n, err)
	}
	if len(exec.attached) != 1 || exec.attached[0].Key() != a.Key() {
		t.Fatalf("attach asked about %+v, want %+v", exec.attached, a.Key())
	}
	step := loadState(t, stack.runtime, stack.writer(t), "run-1").State.Current.(ModelStep)
	if step.Status != ModelExecuting || step.Claim != a.Claim {
		t.Fatalf("step after reattach = %+v, want Executing under the original claim", step)
	}

	// The attempt finishes on the executor; its Outcome reaches the new owner.
	result := textResult("done")
	exec.deliver(t, a.Key(), Outcome{Result: ModelSucceeded{Result: result}})
	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		got := len(reattached)
		mu.Unlock()
		if got == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("reattached outcomes = %d", got)
		case <-time.After(time.Millisecond):
		}
	}
	record, err := stack.runtime.Record(ctx, testSession, "run-1")
	if err != nil || record.Snapshot.State.Status != RunCompleted {
		t.Fatalf("run after reattached outcome = %+v %v", record.Snapshot.State.Status, err)
	}
	for _, f := range record.Facts {
		if _, recovered := f.(ModelStepRecovered); recovered {
			t.Fatal("a reattached attempt must not be recovered")
		}
	}
}

// Takeover without a reachable executor keeps today's disposition.
func TestTakeoverDisposesWhenAttachIsFalse(t *testing.T) {
	stack := newTestStack(t, nil)
	stack.createRun(t, "run-1", AgentInput{ID: "seed", Digest: inputDigest(`{}`)})
	exec := newRecordingExecutor()
	l, err := New(exec, staticBuilder{}, Settings{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := l.Advance(ctx, stack.runtime.Bind(stack.writer(t)), "run-1", nil); err != nil {
		t.Fatal(err)
	}
	a := exec.last()
	stack.open(t)
	n, err := stack.runtime.RecoverInterrupted(ctx, stack.writer(t), &reconcile.Reconciler{Executions: exec, Lifetime: ctx, Deliver: func(Outcome) {}})
	if err != nil || n != 1 || len(exec.attached) != 1 {
		t.Fatalf("RecoverInterrupted = %d %v attached=%d, want one disposition after one refused attach", n, err, len(exec.attached))
	}
	// The unreachable attempt is withdrawn: the Run is Open, the step is not
	// counted, and the next Advance plans again (TRN-DUR-1).
	state := loadState(t, stack.runtime, stack.writer(t), "run-1").State
	if _, open := state.Current.(Open); !open || state.ModelSteps != 0 {
		t.Fatalf("state after disposition = %+v, want Open with no counted step", state)
	}
	res, err := l.Advance(ctx, stack.runtime.Bind(stack.writer(t)), "run-1", nil)
	if err != nil || res.Disposition != LoopDispatched || len(res.Dispatched) != 1 {
		t.Fatalf("advance after disposition = %+v %v, want a fresh dispatch", res, err)
	}
	if replanned := exec.last(); replanned.Key() == a.Key() || mustModel(t, replanned).RequestDigest == "" {
		t.Fatalf("replan reused the disposed attempt: %+v", replanned)
	}
}

// LocalExecutor is the colocated Backend: Start runs the effect under the
// Ref Prepare derived, Outcome reads the eventual result and Cancel stops an
// in-flight effect.
func TestLocalExecutorAttachAndCancel(t *testing.T) {
	block := make(chan struct{})
	seenTarget := make(chan *TargetRef, 1)
	tool := &fakeTool{ref: "echo", def: toolDef("echo"), policy: DirectExecution,
		execute: func(ctx context.Context, req ToolExecutionRequest) ToolExecutionOutcome {
			seenTarget <- req.Target
			select {
			case <-ctx.Done():
				return ToolExecutionUnknown{Failure: ToolFailure{Class: FailureEffectUnknown, Message: ctx.Err().Error()}}
			case <-block:
				return ToolExecutionSucceeded{Result: ToolExecutionResult{Output: req.Arguments}}
			}
		}}
	exec, err := NewLocalExecutor(fakeCatalog{&fakeInvoker{}}, fakeToolCatalog{map[ToolRef]ExecutableTool{"echo": tool}}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	spec := toolSpec(t, "echo", DirectExecution)
	target := TargetRef{Kind: "workspace", ID: "ws-1"}
	a := Assignment{Session: testScope, RunID: "run-1", StepID: "step-1", CallID: "call-1", Claim: "claim-1", Target: &target, Schema: SchemaVersion1,
		Body: ToolAssignment{ToolRef: spec.Ref, DefinitionDigest: spec.DefinitionDigest, Arguments: cj(`{}`), Policy: DirectExecution}}
	ref, err := exec.Prepare(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	if err := exec.Start(context.Background(), ref, a); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-seenTarget:
		if got == nil || *got != target {
			t.Fatalf("tool target = %+v, want %+v", got, target)
		}
	case <-time.After(time.Second):
		t.Fatal("tool did not receive target")
	}
	if dup := exec.Start(context.Background(), ref, a); dup != nil {
		t.Fatalf("idempotent duplicate start = %v", dup)
	}
	attached, err := exec.Attach(context.Background(), ref)
	if err != nil || attached.State != AttachmentActive {
		t.Fatalf("attach running = %+v %v", attached, err)
	}
	if err := exec.Cancel(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Outcome(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, cancelled := out.Result.(Cancelled); !cancelled || out.Key != a.Key() {
		t.Fatalf("cancelled outcome = %+v", out)
	}
	if attached, _ := exec.Attach(context.Background(), ref); attached.State != AttachmentTerminal {
		t.Fatalf("attach after completion = %+v; want terminal", attached)
	}
	if exec.InFlight() != 0 {
		t.Fatalf("in-flight = %d after completion", exec.InFlight())
	}
}

// A model outcome that reports cancellation withdraws the step to Open for
// replanning rather than failing the Run (RUN-LOP-3).
func TestDeliverCancelledModelRecovers(t *testing.T) {
	rt, w := loopRuntime(t)
	exec := newRecordingExecutor()
	l, err := New(exec, staticBuilder{}, Settings{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := l.Advance(ctx, rt.Bind(w), "run-1", nil); err != nil {
		t.Fatal(err)
	}
	key := exec.last().Key()
	res, err := l.Deliver(ctx, rt.Bind(w), Outcome{Key: key, Result: Cancelled{Message: "cancelled"}}, nil)
	if err != nil || res.Disposition != LoopDelivered {
		t.Fatalf("deliver cancelled = %+v %v", res, err)
	}
	state := loadState(t, rt, w, "run-1").State
	if _, open := state.Current.(Open); !open || state.Status != RunActive {
		t.Fatalf("state = %+v, want Open and active", state)
	}
	var _ sdk.ModelResult // keep sdk imported for result helpers above
}

// A frozen body a remote executor reports missing withdraws the step (the
// settlement lands, the Run is Open) and the condition comes back as an error,
// so the drive stops instead of prompt building again against the same missing
// store. A later Advance -- the host's decision -- plans afresh (RUN-LOP-3).
func TestDeliverMissingFrozenBodyWithdrawsAndReturnsTheError(t *testing.T) {
	rt, w := loopRuntime(t)
	exec := newRecordingExecutor()
	l, err := New(exec, staticBuilder{}, Settings{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := l.Advance(ctx, rt.Bind(w), "run-1", nil); err != nil {
		t.Fatal(err)
	}
	first := exec.last()
	res, err := l.Deliver(ctx, rt.Bind(w), Outcome{Key: first.Key(), Result: ModelFailed{Code: FailureFrozenValueMissing, Message: "frozen value missing"}}, nil)
	if !errors.Is(err, frozen.ErrMissing) || res.Disposition != LoopDelivered {
		t.Fatalf("deliver missing body = %+v %v, want delivered plus the missing-body error", res, err)
	}
	if snap := loadState(t, rt, w, "run-1"); snap.State.ModelSteps != 0 {
		t.Fatalf("withdrawn step still counted: %+v", snap.State)
	}
	again, err := l.Advance(ctx, rt.Bind(w), "run-1", nil)
	if err != nil || again.Disposition != LoopDispatched {
		t.Fatalf("advance after missing body = %+v %v", again, err)
	}
	if second := exec.last(); second.Key() == first.Key() {
		t.Fatal("the replan reused the lost attempt")
	}
}

// missingBodyExecutor is a remote executor whose store never has the body: it
// accepts every model assignment and reports the miss as an Outcome.
type missingBodyExecutor struct{ recordingExecutor }

func (e *missingBodyExecutor) Dispatch(ctx context.Context, a Assignment) error {
	if err := e.recordingExecutor.Dispatch(ctx, a); err != nil {
		return err
	}
	e.mu.Lock()
	ch := e.outcomes[a.Key()]
	e.mu.Unlock()
	go func() {
		ch <- Outcome{Key: a.Key(), Result: ModelFailed{Code: FailureFrozenValueMissing, Message: "frozen value missing"}}
	}()
	return nil
}

// A persistently missing body reported by a remote executor must not spin the
// Run: the blocking Run returns the error after one withdrawal. The executor
// owns the execution payload after Dispatch; the authority does not rebuild it
// from Session state during this path.
func TestRunStopsAfterOneMissingBodyRecovery(t *testing.T) {
	cases := []struct {
		name string
		exec func(t *testing.T, rt runtime.RunStore) Executor
	}{
		{"remote outcome", func(*testing.T, runtime.RunStore) Executor {
			return &missingBodyExecutor{recordingExecutor: *newRecordingExecutor()}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt, w := loopRuntime(t)
			l, err := New(tc.exec(t, rt.Bind(w)), staticBuilder{}, Settings{})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err = l.Run(ctx, rt.Bind(w), "run-1", nil)
			if !errors.Is(err, frozen.ErrMissing) {
				t.Fatalf("Run = %v, want the missing-body error", err)
			}
			started, recovered := 0, 0
			for _, f := range recordFacts(t, rt, "run-1") {
				switch f.(type) {
				case ModelStepStarted:
					started++
				case ModelStepRecovered:
					recovered++
				}
			}
			if started != 1 || recovered != 1 {
				t.Fatalf("started=%d recovered=%d, want exactly one round", started, recovered)
			}
			if snap := loadState(t, rt, w, "run-1"); snap.State.ModelSteps != 0 {
				t.Fatalf("state after the failed drive = %+v, want Open with no counted step", snap.State)
			}
		})
	}
}

// mustModel is the model body of an Assignment.
func mustModel(t testing.TB, a Assignment) ModelAssignment {
	t.Helper()
	m, ok := a.Model()
	if !ok {
		t.Fatalf("assignment %+v has no model body", a)
	}
	return m
}
