package loop

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	. "github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/sdk"
)

// recordingExecutor captures Assignments instead of executing them, so a test
// can observe the Loop's dispatch and hand Outcomes back at will.
type recordingExecutor struct {
	mu          sync.Mutex
	dispatched  []Assignment
	delivers    map[AssignmentKey]Deliver
	attached    []Assignment
	attachReply bool
	cancelled   []RunID
}

func newRecordingExecutor() *recordingExecutor {
	return &recordingExecutor{delivers: map[AssignmentKey]Deliver{}}
}

func (e *recordingExecutor) Validate(context.Context, Assignment) (*ToolFailure, error) {
	return nil, nil
}

func (e *recordingExecutor) Dispatch(_ context.Context, a Assignment, deliver Deliver) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.dispatched = append(e.dispatched, a)
	e.delivers[a.Key()] = deliver
	return nil
}

func (e *recordingExecutor) Attach(_ context.Context, a Assignment, deliver Deliver) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.attached = append(e.attached, a)
	if e.attachReply {
		e.delivers[a.Key()] = deliver
	}
	return e.attachReply, nil
}

func (e *recordingExecutor) Cancel(_ context.Context, runID RunID) error {
	e.mu.Lock()
	e.cancelled = append(e.cancelled, runID)
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
	d, ok := e.delivers[key]
	e.mu.Unlock()
	if !ok {
		t.Fatalf("no deliver registered for %+v", key)
	}
	out.Key = key
	d(out)
}

// Advance records the start barrier and hands the model call to the executor
// without waiting for it; Deliver settles the Outcome and the next Advance
// finishes the Run (RUN-EXE-3/4).
func TestAdvanceDispatchesAndDeliverSettles(t *testing.T) {
	rt := loopRuntime(t)
	exec := newRecordingExecutor()
	l, err := New(exec, staticPlanner{}, ExecutionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	res, err := l.Advance(ctx, rt, testSession, "run-1", nil)
	if err != nil || res.Disposition != LoopDispatched || len(res.Dispatched) != 1 {
		t.Fatalf("advance = %+v %v", res, err)
	}
	a := exec.last()
	if a.Kind != AssignmentModel || a.Model == nil || a.Model.RequestDigest == "" || a.Claim == "" || a.Key() != res.Dispatched[0] {
		t.Fatalf("model assignment = %+v", a)
	}
	step := loadState(t, rt, "run-1").State.Current.(ModelStep)
	if step.Status != ModelExecuting || step.Claim != a.Claim {
		t.Fatalf("started step = %+v", step)
	}

	// Nothing moves while the effect is outstanding.
	again, err := l.Advance(ctx, rt, testSession, "run-1", nil)
	if err != nil || again.Disposition != LoopWaiting || !again.ExecutionRecovery {
		t.Fatalf("advance while executing = %+v %v", again, err)
	}

	result := textResult("done")
	delivered, err := l.Deliver(ctx, rt, testSession, Outcome{Key: a.Key(), Model: &result}, nil)
	if err != nil || delivered.Disposition != LoopFinished || delivered.Result == nil || delivered.Result.Status != RunCompleted {
		t.Fatalf("deliver = %+v %v", delivered, err)
	}
}

// A late Outcome -- its attempt already settled or disposed -- is dropped:
// nothing is written and the Loop reports LoopDropped.
func TestDeliverDropsStaleOutcome(t *testing.T) {
	rt := loopRuntime(t)
	exec := newRecordingExecutor()
	l, err := New(exec, staticPlanner{}, ExecutionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := l.Advance(ctx, rt, testSession, "run-1", nil); err != nil {
		t.Fatal(err)
	}
	key := exec.last().Key()
	// The new owner disposes the attempt (no executor to reattach).
	if n, err := rt.RecoverInterrupted(ctx, testSession, nil); err != nil || n != 1 {
		t.Fatalf("RecoverInterrupted = %d %v", n, err)
	}
	before := len(recordFacts(t, rt, "run-1"))
	result := textResult("late")
	res, err := l.Deliver(ctx, rt, testSession, Outcome{Key: key, Model: &result}, nil)
	if err != nil || res.Disposition != LoopDropped {
		t.Fatalf("late deliver = %+v %v", res, err)
	}
	if after := len(recordFacts(t, rt, "run-1")); after != before {
		t.Fatalf("stale outcome wrote %d fact(s)", after-before)
	}
	// A key with the wrong claim is stale too.
	if _, err := l.Advance(ctx, rt, testSession, "run-1", nil); err != nil {
		t.Fatal(err)
	}
	forged := exec.last().Key()
	forged.Claim = "someone-else"
	res, err = l.Deliver(ctx, rt, testSession, Outcome{Key: forged, Model: &result}, nil)
	if err != nil || res.Disposition != LoopDropped {
		t.Fatalf("forged deliver = %+v %v", res, err)
	}
}

// Takeover with a reachable executor: the new owner's RecoverInterrupted asks
// the executor, which still holds the attempt, so the step stays Executing
// with its original Claim and the attempt's Outcome settles it (RUN-CMT-7).
func TestTakeoverReattachesRunningAttempt(t *testing.T) {
	stack := newTestStack(t, nil)
	stack.createRun(t, "run-1", AgentInput{ID: "seed", Payload: cj(`{}`)})
	exec := newRecordingExecutor()
	l, err := New(exec, staticPlanner{}, ExecutionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := l.Advance(ctx, stack.runtime, testSession, "run-1", nil); err != nil {
		t.Fatal(err)
	}
	a := exec.last()

	// Owner dies; a new owner opens the Session. Its executor still runs the
	// attempt (the same recording executor answers true).
	stack.open(t)
	exec.attachReply = true
	newLoop, err := New(exec, staticPlanner{}, ExecutionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	var reattached []Outcome
	var mu sync.Mutex
	deliverToNew := func(out Outcome) {
		mu.Lock()
		reattached = append(reattached, out)
		mu.Unlock()
		if _, err := newLoop.Deliver(ctx, stack.runtime, testSession, out, nil); err != nil {
			t.Errorf("reattached deliver: %v", err)
		}
	}
	n, err := stack.runtime.RecoverInterrupted(ctx, testSession, Reattach(exec, testSession, deliverToNew))
	if err != nil || n != 0 {
		t.Fatalf("RecoverInterrupted with a reachable executor = %d %v, want 0 dispositions", n, err)
	}
	if len(exec.attached) != 1 || exec.attached[0].Key() != a.Key() {
		t.Fatalf("attach asked about %+v, want %+v", exec.attached, a.Key())
	}
	step := loadState(t, stack.runtime, "run-1").State.Current.(ModelStep)
	if step.Status != ModelExecuting || step.Claim != a.Claim {
		t.Fatalf("step after reattach = %+v, want Executing under the original claim", step)
	}

	// The attempt finishes on the executor; its Outcome reaches the new owner.
	result := textResult("done")
	exec.deliver(t, a.Key(), Outcome{Model: &result})
	mu.Lock()
	got := len(reattached)
	mu.Unlock()
	if got != 1 {
		t.Fatalf("reattached outcomes = %d", got)
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
	stack.createRun(t, "run-1", AgentInput{ID: "seed", Payload: cj(`{}`)})
	exec := newRecordingExecutor()
	l, err := New(exec, staticPlanner{}, ExecutionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := l.Advance(ctx, stack.runtime, testSession, "run-1", nil); err != nil {
		t.Fatal(err)
	}
	a := exec.last()
	stack.open(t)
	n, err := stack.runtime.RecoverInterrupted(ctx, testSession, Reattach(exec, testSession, func(Outcome) {}))
	if err != nil || n != 1 || len(exec.attached) != 1 {
		t.Fatalf("RecoverInterrupted = %d %v attached=%d, want one disposition after one refused attach", n, err, len(exec.attached))
	}
	// The unreachable attempt is withdrawn: the Run is Open, the step is not
	// counted, and the next Advance plans again (TRN-DUR-1).
	state := loadState(t, stack.runtime, "run-1").State
	if _, open := state.Current.(Open); !open || state.ModelSteps != 0 {
		t.Fatalf("state after disposition = %+v, want Open with no counted step", state)
	}
	res, err := l.Advance(ctx, stack.runtime, testSession, "run-1", nil)
	if err != nil || res.Disposition != LoopDispatched || len(res.Dispatched) != 1 {
		t.Fatalf("advance after disposition = %+v %v, want a fresh dispatch", res, err)
	}
	if replanned := exec.last(); replanned.Key() == a.Key() || replanned.Model.RequestDigest == "" {
		t.Fatalf("replan reused the disposed attempt: %+v", replanned)
	}
}

// LocalExecutor.Attach answers for attempts this process still runs and
// refuses once they completed; Cancel stops in-flight effects and their
// Outcomes come back marked Cancelled.
func TestLocalExecutorAttachAndCancel(t *testing.T) {
	rt := loopRuntime(t)
	block := make(chan struct{})
	tool := &fakeTool{ref: "echo", def: toolDef("echo"), policy: DirectExecution,
		execute: func(ctx context.Context, req ToolExecutionRequest) ToolExecutionOutcome {
			select {
			case <-ctx.Done():
				return ToolExecutionUnknown{Failure: ToolFailure{Class: FailureEffectUnknown, Message: ctx.Err().Error()}}
			case <-block:
				return ToolExecutionSucceeded{Result: ToolExecutionResult{Output: req.Arguments}}
			}
		}}
	exec, err := NewLocalExecutor(fakeCatalog{&fakeInvoker{}}, fakeToolCatalog{map[ToolRef]ExecutableTool{"echo": tool}}, rt, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	spec := toolSpec(t, "echo", DirectExecution)
	a := Assignment{Session: testSession, RunID: "run-1", StepID: "step-1", CallID: "call-1", Claim: "claim-1", Schema: SchemaVersion1,
		Kind: AssignmentTool, Tool: &ToolAssignment{ToolRef: spec.Ref, DefinitionDigest: spec.DefinitionDigest, Arguments: cj(`{}`), Policy: DirectExecution}}
	outcomes := make(chan Outcome, 2)
	if err := exec.Dispatch(context.Background(), a, func(o Outcome) { outcomes <- o }); err != nil {
		t.Fatal(err)
	}
	if dup := exec.Dispatch(context.Background(), a, func(Outcome) {}); !errors.Is(dup, ErrExecutorRejected) {
		t.Fatalf("duplicate dispatch = %v", dup)
	}
	attached, err := exec.Attach(context.Background(), a, func(o Outcome) { outcomes <- o })
	if err != nil || !attached {
		t.Fatalf("attach running = %v %v", attached, err)
	}
	if err := exec.Cancel(context.Background(), "run-1"); err != nil {
		t.Fatal(err)
	}
	select {
	case out := <-outcomes:
		if !out.Cancelled || out.Key != a.Key() {
			t.Fatalf("cancelled outcome = %+v", out)
		}
		if _, unknown := out.Tool.(ToolExecutionUnknown); !unknown {
			t.Fatalf("cancelled tool outcome = %T", out.Tool)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled effect never reported")
	}
	if attached, _ := exec.Attach(context.Background(), a, func(Outcome) {}); attached {
		t.Fatal("attach after completion must be false")
	}
	if exec.InFlight() != 0 {
		t.Fatalf("in-flight = %d after completion", exec.InFlight())
	}
}

// A model outcome that reports cancellation withdraws the step to Open for
// replanning rather than failing the Run (RUN-LOP-3).
func TestDeliverCancelledModelRecovers(t *testing.T) {
	rt := loopRuntime(t)
	exec := newRecordingExecutor()
	l, err := New(exec, staticPlanner{}, ExecutionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := l.Advance(ctx, rt, testSession, "run-1", nil); err != nil {
		t.Fatal(err)
	}
	key := exec.last().Key()
	res, err := l.Deliver(ctx, rt, testSession, Outcome{Key: key, Err: context.Canceled, Cancelled: true}, nil)
	if err != nil || res.Disposition != LoopDelivered {
		t.Fatalf("deliver cancelled = %+v %v", res, err)
	}
	state := loadState(t, rt, "run-1").State
	if _, open := state.Current.(Open); !open || state.Status != RunActive {
		t.Fatalf("state = %+v, want Open and active", state)
	}
	var _ sdk.ModelResult // keep sdk imported for result helpers above
}

// A frozen body a remote executor reports missing withdraws the step (the
// settlement lands, the Run is Open) and the condition comes back as an error,
// so the drive stops instead of planning again against the same missing
// store. A later Advance -- the host's decision -- plans afresh (RUN-LOP-3).
func TestDeliverMissingFrozenBodyWithdrawsAndReturnsTheError(t *testing.T) {
	rt := loopRuntime(t)
	exec := newRecordingExecutor()
	l, err := New(exec, staticPlanner{}, ExecutionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := l.Advance(ctx, rt, testSession, "run-1", nil); err != nil {
		t.Fatal(err)
	}
	first := exec.last()
	res, err := l.Deliver(ctx, rt, testSession, Outcome{Key: first.Key(), Err: ErrFrozenValueMissing}, nil)
	if !errors.Is(err, ErrFrozenValueMissing) || res.Disposition != LoopDelivered {
		t.Fatalf("deliver missing body = %+v %v, want delivered plus the missing-body error", res, err)
	}
	if snap := loadState(t, rt, "run-1"); snap.State.ModelSteps != 0 {
		t.Fatalf("withdrawn step still counted: %+v", snap.State)
	}
	again, err := l.Advance(ctx, rt, testSession, "run-1", nil)
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

func (e *missingBodyExecutor) Dispatch(ctx context.Context, a Assignment, deliver Deliver) error {
	if err := e.recordingExecutor.Dispatch(ctx, a, deliver); err != nil {
		return err
	}
	go deliver(Outcome{Key: a.Key(), Err: ErrFrozenValueMissing})
	return nil
}

// missingFrozen is a frozen store with nothing in it, standing in for an
// executor whose store diverged from the authority's.
type missingFrozen struct{}

func (missingFrozen) FrozenRequest(context.Context, Digest) (ModelRequest, error) {
	return ModelRequest{}, ErrFrozenValueMissing
}

// A persistently missing body must not spin the Run: the blocking Run returns
// the error after one withdrawal, whether the miss is reported by a remote
// executor as an Outcome or found by LocalExecutor.Dispatch before the effect
// starts. Either way the stream grows by one Prepared/Started/Recovered round
// per drive, and only the host can start another.
func TestRunStopsAfterOneMissingBodyRecovery(t *testing.T) {
	cases := []struct {
		name string
		exec func(t *testing.T, rt Runtime) Executor
	}{
		{"remote outcome", func(*testing.T, Runtime) Executor {
			return &missingBodyExecutor{recordingExecutor: *newRecordingExecutor()}
		}},
		{"local dispatch", func(t *testing.T, rt Runtime) Executor {
			exec, err := NewLocalExecutor(fakeCatalog{&fakeInvoker{}}, fakeToolCatalog{}, missingFrozen{}, nil, false)
			if err != nil {
				t.Fatal(err)
			}
			return exec
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt := loopRuntime(t)
			l, err := New(tc.exec(t, rt), staticPlanner{}, ExecutionPolicy{})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err = l.Run(ctx, rt, testSession, "run-1", nil)
			if !errors.Is(err, ErrFrozenValueMissing) {
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
			if snap := loadState(t, rt, "run-1"); snap.State.ModelSteps != 0 {
				t.Fatalf("state after the failed drive = %+v, want Open with no counted step", snap.State)
			}
		})
	}
}
