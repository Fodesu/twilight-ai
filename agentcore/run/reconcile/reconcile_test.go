package reconcile

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/plan"
	"github.com/felinics/twilight/agentcore/run/runtime"
	"github.com/felinics/twilight/sdk"
)

// fakePort is an execution store whose Attach answers a fixed state and
// whose GetOutcome is scripted per test.
type fakePort struct {
	mu       sync.Mutex
	state    effect.AttachmentState
	asked    []effect.AssignmentKey
	outcome  func(context.Context, effect.AssignmentKey) (effect.Outcome, error)
	attachEr error
}

func (p *fakePort) Validate(context.Context, effect.Assignment) (*run.ToolFailure, error) {
	return nil, nil
}
func (p *fakePort) Dispatch(context.Context, effect.Assignment) error { return nil }
func (p *fakePort) Attach(_ context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	p.mu.Lock()
	p.asked = append(p.asked, key)
	p.mu.Unlock()
	if p.attachEr != nil {
		return effect.Attachment{}, p.attachEr
	}
	return effect.Attachment{State: p.state}, nil
}
func (p *fakePort) GetStatus(context.Context, effect.AssignmentKey) (effect.ExecutionStatus, error) {
	return effect.ExecutionRunning, nil
}
func (p *fakePort) GetOutcome(ctx context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
	if p.outcome == nil {
		return effect.Outcome{}, effect.ErrOutcomeNotReady
	}
	return p.outcome(ctx, key)
}
func (p *fakePort) Cancel(context.Context, effect.AssignmentKey) error { return nil }

func executingModel(eff run.EffectID) *runtime.Snapshot {
	return &runtime.Snapshot{State: run.MachineState{
		RunID: "r1", Status: run.RunActive,
		Current: run.ModelStep{RefValue: run.StepRef{RunID: "r1", ID: "s1"}, Model: "m", RequestDigest: "sha256:req", Status: run.ModelExecuting, Effect: eff},
	}}
}

// Each executor observation maps to exactly one verdict; only missing
// produces a recovery command, and only an unknown state is an error.
func TestPlanVerdicts(t *testing.T) {
	for _, tc := range []struct {
		state    effect.AttachmentState
		want     Verdict
		disposes bool
	}{
		{effect.AttachmentActive, Keep, false},
		{effect.AttachmentTerminal, Keep, false},
		{effect.AttachmentOrphaned, Defer, false},
		{effect.AttachmentMissing, Dispose, true},
	} {
		port := &fakePort{state: tc.state}
		r := &Reconciler{Executions: port}
		decisions, err := r.Plan(context.Background(), "s", executingModel("c1"))
		if err != nil || len(decisions) != 1 {
			t.Fatalf("%s: plan = %+v %v", tc.state, decisions, err)
		}
		d := decisions[0]
		if d.Verdict != tc.want || (d.Recovery != nil) != tc.disposes || d.Observed != tc.state {
			t.Fatalf("%s: decision = %+v, want %s disposes=%v", tc.state, d, tc.want, tc.disposes)
		}
		if len(port.asked) != 1 || port.asked[0] != (effect.AssignmentKey{Session: "s", RunID: "r1", Effect: "c1"}) {
			t.Fatalf("%s: asked = %+v", tc.state, port.asked)
		}
	}
	if _, err := (&Reconciler{Executions: &fakePort{state: "weird"}}).Plan(context.Background(), "s", executingModel("c1")); err == nil {
		t.Fatal("unknown attachment state accepted")
	}
	// No executor to ask proves nothing: an error, not a disposal. Only an
	// explicit Abandon disposes without asking, and it does so even with an
	// executor present.
	if _, err := (&Reconciler{}).Plan(context.Background(), "s", executingModel("c1")); !errors.Is(err, ErrNoExecutionPort) {
		t.Fatalf("plan without executor = %v, want ErrNoExecutionPort", err)
	}
	port := &fakePort{state: effect.AttachmentActive}
	decisions, err := (&Reconciler{Abandon: true, Executions: port}).Plan(context.Background(), "s", executingModel("c1"))
	if err != nil || len(decisions) != 1 || decisions[0].Verdict != Dispose || decisions[0].Recovery == nil || len(port.asked) != 0 {
		t.Fatalf("plan with Abandon = %+v %v asked=%d", decisions, err, len(port.asked))
	}
	if _, ok := decisions[0].Recovery.Command.(run.RecoverModelExecution); !ok {
		t.Fatalf("model disposal = %T", decisions[0].Recovery.Command)
	}
	// A target whose start fact recorded no effect cannot be asked about.
	if _, err := (&Reconciler{Executions: port}).Plan(context.Background(), "s", executingModel("")); !errors.Is(err, ErrTargetWithoutEffect) {
		t.Fatalf("plan of a target without effect = %v, want ErrTargetWithoutEffect", err)
	}
}

// AssignmentFromTarget carries the digest-level description of the target
// and never an inline request body (RUN-EXE-7).
func TestAssignmentFromTarget(t *testing.T) {
	targets := plan.RecoveryTargets(&executingModel("c1").State)
	if len(targets) != 1 {
		t.Fatalf("targets = %d", len(targets))
	}
	a := AssignmentFromTarget("s", targets[0])
	if model, ok := a.Model(); !ok || model.Request != nil || model.RequestDigest != "sha256:req" {
		t.Fatalf("assignment = %+v", a)
	}
	if a.Key() != (effect.AssignmentKey{Session: "s", RunID: "r1", Effect: "c1"}) {
		t.Fatalf("key = %+v", a.Key())
	}
}

// A kept target's Outcome read retries transport errors and never
// fabricates an Outcome; the real one is delivered when it arrives.
func TestKeptOutcomeReadRetries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	failed := make(chan struct{})
	ready := make(chan struct{})
	var once sync.Once
	result := sdk.ModelResult{Text: "eventual"}
	port := &fakePort{state: effect.AttachmentActive, outcome: func(ctx context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
		select {
		case <-ready:
			return effect.Outcome{Key: key, Result: effect.ModelSucceeded{Result: result}}, nil
		default:
			once.Do(func() { close(failed) })
			return effect.Outcome{}, errors.New("temporary transport error")
		}
	}}
	delivered := make(chan effect.Outcome, 1)
	r := &Reconciler{Executions: port, Lifetime: ctx, Deliver: func(out effect.Outcome) { delivered <- out }}
	if _, err := r.Plan(ctx, "s", executingModel("c1")); err != nil {
		t.Fatal(err)
	}
	<-failed
	select {
	case out := <-delivered:
		t.Fatalf("read failure fabricated outcome: %+v", out)
	default:
	}
	close(ready)
	select {
	case out := <-delivered:
		if r, ok := out.ModelResult(); !ok || r.Text != "eventual" {
			t.Fatalf("delivered = %+v", out)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

// The Outcome watcher of a kept target stops with the reconciler's Lifetime.
func TestLifetimeStopsOutcomeWatcher(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	lifetime, stop := context.WithCancel(ctx)
	started, stopped := make(chan struct{}), make(chan struct{})
	port := &fakePort{state: effect.AttachmentOrphaned, outcome: func(ctx context.Context, _ effect.AssignmentKey) (effect.Outcome, error) {
		close(started)
		<-ctx.Done()
		close(stopped)
		return effect.Outcome{}, ctx.Err()
	}}
	delivered := make(chan effect.Outcome, 1)
	r := &Reconciler{Executions: port, Lifetime: lifetime, Deliver: func(out effect.Outcome) { delivered <- out }}
	decisions, err := r.Plan(ctx, "s", executingModel("c1"))
	if err != nil || decisions[0].Verdict != Defer {
		t.Fatalf("plan = %+v %v", decisions, err)
	}
	<-started
	stop()
	select {
	case <-stopped:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case out := <-delivered:
		t.Fatalf("cancelled watcher delivered %+v", out)
	case <-time.After(20 * time.Millisecond):
	}
}

// A read the executor answers definitively (no record for the key) stops the
// watcher and reports through Fail; a read that keeps failing stops after
// ReadRetries; neither fabricates an Outcome.
func TestOutcomeReadErrorTaxonomy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cases := []struct {
		name    string
		err     error
		retries int
		wantErr error
	}{
		{"execution not found is definitive", effect.ErrExecutionNotFound, 0, effect.ErrExecutionNotFound},
		{"outcome unavailable is definitive", effect.ErrOutcomeUnavailable, 0, effect.ErrOutcomeUnavailable},
		{"transport failures exhaust the budget", errors.New("boom"), 3, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var reads int
			port := &fakePort{state: effect.AttachmentActive, outcome: func(context.Context, effect.AssignmentKey) (effect.Outcome, error) {
				reads++
				return effect.Outcome{}, tc.err
			}}
			failed := make(chan error, 1)
			r := &Reconciler{Executions: port, Lifetime: ctx, ReadRetries: tc.retries,
				Deliver: func(out effect.Outcome) { t.Errorf("delivered %+v", out) },
				Fail:    func(_ effect.AssignmentKey, err error) { failed <- err }}
			if _, err := r.Plan(ctx, "s", executingModel("c1")); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-failed:
				if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
					t.Fatalf("fail = %v, want %v", err, tc.wantErr)
				}
				if tc.wantErr == nil && (!errors.Is(err, tc.err) || reads != tc.retries) {
					t.Fatalf("fail = %v after %d reads, want %d", err, reads, tc.retries)
				}
				if tc.wantErr != nil && reads != 1 {
					t.Fatalf("definitive error read %d times", reads)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}

// recoveringPort is a fakePort that can also take records back.
type recoveringPort struct {
	*fakePort
	recovered []effect.AssignmentKey
}

func (p *recoveringPort) RecoverExecution(_ context.Context, key effect.AssignmentKey) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.recovered = append(p.recovered, key)
	return nil
}

// An orphaned target is deferred, and a port that can recover is asked to
// take the record back once, at Plan time; a port that cannot is only
// observed. Keep and Dispose never ask (RUN-CMT-7, RUN-EXE-6).
func TestPlanAsksRecovererForOrphans(t *testing.T) {
	for _, tc := range []struct {
		state         effect.AttachmentState
		wantRecovered int
	}{
		{effect.AttachmentOrphaned, 1},
		{effect.AttachmentActive, 0},
		{effect.AttachmentMissing, 0},
	} {
		port := &recoveringPort{fakePort: &fakePort{state: tc.state}}
		if _, err := (&Reconciler{Executions: port}).Plan(context.Background(), "s", executingModel("c1")); err != nil {
			t.Fatalf("%s: plan: %v", tc.state, err)
		}
		if len(port.recovered) != tc.wantRecovered {
			t.Fatalf("%s: recovered = %v, want %d", tc.state, port.recovered, tc.wantRecovered)
		}
	}
	plain := &fakePort{state: effect.AttachmentOrphaned}
	if _, err := (&Reconciler{Executions: plain}).Plan(context.Background(), "s", executingModel("c1")); err != nil {
		t.Fatalf("plain port: %v", err)
	}
}

// A kept target whose Outcome stays not-ready is probed once the backoff has
// settled: a record that has become orphaned is recovered once per episode,
// and the Outcome the recovery produces is delivered.
func TestKeptOutcomeProbeRecoversOrphan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var mu sync.Mutex
	ready := false
	port := &recoveringPort{fakePort: &fakePort{state: effect.AttachmentActive}}
	port.outcome = func(context.Context, effect.AssignmentKey) (effect.Outcome, error) {
		mu.Lock()
		defer mu.Unlock()
		if !ready {
			return effect.Outcome{}, effect.ErrOutcomeNotReady
		}
		return effect.Outcome{Result: effect.ModelSucceeded{Result: sdk.ModelResult{Text: "recovered"}}}, nil
	}
	delivered := make(chan effect.Outcome, 1)
	r := &Reconciler{Executions: port, Lifetime: ctx, Deliver: func(out effect.Outcome) { delivered <- out }}
	if _, err := r.Plan(ctx, "s", executingModel("c1")); err != nil {
		t.Fatal(err)
	}
	// The Worker dies: the record reads as orphaned from now on.
	port.mu.Lock()
	port.state = effect.AttachmentOrphaned
	port.mu.Unlock()
	deadline := time.Now().Add(8 * time.Second)
	for {
		port.mu.Lock()
		n := len(port.recovered)
		port.mu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("recovery requests = %d, want 1", n)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Recovery took the record back and finished it.
	mu.Lock()
	ready = true
	mu.Unlock()
	select {
	case out := <-delivered:
		if m, ok := out.Result.(effect.ModelSucceeded); !ok || m.Result.Text != "recovered" {
			t.Fatalf("delivered %+v", out)
		}
	case <-ctx.Done():
		t.Fatal("outcome was not delivered after recovery")
	}
	port.mu.Lock()
	n := len(port.recovered)
	port.mu.Unlock()
	if n != 1 {
		t.Fatalf("recovery requests = %d, want exactly 1", n)
	}
}
