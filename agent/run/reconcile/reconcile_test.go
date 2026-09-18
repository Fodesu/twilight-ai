package reconcile

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/effect"
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

func executingModel(claim run.ExecutionClaim) *run.RuntimeSnapshot {
	return &run.RuntimeSnapshot{SchemaVersion: run.SchemaVersion1, State: run.MachineState{
		RunID: "r1", Status: run.RunActive,
		Current: run.ModelStep{RefValue: run.StepRef{RunID: "r1", ID: "s1"}, Model: "m", RequestDigest: "sha256:req", Status: run.ModelExecuting, Claim: claim},
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
		decisions, err := r.Plan(context.Background(), "s", executingModel("c1"), "takeover")
		if err != nil || len(decisions) != 1 {
			t.Fatalf("%s: plan = %+v %v", tc.state, decisions, err)
		}
		d := decisions[0]
		if d.Verdict != tc.want || (d.Recovery != nil) != tc.disposes || d.Observed != tc.state {
			t.Fatalf("%s: decision = %+v, want %s disposes=%v", tc.state, d, tc.want, tc.disposes)
		}
		if len(port.asked) != 1 || port.asked[0] != (effect.AssignmentKey{Session: "s", RunID: "r1", StepID: "s1", Claim: "c1"}) {
			t.Fatalf("%s: asked = %+v", tc.state, port.asked)
		}
	}
	if _, err := (&Reconciler{Executions: &fakePort{state: "weird"}}).Plan(context.Background(), "s", executingModel("c1"), "t"); err == nil {
		t.Fatal("unknown attachment state accepted")
	}
	// No executor: every target is missing and disposed; a target without a
	// Claim is disposed without asking.
	decisions, err := (&Reconciler{}).Plan(context.Background(), "s", executingModel("c1"), "t")
	if err != nil || len(decisions) != 1 || decisions[0].Verdict != Dispose || decisions[0].Recovery == nil {
		t.Fatalf("plan without executor = %+v %v", decisions, err)
	}
	if _, ok := decisions[0].Recovery.Command.(run.RecoverModelExecution); !ok {
		t.Fatalf("model disposal = %T", decisions[0].Recovery.Command)
	}
}

// AssignmentFromTarget carries the digest-level description of the target
// and never an inline request body (RUN-EXE-7).
func TestAssignmentFromTarget(t *testing.T) {
	targets := run.RecoveryTargets(&executingModel("c1").State)
	if len(targets) != 1 {
		t.Fatalf("targets = %d", len(targets))
	}
	targets[0].Schema = run.SchemaVersion1
	a := AssignmentFromTarget("s", targets[0])
	if a.Kind != effect.AssignmentModel || a.Model == nil || a.Model.Request != nil || a.Model.RequestDigest != "sha256:req" || a.Schema != run.SchemaVersion1 {
		t.Fatalf("assignment = %+v", a)
	}
	if a.Key() != (effect.AssignmentKey{Session: "s", RunID: "r1", StepID: "s1", Claim: "c1"}) {
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
			return effect.Outcome{Key: key, Model: &result}, nil
		default:
			once.Do(func() { close(failed) })
			return effect.Outcome{}, errors.New("temporary transport error")
		}
	}}
	delivered := make(chan effect.Outcome, 1)
	r := &Reconciler{Executions: port, Lifetime: ctx, Deliver: func(out effect.Outcome) { delivered <- out }}
	if _, err := r.Plan(ctx, "s", executingModel("c1"), "t"); err != nil {
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
		if out.Model == nil || out.Model.Text != "eventual" {
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
	decisions, err := r.Plan(ctx, "s", executingModel("c1"), "t")
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
			if _, err := r.Plan(ctx, "s", executingModel("c1"), "t"); err != nil {
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
