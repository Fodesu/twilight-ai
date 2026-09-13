package host_test

import (
	"context"
	"sync"
	"testing"

	"github.com/felinics/twilight/agent/host"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/filestore"
	runmod "github.com/felinics/twilight/agent/session/run"
	"github.com/felinics/twilight/agent/turn"
	"github.com/felinics/twilight/sdk"
)

// gateModel blocks its first Generate until release, capturing the request:
// the process "crashes" while the ModelStep is Executing.
type gateModel struct {
	started chan sdk.Request
	release chan struct{}
}

func (m *gateModel) Generate(_ context.Context, req sdk.Request) (sdk.ModelResult, error) {
	m.started <- req
	<-m.release
	return sdk.ModelResult{Text: "late", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}, nil
}

// crashMidModel runs process 1 on root until its model call is Executing and
// returns the request it was sent plus the profile ref; the process is then
// considered dead (its Send is left blocked and fenced later).
func crashMidModel(t *testing.T, root string, sid session.SessionID) (turn.ProfileRef, turn.Profile, sdk.Request, *gateModel, chan error) {
	t.Helper()
	ctx := context.Background()
	profile := mustProfile("m-1", nil, host.WithSystemPrompt("be brief"))
	store1, err := filestore.New(root)
	if err != nil {
		t.Fatal(err)
	}
	content1, err := filestore.NewContentStore(root, runmod.FrozenAuthority, filestore.ContentStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	gate := &gateModel{started: make(chan sdk.Request, 1), release: make(chan struct{})}
	p1 := newHost(host.Ports{Store: store1, Content: content1}, map[run.ModelRef]loop.ModelInvoker{"m-1": gate})
	profileRef, err := p1.Profiles.Register("a1", profile)
	if err != nil {
		t.Fatal(err)
	}
	s1, err := p1.OpenSession(ctx, sid, host.SessionOptions{Profile: profileRef})
	if err != nil {
		t.Fatal(err)
	}
	sendErr := make(chan error, 1)
	go func() {
		_, err := s1.Send(ctx, "what is the weather?")
		sendErr <- err
	}()
	var sent sdk.Request
	select {
	case sent = <-gate.started:
	case err := <-sendErr:
		t.Fatalf("send returned before the model executed: %v", err)
	}
	return profileRef, profile, sent, gate, sendErr
}

// Process 2 cannot reattach (a colocated executor died with process 1), so the
// takeover withdraws the Executing step and Resume plans again from the state
// at recovery time: a new request, not a replay of the frozen one (RUN-CMT-7,
// TRN-DUR-1). The frozen body on disk is a transfer copy, not a recovery input.
func TestRestartWithoutReattachReplans(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	const sid session.SessionID = "s-frozen-replan"
	profileRef, profile, sent, gate, sendErr := crashMidModel(t, root, sid)

	store2, err := filestore.New(root)
	if err != nil {
		t.Fatal(err)
	}
	content2, err := filestore.NewContentStore(root, runmod.FrozenAuthority, filestore.ContentStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	replan := &scriptedRequests{}
	p2 := newHost(host.Ports{Store: store2, Content: content2, Ownership: session.OpenOptions{Takeover: true}}, map[run.ModelRef]loop.ModelInvoker{"m-1": replan})
	if _, err := p2.Profiles.Register("a1", profile); err != nil {
		t.Fatal(err)
	}
	s2, err := p2.OpenSession(ctx, sid, host.SessionOptions{Profile: profileRef})
	if err != nil {
		t.Fatal(err)
	}
	if s2.Recovered != 1 {
		t.Fatalf("recovered = %d, want 1 (the executing ModelStep withdrawn)", s2.Recovered)
	}
	tsurf, err := p2.TurnSurface(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	active, ok := tsurf.Active()
	if !ok {
		t.Fatal("takeover lost the active turn")
	}
	snap, err := p2.Runtime.Load(ctx, sid, active.ActiveRun)
	if err != nil {
		t.Fatal(err)
	}
	if _, open := snap.State.Current.(run.Open); !open || snap.State.ModelSteps != 0 {
		t.Fatalf("run after takeover = %+v, want Open with the aborted step uncounted", snap.State)
	}

	results, resumed, err := s2.Resume(ctx)
	if err != nil || !resumed || len(results) == 0 || results[0].Status != turn.TurnCompleted {
		t.Fatalf("resume = %+v %v %v", results, resumed, err)
	}
	// One fresh model call, planned by process 2 -- the same conversation, so
	// the same messages, but a new decision at recovery time.
	seen := replan.requests()
	if len(seen) != 1 {
		t.Fatalf("replanning model saw %d requests, want 1", len(seen))
	}
	if got, want := len(seen[0].Messages), len(sent.Messages); got != want {
		t.Fatalf("replanned request has %d messages, the aborted one had %d", got, want)
	}
	final, err := p2.Runtime.Load(ctx, sid, active.ActiveRun)
	if err != nil || final.State.ModelSteps != 1 {
		t.Fatalf("model steps after replan = %d %v, want exactly the replanned step", final.State.ModelSteps, err)
	}

	// The dead process's worker returns and is fenced.
	close(gate.release)
	if err := <-sendErr; err == nil {
		t.Fatal("the superseded process's Send settled without an ownership error")
	}
}

// reattachingExecutor answers Attach with true for one key and delivers the
// attempt's real Outcome to whoever attached: the executor outlived the
// authority, as a remote executor does.
type reattachingExecutor struct {
	mu       sync.Mutex
	attached []loop.Assignment
	deliver  loop.Deliver
	reply    string
}

func (e *reattachingExecutor) Validate(context.Context, loop.Assignment) (*run.ToolFailure, error) {
	return nil, nil
}
func (e *reattachingExecutor) Dispatch(_ context.Context, a loop.Assignment, deliver loop.Deliver) error {
	go deliver(loop.Outcome{Key: a.Key(), Model: &sdk.ModelResult{Text: e.reply, FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}})
	return nil
}
func (e *reattachingExecutor) Attach(_ context.Context, a loop.Assignment, deliver loop.Deliver) (bool, error) {
	e.mu.Lock()
	e.attached = append(e.attached, a)
	e.deliver = deliver
	e.mu.Unlock()
	return true, nil
}
func (e *reattachingExecutor) Cancel(context.Context, run.RunID) error { return nil }

// When the attempt is still running on an executor that outlived the
// authority, the new owner reattaches: nothing is withdrawn, no new plan is
// made, and the attempt's own Outcome completes the same step (RUN-CMT-7).
func TestRestartReattachesRunningModelAttempt(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	const sid session.SessionID = "s-frozen-reattach"
	profileRef, profile, _, gate, sendErr := crashMidModel(t, root, sid)

	store2, err := filestore.New(root)
	if err != nil {
		t.Fatal(err)
	}
	content2, err := filestore.NewContentStore(root, runmod.FrozenAuthority, filestore.ContentStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	exec := &reattachingExecutor{reply: "reattached"}
	p2, err := host.New(host.Ports{Store: store2, Content: content2, Executor: exec, Ownership: session.OpenOptions{Takeover: true}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p2.Profiles.Register("a1", profile); err != nil {
		t.Fatal(err)
	}
	s2, err := p2.OpenSession(ctx, sid, host.SessionOptions{Profile: profileRef})
	if err != nil {
		t.Fatal(err)
	}
	if s2.Recovered != 0 {
		t.Fatalf("recovered = %d, want 0 (the attempt was reattached, not disposed)", s2.Recovered)
	}
	exec.mu.Lock()
	attached, deliver := len(exec.attached), exec.deliver
	exec.mu.Unlock()
	if attached != 1 || deliver == nil {
		t.Fatalf("attach calls = %d, deliver registered = %v", attached, deliver != nil)
	}
	tsurf, err := p2.TurnSurface(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	active, ok := tsurf.Active()
	if !ok {
		t.Fatal("takeover lost the active turn")
	}
	snap, err := p2.Runtime.Load(ctx, sid, active.ActiveRun)
	if err != nil {
		t.Fatal(err)
	}
	step, isModel := snap.State.Current.(run.ModelStep)
	if !isModel || step.Status != run.ModelExecuting {
		t.Fatalf("run after reattach = %+v, want the step still Executing", snap.State)
	}

	// The executor finishes the original attempt; its Outcome reaches process 2.
	deliver(loop.Outcome{Key: exec.attached[0].Key(), Model: &sdk.ModelResult{Text: "reattached", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}})
	if err := s2.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	tsurf, err = p2.TurnSurface(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	if v := tsurf.Turns[active.TurnID]; v.Status != turn.TurnCompleted {
		t.Fatalf("turn after reattached outcome = %s, want completed", v.Status)
	}
	final, err := p2.Runtime.Load(ctx, sid, active.ActiveRun)
	if err != nil || final.State.ModelSteps != 1 {
		t.Fatalf("model steps = %d %v, want the one original step", final.State.ModelSteps, err)
	}
	for _, f := range mustRecord(t, p2, sid, active.ActiveRun).Facts {
		if _, recovered := f.(run.ModelStepRecovered); recovered {
			t.Fatal("a reattached attempt must not be recovered")
		}
	}

	close(gate.release)
	if err := <-sendErr; err == nil {
		t.Fatal("the superseded process's Send settled without an ownership error")
	}
}

func mustRecord(t *testing.T, h *host.Host, sid session.SessionID, runID run.RunID) run.RunRecord {
	t.Helper()
	rec, err := h.Runtime.Record(context.Background(), sid, runID)
	if err != nil {
		t.Fatal(err)
	}
	return rec
}
