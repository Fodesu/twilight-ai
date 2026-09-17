package app_test

import (
	"context"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/app"
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
// returns the request it was sent plus the preset ref; the process is then
// considered dead (its Send is left blocked and fenced later).
func crashMidModel(t *testing.T, root string, sid session.SessionID) (turn.PresetRef, turn.AgentPreset, sdk.Request, *gateModel, chan error) {
	t.Helper()
	ctx := context.Background()
	preset := mustPreset("m-1", nil, app.WithSystemPrompt("be brief"))
	store1, err := filestore.New(root)
	if err != nil {
		t.Fatal(err)
	}
	content1, err := filestore.NewContentStore(root, runmod.FrozenAuthority, filestore.ContentStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	gate := &gateModel{started: make(chan sdk.Request, 1), release: make(chan struct{})}
	p1 := newHost(app.Config{Store: store1, Content: content1}, map[run.ModelRef]loop.ModelInvoker{"m-1": gate})
	presetRef, err := p1.RegisterPreset("a1", preset)
	if err != nil {
		t.Fatal(err)
	}
	s1, err := p1.OpenSession(ctx, sid, app.SessionOptions{Preset: presetRef})
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
	return presetRef, preset, sent, gate, sendErr
}

// Process 2 cannot reattach (a colocated executor died with process 1), so the
// takeover withdraws the Executing step and Resume plans again from the state
// at recovery time: a new request, not a replay of the frozen one (RUN-CMT-7,
// TRN-DUR-1). The frozen body on disk is a transfer copy, not a recovery input.
func TestRestartWithoutReattachReplans(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	const sid session.SessionID = "s-frozen-replan"
	presetRef, preset, sent, gate, sendErr := crashMidModel(t, root, sid)

	store2, err := filestore.New(root)
	if err != nil {
		t.Fatal(err)
	}
	content2, err := filestore.NewContentStore(root, runmod.FrozenAuthority, filestore.ContentStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	replan := &scriptedRequests{}
	p2 := newHost(app.Config{Store: store2, Content: content2, Ownership: session.OpenOptions{Takeover: true}}, map[run.ModelRef]loop.ModelInvoker{"m-1": replan})
	if _, err := p2.RegisterPreset("a1", preset); err != nil {
		t.Fatal(err)
	}
	s2, err := p2.OpenSession(ctx, sid, app.SessionOptions{Preset: presetRef})
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
	snap, err := p2.Authority.Runtime.Load(ctx, sid, active.ActiveRun)
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
	final, err := p2.Authority.Runtime.Load(ctx, sid, active.ActiveRun)
	if err != nil || final.State.ModelSteps != 1 {
		t.Fatalf("model steps after replan = %d %v, want exactly the replanned step", final.State.ModelSteps, err)
	}

	// The dead process's worker returns and is fenced.
	close(gate.release)
	if err := <-sendErr; err == nil {
		t.Fatal("the superseded process's Send settled without an ownership error")
	}
}

// reattachingExecutor answers Attach with true for one key and retains the
// attempt's real Outcome for GetOutcome: the executor outlived the authority,
// as a remote executor does.
type reattachingExecutor struct {
	recordingExecutor
	attached []loop.AssignmentKey
	reads    chan context.Context
}

func (e *reattachingExecutor) GetOutcome(ctx context.Context, key loop.AssignmentKey) (loop.Outcome, error) {
	if e.reads != nil {
		e.reads <- ctx
	}
	return e.recordingExecutor.GetOutcome(ctx, key)
}

func (e *reattachingExecutor) Attach(_ context.Context, key loop.AssignmentKey) (loop.Attachment, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.attached = append(e.attached, key)
	if e.outcomes == nil {
		e.outcomes = make(map[loop.AssignmentKey]chan loop.Outcome)
	}
	if e.outcomes[key] == nil {
		e.outcomes[key] = make(chan loop.Outcome, 1)
	}
	return loop.Attachment{State: loop.AttachmentActive, Execution: loop.ExecutionRunning, BackendAttached: true}, nil
}

func (e *reattachingExecutor) complete(key loop.AssignmentKey, out loop.Outcome) {
	e.mu.Lock()
	ch := e.outcomes[key]
	e.mu.Unlock()
	out.Key = key
	ch <- out
}

// When the attempt is still running on an executor that outlived the
// authority, the new owner reattaches: nothing is withdrawn, no new plan is
// made, and the attempt's own Outcome completes the same step (RUN-CMT-7).
func TestRestartReattachesRunningModelAttempt(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	const sid session.SessionID = "s-frozen-reattach"
	presetRef, preset, _, gate, sendErr := crashMidModel(t, root, sid)

	store2, err := filestore.New(root)
	if err != nil {
		t.Fatal(err)
	}
	content2, err := filestore.NewContentStore(root, runmod.FrozenAuthority, filestore.ContentStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	exec := &reattachingExecutor{recordingExecutor: recordingExecutor{reply: "reattached"}, reads: make(chan context.Context, 4)}
	p2, err := app.Build(app.Config{Store: store2, Content: content2, Executor: app.ExecutorConfig{Port: exec}, Ownership: session.OpenOptions{Takeover: true}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p2.RegisterPreset("a1", preset); err != nil {
		t.Fatal(err)
	}
	openCtx, cancelOpen := context.WithCancel(ctx)
	defer cancelOpen()
	s2, err := p2.OpenSession(openCtx, sid, app.SessionOptions{Preset: presetRef})
	if err != nil {
		t.Fatal(err)
	}
	cancelOpen()
	var readCtx context.Context
	select {
	case readCtx = <-exec.reads:
	case <-time.After(2 * time.Second):
		t.Fatal("reattach did not start outcome reading")
	}
	if err := readCtx.Err(); err != nil {
		t.Fatalf("open request cancellation stopped recovery: %v", err)
	}
	if s2.Recovered != 0 {
		t.Fatalf("recovered = %d, want 0 (the attempt was reattached, not disposed)", s2.Recovered)
	}
	exec.mu.Lock()
	var key loop.AssignmentKey
	if len(exec.attached) > 0 {
		key = exec.attached[0]
	}
	attached, hasOutcome := len(exec.attached), exec.outcomes[key] != nil
	exec.mu.Unlock()
	if attached != 1 || !hasOutcome {
		t.Fatalf("attach calls = %d, outcome record = %v", attached, hasOutcome)
	}
	tsurf, err := p2.TurnSurface(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	active, ok := tsurf.Active()
	if !ok {
		t.Fatal("takeover lost the active turn")
	}
	snap, err := p2.Authority.Runtime.Load(ctx, sid, active.ActiveRun)
	if err != nil {
		t.Fatal(err)
	}
	step, isModel := snap.State.Current.(run.ModelStep)
	if !isModel || step.Status != run.ModelExecuting {
		t.Fatalf("run after reattach = %+v, want the step still Executing", snap.State)
	}

	// The executor finishes the original attempt; its Outcome reaches process 2.
	exec.complete(exec.attached[0], loop.Outcome{Model: &sdk.ModelResult{Text: "reattached", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}})
	deadline := time.After(2 * time.Second)
	for {
		tsurf, err = p2.TurnSurface(ctx, sid)
		if err != nil {
			t.Fatal(err)
		}
		if v := tsurf.Turns[active.TurnID]; v.Status == turn.TurnCompleted {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("turn after reattached outcome = %s, want completed", tsurf.Turns[active.TurnID].Status)
		case <-time.After(time.Millisecond):
		}
	}
	final, err := p2.Authority.Runtime.Load(ctx, sid, active.ActiveRun)
	if err != nil || final.State.ModelSteps != 1 {
		t.Fatalf("model steps = %d %v, want the one original step", final.State.ModelSteps, err)
	}
	for _, f := range mustRecord(t, p2, sid, active.ActiveRun).Facts {
		if _, recovered := f.(run.ModelStepRecovered); recovered {
			t.Fatal("a reattached attempt must not be recovered")
		}
	}
	if err := s2.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-readCtx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("session close left recovery lifetime active")
	}

	close(gate.release)
	if err := <-sendErr; err == nil {
		t.Fatal("the superseded process's Send settled without an ownership error")
	}
}

func TestCloseStopsPendingRecoveryRead(t *testing.T) {
	for _, scope := range []string{"host", "session"} {
		t.Run(scope, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			const sid session.SessionID = "close-recovery"
			ref, preset, _, gate, sendErr := crashMidModel(t, root, sid)
			t.Cleanup(func() {
				close(gate.release)
				<-sendErr
			})
			store, err := filestore.New(root)
			if err != nil {
				t.Fatal(err)
			}
			content, err := filestore.NewContentStore(root, runmod.FrozenAuthority, filestore.ContentStoreOptions{})
			if err != nil {
				t.Fatal(err)
			}
			exec := &reattachingExecutor{reads: make(chan context.Context, 4)}
			h, err := app.Build(app.Config{Store: store, Content: content, Executor: app.ExecutorConfig{Port: exec}, Ownership: session.OpenOptions{Takeover: true}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := h.RegisterPreset("a1", preset); err != nil {
				t.Fatal(err)
			}
			s, err := h.OpenSession(ctx, sid, app.SessionOptions{Preset: ref})
			if err != nil {
				t.Fatal(err)
			}
			var readCtx context.Context
			select {
			case readCtx = <-exec.reads:
			case <-time.After(2 * time.Second):
				t.Fatal("recovery did not start reading")
			}
			if scope == "host" {
				err = h.Close(ctx)
			} else {
				err = s.Close(ctx)
			}
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-readCtx.Done():
			case <-time.After(2 * time.Second):
				t.Fatal("close left a recovery read active")
			}
		})
	}
}

func mustRecord(t *testing.T, h *app.Application, sid session.SessionID, runID run.RunID) run.RunRecord {
	t.Helper()
	rec, err := h.Authority.Runtime.Record(context.Background(), sid, runID)
	if err != nil {
		t.Fatal(err)
	}
	return rec
}
