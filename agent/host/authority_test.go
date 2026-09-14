package host_test

import (
	"context"
	"sync"
	"testing"

	"github.com/felinics/twilight/agent/host"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
	runmod "github.com/felinics/twilight/agent/session/run"
	"github.com/felinics/twilight/agent/turn"
	"github.com/felinics/twilight/sdk"
)

// recordingExecutor stands in for a remote worker: it holds no ModelInvoker
// and no ExecutableTool, records every Assignment it is handed and answers
// each model assignment with a scripted Outcome from another goroutine.
type recordingExecutor struct {
	mu       sync.Mutex
	assigned []loop.Assignment
	reply    string
	outcomes map[loop.AssignmentKey]chan loop.Outcome
}

func (e *recordingExecutor) Validate(context.Context, loop.Assignment) (*run.ToolFailure, error) {
	return nil, nil
}

func (e *recordingExecutor) Dispatch(_ context.Context, a loop.Assignment) error {
	e.mu.Lock()
	if e.outcomes == nil {
		e.outcomes = make(map[loop.AssignmentKey]chan loop.Outcome)
	}
	e.assigned = append(e.assigned, a)
	e.outcomes[a.Key()] = make(chan loop.Outcome, 1)
	ch := e.outcomes[a.Key()]
	reply := e.reply
	e.mu.Unlock()
	go func() {
		ch <- loop.Outcome{Key: a.Key(), Model: &sdk.ModelResult{Text: reply, FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}}
	}()
	return nil
}

func (e *recordingExecutor) Attach(context.Context, loop.AssignmentKey) (loop.Attachment, error) {
	return loop.Attachment{State: loop.AttachmentMissing, Execution: loop.ExecutionNotFound}, nil
}

func (e *recordingExecutor) GetStatus(context.Context, loop.AssignmentKey) (loop.ExecutionStatus, error) {
	return loop.ExecutionRunning, nil
}

func (e *recordingExecutor) GetOutcome(ctx context.Context, key loop.AssignmentKey) (loop.Outcome, error) {
	e.mu.Lock()
	ch := e.outcomes[key]
	e.mu.Unlock()
	if ch == nil {
		return loop.Outcome{}, loop.ErrExecutionNotFound
	}
	select {
	case out := <-ch:
		return out, nil
	case <-ctx.Done():
		return loop.Outcome{}, ctx.Err()
	}
}

func (e *recordingExecutor) Cancel(context.Context, loop.AssignmentKey) error { return nil }

func (e *recordingExecutor) assignments() []loop.Assignment {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]loop.Assignment(nil), e.assigned...)
}

// The authority side needs no effect implementation (HST-PRT-2): a Host built
// over an Executor that is only a recorder registers an AgentPreset, starts a Turn,
// dispatches the model Assignment with the frozen request's digest, and
// settles the Outcome the executor sends back. Every model client and tool
// lives on the executor's side of the port.
func TestAuthorityRunsWithoutEffectImplementations(t *testing.T) {
	ctx := context.Background()
	exec := &recordingExecutor{reply: "hello from the executor"}
	content := memoryContent()
	frozen := runmod.FrozenValues(content)
	h, err := host.New(host.Ports{Executor: exec, Content: content})
	if err != nil {
		t.Fatal(err)
	}
	presetRef, err := h.Presets.Register("remote", mustPreset("m-remote", nil, host.WithSystemPrompt("be brief")))
	if err != nil {
		t.Fatal(err)
	}
	s, err := h.OpenSession(ctx, "s-authority", host.SessionOptions{Preset: presetRef})
	if err != nil {
		t.Fatal(err)
	}
	results, err := s.Send(ctx, "what is the weather?")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Status != turn.TurnCompleted || results[0].Reply != "hello from the executor" {
		t.Fatalf("results = %+v", results)
	}

	assigned := exec.assignments()
	if len(assigned) != 1 {
		t.Fatalf("assignments = %d, want 1 model assignment", len(assigned))
	}
	a := assigned[0]
	if a.Kind != loop.AssignmentModel || a.Model == nil || a.Model.Model != "m-remote" || a.Claim == "" || a.RunID == "" || a.StepID == "" {
		t.Fatalf("assignment = %+v", a)
	}
	// The body the executor would fetch is in the shared frozen store under
	// the digest the Assignment carries (RUN-WIR-4).
	raw, ok, err := frozen.Get(ctx, a.Model.RequestDigest)
	if err != nil || !ok || len(raw) == 0 {
		t.Fatalf("frozen request %s: ok=%v err=%v", a.Model.RequestDigest, ok, err)
	}
	req, err := run.DecodeFrozenRequest(raw, a.Model.RequestDigest)
	if err != nil {
		t.Fatal(err)
	}
	if req.Model != "m-remote" || len(req.Messages) == 0 {
		t.Fatalf("frozen request = %+v", req)
	}
}

// A PresetRef whose digest no longer matches the registration is
// unavailable, so a Turn recorded under an older configuration is refused
// rather than driven with a different decision function (HST-PST-2).
func TestStalePresetRefIsUnavailable(t *testing.T) {
	presets := host.NewPresets()
	ref, err := presets.Register("p", mustPreset("m-1", nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := presets.Register("p", mustPreset("m-1", nil, host.WithStreaming(true))); err != nil {
		t.Fatal(err)
	}
	if _, err := presets.Resolve(ref); err == nil {
		t.Fatal("stale ref resolved")
	}
	if _, err := presets.Resolve(turn.PresetRef{ID: "missing", Digest: ref.Digest}); err == nil {
		t.Fatal("unknown preset resolved")
	}
}
