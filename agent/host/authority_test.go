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
}

func (e *recordingExecutor) Validate(context.Context, loop.Assignment) (*run.ToolFailure, error) {
	return nil, nil
}

func (e *recordingExecutor) Dispatch(_ context.Context, a loop.Assignment, deliver loop.Deliver) error {
	e.mu.Lock()
	e.assigned = append(e.assigned, a)
	e.mu.Unlock()
	go deliver(loop.Outcome{Key: a.Key(), Model: &sdk.ModelResult{Text: e.reply, FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}})
	return nil
}

func (e *recordingExecutor) Attach(context.Context, loop.Assignment, loop.Deliver) (bool, error) {
	return false, nil
}

func (e *recordingExecutor) Cancel(context.Context, run.RunID) error { return nil }

func (e *recordingExecutor) assignments() []loop.Assignment {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]loop.Assignment(nil), e.assigned...)
}

// The authority side needs no effect implementation (HST-PRT-2): a Host built
// over an Executor that is only a recorder registers a Profile, starts a Turn,
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
	profileRef, err := h.Profiles.Register("remote", mustProfile("m-remote", nil, host.WithSystemPrompt("be brief")))
	if err != nil {
		t.Fatal(err)
	}
	s, err := h.OpenSession(ctx, "s-authority", host.SessionOptions{Profile: profileRef})
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

// A ProfileRef whose digest no longer matches the registration is
// unavailable, so a Turn recorded under an older configuration is refused
// rather than driven with a different decision function (HST-PRF-2).
func TestStaleProfileRefIsUnavailable(t *testing.T) {
	profiles := host.NewProfiles()
	ref, err := profiles.Register("p", mustProfile("m-1", nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := profiles.Register("p", mustProfile("m-1", nil, host.WithStreaming(true))); err != nil {
		t.Fatal(err)
	}
	if _, err := profiles.Resolve(ref); err == nil {
		t.Fatal("stale ref resolved")
	}
	if _, err := profiles.Resolve(turn.ProfileRef{ID: "missing", Digest: ref.Digest}); err == nil {
		t.Fatal("unknown profile resolved")
	}
}
