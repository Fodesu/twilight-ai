package runtime_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/felinics/twilight/agentcore/compaction"
	"github.com/felinics/twilight/agentcore/driver"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/prompt"
	"github.com/felinics/twilight/sdk"
)

type openCompactionBuilder struct {
	inner  prompt.Builder
	builds *atomic.Int32
}

func (b openCompactionBuilder) Build(ctx context.Context, input prompt.Input) (prompt.Prompt, error) {
	b.builds.Add(1)
	p, err := b.inner.Build(ctx, input)
	// Freshness follows the maintenance gate, not a Builder token convention.
	p.Token = "opaque"
	return p, err
}

func TestCompactionAcquireAndStepDoNotWaitForSummary(t *testing.T) {
	started, release := make(chan struct{}, 1), make(chan struct{})
	sent := make(chan sdk.Request, 1)
	f := newCompactFixture(t, func(ctx context.Context, req sdk.Request) (sdk.ModelResult, error) {
		if req.System == "summary" {
			started <- struct{}{}
			select {
			case <-release:
				return sdk.ModelResult{Text: compactSummary}, nil
			case <-ctx.Done():
				return sdk.ModelResult{}, ctx.Err()
			}
		}
		sent <- req
		return sdk.ModelResult{Text: "answer"}, nil
	})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	var builds atomic.Int32
	var err error
	f.builders, err = prompt.NewCatalog(map[prompt.BuilderRef]prompt.PromptBuilderFactory{
		"context": func(ap preset.AgentPreset, src prompt.Sources) prompt.Builder {
			return openCompactionBuilder{inner: compactBuilder{ap: ap, src: src}, builds: &builds}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	x := f.execution(f.worker, nil, true)
	f.history()
	current := f.startTurn("current", false)
	f.accept(x, "before-step")
	result, err := x.Driver.Step(f.ctx, f.w, current.Ref.TurnID)
	if err != nil || result.Disposition != driver.Waiting {
		t.Fatalf("Step=%+v, %v", result, err)
	}
	waitCompact(t, f.ctx, started)
	if builds.Load() != 0 {
		t.Fatal("ordinary candidate built while summary was pending")
	}
	epoch := f.w.Epoch()
	// Closing/reacquiring is explicit host work and must not await the model.
	f.reopenWriter()
	if f.w.Epoch() <= epoch {
		t.Fatal("acquisition did not advance epoch")
	}
	replacement := f.execution(f.worker, nil, false)
	result, err = replacement.Driver.Step(f.ctx, f.w, current.Ref.TurnID)
	if err != nil || result.Disposition != driver.Waiting || builds.Load() != 0 {
		t.Fatalf("pending replacement Step=%+v, %v, builds=%d", result, err, builds.Load())
	}
	close(release)
	if _, err := f.drive(replacement, f.w, current.Ref.TurnID); err != nil {
		t.Fatal(err)
	}
	req := waitCompact(t, f.ctx, sent)
	if builds.Load() != 1 || len(req.Messages) != 2 {
		t.Fatalf("builds=%d messages=%d, want 1/2", builds.Load(), len(req.Messages))
	}
	text, ok := req.Messages[0].Content[0].(sdk.TextPart)
	if !ok || text.Text != compactSummary {
		t.Fatalf("model received old context: %+v", req.Messages)
	}
	f.counts(1, 1, 1)
}

func TestCompactionStepSummaryFailureReturnsToCaller(t *testing.T) {
	var normal atomic.Int32
	f := newCompactFixture(t, func(_ context.Context, req sdk.Request) (sdk.ModelResult, error) {
		if req.System == "summary" {
			return sdk.ModelResult{}, errors.New("summary unavailable")
		}
		normal.Add(1)
		return sdk.ModelResult{Text: "unexpected answer"}, nil
	})
	x := f.execution(f.worker, nil, true)
	f.history()
	current := f.startTurn("current", false)
	f.accept(x, "step-failed")
	if _, err := f.drive(x, f.w, current.Ref.TurnID); !errors.Is(err, driver.ErrSummaryFailed) {
		t.Fatalf("Step=%v, want summary failure", err)
	}
	if op := f.state().Operations["step-failed"]; op.Status != compaction.Failed || !op.Acknowledged {
		t.Fatalf("failure was not durably settled: %+v", op)
	}
	if normal.Load() != 0 {
		t.Fatal("failed maintenance started a normal model call")
	}
	f.counts(0, 0, 0)
}

func TestCompactionAdvanceCancellationLeavesAcceptedWork(t *testing.T) {
	for _, dispatched := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-dispatch", true: "after-dispatch"}[dispatched], func(t *testing.T) {
			started, release := make(chan struct{}, 1), make(chan struct{})
			f := newCompactFixture(t, func(ctx context.Context, _ sdk.Request) (sdk.ModelResult, error) {
				started <- struct{}{}
				select {
				case <-release:
					return sdk.ModelResult{Text: compactSummary}, nil
				case <-ctx.Done():
					return sdk.ModelResult{}, ctx.Err()
				}
			})
			t.Cleanup(func() {
				select {
				case <-release:
				default:
					close(release)
				}
			})
			x := f.execution(f.worker, nil, true)
			f.history()
			f.accept(x, "cancel-advance")
			if dispatched {
				if result, err := x.Compactions.Advance(f.ctx, f.w); err != nil || !result.Pending {
					t.Fatalf("dispatch=%+v, %v", result, err)
				}
				waitCompact(t, f.ctx, started)
			}
			ctx, cancel := context.WithCancel(f.ctx)
			cancel()
			if _, err := x.Compactions.Advance(ctx, f.w); !errors.Is(err, context.Canceled) {
				t.Fatalf("Advance=%v, want cancellation", err)
			}
			if op := f.state().Operations["cancel-advance"]; op.Status != compaction.Pending || op.Acknowledged {
				t.Fatalf("cancelled coordination lost work: %+v", op)
			}
			f.reopenWriter()
			close(release)
			f.settleMaintenance(f.execution(f.worker, nil, false))
			if op := f.state().Operations["cancel-advance"]; op.Status != compaction.Applied || !op.Acknowledged {
				t.Fatalf("replacement did not adopt result: %+v", op)
			}
			f.counts(1, 1, 0)
		})
	}
}

func TestCompactionDeliverValidatesIdentityAndIsIdempotent(t *testing.T) {
	f := newCompactFixture(t, func(context.Context, sdk.Request) (sdk.ModelResult, error) {
		return sdk.ModelResult{Text: compactSummary}, nil
	})
	x := f.execution(f.worker, nil, true)
	f.history()
	f.accept(x, "delivery")
	accepted := f.state().Operations["delivery"]
	key := accepted.Request.Key(compactSession)
	if result, err := x.Compactions.Advance(f.ctx, f.w); err != nil || !result.Pending {
		t.Fatalf("dispatch=%+v, %v", result, err)
	}
	out := f.awaitOutcome(f.worker, key)
	wrong := out
	wrong.Key.Session = "another-session"
	if _, err := x.Compactions.Deliver(f.ctx, f.w, wrong); err == nil {
		t.Fatal("accepted another Session's outcome")
	}
	if f.state().Pending != "delivery" {
		t.Fatal("mismatched delivery consumed intent")
	}
	if result, err := x.Compactions.Deliver(f.ctx, f.w, out); err != nil || result.Pending || !result.Progress {
		t.Fatalf("Deliver=%+v, %v", result, err)
	}
	if result, err := x.Compactions.Deliver(f.ctx, f.w, out); err != nil || result.Pending || result.Progress {
		t.Fatalf("duplicate Deliver=%+v, %v", result, err)
	}
	// A late duplicate may arrive while a different intent is pending. It
	// cannot consume or replace that new operation.
	f.startTurn("more-history", true)
	f.accept(x, "next")
	if result, err := x.Compactions.Deliver(f.ctx, f.w, out); err != nil || !result.Pending || result.Key == key {
		t.Fatalf("late duplicate=%+v, %v", result, err)
	}
	f.counts(1, 1, 0)
	f.assertAtomicFinish()
}
