package runtime_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/felinics/twilight/agentcore/compaction"
	"github.com/felinics/twilight/agentcore/driver"
	"github.com/felinics/twilight/agentcore/owner"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/prompt"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/sdk"
)

type openCompactionBuilder struct {
	inner  prompt.Builder
	builds *atomic.Int32
}

func (b openCompactionBuilder) Build(ctx context.Context, input prompt.Input) (prompt.Prompt, error) {
	b.builds.Add(1)
	p, err := b.inner.Build(ctx, input)
	// Freshness must follow opening order, not a Builder token convention.
	p.Token = "opaque"
	return p, err
}

func TestCompactionOpenWaitsBeforeNormalDrive(t *testing.T) {
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
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	f.history()
	current := f.startTurn("current", false)
	f.accept(x, "open-before-drive")
	observed := &compactWriter{Writer: f.w, modelCompleted: make(chan struct{}, 1)}
	opened := make(chan error, 1)
	go func() {
		_, err := x.Open(f.ctx, observed)
		if err == nil {
			state, readErr := compaction.Read(f.ctx, f.w.Projections(), compactSession)
			if readErr != nil {
				err = readErr
			} else if state.Operations["open-before-drive"].Status != compaction.Applied {
				err = errors.New("Open returned before adopting the summary")
			}
		}
		opened <- err
	}()
	waitCompact(t, f.ctx, started)
	if builds.Load() != 0 {
		t.Fatal("ordinary request built while opening waited on summary")
	}
	select {
	case err := <-opened:
		t.Fatalf("Open returned while summary blocked: %v", err)
	default:
	}
	close(release)
	if err := waitCompact(t, f.ctx, opened); err != nil {
		t.Fatal(err)
	}
	// Open may resume the active Turn itself. A caller's Drive either carries
	// it or observes that continuation; both must see the adopted summary.
	if _, err := x.Driver.Drive(f.ctx, observed, current.Ref.TurnID); err != nil {
		t.Fatal(err)
	}
	waitCompact(t, f.ctx, observed.modelCompleted)
	x.Stop(compactSession)
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

func TestCompactionOwnerCloseCancelsOpening(t *testing.T) {
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
	x := f.execution(f.worker, nil, true)
	f.history()
	f.accept(x, "owner-opening")
	a := owner.New(f.k, x)
	t.Cleanup(func() { _ = a.Close(context.Background()) })
	// No caller deadline: Owner.Close itself must cancel the waiting Open.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	opened := make(chan error, 1)
	go func() {
		h, err := a.Open(ctx, compactSession)
		if err == nil {
			_ = h.Close(context.Background())
			err = errors.New("unexpected Handle while summary is blocked")
		}
		opened <- err
	}()
	waitCompact(t, f.ctx, started)
	closed := make(chan error, 1)
	go func() { closed <- a.Close(f.ctx) }()
	if err := waitCompact(t, f.ctx, closed); err != nil {
		t.Fatal(err)
	}
	if err := waitCompact(t, f.ctx, opened); !errors.Is(err, context.Canceled) {
		t.Fatalf("Open=%v, want cancellation", err)
	}
	// Close has released the Writer, not consumed or cancelled the summary.
	state, err := compaction.Read(f.ctx, f.k.Projections, compactSession)
	if err != nil {
		t.Fatal(err)
	}
	if op := state.Operations["owner-opening"]; op.Status != compaction.Pending || op.Acknowledged {
		t.Fatalf("shutdown lost work: %+v", op)
	}
	close(release)
	replacement := owner.New(f.k, x)
	t.Cleanup(func() { _ = replacement.Close(context.Background()) })
	h, err := replacement.Open(f.ctx, compactSession)
	if err != nil {
		t.Fatal(err)
	}
	state, err = compaction.Read(f.ctx, h.Writer().Projections(), compactSession)
	if err != nil {
		t.Fatal(err)
	}
	if op := state.Operations["owner-opening"]; op.Status != compaction.Applied || !op.Acknowledged {
		t.Fatalf("replacement did not recover: %+v", op)
	}
}

func TestCompactionOpenSummaryFailureReturnsToCaller(t *testing.T) {
	var normal, notifications atomic.Int32
	f := newCompactFixture(t, func(_ context.Context, req sdk.Request) (sdk.ModelResult, error) {
		if req.System == "summary" {
			return sdk.ModelResult{}, errors.New("summary unavailable")
		}
		normal.Add(1)
		return sdk.ModelResult{Text: "unexpected answer"}, nil
	})
	x := f.execution(f.worker, nil, true)
	x.Recovery.Fail = func(_ session.SessionID, _ error) { notifications.Add(1) }
	f.history()
	f.startTurn("current", false)
	f.accept(x, "opening-failed")
	if _, err := x.Open(f.ctx, f.w); !errors.Is(err, driver.ErrSummaryFailed) {
		t.Fatalf("Open=%v, want summary failure", err)
	}
	x.Stop(compactSession)
	if op := f.state().Operations["opening-failed"]; op.Status != compaction.Failed || !op.Acknowledged {
		t.Fatalf("failure was not durably settled: %+v", op)
	}
	if normal.Load() != 0 || notifications.Load() != 0 {
		t.Fatal("failed opening must return its error, not start a normal drive or report a background error")
	}
	f.counts(0, 0, 0)
}

func TestCompactionOpenCancellationLeavesAcceptedWork(t *testing.T) {
	for _, cause := range []string{"caller", "session-stop", "execution-close"} {
		t.Run(cause, func(t *testing.T) {
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
			x := f.execution(f.worker, nil, true)
			t.Cleanup(func() {
				select {
				case <-release:
				default:
					close(release)
				}
			})
			f.history()
			f.accept(x, "cancel-opening")
			ctx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			opened := make(chan error, 1)
			go func() { _, err := x.Open(ctx, f.w); opened <- err }()
			waitCompact(t, f.ctx, started)
			if cause == "caller" {
				cancel()
			} else {
				stopped := make(chan struct{})
				go func() {
					if cause == "execution-close" {
						x.Close()
					} else {
						x.Stop(compactSession)
					}
					close(stopped)
				}()
				waitCompact(t, f.ctx, stopped)
			}
			if err := waitCompact(t, f.ctx, opened); !errors.Is(err, context.Canceled) {
				t.Fatalf("Open=%v, want cancellation", err)
			}
			if op := f.state().Operations["cancel-opening"]; op.Status != compaction.Pending || op.Acknowledged {
				t.Fatalf("cancelled opening lost work: %+v", op)
			}
			close(release)
			replacement := x
			if cause == "execution-close" {
				replacement = f.execution(f.worker, nil, false)
			}
			if _, err := replacement.Open(f.ctx, f.w); err != nil {
				t.Fatal(err)
			}
			if op := f.state().Operations["cancel-opening"]; op.Status != compaction.Applied || !op.Acknowledged {
				t.Fatalf("reopening did not adopt result: %+v", op)
			}
			f.counts(1, 1, 0)
		})
	}
}
