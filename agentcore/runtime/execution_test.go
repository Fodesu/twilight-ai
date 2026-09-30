package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/decision"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	rt "github.com/felinics/twilight/agentcore/runtime"
	"github.com/felinics/twilight/agentcore/session"
)

// idlePort is an ExecutionPort that accepts nothing and holds nothing: the
// Execution's assembly is under test, not any effect.
type idlePort struct{}

func (idlePort) Validate(context.Context, effect.Assignment) (*run.ToolFailure, error) { return nil, nil }
func (idlePort) Dispatch(context.Context, effect.Assignment) error                     { return nil }
func (idlePort) Attach(context.Context, effect.AssignmentKey) (effect.Attachment, error) {
	return effect.Attachment{State: effect.AttachmentMissing, Execution: effect.ExecutionNotFound}, nil
}
func (idlePort) Abort(context.Context, effect.AssignmentKey) (effect.Attachment, error) {
	return effect.Attachment{State: effect.AttachmentAborted, Execution: effect.ExecutionAborted}, nil
}
func (idlePort) GetStatus(context.Context, effect.AssignmentKey) (effect.ExecutionStatus, error) {
	return effect.ExecutionNotFound, effect.ErrExecutionNotFound
}
func (idlePort) GetOutcome(context.Context, effect.AssignmentKey) (effect.Outcome, error) {
	return effect.Outcome{}, effect.ErrExecutionNotFound
}
func (idlePort) Cancel(context.Context, effect.AssignmentKey) error { return nil }

func newExecution(t *testing.T, cfg rt.ExecutionConfig) *rt.Execution {
	t.Helper()
	cfg.Executor = idlePort{}
	if cfg.Decisions == nil {
		catalog, err := decision.NewCatalog(nil)
		if err != nil {
			t.Fatal(err)
		}
		cfg.Decisions = catalog
	}
	x, err := rt.NewExecution(cfg, rt.ExecutionSources{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(x.Close)
	return x
}

// A component failure reaches the transient stream exactly once: through
// the configured Fail callback, which owns that delivery, or directly when
// no callback is configured.
func TestComponentFailureReportedOnce(t *testing.T) {
	const sid session.SessionID = "s-fail"
	boom := errors.New("boom")
	for _, tc := range []struct {
		name         string
		withCallback bool
		wantCalls    int
		wantEvents   int
	}{
		{"callback owns delivery", true, 1, 0},
		{"no callback publishes directly", false, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			cfg := rt.ExecutionConfig{}
			if tc.withCallback {
				cfg.Fail = func(session.SessionID, error) { calls++ }
			}
			x := newExecution(t, cfg)
			events := x.Progress.Subscribe(ctx, sid)
			x.Recovery.Fail(sid, boom)
			got := 0
			timeout := time.After(100 * time.Millisecond)
		drain:
			for {
				select {
				case e := <-events:
					if !errors.Is(e.Err, boom) {
						t.Fatalf("event = %+v, want Err boom", e)
					}
					got++
				case <-timeout:
					break drain
				}
			}
			if calls != tc.wantCalls || got != tc.wantEvents {
				t.Fatalf("callback calls = %d, progress events = %d; want %d and %d", calls, got, tc.wantCalls, tc.wantEvents)
			}
		})
	}
}

// The redispatch budget of the config is the Recovery's.
func TestMaxRedispatchesReachesRecovery(t *testing.T) {
	x := newExecution(t, rt.ExecutionConfig{MaxRedispatches: 7})
	if x.Recovery.MaxRedispatches != 7 {
		t.Fatalf("Recovery.MaxRedispatches = %d, want 7", x.Recovery.MaxRedispatches)
	}
}
