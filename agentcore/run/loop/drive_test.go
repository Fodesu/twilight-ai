package loop

import (
	"context"
	"errors"
	"fmt"
	"time"

	run "github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/store"
)

// DriveForTest steps one Run the way a host does, on the caller's goroutine:
// Advance, read the Outcomes of what it dispatched, Deliver, repeat, until
// the Run finishes, waits, or a step fails. A ctx that ends cancels the
// effects in flight once and still delivers their Outcomes, then reports
// ctx's error. It exists for the tests of this package.
func DriveForTest(ctx context.Context, l *Loop, rt store.RunStore, runID run.RunID) (LoopResult, error) {
	settleCtx := context.WithoutCancel(ctx)
	pending := map[AssignmentKey]struct{}{}
	cancelled := false
	for {
		if len(pending) == 0 {
			if cancelled {
				return LoopResult{}, ctx.Err()
			}
			res, err := l.Advance(ctx, rt, runID)
			if err != nil {
				return LoopResult{}, err
			}
			if res.Disposition != LoopDispatched {
				return res, nil
			}
			for _, k := range res.Dispatched {
				pending[k] = struct{}{}
			}
		}
		key, out, err := readAny(ctx, settleCtx, l.Executor, pending, &cancelled)
		if err != nil {
			return LoopResult{}, err
		}
		delete(pending, key)
		res, err := l.Deliver(settleCtx, rt, out)
		if err != nil {
			if ownershipLost(err) {
				for k := range pending {
					_ = l.Executor.Cancel(settleCtx, k)
				}
			}
			return res, err
		}
		if res.Disposition == LoopFinished {
			return res, nil
		}
	}
}

// readAny reads the pending keys until one Outcome is readable. When ctx
// ends it cancels every pending effect once and keeps reading under
// settleCtx, so each cancelled effect still settles.
func readAny(ctx, settleCtx context.Context, port Executor, pending map[AssignmentKey]struct{}, cancelled *bool) (AssignmentKey, Outcome, error) {
	for {
		for key := range pending {
			out, err := port.GetOutcome(settleCtx, key)
			switch {
			case err == nil:
				return key, out, nil
			case errors.Is(err, ErrOutcomeNotReady):
			case errors.Is(err, ErrExecutionNotFound), errors.Is(err, effect.ErrOutcomeUnavailable):
				return key, Outcome{}, fmt.Errorf("agent: loop: read outcome: %w", err)
			default:
				// A read failure says nothing about the execution: read again.
			}
		}
		if !*cancelled && ctx.Err() != nil {
			*cancelled = true
			for key := range pending {
				_ = port.Cancel(settleCtx, key)
			}
		}
		time.Sleep(time.Millisecond)
	}
}
