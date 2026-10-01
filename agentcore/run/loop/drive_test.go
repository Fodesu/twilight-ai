package loop

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/felinics/twilight/agentcore/decision"
	"github.com/felinics/twilight/agentcore/ledger"
	run "github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/store"
)

// Driven is a Loop with the Builder its tests step under: what a host holds
// per preset, folded into one value so a test reads like a drive.
type Driven struct {
	*Loop
	Builder decision.Builder
}

// NewDriven is New with the Builder every Advance of the result uses.
func NewDriven(ports effect.Ports, builder decision.Builder, settings Settings) (*Driven, error) {
	if builder == nil {
		return nil, errors.New("agent: loop: nil builder")
	}
	l, err := New(ports, settings)
	if err != nil {
		return nil, err
	}
	return &Driven{Loop: l, Builder: builder}, nil
}

// Advance is Loop.Advance under the Builder.
func (d *Driven) Advance(ctx context.Context, rt store.RunStore, runID run.RunID) (LoopResult, error) {
	return d.Loop.Advance(ctx, rt, d.Builder, runID)
}

// DriveForTest steps one Run the way a host does, on the caller's goroutine:
// Advance, read the Outcomes of what it dispatched, Deliver, repeat, until
// the Run finishes, waits, or a step fails. A ctx that ends cancels the
// effects in flight once and still delivers their Outcomes, then reports
// ctx's error. It exists for the tests of this package.
func DriveForTest(ctx context.Context, d *Driven, rt store.RunStore, runID run.RunID) (LoopResult, error) {
	settleCtx := context.WithoutCancel(ctx)
	pending := map[effect.AssignmentKey]struct{}{}
	cancelled := false
	for {
		if len(pending) == 0 {
			if cancelled {
				return LoopResult{}, ctx.Err()
			}
			res, err := d.Advance(ctx, rt, runID)
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
		key, out, err := readAny(ctx, settleCtx, d.Ports.Execution, pending, &cancelled)
		if err != nil {
			return LoopResult{}, err
		}
		delete(pending, key)
		res, err := d.Deliver(settleCtx, rt, out)
		if err != nil {
			if ledger.IsOwnershipLost(err) {
				for k := range pending {
					_ = d.Ports.Execution.Cancel(settleCtx, k)
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
func readAny(ctx, settleCtx context.Context, port effect.ExecutionPort, pending map[effect.AssignmentKey]struct{}, cancelled *bool) (effect.AssignmentKey, effect.Outcome, error) {
	for {
		for key := range pending {
			out, err := port.GetOutcome(settleCtx, key)
			switch {
			case err == nil:
				return key, out, nil
			case errors.Is(err, effect.ErrOutcomeNotReady):
			case errors.Is(err, effect.ErrExecutionNotFound), errors.Is(err, effect.ErrOutcomeUnavailable):
				return key, effect.Outcome{}, fmt.Errorf("agent: loop: read outcome: %w", err)
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
