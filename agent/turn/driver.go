package turn

import (
	"context"

	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/run/loop"
)

// LoopDriver is the reference RunDriver: it drives a Run with one loop.Loop
// and reports nothing about the outcome, which the Coordinator reads from
// the Runtime.
type LoopDriver struct {
	Loop    *loop.Loop
	Runtime run.Runtime
	Events  loop.EventSink
}

func (d LoopDriver) Drive(ctx context.Context, runID run.RunID) error {
	_, err := d.Loop.Run(ctx, d.Runtime, runID, d.Events)
	return err
}
