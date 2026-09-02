package loop

import (
	"context"
	"errors"
	"time"

	run "github.com/memohai/twilight/agent/run"
)

// keepLease renews the execution lease behind grant every interval until the
// returned stop function is called or the renewal is rejected. The worker
// context it returns is cancelled when the Runtime reports the lease is no
// longer ours (ErrStaleRuntime): the target was recovered under us, so
// continuing the effect can only produce a result nobody will accept.
//
// With interval <= 0 renewal is disabled and the worker context is ctx itself.
func (l *Loop) keepLease(ctx context.Context, runtime run.Runtime, runID run.RunID, stepID run.StepID, callID run.CallID, grant run.ExecutionGrant) (workerCtx context.Context, stop func()) {
	interval := l.Execution.LeaseRenewInterval
	if interval <= 0 {
		return ctx, func() {}
	}
	workerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	control := context.WithoutCancel(ctx)
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-workerCtx.Done():
				return
			case <-ticker.C:
			}
			err := runtime.RenewLease(control, runID, stepID, callID, grant)
			if err == nil {
				continue
			}
			if errors.Is(err, run.ErrStaleRuntime) || errors.Is(err, run.ErrRunTerminal) || errors.Is(err, run.ErrRunNotFound) {
				cancel()
				return
			}
			// Transport failure: keep the worker running and retry on the
			// next tick; the lease still has TTL minus one interval left.
		}
	}()
	var once bool
	return workerCtx, func() {
		if once {
			return
		}
		once = true
		cancel()
		<-done
	}
}
