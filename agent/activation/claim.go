package activation

import (
	"context"
	"fmt"
	"time"

	"github.com/felinics/twilight/agent/scheduling"
)

// claimLifetime covers acquisition, projection rebuilding, command admission
// and execution reconciliation, not only the final Step loop. Its context is
// local computation control, never an external execution's cancellation grant.
type claimLifetime struct {
	stop context.CancelFunc
	done chan struct{}
	err  error // read only after done is joined
}

func maintainClaim(parent context.Context, queue scheduling.Store, claim scheduling.Claim, duration time.Duration) *claimLifetime {
	renew, stop := context.WithCancel(parent)
	lifetime := &claimLifetime{stop: stop, done: make(chan struct{})}
	go func() {
		defer close(lifetime.done)
		ticker := time.NewTicker(max(duration/3, time.Microsecond))
		defer ticker.Stop()
		for {
			if err := queue.Renew(renew, claim, duration); err != nil {
				if renew.Err() == nil {
					lifetime.err = fmt.Errorf("activation: renewing job claim: %w", err)
					// A scheduling claim is not write authority. Losing it must
					// not force every slow Bind to abandon its first Step. The
					// bounded computation may finish under the Session fence;
					// stale-token Finish cannot acknowledge a successor's work.
				}
				return
			}
			select {
			case <-renew.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return lifetime
}

func (l *claimLifetime) close() error {
	l.stop()
	<-l.done
	return l.err
}
