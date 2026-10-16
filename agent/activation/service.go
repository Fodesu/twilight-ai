package activation

import (
	"context"
	"errors"
	"time"
)

// Service is a process-level worker, not a Session Owner. It fairly revisits
// durable discovery even under a continuously nonempty activation queue. More
// replicas may serve the same authorities; queue claims are not write grants.
type Service struct {
	Discovery     *Discovery
	Runner        *Runner
	SweepInterval time.Duration
	IdleInterval  time.Duration
	// Report receives recoverable sweep/activation errors. Without it Run
	// returns the error for supervision instead of silently discarding it.
	Report func(error)
}

func (s *Service) Run(ctx context.Context) error {
	if s.Discovery == nil || s.Runner == nil {
		return errors.New("activation: discovery and runner are required")
	}
	sweepEvery := s.SweepInterval
	if sweepEvery <= 0 {
		sweepEvery = time.Second
	}
	idle := s.IdleInterval
	if idle <= 0 {
		idle = 100 * time.Millisecond
	}
	var nextSweep time.Time
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !time.Now().Before(nextSweep) {
			if err := s.Discovery.Sweep(ctx); err != nil {
				if s.Report == nil {
					return err
				}
				s.Report(err)
			}
			nextSweep = time.Now().Add(sweepEvery)
		}
		worked, err := s.Runner.RunOne(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if s.Report == nil {
				return err
			}
			s.Report(err)
		}
		if worked {
			continue
		}
		delay := min(idle, max(0, time.Until(nextSweep)))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
