package run

import (
	"context"
	"errors"
	"time"
)

// RunExpiredRecovery calls RecoverExpired immediately and then every
// interval until ctx is cancelled. Hosts own this loop; Loop does not.
func RunExpiredRecovery(ctx context.Context, rt Runtime, interval time.Duration) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if rt == nil {
		return errors.New("agent: recover: nil runtime")
	}
	if interval <= 0 {
		return errors.New("agent: recover: interval must be positive")
	}
	if _, err := rt.RecoverExpired(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := rt.RecoverExpired(ctx); err != nil {
				return err
			}
		}
	}
}
