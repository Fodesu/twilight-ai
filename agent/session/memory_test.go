package session_test

import (
	"testing"
	"time"

	"github.com/memohai/twilight/agent/session"
	"github.com/memohai/twilight/agent/session/sessiontest"
)

func TestMemoryStoreConformance(t *testing.T) {
	sessiontest.Run(t, func(t *testing.T) sessiontest.Fixture {
		now := time.Unix(1_700_000_000, 0)
		store := session.NewMemoryStoreWithClock(func() time.Time { return now })
		return sessiontest.Fixture{Store: store, Advance: func(d time.Duration) { now = now.Add(d) }}
	})
}
