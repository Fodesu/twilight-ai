package runtimetest

import (
	"testing"
	"time"

	"github.com/memohai/twilight/agent/session"
)

func TestMemoryStoreConformance(t *testing.T) {
	Run(t, func(testing.TB) Fixture {
		now := time.Unix(1_000_000, 0)
		store := session.NewMemoryStoreWithClock(func() time.Time { return now })
		return Fixture{Store: store, Advance: func(d time.Duration) { now = now.Add(d) }}
	})
}
