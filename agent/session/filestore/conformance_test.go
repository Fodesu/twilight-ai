package filestore_test

import (
	"sync"
	"testing"
	"time"

	"github.com/memohai/twilight/agent/session/filestore"
	"github.com/memohai/twilight/agent/session/run/runtimetest"
	"github.com/memohai/twilight/agent/session/sessiontest"
)

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func newFixture(t testing.TB) (*filestore.Store, *clock) {
	t.Helper()
	c := &clock{now: time.Unix(1_000_000, 0)}
	store, err := filestore.New(t.TempDir(), filestore.Options{Now: c.Now})
	if err != nil {
		t.Fatal(err)
	}
	return store, c
}

func TestKernelConformance(t *testing.T) {
	sessiontest.Run(t, func(t *testing.T) sessiontest.Fixture {
		store, c := newFixture(t)
		return sessiontest.Fixture{Store: store, Advance: c.Advance}
	})
}

func TestRuntimeConformance(t *testing.T) {
	runtimetest.Run(t, func(t testing.TB) runtimetest.Fixture {
		store, c := newFixture(t)
		return runtimetest.Fixture{Store: store, Advance: c.Advance}
	})
}
