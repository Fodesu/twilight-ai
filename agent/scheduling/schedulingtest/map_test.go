package schedulingtest

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/scheduling"
	"github.com/felinics/twilight/agentcore/session"
)

type clock struct {
	mu sync.Mutex
	at time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}

func TestMapConformance(t *testing.T) {
	Run(t, func(*testing.T) Fixture {
		c := &clock{at: time.Unix(1_700_000_000, 123_456_789)}
		m := NewMap(c.now)
		return Fixture{
			Store: m, Now: c.now, Advance: c.advance,
			// Map has no durable handles; sharing it models atomic operations
			// from independent consumers, not process/crash durability.
			Reopen: func(*testing.T) scheduling.Store { return m },
		}
	})
}

func TestMapZeroValue(t *testing.T) {
	var m Map
	signal(t, &m, "s", "ledger", 0, 0)
	c := claim(t, &m, "s", 1, time.Minute)
	if c.UntilUnixMilli <= time.Now().UnixMilli() {
		t.Fatalf("zero-value clock did not select time.Now: %+v", c)
	}
	finish(t, &m, c, 0)
}

func TestMapGenerationDoesNotWrap(t *testing.T) {
	m := &Map{jobs: map[session.SessionID]*job{
		"s": {sources: map[string]uint64{"ledger": 1}, generation: math.MaxUint64},
	}}
	// A duplicate is still a successful no-op; accepted progress at
	// exhaustion must fail atomically, not turn a new signal into old work.
	signal(t, m, "s", "ledger", 1, 0)
	wantError(t, m.Signal(context.Background(), scheduling.Signal{Session: "s", Source: "ledger", Revision: 2}), scheduling.ErrInvalid)
	if m.jobs["s"].generation != math.MaxUint64 || m.jobs["s"].sources["ledger"] != 1 {
		t.Fatal("generation exhaustion mutated source or wrapped generation")
	}
	empty(t, m)
}

func TestMapCancellationAfterLockWait(t *testing.T) {
	var m Map
	ctx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	done := make(chan error, 1)
	go func() {
		done <- m.Signal(ctx, scheduling.Signal{Session: "s", Source: "inbox", Revision: 1})
	}()
	cancel()
	m.mu.Unlock()
	wantError(t, <-done, context.Canceled)
	empty(t, &m)
}

func TestMapExpiryOverflowIsAtomic(t *testing.T) {
	m := NewMap(func() time.Time { return time.UnixMilli(math.MaxInt64 - 1) })
	signal(t, m, "s", "ledger", 1, 0)
	_, _, err := m.Claim(context.Background(), "worker", 2*time.Millisecond)
	wantError(t, err, scheduling.ErrInvalid)
	c := claim(t, m, "s", 1, time.Millisecond)
	wantError(t, m.Renew(context.Background(), c, 2*time.Millisecond), scheduling.ErrInvalid)
	finish(t, m, c, 0)
}
