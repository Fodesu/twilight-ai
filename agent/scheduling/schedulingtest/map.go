// Package schedulingtest provides a test-only Map and an adapter conformance
// suite for scheduling.Store. Map is an executable protocol model, not durable
// storage, and must not be used as a production activation queue.
package schedulingtest

import (
	"context"
	"crypto/rand"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/felinics/twilight/agent/scheduling"
	"github.com/felinics/twilight/agentcore/session"
)

// Map implements scheduling.Store under one mutex. Its zero value is ready.
// Now is the adapter clock, nil selecting time.Now. Configure it before use;
// a clock advanced concurrently must itself be concurrency-safe.
type Map struct {
	Now func() time.Time

	mu   sync.Mutex
	jobs map[session.SessionID]*job
}

type job struct {
	sources    map[string]uint64
	generation uint64
	pending    bool
	due        int64 // signals/retry not covered by the active claim
	claim      scheduling.Claim
	coveredDue int64 // retained until Finish, for recovery after expiry
}

var _ scheduling.Store = (*Map)(nil)

// NewMap constructs a test-only queue using now as its adapter clock.
func NewMap(now func() time.Time) *Map { return &Map{Now: now} }

func (m *Map) now() int64 {
	if m.Now == nil {
		return time.Now().UnixMilli()
	}
	return m.Now().UnixMilli()
}

func invalid(reason string) error {
	return fmt.Errorf("%w: %s", scheduling.ErrInvalid, reason)
}

func expiry(now int64, duration time.Duration) (int64, error) {
	if duration < time.Millisecond {
		return 0, invalid("duration must be at least one millisecond")
	}
	millis := duration.Milliseconds()
	if duration%time.Millisecond != 0 {
		millis++
	}
	if now > math.MaxInt64-millis {
		return 0, invalid("expiry overflows millisecond clock")
	}
	return now + millis, nil
}

// Signal atomically advances one source and preserves work arriving in flight.
func (m *Map) Signal(ctx context.Context, signal scheduling.Signal) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if signal.Session == "" || signal.Source == "" || signal.NotBeforeUnixMilli < 0 {
		return invalid("empty signal key or negative due time")
	}
	j := m.jobs[signal.Session]
	if j != nil {
		if revision, ok := j.sources[signal.Source]; ok && signal.Revision <= revision {
			return nil
		}
		if j.generation == math.MaxUint64 {
			return invalid("generation exhausted")
		}
	} else {
		if m.jobs == nil {
			m.jobs = make(map[session.SessionID]*job)
		}
		j = &job{sources: make(map[string]uint64)}
		m.jobs[signal.Session] = j
	}
	due := signal.NotBeforeUnixMilli
	if due == 0 {
		due = m.now()
	}
	j.sources[signal.Source] = signal.Revision
	j.generation++
	if !j.pending || due < j.due {
		j.due = due
	}
	j.pending = true
	return nil
}

// eligibleDue excludes live claims and recovers their covered due on expiry.
func (j *job) eligibleDue(now int64) (int64, bool) {
	if j.claim.Token != "" {
		if j.claim.UntilUnixMilli > now {
			return 0, false
		}
		due := j.coveredDue
		if j.pending && j.due < due {
			due = j.due
		}
		return due, true
	}
	return j.due, j.pending
}

// Claim takes the earliest due eligible Session, with SessionID breaking ties.
func (m *Map) Claim(ctx context.Context, owner string, duration time.Duration) (scheduling.Claim, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return scheduling.Claim{}, false, err
	}
	if owner == "" {
		return scheduling.Claim{}, false, invalid("empty owner")
	}
	now := m.now()
	until, err := expiry(now, duration)
	if err != nil {
		return scheduling.Claim{}, false, err
	}
	var chosen *job
	var sid session.SessionID
	var due int64
	for id, j := range m.jobs {
		at, pending := j.eligibleDue(now)
		if !pending || at > now {
			continue
		}
		if chosen == nil || at < due || (at == due && id < sid) {
			chosen, sid, due = j, id, at
		}
	}
	if chosen == nil {
		return scheduling.Claim{}, false, nil
	}
	claim := scheduling.Claim{Session: sid, Token: rand.Text(), Generation: chosen.generation, UntilUnixMilli: until}
	chosen.claim, chosen.coveredDue = claim, due
	chosen.pending, chosen.due = false, 0
	return claim, true, nil
}

func (m *Map) held(claim scheduling.Claim, now int64) (*job, error) {
	if claim.Session == "" || claim.Token == "" || claim.Generation == 0 {
		return nil, invalid("empty claim key, token or generation")
	}
	j := m.jobs[claim.Session]
	if j == nil || j.claim.Token != claim.Token || j.claim.Generation != claim.Generation || j.claim.UntilUnixMilli <= now {
		return nil, scheduling.ErrClaimLost
	}
	return j, nil
}

// Renew never changes the generation snapshot or consumes concurrent signals.
func (m *Map) Renew(ctx context.Context, claim scheduling.Claim, duration time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	now := m.now()
	until, err := expiry(now, duration)
	if err != nil {
		return err
	}
	j, err := m.held(claim, now)
	if err != nil {
		return err
	}
	if until > j.claim.UntilUnixMilli {
		j.claim.UntilUnixMilli = until
	}
	return nil
}

// Finish acknowledges only the claim's snapshot, never signals it did not see.
func (m *Map) Finish(ctx context.Context, claim scheduling.Claim, nextAtUnixMilli int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if nextAtUnixMilli < 0 {
		return invalid("negative retry time")
	}
	j, err := m.held(claim, m.now())
	if err != nil {
		return err
	}
	if j.generation == claim.Generation {
		j.pending = nextAtUnixMilli != 0
		j.due = nextAtUnixMilli
	} else if nextAtUnixMilli > 0 && (!j.pending || nextAtUnixMilli < j.due) {
		// New signals and the covered computation can owe distinct work.
		// Retain both at their earliest due, never postponing a continuation
		// behind an unrelated future execution probe.
		j.pending = true
		j.due = nextAtUnixMilli
	}
	j.claim = scheduling.Claim{}
	j.coveredDue = 0
	return nil
}
