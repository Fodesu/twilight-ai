package schedulingtest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/scheduling"
	"github.com/felinics/twilight/agentcore/session"
)

// Fixture is a fresh empty adapter and its controllable, millisecond clock.
// Now must initially return a positive Unix time. Advance synchronously moves
// that same adapter clock. Reopen, if non-nil, returns another handle to the
// same durable state (including drained high-watermarks), not an empty store.
// A real durable adapter should reopen its persisted storage here. Sharing Map
// is only an executable model of the transaction contract, not a durability test.
type Fixture struct {
	Store   scheduling.Store
	Now     func() time.Time
	Advance func(time.Duration)
	Reopen  func(t *testing.T) scheduling.Store
}

// Factory creates an isolated fixture for each subtest.
type Factory func(t *testing.T) Fixture

func (f Fixture) other(t *testing.T) scheduling.Store {
	t.Helper()
	if f.Reopen != nil {
		return f.Reopen(t)
	}
	return f.Store
}

// Run exercises the persistent protocol and its clock/fencing boundaries.
// It does not certify a deployment's storage durability or distributed CP.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	tests := []struct {
		name string
		run  func(*testing.T, Fixture)
	}{
		{"versions_and_generation", testVersions},
		{"duplicate_preserves_backoff_and_drained_highwater", testBackoff},
		{"cross_source_progress_wakes_backoff", testCrossSource},
		{"in_flight_signals_survive_finish", testInFlight},
		{"signal_finish_race", testRace},
		{"concurrent_duplicate_signals", testConcurrentSignals},
		{"expired_claim_takeover_and_late_token", testExpiry},
		{"renew_adapter_clock_and_snapshot", testRenew},
		{"due_order_and_bounded_claims", testFairness},
		{"invalid_arguments_are_noops", testInvalid},
		{"context_cancellation_is_noop", testContext},
		{"downstream_owned_requires_retry", testOwnedRetry},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := factory(t)
			if f.Store == nil || f.Now == nil || f.Advance == nil || f.Now().UnixMilli() <= 0 {
				t.Fatal("fixture requires Store and a positive controllable adapter clock")
			}
			tc.run(t, f)
		})
	}
}

func signal(t *testing.T, s scheduling.Store, sid session.SessionID, source string, revision uint64, due int64) {
	t.Helper()
	if err := s.Signal(context.Background(), scheduling.Signal{Session: sid, Source: source, Revision: revision, NotBeforeUnixMilli: due}); err != nil {
		t.Fatal(err)
	}
}

func claim(t *testing.T, s scheduling.Store, sid session.SessionID, generation uint64, duration time.Duration) scheduling.Claim {
	t.Helper()
	c, ok, err := s.Claim(context.Background(), "worker", duration)
	if err != nil || !ok || c.Session != sid || c.Generation != generation || c.Token == "" {
		t.Fatalf("Claim = %+v, %v, %v; want session %s generation %d", c, ok, err, sid, generation)
	}
	return c
}

func empty(t *testing.T, s scheduling.Store) {
	t.Helper()
	c, ok, err := s.Claim(context.Background(), "worker", time.Minute)
	if err != nil || ok || c != (scheduling.Claim{}) {
		t.Fatalf("Claim = %+v, %v, %v; want zero/false/nil", c, ok, err)
	}
}

func finish(t *testing.T, s scheduling.Store, c scheduling.Claim, due int64) {
	t.Helper()
	if err := s.Finish(context.Background(), c, due); err != nil {
		t.Fatal(err)
	}
}

func wantError(t *testing.T, got, want error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Fatalf("error = %v; want %v", got, want)
	}
}

func testVersions(t *testing.T, f Fixture) {
	a, b := f.Store, f.other(t)
	empty(t, a)
	// Source namespaces are independent. Zero is a valid first revision.
	signal(t, a, "s", "ledger", 0, 0)
	signal(t, b, "s", "ledger", 0, 0)
	signal(t, a, "s", "ledger", 3, 0)
	signal(t, b, "s", "ledger", 2, 0)
	signal(t, b, "s", "execution/e", 0, 0)
	c := claim(t, b, "s", 3, time.Minute)
	empty(t, a) // even the same owner must not get a second live token
	finish(t, a, c, 0)
	wantError(t, b.Finish(context.Background(), c, 0), scheduling.ErrClaimLost)
	empty(t, b)
	// Reopen after drain: neither the namespace high-watermarks nor the
	// Session generation may reset when there is no outstanding work.
	b = f.other(t)
	signal(t, b, "s", "ledger", 3, 0)
	signal(t, b, "s", "execution/e", 0, 0)
	empty(t, a)
	signal(t, b, "s", "ledger", 4, 0)
	c = claim(t, a, "s", 4, time.Minute)
	finish(t, a, c, 0)
	// The same source/revision in another Session is independent.
	signal(t, a, "other", "ledger", 0, 0)
	finish(t, a, claim(t, a, "other", 1, time.Minute), 0)
}

func testBackoff(t *testing.T, f Fixture) {
	a, b := f.Store, f.other(t)
	signal(t, a, "s", "inbox", 7, 0)
	c := claim(t, a, "s", 1, time.Minute)
	retry := f.Now().Add(time.Hour).UnixMilli()
	finish(t, a, c, retry)
	// Even an earlier due time on a duplicate must not undo backoff.
	signal(t, b, "s", "inbox", 7, 0)
	signal(t, b, "s", "inbox", 6, f.Now().UnixMilli()-1)
	empty(t, b)
	f.Advance(time.Hour - time.Millisecond)
	empty(t, a)
	f.Advance(time.Millisecond)
	c = claim(t, b, "s", 1, time.Minute)
	finish(t, b, c, 0)
	signal(t, f.other(t), "s", "inbox", 7, 0)
	empty(t, a)
}

func testCrossSource(t *testing.T, f Fixture) {
	a := f.Store
	signal(t, a, "s", "ledger", 99, 0)
	finish(t, a, claim(t, a, "s", 1, time.Minute), f.Now().Add(time.Hour).UnixMilli())
	signal(t, f.other(t), "s", "execution/new", 0, 0)
	c := claim(t, a, "s", 2, time.Minute)
	finish(t, a, c, 0)
	// Accepted higher progress in the same source also wakes backoff.
	signal(t, a, "s", "ledger", 100, 0)
	finish(t, a, claim(t, a, "s", 3, time.Minute), f.Now().Add(time.Hour).UnixMilli())
	signal(t, a, "s", "ledger", 101, 0)
	finish(t, a, claim(t, a, "s", 4, time.Minute), 0)
}

func testInFlight(t *testing.T, f Fixture) {
	// Draining cannot clear new work; a later retry cannot postpone it.
	// An earlier continuation must also survive a concurrent future probe.
	for i, delta := range []time.Duration{0, time.Second, time.Hour} {
		sid := session.SessionID(fmt.Sprintf("s-%d", i))
		a, b := f.Store, f.other(t)
		signal(t, a, sid, "ledger", 1, 0)
		c := claim(t, a, sid, 1, time.Hour)
		now := f.Now()
		signal(t, b, sid, "ledger", 2, now.Add(3*time.Minute).UnixMilli())
		signal(t, b, sid, "inbox", 1, now.Add(2*time.Minute).UnixMilli())
		// Duplicate cannot pull forward the new work's due either.
		signal(t, a, sid, "inbox", 1, 0)
		empty(t, b)
		if err := a.Renew(context.Background(), c, time.Hour); err != nil {
			t.Fatal(err)
		}
		next := int64(0)
		if delta != 0 {
			next = now.Add(delta).UnixMilli()
		}
		finish(t, a, c, next)
		empty(t, b)
		expected := 2 * time.Minute
		if delta > 0 {
			expected = min(expected, delta)
		}
		f.Advance(expected - time.Millisecond)
		empty(t, a)
		f.Advance(time.Millisecond)
		newClaim := claim(t, b, sid, 3, time.Minute)
		if newClaim.Token == c.Token {
			t.Fatal("new acquisition reused token")
		}
		finish(t, b, newClaim, 0)
		empty(t, a)
	}
}

func testRace(t *testing.T, f Fixture) {
	a, b := f.Store, f.other(t)
	signal(t, a, "s", "inbox", 0, 0)
	c := claim(t, a, "s", 1, time.Hour)
	for revision := uint64(1); revision <= 32; revision++ {
		start := make(chan struct{})
		errs := make(chan error, 2)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			errs <- b.Signal(context.Background(), scheduling.Signal{Session: "s", Source: "inbox", Revision: revision})
		}()
		next := int64(0)
		if revision%2 == 0 {
			next = f.Now().Add(time.Hour).UnixMilli()
		}
		go func() {
			defer wg.Done()
			<-start
			errs <- a.Finish(context.Background(), c, next)
		}()
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		// Whichever transaction won, new progress must be immediately due.
		c = claim(t, a, "s", revision+1, time.Hour)
	}
	finish(t, a, c, 0)
}

func testConcurrentSignals(t *testing.T, f Fixture) {
	a, b := f.Store, f.other(t)
	start := make(chan struct{})
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			s := a
			if i%2 == 0 {
				s = b
			}
			errs <- s.Signal(context.Background(), scheduling.Signal{Session: "s", Source: "inbox", Revision: 1})
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	// Concurrent rediscovery still creates exactly one generation.
	finish(t, a, claim(t, b, "s", 1, time.Minute), 0)
	empty(t, a)
}

func testExpiry(t *testing.T, f Fixture) {
	ctx := context.Background()
	a, b := f.Store, f.other(t)
	signal(t, a, "s", "ledger", 1, 0)
	old := claim(t, a, "s", 1, time.Second)
	signal(t, b, "s", "inbox", 1, f.Now().Add(time.Hour).UnixMilli())
	f.Advance(time.Second - time.Millisecond)
	empty(t, b)
	f.Advance(time.Millisecond) // equality is expired, not still owned
	wantError(t, a.Renew(ctx, old, time.Hour), scheduling.ErrClaimLost)
	wantError(t, a.Finish(ctx, old, 0), scheduling.ErrClaimLost)
	// The lost worker's original due is recovered, not postponed to the
	// future signal. Takeover incorporates the current generation.
	current := claim(t, b, "s", 2, time.Minute)
	if current.Token == old.Token {
		t.Fatal("takeover reused token")
	}
	wantError(t, a.Renew(ctx, old, time.Hour), scheduling.ErrClaimLost)
	wantError(t, a.Finish(ctx, old, 0), scheduling.ErrClaimLost)
	wantError(t, a.Finish(ctx, old, f.Now().Add(time.Hour).UnixMilli()), scheduling.ErrClaimLost)
	forged := current
	forged.Generation++
	wantError(t, a.Finish(ctx, forged, 0), scheduling.ErrClaimLost)
	forged = current
	forged.Token = "foreign-token"
	wantError(t, a.Renew(ctx, forged, time.Hour), scheduling.ErrClaimLost)
	forged = current
	forged.Session = "absent"
	wantError(t, a.Finish(ctx, forged, 0), scheduling.ErrClaimLost)
	empty(t, a) // late/forged calls neither release nor consume the new claim
	finish(t, b, current, 0)
	wantError(t, a.Finish(ctx, old, 0), scheduling.ErrClaimLost)
	empty(t, a)
	// Disconnection without any intervening Signal is also recoverable.
	signal(t, a, "s", "ledger", 2, 0)
	lost := claim(t, a, "s", 3, time.Millisecond)
	f.Advance(time.Millisecond)
	reclaimed := claim(t, b, "s", 3, time.Minute)
	if reclaimed.Token == lost.Token {
		t.Fatal("same-owner reclaim reused token")
	}
	finish(t, b, reclaimed, 0)
}

func testRenew(t *testing.T, f Fixture) {
	a, b := f.Store, f.other(t)
	ctx := context.Background()
	signal(t, a, "s", "ledger", 1, f.Now().Add(time.Second).UnixMilli())
	empty(t, b)
	f.Advance(time.Second)
	start := f.Now()
	c := claim(t, a, "s", 1, 1500*time.Microsecond)
	if c.UntilUnixMilli != start.UnixMilli()+2 {
		t.Fatalf("expiry = %d; want adapter now + duration rounded up", c.UntilUnixMilli)
	}
	f.Advance(time.Millisecond)
	if err := b.Renew(ctx, c, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := a.Renew(ctx, c, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	f.Advance(time.Second)
	empty(t, b) // short renewal did not shorten the minute-long extension
	// Input expiry is not authority; the original snapshot is now expired,
	// but the stored renewal is still live.
	finish(t, b, c, 0)
	empty(t, a)
	// Renewal is also bounded by the adapter's renewed expiry.
	signal(t, a, "s", "ledger", 2, 0)
	c = claim(t, a, "s", 2, time.Second)
	f.Advance(500 * time.Millisecond)
	if err := b.Renew(ctx, c, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	f.Advance(2*time.Second - time.Millisecond)
	empty(t, a)
	f.Advance(time.Millisecond)
	wantError(t, a.Finish(ctx, c, 0), scheduling.ErrClaimLost)
	finish(t, b, claim(t, b, "s", 2, time.Minute), 0)
	// Future claimed timestamps also cannot revive an expired stored token.
	signal(t, a, "s", "ledger", 3, 0)
	c = claim(t, a, "s", 3, time.Millisecond)
	c.UntilUnixMilli = f.Now().Add(time.Hour).UnixMilli()
	f.Advance(time.Millisecond)
	wantError(t, b.Renew(ctx, c, time.Hour), scheduling.ErrClaimLost)
	wantError(t, b.Finish(ctx, c, 0), scheduling.ErrClaimLost)
	finish(t, b, claim(t, b, "s", 3, time.Minute), 0)
}

func testFairness(t *testing.T, f Fixture) {
	a, b := f.Store, f.other(t)
	now := f.Now().UnixMilli()
	// Due wins over lexical order; ties use SessionID, not map iteration.
	signal(t, a, "z", "ledger", 1, now-1)
	for _, sid := range []session.SessionID{"d", "c", "b", "a"} {
		due := now
		if sid == "a" {
			due = 0 // zero must normalize to adapter now, not Unix epoch
		}
		signal(t, a, sid, "ledger", 1, due)
	}
	signal(t, a, "future", "ledger", 1, now+1000)
	held := make([]scheduling.Claim, 0, 5)
	for _, sid := range []session.SessionID{"z", "a", "b", "c", "d"} {
		// Each Claim takes just one. Previously claimed Sessions cannot hide
		// the rest behind an index page/limit, even if they are not drained.
		held = append(held, claim(t, b, sid, 1, time.Minute))
	}
	empty(t, a)
	for _, c := range held {
		finish(t, a, c, 0)
		signal(t, b, c.Session, "ledger", 1, 0)
	}
	empty(t, a)
	f.Advance(time.Second)
	finish(t, b, claim(t, b, "future", 1, time.Minute), 0)
}

func testInvalid(t *testing.T, f Fixture) {
	a := f.Store
	ctx := context.Background()
	for _, s := range []scheduling.Signal{
		{Source: "ledger"}, {Session: "s"},
		{Session: "s", Source: "ledger", NotBeforeUnixMilli: -1},
	} {
		wantError(t, a.Signal(ctx, s), scheduling.ErrInvalid)
	}
	for _, duration := range []time.Duration{0, -time.Second, time.Nanosecond, time.Millisecond - 1} {
		_, _, err := a.Claim(ctx, "worker", duration)
		wantError(t, err, scheduling.ErrInvalid)
	}
	_, _, err := a.Claim(ctx, "", time.Minute)
	wantError(t, err, scheduling.ErrInvalid)
	empty(t, a)
	signal(t, a, "s", "ledger", 1, 0)
	c := claim(t, a, "s", 1, time.Minute)
	for _, duration := range []time.Duration{0, -time.Second, time.Nanosecond} {
		wantError(t, a.Renew(ctx, c, duration), scheduling.ErrInvalid)
	}
	wantError(t, a.Finish(ctx, c, -1), scheduling.ErrInvalid)
	for _, mutate := range []func(*scheduling.Claim){
		func(c *scheduling.Claim) { c.Session = "" },
		func(c *scheduling.Claim) { c.Token = "" },
		func(c *scheduling.Claim) { c.Generation = 0 },
	} {
		bad := c
		mutate(&bad)
		wantError(t, a.Renew(ctx, bad, time.Minute), scheduling.ErrInvalid)
		wantError(t, a.Finish(ctx, bad, 0), scheduling.ErrInvalid)
	}
	empty(t, a)
	finish(t, a, c, 0) // none of the invalid calls changed state
}

func testContext(t *testing.T, f Fixture) {
	a := f.Store
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	deadline, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()
	for _, tc := range []struct {
		ctx context.Context
		err error
	}{{cancelled, context.Canceled}, {deadline, context.DeadlineExceeded}} {
		wantError(t, a.Signal(tc.ctx, scheduling.Signal{Session: "s", Source: "ledger", Revision: 1}), tc.err)
		_, _, err := a.Claim(tc.ctx, "worker", time.Minute)
		wantError(t, err, tc.err)
	}
	empty(t, a)
	signal(t, a, "s", "ledger", 1, 0)
	for _, ctx := range []context.Context{cancelled, deadline} {
		_, _, err := a.Claim(ctx, "worker", time.Minute)
		wantError(t, err, ctx.Err())
	}
	c := claim(t, a, "s", 1, time.Minute)
	for _, tc := range []struct {
		ctx context.Context
		err error
	}{{cancelled, context.Canceled}, {deadline, context.DeadlineExceeded}} {
		wantError(t, a.Signal(tc.ctx, scheduling.Signal{Session: "s", Source: "ledger", Revision: 2}), tc.err)
		wantError(t, a.Renew(tc.ctx, c, time.Hour), tc.err)
		wantError(t, a.Finish(tc.ctx, c, 0), tc.err)
	}
	finish(t, a, c, 0)
	empty(t, a)
	// The cancelled update did not consume revision 2 or increment generation.
	signal(t, a, "s", "ledger", 2, 0)
	c = claim(t, a, "s", 2, time.Millisecond)
	wantError(t, a.Renew(cancelled, c, time.Hour), context.Canceled)
	f.Advance(time.Millisecond)
	// A cancelled renewal did not extend the original expiry.
	finish(t, a, claim(t, a, "s", 2, time.Minute), 0)
}

func testOwnedRetry(t *testing.T, f Fixture) {
	a := f.Store
	signal(t, a, "s", "inbox", 1, 0)
	c := claim(t, a, "s", 1, time.Minute)
	// Model the consumer's downstream ownership failure without depending
	// on a writer/activation API. Scheduling claims grant no write lease.
	errOwned := errors.New("downstream session: owned")
	consume := func() error { return errOwned }
	retryAt := f.Now().Add(time.Second).UnixMilli()
	if err := consume(); errors.Is(err, errOwned) {
		finish(t, a, c, retryAt)
	} else {
		t.Fatalf("consume = %v; want downstream ownership failure", err)
	}
	empty(t, a)
	f.Advance(time.Second)
	retry := claim(t, f.other(t), "s", 1, time.Minute)
	if retry.Token == c.Token {
		t.Fatal("retry reused finished token")
	}
	finish(t, a, retry, 0)
	empty(t, a)
}
