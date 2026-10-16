package ownership_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/ownership"
	"github.com/felinics/twilight/agentcore/session"
)

// A renewal blocked in transport lets Close prove it cancels and joins its
// own loop before explicitly releasing the acquired epoch.
type renewalStore struct {
	session.Store
	started         chan struct{}
	stopped         chan struct{}
	release         chan struct{}
	renews          atomic.Int32
	closes          atomic.Int32
	once            sync.Once
	closeBeforeJoin atomic.Bool
	block           bool
}

func (s *renewalStore) Open(ctx context.Context, sid session.SessionID, opts session.OpenOptions) (session.Handle, error) {
	h, err := s.Store.Open(ctx, sid, opts)
	if err != nil {
		return nil, err
	}
	return &renewalHandle{Handle: h, store: s}, nil
}

type renewalHandle struct {
	session.Handle
	store *renewalStore
}

func (h *renewalHandle) Renew(ctx context.Context) error {
	s := h.store
	s.renews.Add(1)
	if !s.block {
		return h.Handle.Renew(ctx)
	}
	s.once.Do(func() { close(s.started) })
	<-ctx.Done()
	close(s.stopped)
	<-s.release
	return ctx.Err()
}
func (h *renewalHandle) Close(ctx context.Context) error {
	s := h.store
	if s.block {
		select {
		case <-s.release:
		default:
			s.closeBeforeJoin.Store(true)
		}
	}
	s.closes.Add(1)
	return h.Handle.Close(ctx)
}

func TestCloseJoinsRenewalBeforeRelease(t *testing.T) {
	ctx := context.Background()
	m, store := setup(t, 30*time.Millisecond)
	transport := &renewalStore{Store: store, started: make(chan struct{}), stopped: make(chan struct{}), release: make(chan struct{}), block: true}
	m.Writers.Store = transport
	h, err := m.Acquire(ctx, "s")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-transport.started:
	case <-time.After(time.Second):
		t.Fatal("renewal did not start")
	}
	closed := make(chan error, 1)
	go func() { closed <- h.Close(ctx) }()
	select {
	case <-transport.stopped:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel renewal")
	}
	select {
	case err := <-closed:
		t.Fatalf("Close returned before renewal joined: %v", err)
	default:
	}
	if transport.closes.Load() != 0 {
		t.Fatal("Close released epoch while renewal was in flight")
	}
	close(transport.release)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close failed to join")
	}
	if transport.closeBeforeJoin.Load() || transport.closes.Load() != 1 {
		t.Fatalf("release before join=%v closes=%d", transport.closeBeforeJoin.Load(), transport.closes.Load())
	}
	if _, err := h.Writer().Commit(ctx, note("late")); err == nil {
		t.Fatal("Close left Writer open")
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if transport.closes.Load() != 1 {
		t.Fatal("Close released twice")
	}
}

func TestNonExpiringHandleDoesNotStartRenewal(t *testing.T) {
	ctx := context.Background()
	m, store := setup(t, 0)
	transport := &renewalStore{Store: store}
	m.Writers.Store = transport
	h, err := m.Acquire(ctx, "s")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if transport.renews.Load() != 0 {
		t.Fatal("nonexpiring handle was renewed in background")
	}
	if err := h.Renew(ctx); err != nil {
		t.Fatal(err)
	}
	if transport.renews.Load() != 1 {
		t.Fatal("explicit Renew was not forwarded")
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestFencedDuringBindCannotReleaseNewEpoch(t *testing.T) {
	ctx := context.Background()
	m, store := setup(t, 30*time.Millisecond)
	blocked := &blockingStore{Store: store, entered: make(chan struct{}), unblock: make(chan struct{}), fail: true}
	m.Writers.Store = blocked
	result := make(chan error, 1)
	go func() { _, err := m.Acquire(ctx, "s"); result <- err }()
	<-blocked.entered
	taker, err := store.Open(ctx, "s", session.OpenOptions{Takeover: true})
	if err != nil {
		t.Fatal(err)
	}
	defer taker.Close(ctx)
	close(blocked.unblock)
	if err := <-result; err == nil {
		t.Fatal("failed Bind succeeded")
	}
	lease, ok, err := store.LeaseOf(ctx, "s")
	if err != nil || !ok || lease.Epoch != taker.Epoch() {
		t.Fatalf("failed Acquire released new epoch: %+v %v %v", lease, ok, err)
	}
}

func TestConcurrentTakeoversCloseOnlyTheirEpochs(t *testing.T) {
	ctx := context.Background()
	m, store := setup(t, 0)
	m.Options.Takeover = true
	const n = 6
	handles := make(chan *ownership.Handle, n)
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h, err := m.Acquire(ctx, "s")
			if err != nil {
				t.Error(err)
				return
			}
			handles <- h
		}()
	}
	wg.Wait()
	close(handles)
	var newest *ownership.Handle
	var all []*ownership.Handle
	for h := range handles {
		all = append(all, h)
		if newest == nil || h.Lease().Epoch > newest.Lease().Epoch {
			newest = h
		}
	}
	if len(all) != n {
		t.Fatalf("acquisitions=%d", len(all))
	}
	for _, h := range all {
		if h != newest {
			if err := h.Close(ctx); err != nil {
				t.Error(err)
			}
		}
	}
	lease, ok, err := store.LeaseOf(ctx, "s")
	if err != nil || !ok || lease.Epoch != newest.Lease().Epoch {
		t.Fatalf("older Close released newest: %+v %v %v", lease, ok, err)
	}
	if err := newest.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := newest.Renew(ctx); err == nil {
		t.Fatal("closed capability renewed")
	}
}
