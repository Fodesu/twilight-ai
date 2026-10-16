package ownership_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/ownership"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/agentcore/session/writer"
)

func setup(t *testing.T, duration time.Duration) (*ownership.Manager, session.Stores) {
	t.Helper()
	store := filestoretest.Store(t)
	if _, err := store.Create(context.Background(), session.CreateRequest{SessionID: "s"}); err != nil {
		t.Fatal(err)
	}
	registry, err := module.BuildRegistry(chatlog.Module, sessionstore.Module)
	if err != nil {
		t.Fatal(err)
	}
	return &ownership.Manager{Writers: &writer.Factory{Store: store, Registry: registry}, Options: session.OpenOptions{Owner: "a", LeaseDuration: duration}}, store
}
func note(id ledger.CommitID) writer.CommitFn {
	return func(writer.View) (*writer.SemanticGroup, error) {
		return &writer.SemanticGroup{CommitID: id, Batches: []writer.TypedBatch{{Domain: chatlog.Stream, Events: []writer.TypedEvent{{Type: chatlog.TypeInputSubmitted, Value: chatlog.InputSubmittedPayload{InputID: chatlog.InputID(id), Content: jsonstable.MustParse(`{"text":"hi"}`)}}}}}}, nil
	}
}

func TestHeartbeatRenewsAndFencedCommit(t *testing.T) {
	ctx := context.Background()
	m, store := setup(t, 60*time.Millisecond)
	h, err := m.Acquire(ctx, "s")
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(ctx)
	time.Sleep(150 * time.Millisecond)
	if _, err := store.Open(ctx, "s", session.OpenOptions{Owner: "b", LeaseDuration: time.Second}); !session.IsCode(err, session.ErrOwned) {
		t.Fatalf("open while renewing = %v", err)
	}
	if res, err := h.Writer().Commit(ctx, note("c1")); err != nil || res.Outcome != writer.CommitApplied {
		t.Fatalf("renewed commit = %+v %v", res, err)
	}
	taker, err := store.Open(ctx, "s", session.OpenOptions{Takeover: true, Owner: "b"})
	if err != nil {
		t.Fatal(err)
	}
	defer taker.Close(ctx)
	if err := h.Renew(ctx); !session.IsCode(err, session.ErrOwnershipLost) {
		t.Fatalf("renew after takeover = %v", err)
	}
	// Ownership has no effect/executor port to cancel anything. The Writer is
	// still fenced by the store, independently of renewal's error.
	if _, err := h.Writer().Commit(ctx, note("c2")); !errors.Is(err, &ledger.Error{Code: ledger.CodeOwnershipLost}) {
		t.Fatalf("fenced commit = %v", err)
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	lease, ok, err := store.LeaseOf(ctx, "s")
	if err != nil || !ok || lease.Epoch != taker.Epoch() {
		t.Fatalf("old Close released new epoch: %+v %v %v", lease, ok, err)
	}
	page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s"})
	if err != nil || len(page.Commits) != 1 {
		t.Fatalf("ledger = %d commits %v", len(page.Commits), err)
	}
}

type blockingStore struct {
	session.Store
	entered chan struct{}
	unblock chan struct{}
	once    sync.Once
	fail    bool
}

func (s *blockingStore) ReadCommits(ctx context.Context, req session.CommitReadRequest) (session.CommitPage, error) {
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.unblock:
	case <-ctx.Done():
		return session.CommitPage{}, ctx.Err()
	}
	if s.fail {
		return session.CommitPage{}, errors.New("bind read failed")
	}
	return s.Store.ReadCommits(ctx, req)
}

func TestRenewalDuringBindAndFailedBindCleanup(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			ctx := context.Background()
			m, store := setup(t, 60*time.Millisecond)
			blocked := &blockingStore{Store: store, entered: make(chan struct{}), unblock: make(chan struct{}), fail: fail}
			m.Writers.Store = blocked
			type result struct {
				h   *ownership.Handle
				err error
			}
			done := make(chan result, 1)
			go func() { h, err := m.Acquire(ctx, "s"); done <- result{h, err} }()
			<-blocked.entered
			time.Sleep(150 * time.Millisecond)
			if _, err := store.Open(ctx, "s", session.OpenOptions{}); !session.IsCode(err, session.ErrOwned) {
				t.Fatalf("lease expired during Bind: %v", err)
			}
			close(blocked.unblock)
			got := <-done
			if fail {
				if got.err == nil {
					t.Fatal("Bind failure swallowed")
				}
			} else {
				if got.err != nil {
					t.Fatal(got.err)
				}
				if err := got.h.Close(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if _, ok, err := store.LeaseOf(ctx, "s"); err != nil || ok {
				t.Fatalf("lease leaked after cleanup: %v %v", ok, err)
			}
		})
	}
}

func TestConcurrentAcquireAndClose(t *testing.T) {
	ctx := context.Background()
	m, store := setup(t, 0)
	const n = 8
	handles := make(chan *ownership.Handle, n)
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h, err := m.Acquire(ctx, "s")
			if err != nil {
				errs <- err
			} else {
				handles <- h
			}
		}()
	}
	wg.Wait()
	close(handles)
	close(errs)
	if len(handles) != 1 || len(errs) != n-1 {
		t.Fatalf("acquisition winners=%d errors=%d", len(handles), len(errs))
	}
	for err := range errs {
		if !session.IsCode(err, session.ErrOwned) {
			t.Fatal(err)
		}
	}
	h := <-handles
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := h.Close(ctx); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if err := h.Renew(ctx); err == nil {
		t.Fatal("closed handle renewed")
	}
	if _, ok, err := store.LeaseOf(ctx, "s"); err != nil || ok {
		t.Fatalf("lease after close = %v %v", ok, err)
	}
	next, err := m.Acquire(ctx, "s")
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close(ctx)
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := next.Renew(ctx); err != nil {
		t.Fatal(err)
	}
}
