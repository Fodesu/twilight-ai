package writer

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
)

// faultStore wraps a Store so one Append can be made to fail either before
// the kernel writes (nothing reaches the log) or after it has written (the
// group is durable but the caller gets an error instead of the rows). Both
// are "outcome unknown" from the Writer's side; the log tells them apart.
type faultStore struct {
	session.Store
	mu   sync.Mutex
	mode string // "", "before", "after"; consumed by the next Append
}

func (f *faultStore) arm(mode string) {
	f.mu.Lock()
	f.mode = mode
	f.mu.Unlock()
}

func (f *faultStore) take() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	m := f.mode
	f.mode = ""
	return m
}

func (f *faultStore) Open(ctx context.Context, sid session.SessionID, opts session.OpenOptions) (session.Handle, error) {
	h, err := f.Store.Open(ctx, sid, opts)
	if err != nil {
		return nil, err
	}
	return &faultHandle{Handle: h, store: f}, nil
}

type faultHandle struct {
	session.Handle
	store *faultStore
}

var errInjected = errors.New("injected transport failure")

func (h *faultHandle) Append(ctx context.Context, g session.Group) ([]session.SessionEvent, error) {
	switch h.store.take() {
	case "before":
		return nil, errInjected
	case "after":
		if _, err := h.Handle.Append(ctx, g); err != nil {
			return nil, err
		}
		return nil, errInjected // durable, but the response is lost
	}
	return h.Handle.Append(ctx, g)
}

// EXT-WRT-4(b): an Append whose outcome is unknown fails the Writer closed. A
// reopened Writer replays the same group and the kernel's index answers:
// AlreadyApplied when the group reached the log, Applied when it did not. In
// both cases the log stays a valid chain with contiguous Seqs.
func TestWriterFailsClosedWhenAppendOutcomeUnknown(t *testing.T) {
	ctx := context.Background()
	base := newFixture(t)
	fs := &faultStore{Store: base.store}
	open := func(takeover bool) Writer {
		t.Helper()
		w, err := OpenWriter(ctx, fs, base.registry, base.admission(), "s", session.OpenOptions{Takeover: takeover})
		if err != nil {
			t.Fatalf("open writer: %v", err)
		}
		return w
	}
	unknown := &extension.Error{Code: extension.ErrUnknownOutcome}

	w1 := open(false)
	if res, err := w1.Commit(ctx, noteGroup("c1", "one")); err != nil || res.Outcome != CommitApplied {
		t.Fatalf("c1 = %+v %v", res, err)
	}

	// Durable append, lost response.
	fs.arm("after")
	if _, err := w1.Commit(ctx, noteGroup("c2", "two")); !errors.Is(err, unknown) {
		t.Fatalf("commit after lost response = %v, want unknown_outcome", err)
	}
	if _, err := w1.Commit(ctx, noteGroup("c3", "three")); !errors.Is(err, unknown) {
		t.Fatalf("writer did not stay failed: %v", err)
	}
	if page, _ := base.store.Read(ctx, session.ReadRequest{SessionID: "s"}); len(page.Events) != 2 {
		t.Fatalf("log has %d rows after the lost-response append, want 2", len(page.Events))
	}

	// Reopen: the replay is answered by the kernel, the next group continues
	// from the real head.
	_ = w1.Close(ctx)
	w2 := open(true)
	res, err := w2.Commit(ctx, noteGroup("c2", "two"))
	if err != nil || res.Outcome != CommitAlreadyApplied || len(res.Events) != 1 || res.Events[0].Seq != 1 {
		t.Fatalf("replay of the durable group = %+v %v, want already_applied at seq 1", res, err)
	}
	res, err = w2.Commit(ctx, noteGroup("c3", "three"))
	if err != nil || res.Outcome != CommitApplied || res.Events[0].Seq != 2 {
		t.Fatalf("next group = %+v %v, want applied at seq 2", res, err)
	}
	if got := notes(t, w2); len(got) != 3 || got[0] != "one" || got[1] != "two" || got[2] != "three" {
		t.Fatalf("notes after reopen = %v", got)
	}

	// Failure before any write: the reopened Writer applies the group.
	fs.arm("before")
	if _, err := w2.Commit(ctx, noteGroup("c4", "four")); !errors.Is(err, unknown) {
		t.Fatalf("commit after pre-write failure = %v, want unknown_outcome", err)
	}
	_ = w2.Close(ctx)
	w3 := open(true)
	res, err = w3.Commit(ctx, noteGroup("c4", "four"))
	if err != nil || res.Outcome != CommitApplied || res.Events[0].Seq != 3 {
		t.Fatalf("replay of the unwritten group = %+v %v, want applied at seq 3", res, err)
	}

	// A context error is returned before the kernel writes, so it is a known
	// outcome and does not fail the Writer.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := w3.Commit(cancelled, noteGroup("c5", "five")); !errors.Is(err, context.Canceled) {
		t.Fatalf("commit with cancelled ctx = %v, want context.Canceled", err)
	}
	if res, err := w3.Commit(ctx, noteGroup("c5", "five")); err != nil || res.Outcome != CommitApplied || res.Events[0].Seq != 4 {
		t.Fatalf("commit after a cancelled attempt = %+v %v, want applied at seq 4", res, err)
	}

	page, err := base.store.Read(ctx, session.ReadRequest{SessionID: "s"})
	if err != nil || len(page.Events) != 5 {
		t.Fatalf("final log = %d rows %v, want 5", len(page.Events), err)
	}
	for i := range page.Events {
		if page.Events[i].Seq != session.Seq(i) {
			t.Fatalf("seq at position %d is %d", i, page.Events[i].Seq)
		}
	}
	if err := session.ValidateChain(session.ProfileV1(), page.Header, page.Events); err != nil {
		t.Fatalf("chain after faults: %v", err)
	}
}
