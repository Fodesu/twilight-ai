package filestore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/session"
)

func group(commit string, n int) session.Group {
	g := session.Group{CommitID: session.CommitID(commit)}
	for i := 0; i < n; i++ {
		g.Events = append(g.Events, session.UncommittedEvent{Type: "twilight/x/e", RecordedAtUnixMilli: 1,
			Payload: jsonstable.MustParse(fmt.Sprintf(`{"c":%q,"i":%d}`, commit, i))})
	}
	return g
}

// SES-APP-1: a group whose bytes were written but whose fsync failed is
// unknown to the handle. The handle refuses further appends; a reopen finds
// the complete group on disk, indexes it, and the stream continues after it
// with a valid chain.
func TestAppendSyncFailurePoisonsHandle(t *testing.T) {
	ctx := context.Background()
	const sid session.SessionID = "sync"
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: sid, CreatedAtUnixMilli: 1}); err != nil {
		t.Fatal(err)
	}
	h, err := s.Open(ctx, sid, session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Append(ctx, group("c0", 1)); err != nil {
		t.Fatal(err)
	}

	s.sync = func(*os.File) error { return errors.New("injected fsync failure") }
	if _, err := h.Append(ctx, group("c1", 2)); !session.IsCode(err, session.ErrHandleFailed) {
		t.Fatalf("append with failing sync = %v, want handle_failed", err)
	}
	s.sync = nil
	if _, err := h.Append(ctx, group("c2", 1)); !session.IsCode(err, session.ErrHandleFailed) {
		t.Fatalf("poisoned handle accepted an append: %v", err)
	}
	if h.Committed("c1") {
		t.Fatal("poisoned handle claims to know a group whose outcome is unknown")
	}

	// The bytes did land: the reopen sees the complete group.
	h2, err := s.Open(ctx, sid, session.OpenOptions{Takeover: true})
	if err != nil {
		t.Fatalf("reopen after sync failure: %v", err)
	}
	if !h2.Committed("c1") {
		t.Fatal("reopened handle does not index the group that reached disk")
	}
	rows, ok, err := h2.LookupCommit("c1")
	if err != nil || !ok || len(rows) != 2 || rows[0].Seq != 1 || !rows[1].Last {
		t.Fatalf("lookup c1 = %+v %v %v", rows, ok, err)
	}
	if _, err := h2.Append(ctx, group("c1", 2)); !session.IsCode(err, session.ErrConflict) {
		t.Fatalf("replaying the durable group = %v, want conflict", err)
	}
	next, err := h2.Append(ctx, group("c2", 1))
	if err != nil || next[0].Seq != 3 {
		t.Fatalf("append after reopen = %+v %v, want seq 3", next, err)
	}
	page, err := s.Read(ctx, session.ReadRequest{SessionID: sid})
	if err != nil || len(page.Events) != 4 || page.Head.Next != 4 {
		t.Fatalf("read = %d rows head %+v %v", len(page.Events), page.Head, err)
	}
	if err := session.ValidateChain(s.profile, page.Header, page.Events); err != nil {
		t.Fatalf("chain after sync failure and reopen: %v", err)
	}
}
