package filestore

import (
	"context"
	"fmt"
	"testing"

	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/session"
)

// TestReadIndexedMatchesFullParse checks the byte-offset read path against a
// Store instance that has no index and parses the whole log: every From,
// filter and Limit must give the same page, and the index must fall back to a
// full parse once another instance changes the file.
func TestReadIndexedMatchesFullParse(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	const sid session.SessionID = "idx"
	indexed, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := indexed.Create(ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: sid, CreatedAtUnixMilli: 1}); err != nil {
		t.Fatal(err)
	}
	w, err := indexed.Open(ctx, sid, session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	appendGroups(t, w, 0, [][]string{{"x", "y"}, {"x"}, {"x", "x", "y"}}) // rows 0..5
	if indexed.currentIndex(sid, indexed.LogPath(sid)) == nil {
		t.Fatal("append did not keep the index current")
	}
	fresh, err := New(root) // never opened: every Read is a full parse
	if err != nil {
		t.Fatal(err)
	}
	filters := [][]session.EventType{nil, {"twilight/x/"}, {"twilight/y/"}}
	for from := session.Seq(0); from <= 7; from++ {
		for _, types := range filters {
			for _, limit := range []uint32{0, 1, 2} {
				req := session.ReadRequest{SessionID: sid, From: from, Types: types, Limit: limit}
				samePage(t, fmt.Sprintf("from=%d types=%v limit=%d", from, types, limit), indexed, fresh, req)
			}
		}
	}
	if indexed.currentIndex(sid, indexed.LogPath(sid)) == nil {
		t.Fatal("reads dropped the index")
	}

	// Another instance takes over and appends: the file changed under the
	// index, so the next Read must rebuild rather than trust it.
	w2, err := fresh.Open(ctx, sid, session.OpenOptions{Takeover: true})
	if err != nil {
		t.Fatal(err)
	}
	appendGroups(t, w2, 3, [][]string{{"y", "y"}}) // rows 6,7
	page, err := indexed.Read(ctx, session.ReadRequest{SessionID: sid})
	if err != nil || page.Head.Next != 8 || len(page.Events) != 8 {
		t.Fatalf("stale index survived a foreign append: head=%+v rows=%d err=%v", page.Head, len(page.Events), err)
	}
	for from := session.Seq(0); from <= 9; from++ {
		samePage(t, fmt.Sprintf("after foreign append from=%d", from), indexed, fresh, session.ReadRequest{SessionID: sid, From: from})
	}

	// A torn tail on disk is excluded by both paths alike.
	if err := indexed.CrashTail(sid, 7); err != nil {
		t.Fatal(err)
	}
	for from := session.Seq(0); from <= 8; from++ {
		samePage(t, fmt.Sprintf("torn tail from=%d", from), indexed, fresh, session.ReadRequest{SessionID: sid, From: from})
	}
	page, err = indexed.Read(ctx, session.ReadRequest{SessionID: sid})
	if err != nil || page.Head.Next != 6 {
		t.Fatalf("torn group exposed: head=%+v err=%v", page.Head, err)
	}
}

func appendGroups(t *testing.T, w session.Handle, firstCommit int, groups [][]string) {
	t.Helper()
	for i, g := range groups {
		events := make([]session.UncommittedEvent, len(g))
		for j, kind := range g {
			events[j] = session.UncommittedEvent{Type: session.EventType("twilight/" + kind + "/e"), RecordedAtUnixMilli: 1,
				Payload: jsonstable.MustParse(fmt.Sprintf(`{"g":%d,"i":%d}`, firstCommit+i, j))}
		}
		if _, err := w.Append(context.Background(), session.Group{CommitID: session.CommitID(fmt.Sprintf("c%d", firstCommit+i)), Events: events}); err != nil {
			t.Fatalf("append c%d: %v", firstCommit+i, err)
		}
	}
}

// samePage compares indexed against a Store instance created for this one
// call: it has never opened or read the Session, so its Read is a full parse.
func samePage(t *testing.T, name string, indexed, _ *Store, req session.ReadRequest) {
	t.Helper()
	fresh, err := New(indexed.root)
	if err != nil {
		t.Fatal(err)
	}
	a, errA := indexed.Read(context.Background(), req)
	b, errB := fresh.Read(context.Background(), req)
	if (errA != nil) != (errB != nil) {
		t.Fatalf("%s: errors differ: indexed=%v fresh=%v", name, errA, errB)
	}
	if errA != nil {
		return
	}
	if a.Head != b.Head || a.HasMore != b.HasMore || len(a.Events) != len(b.Events) {
		t.Fatalf("%s: pages differ: indexed head=%+v more=%v rows=%d; fresh head=%+v more=%v rows=%d",
			name, a.Head, a.HasMore, len(a.Events), b.Head, b.HasMore, len(b.Events))
	}
	for i := range a.Events {
		if a.Events[i].Seq != b.Events[i].Seq || a.Events[i].Digest != b.Events[i].Digest || a.Events[i].Type != b.Events[i].Type {
			t.Fatalf("%s: row %d differs: %+v vs %+v", name, i, a.Events[i], b.Events[i])
		}
	}
}
