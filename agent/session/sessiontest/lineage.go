package sessiontest

import (
	"context"
	"testing"

	"github.com/felinics/twilight/agent/session"
)

// SES-GC-1/2: Sessions are roots into a DAG of immutable segments. Delete
// drops a root and nothing else; Collect keeps every commit a live root still
// reaches through fork anchors and reclaims the rest, transitively.
//
//	A: c0 c1 c2 c3        B -> A@1        C -> A@2        D -> C@(C's own first commit)
func testLineage(t *testing.T, f Fixture) {
	ctx := context.Background()
	store := f.Store
	create(t, store, "A")
	aw := open(t, store, "A", false)
	var a []session.Commit
	for i := 0; i < 4; i++ {
		a = append(a, appendCommit(t, aw, "a"+string(rune('0'+i)), batch(sessionStream(), "twilight/x/a", `{"n":`+string(rune('0'+i))+`}`)))
	}
	fork := func(child, parent session.SessionID, at session.Commit) {
		t.Helper()
		_, err := store.Create(ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: child, CreatedAtUnixMilli: 2,
			ParentFork: &session.ForkPoint{ParentSessionID: parent, Seq: at.Seq, Digest: at.Digest}})
		if err != nil {
			t.Fatalf("fork %s: %v", child, err)
		}
	}
	fork("B", "A", a[1])
	fork("C", "A", a[2])
	cw := open(t, store, "C", false)
	c3 := appendCommit(t, cw, "c3", batch(sessionStream(), "twilight/x/c", `{"n":3}`))
	_ = cw.Close(ctx)
	fork("D", "C", c3)

	// Delete refuses an owned Session and an unknown one.
	if err := store.Delete(ctx, "A"); !session.IsCode(err, session.ErrOwned) {
		t.Fatalf("delete owned = %v", err)
	}
	if err := store.Delete(ctx, "ghost"); !session.IsCode(err, session.ErrNotFound) {
		t.Fatalf("delete unknown = %v", err)
	}
	_ = aw.Close(ctx)
	if err := store.Delete(ctx, "A"); err != nil {
		t.Fatal(err)
	}
	// A is no Session any more: not found, not openable, not forkable, not
	// recreatable while its segment awaits collection; a second Delete is
	// not found.
	if _, err := store.Header(ctx, "A"); !session.IsCode(err, session.ErrNotFound) {
		t.Fatalf("header after delete = %v", err)
	}
	if _, err := store.Open(ctx, "A", session.OpenOptions{}); !session.IsCode(err, session.ErrNotFound) {
		t.Fatalf("open after delete = %v", err)
	}
	if _, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "A"}); !session.IsCode(err, session.ErrNotFound) {
		t.Fatalf("read after delete = %v", err)
	}
	if _, err := store.Create(ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: "E", CreatedAtUnixMilli: 3,
		ParentFork: &session.ForkPoint{ParentSessionID: "A", Seq: a[0].Seq, Digest: a[0].Digest}}); !session.IsCode(err, session.ErrNotFound) {
		t.Fatalf("fork of a deleted session = %v", err)
	}
	if _, err := store.Create(ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: "A", CreatedAtUnixMilli: 9}); !session.IsCode(err, session.ErrConflict) {
		t.Fatalf("recreate before collect = %v", err)
	}
	if err := store.Delete(ctx, "A"); !session.IsCode(err, session.ErrNotFound) {
		t.Fatalf("second delete = %v", err)
	}
	// B, C and D still read their prefixes, and D through the deleted A.
	for sid, want := range map[session.SessionID]string{"B": "a0,a1", "C": "a0,a1,a2,c3", "D": "a0,a1,a2,c3"} {
		page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid})
		if err != nil || ids(page.Commits) != want {
			t.Fatalf("%s after deleting A = %s %v, want %s", sid, ids(page.Commits), err, want)
		}
	}
	// Collect keeps A's commits up to the furthest live anchor (C@2) and
	// drops a3; B, C, D are untouched.
	report, err := store.Collect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Removed) != 0 || report.Truncated["A"] != a[2].Seq+1 {
		t.Fatalf("collect = %+v, want A truncated after %d", report, a[2].Seq)
	}
	for sid, want := range map[session.SessionID]string{"B": "a0,a1", "C": "a0,a1,a2,c3", "D": "a0,a1,a2,c3"} {
		page, _ := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid})
		if ids(page.Commits) != want {
			t.Fatalf("%s after collect = %s, want %s", sid, ids(page.Commits), want)
		}
	}
	// A live fork of a deleted segment opens, looks up inherited commits and
	// appends as before.
	dw := open(t, store, "D", false)
	if !dw.Committed("a0") || dw.Committed("a3") {
		t.Fatal("D prefix membership wrong after collect")
	}
	if c, ok, _ := dw.LookupCommit("a2"); !ok || c.Digest != a[2].Digest {
		t.Fatalf("D lookup a2 = %+v %v", c, ok)
	}
	d4 := appendCommit(t, dw, "d4", batch(sessionStream(), "twilight/x/d", `{"n":4}`))
	if d4.Seq != c3.Seq+1 || d4.PrevDigest != c3.Digest {
		t.Fatalf("D own commit = %+v", d4)
	}
	_ = dw.Close(ctx)
	// Collect is idempotent.
	if report, err := store.Collect(ctx); err != nil || len(report.Removed) != 0 || len(report.Truncated) != 0 {
		t.Fatalf("second collect = %+v %v", report, err)
	}
	// Deleting C (still reached by D) keeps its segment; deleting B, whose
	// prefix ends at A@1, lets A shrink to a2 only if something else reaches
	// A@2: C does, through D. Deleting D last makes A, C and D unreachable.
	if err := store.Delete(ctx, "C"); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, "B"); err != nil {
		t.Fatal(err)
	}
	report, _ = store.Collect(ctx)
	if len(report.Removed) != 1 || report.Removed[0] != "B" || len(report.Truncated) != 0 {
		t.Fatalf("collect after deleting B and C = %+v, want only B removed", report)
	}
	if page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "D"}); err != nil || ids(page.Commits) != "a0,a1,a2,c3,d4" {
		t.Fatalf("D after collect = %s %v", ids(page.Commits), err)
	}
	if err := store.Delete(ctx, "D"); err != nil {
		t.Fatal(err)
	}
	report, _ = store.Collect(ctx)
	if len(report.Removed) != 3 || len(report.Truncated) != 0 {
		t.Fatalf("final collect = %+v, want A, C, D removed", report)
	}
	// The identities are free again.
	if _, err := store.Create(ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: "A", CreatedAtUnixMilli: 9}); err != nil {
		t.Fatalf("recreate after collect: %v", err)
	}
	if page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "A"}); err != nil || len(page.Commits) != 0 {
		t.Fatalf("recreated A = %+v %v", page, err)
	}
}
