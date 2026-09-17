package sessiontest

import (
	"context"
	"testing"

	"github.com/felinics/twilight/agent/session"
)

// forkAt creates child from the history of parent at commit seq.
func forkAt(t *testing.T, store session.Store, child, parent session.SessionID, seq session.CommitSeq) (session.SegmentHeader, error) {
	t.Helper()
	return store.Create(context.Background(), session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: child, CreatedAtUnixMilli: 2,
		Fork: &session.ForkOrigin{Session: parent, Seq: seq}})
}

// SES-FRK-1/2/3: a fork is a new segment whose edge names the commit of its
// parent's history it inherits; it reads the inherited prefix followed by its
// own commits, chains its own commits from the edge, and treats inherited
// CommitIDs as its own for lookup and duplicate rejection. The parent is
// unaffected.
func testFork(t *testing.T, f Fixture) {
	ctx := context.Background()
	store := f.Store
	parent := create(t, store, "parent")
	pw := open(t, store, "parent", false)
	c0 := appendCommit(t, pw, "c0", batch(sessionStream(), "twilight/x/a", `{"n":0}`))
	c1 := appendCommit(t, pw, "c1", batch(sessionStream(), "twilight/x/a", `{"n":1}`), batch(runStream("r1"), "twilight/x/r", `{"n":1}`))
	appendCommit(t, pw, "c2", batch(runStream("r1"), "twilight/x/r", `{"n":2}`))

	// Rejections write nothing: unknown parent, a commit the parent does not
	// have, a Session forking itself.
	rejects := []struct {
		name   string
		sid    session.SessionID
		parent session.SessionID
		seq    session.CommitSeq
		code   session.ErrorCode
	}{
		{"unknown parent", "child", "ghost", 0, session.ErrNotFound},
		{"commit past the parent head", "child", "parent", 9, session.ErrInvalid},
		{"self parent", "parent2", "parent2", 0, session.ErrInvalid},
	}
	for _, tc := range rejects {
		if _, err := forkAt(t, store, tc.sid, tc.parent, tc.seq); !session.IsCode(err, tc.code) {
			t.Fatalf("%s: err = %v, want %s", tc.name, err, tc.code)
		}
		if _, err := store.Header(ctx, tc.sid); !session.IsCode(err, session.ErrNotFound) {
			t.Fatalf("%s: a rejected fork left a header: %v", tc.name, err)
		}
	}

	child, err := forkAt(t, store, "child", "parent", c1.Seq)
	if err != nil {
		t.Fatalf("fork: %v", err)
	}
	// The edge names the parent's segment, not the parent Session, and
	// carries the anchor commit's digest.
	wantEdge := session.LedgerRef{Segment: session.SegmentIDOf(parent), Seq: c1.Seq, Digest: c1.Digest}
	if child.Parent == nil || *child.Parent != wantEdge || child.HeaderDigest == parent.HeaderDigest {
		t.Fatalf("child header = %+v, want edge %+v", child, wantEdge)
	}
	// Idempotent repeat; a different origin for the same SessionID conflicts.
	if again, err := forkAt(t, store, "child", "parent", c1.Seq); err != nil || again.HeaderDigest != child.HeaderDigest {
		t.Fatalf("repeat fork = %+v %v", again, err)
	}
	if _, err := forkAt(t, store, "child", "parent", c0.Seq); !session.IsCode(err, session.ErrConflict) {
		t.Fatalf("conflicting fork = %v", err)
	}

	// The empty child seeds at the edge and reads the inherited prefix.
	seed := session.LedgerSeed(child)
	if seed != (session.Head{Next: c1.Seq + 1, Digest: c1.Digest}) {
		t.Fatalf("seed = %+v", seed)
	}
	page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "child"})
	if err != nil {
		t.Fatal(err)
	}
	if page.Head != seed || len(page.Commits) != 2 || page.Commits[0].CommitID != "c0" || page.Commits[1].CommitID != "c1" || page.HasMore {
		t.Fatalf("empty child page = %+v", page)
	}
	if page.Header.HeaderDigest != child.HeaderDigest {
		t.Fatalf("child page carries header %s, want the child's tip", page.Header.HeaderDigest)
	}
	// The prefix is validated under the parent segment's header; the child's
	// own commits (none yet) under the child's.
	if err := session.ValidateLedger(session.ProfileV1(), parent, page.Commits); err != nil {
		t.Fatalf("prefix under parent header: %v", err)
	}

	// Own commits continue the chain from the edge.
	cw := open(t, store, "child", false)
	if cw.Head() != seed {
		t.Fatalf("child head = %+v, want %+v", cw.Head(), seed)
	}
	if !cw.Committed("c0") || !cw.Committed("c1") || cw.Committed("c2") {
		t.Fatal("inherited CommitIDs are not visible as committed, or the excluded tail is")
	}
	got, ok, err := cw.LookupCommit("c1")
	if err != nil || !ok || got.Digest != c1.Digest || got.Seq != c1.Seq {
		t.Fatalf("lookup inherited = %+v %v %v", got, ok, err)
	}
	if _, err := cw.Append(ctx, session.Proposal{CommitID: "c0", Batches: []session.StreamBatch{batch(sessionStream(), "twilight/x/a", `{"dup":true}`)}}); !session.IsCode(err, session.ErrConflict) {
		t.Fatalf("append of an inherited CommitID = %v, want conflict", err)
	}
	c3 := appendCommit(t, cw, "c3", batch(sessionStream(), "twilight/x/a", `{"n":3}`), batch(runStream("r1"), "twilight/x/r", `{"n":3}`))
	if c3.Seq != c1.Seq+1 || c3.PrevDigest != c1.Digest {
		t.Fatalf("first own commit = seq %d prev %s, want %d %s", c3.Seq, c3.PrevDigest, c1.Seq+1, c1.Digest)
	}
	// The parent keeps appending; neither side sees the other.
	c4 := appendCommit(t, pw, "c4", batch(sessionStream(), "twilight/x/a", `{"n":4}`))
	childPage, _ := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "child"})
	if ids(childPage.Commits) != "c0,c1,c3" || childPage.Head != (session.Head{Next: c3.Seq + 1, Digest: c3.Digest}) {
		t.Fatalf("child commits = %s head %+v", ids(childPage.Commits), childPage.Head)
	}
	parentPage, _ := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "parent"})
	if ids(parentPage.Commits) != "c0,c1,c2,c4" || parentPage.Head.Next != c4.Seq+1 {
		t.Fatalf("parent commits = %s", ids(parentPage.Commits))
	}
	// Own commits validate under the child's header from the edge.
	own, _ := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "child", From: seed.Next})
	if ids(own.Commits) != "c3" {
		t.Fatalf("own commits = %s", ids(own.Commits))
	}
	if err := session.ValidateLedger(session.ProfileV1(), child, own.Commits); err != nil {
		t.Fatalf("own commits under child header: %v", err)
	}

	// Paging crosses the prefix boundary: From and Limit count the stitched
	// sequence.
	for _, tc := range []struct {
		from    session.CommitSeq
		limit   uint32
		want    string
		hasMore bool
	}{
		{0, 1, "c0", true}, {0, 2, "c0,c1", true}, {0, 3, "c0,c1,c3", false}, {1, 0, "c1,c3", false}, {2, 1, "c3", false}, {3, 0, "", false},
	} {
		p, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "child", From: tc.from, Limit: tc.limit})
		if err != nil || ids(p.Commits) != tc.want || p.HasMore != tc.hasMore {
			t.Fatalf("read from %d limit %d = %s more=%v %v, want %s more=%v", tc.from, tc.limit, ids(p.Commits), p.HasMore, err, tc.want, tc.hasMore)
		}
	}
	// Stream positions count the inherited events: r1 has one inherited event
	// (c1) and one own (c3); the parent's c2 and c4 are not the child's.
	sp, err := store.ReadStream(ctx, session.StreamReadRequest{SessionID: "child", Stream: runStream("r1")})
	if err != nil || len(sp.Events) != 2 || sp.Events[0].Payload.String() != `{"n":1}` || sp.Events[1].Payload.String() != `{"n":3}` {
		t.Fatalf("child run stream = %+v %v", sp.Events, err)
	}
	sp, _ = store.ReadStream(ctx, session.StreamReadRequest{SessionID: "child", Stream: runStream("r1"), From: 1})
	if len(sp.Events) != 1 || sp.Events[0].Payload.String() != `{"n":3}` {
		t.Fatalf("child run stream from 1 = %+v", sp.Events)
	}

	// Reopen validates the own chain from the edge and keeps the prefix
	// visible.
	if err := cw.Close(ctx); err != nil {
		t.Fatal(err)
	}
	cw = open(t, store, "child", false)
	if !cw.Committed("c0") || !cw.Committed("c3") || cw.Head().Next != c3.Seq+1 {
		t.Fatalf("reopened child head = %+v", cw.Head())
	}
	_ = cw.Close(ctx)

	// A fork of a fork reads through both prefixes; forking at an inherited
	// commit names the segment that holds it, so the grandchild's edge points
	// at the root segment directly.
	grand, err := forkAt(t, store, "grandchild", "child", c3.Seq)
	if err != nil {
		t.Fatalf("fork of fork: %v", err)
	}
	if grand.Parent.Segment != session.SegmentIDOf(child) {
		t.Fatalf("grandchild edge = %+v, want the child's segment", grand.Parent)
	}
	gw := open(t, store, "grandchild", false)
	c5 := appendCommit(t, gw, "c5", batch(sessionStream(), "twilight/x/a", `{"n":5}`))
	gp, _ := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "grandchild"})
	if ids(gp.Commits) != "c0,c1,c3,c5" || c5.Seq != c3.Seq+1 || c5.PrevDigest != c3.Digest || gp.Header.HeaderDigest != grand.HeaderDigest {
		t.Fatalf("grandchild commits = %s", ids(gp.Commits))
	}
	if !gw.Committed("c0") || gw.Committed("c2") {
		t.Fatal("grandchild prefix membership wrong")
	}
	if c, ok, _ := gw.LookupCommit("c0"); !ok || c.Digest != c0.Digest {
		t.Fatalf("grandchild lookup c0 = %+v %v", c, ok)
	}
	_ = gw.Close(ctx)
	flat, err := forkAt(t, store, "flat", "child", c1.Seq)
	if err != nil {
		t.Fatal(err)
	}
	if flat.Parent.Segment != session.SegmentIDOf(parent) {
		t.Fatalf("fork at an inherited commit edge = %+v, want the root segment", flat.Parent)
	}
	_ = pw.Close(ctx)
}

func ids(commits []session.Commit) string {
	out := ""
	for i, c := range commits {
		if i > 0 {
			out += ","
		}
		out += string(c.CommitID)
	}
	return out
}
