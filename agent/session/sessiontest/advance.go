package sessiontest

import (
	"context"
	"testing"

	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/session"
)

// SES-ADV-1/2: Advance publishes a child segment of the tip, anchored at the
// head and carrying its bootstrap commits, as the root's new tip in one
// step. The Session keeps its identity and stitched history; the new
// segment's own streams start empty; the previous tip stays a node the
// Session reaches through the edge.
func testAdvance(t *testing.T, f Fixture) {
	ctx := context.Background()
	store := f.Store
	headerA := create(t, store, "A")
	segA := session.SegmentIDOf(headerA)
	aw := open(t, store, "A", false)

	// A tip without a commit has nothing to anchor the edge to.
	if _, _, err := aw.Advance(ctx, session.AdvanceRequest{}); !session.IsCode(err, session.ErrInvalid) {
		t.Fatalf("advance of an empty tip = %v, want invalid", err)
	}
	var a []session.Commit
	for i := 0; i < 3; i++ {
		a = append(a, appendCommit(t, aw, "a"+string(rune('0'+i)),
			batch(chatStream(), "twilight/x/a", `{"n":`+string(rune('0'+i))+`}`),
			batch(runStream("r1"), "twilight/x/r", `{"n":`+string(rune('0'+i))+`}`)))
	}
	if _, ok := aw.StreamHead(runStream("r1")); !ok {
		t.Fatal("run stream missing before advance")
	}

	// A bootstrap CommitID already in the history is a conflict and nothing
	// is published.
	_, _, err := aw.Advance(ctx, session.AdvanceRequest{Bootstrap: []session.Proposal{{CommitID: "a1", Batches: []session.StreamBatch{batch(chatStream(), "twilight/x/b", `{"n":0}`)}}}})
	if !session.IsCode(err, session.ErrConflict) {
		t.Fatalf("advance with a known CommitID = %v, want conflict", err)
	}
	if h, _ := store.Header(ctx, "A"); session.SegmentIDOf(h) != segA {
		t.Fatal("a refused advance moved the tip")
	}

	meta := jsonstable.MustParse(`{"schema":2}`)
	headerB, sealed, err := aw.Advance(ctx, session.AdvanceRequest{CausationID: "migrate", Metadata: meta, Bootstrap: []session.Proposal{
		{CommitID: "b0", Intent: "sha256:intent", Batches: []session.StreamBatch{batch(chatStream(), "twilight/x/b", `{"n":0}`)}},
		{CommitID: "b1", Batches: []session.StreamBatch{batch(runStream("r2"), "twilight/x/r", `{"n":1}`)}},
	}})
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	segB := session.SegmentIDOf(headerB)
	if segB == segA || headerB.Parent == nil || headerB.Parent.Segment != segA || headerB.Parent.Seq != a[2].Seq || headerB.Parent.Digest != a[2].Digest {
		t.Fatalf("advanced header = %+v, want edge to %s@%d", headerB, segA, a[2].Seq)
	}
	if !headerB.Metadata.Equal(meta) || headerB.CausationID != "migrate" || headerB.ProtocolVersion != headerA.ProtocolVersion {
		t.Fatalf("advanced header fields = %+v", headerB)
	}
	// The root names the new tip; the header is the new segment's.
	if rec, err := store.Record(ctx, "A"); err != nil || rec.Tip != segB {
		t.Fatalf("record after advance = %+v %v", rec, err)
	}
	if h, err := store.Header(ctx, "A"); err != nil || session.SegmentIDOf(h) != segB {
		t.Fatalf("header after advance = %+v %v", h, err)
	}
	// The stitched history continues: a0..a2 then b0, b1 chained from a2.
	page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "A"})
	if err != nil || ids(page.Commits) != "a0,a1,a2,b0,b1" {
		t.Fatalf("read after advance = %s %v", ids(page.Commits), err)
	}
	b0, b1 := page.Commits[3], page.Commits[4]
	if b0.Seq != a[2].Seq+1 || b0.PrevDigest != a[2].Digest || b0.Intent != "sha256:intent" || b1.PrevDigest != b0.Digest || b0.Epoch != aw.Epoch() {
		t.Fatalf("bootstrap commits = %+v %+v", b0, b1)
	}
	// Advance returns the bootstrap sealed exactly as the ledger holds it.
	if len(sealed) != 2 || sealed[0].Digest != b0.Digest || sealed[1].Digest != b1.Digest || sealed[0].Seq != b0.Seq || sealed[1].CommitID != "b1" {
		t.Fatalf("sealed bootstrap = %+v, want %+v %+v", sealed, b0, b1)
	}
	if page.Head != (session.Head{Next: b1.Seq + 1, Digest: b1.Digest}) || aw.Head() != page.Head {
		t.Fatalf("head after advance = %+v, handle %+v", page.Head, aw.Head())
	}
	// The handle continues on the new tip: membership covers the inherited
	// prefix and the bootstrap; own streams are the new segment's only
	// (SES-FRK-5).
	if !aw.Committed("a1") || !aw.Committed("b0") || aw.Committed("c9") {
		t.Fatal("membership after advance wrong")
	}
	if _, ok := aw.StreamHead(runStream("r1")); ok {
		t.Fatal("previous tip's run stream counted as the new tip's")
	}
	if n, ok := aw.StreamHead(runStream("r2")); !ok || n != 1 {
		t.Fatalf("bootstrap run stream head = %d %v", n, ok)
	}
	if n, ok := aw.StreamHead(chatStream()); !ok || n != 1 {
		t.Fatalf("chat stream head after advance = %d %v, want the bootstrap's 1", n, ok)
	}
	if c, ok, err := aw.LookupCommit("a2"); err != nil || !ok || c.Digest != a[2].Digest {
		t.Fatalf("lookup inherited after advance = %+v %v %v", c, ok, err)
	}
	c2 := appendCommit(t, aw, "c2", batch(chatStream(), "twilight/x/c", `{"n":2}`))
	if c2.Seq != b1.Seq+1 || c2.PrevDigest != b1.Digest {
		t.Fatalf("append after advance = %+v", c2)
	}
	// A LineageSession read stitches across the boundary.
	sp, err := store.ReadStream(ctx, session.StreamReadRequest{SessionID: "A", Stream: chatStream(), Lineage: session.LineageSession})
	if err != nil || len(sp.Events) != 5 {
		t.Fatalf("chat stream after advance = %d events %v", len(sp.Events), err)
	}
	_ = aw.Close(ctx)

	// A fresh Open validates and continues on the new tip.
	aw2 := open(t, store, "A", false)
	if aw2.Head() != (session.Head{Next: c2.Seq + 1, Digest: c2.Digest}) || !aw2.Committed("b1") || !aw2.Committed("a0") {
		t.Fatalf("reopen after advance: head %+v", aw2.Head())
	}
	// A superseded handle cannot advance.
	aw3 := open(t, store, "A", true)
	if _, _, err := aw2.Advance(ctx, session.AdvanceRequest{}); !session.IsCode(err, session.ErrOwnershipLost) {
		t.Fatalf("superseded advance = %v", err)
	}
	// A fork at an inherited commit resolves the edge to the segment that
	// contributes it, across the advance boundary.
	hf, err := forkAt(t, store, "F", "A", a[1].Seq)
	if err != nil || hf.Parent.Segment != segA {
		t.Fatalf("fork across advance = %+v %v", hf, err)
	}
	hg, err := forkAt(t, store, "G", "A", b0.Seq)
	if err != nil || hg.Parent.Segment != segB {
		t.Fatalf("fork at bootstrap = %+v %v", hg, err)
	}
	_ = aw3.Close(ctx)
	// Collect keeps both segments: the root reaches the previous tip through
	// the edge.
	if report, err := store.Collect(ctx); err != nil || len(report.Removed) != 0 || len(report.Truncated) != 0 {
		t.Fatalf("collect after advance = %+v %v", report, err)
	}
	// Deleting A and its forks reclaims the whole chain.
	for _, sid := range []session.SessionID{"A", "F", "G"} {
		if err := store.Delete(ctx, sid); err != nil {
			t.Fatal(err)
		}
	}
	if report, err := store.Collect(ctx); err != nil || len(report.Removed) != 4 {
		t.Fatalf("collect after deletes = %+v %v, want 4 segments removed", report, err)
	}
}
