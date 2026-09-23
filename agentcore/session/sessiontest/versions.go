package sessiontest

import (
	"context"
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/session"
)

// Versioned is what a Store exposes when its Ledger serves more than the
// published kernel version (session.WithProtocolVersion). The versions case
// needs a second version to advance to and skips without one.
type Versioned interface {
	Supports(uint16) bool
}

// SES-ADV-1, SES-VER-2, SES-FRK-1: a Session moves to a later kernel version
// by advancing onto a new tip segment; its earlier segments stay as stored
// and forks cross versions in either direction. Nothing is rewritten.
func testVersions(t *testing.T, f Fixture) {
	store := f.Store
	versioned, ok := store.(Versioned)
	if !ok || !versioned.Supports(2) {
		t.Skip("store has no second kernel version to advance to")
	}
	ctx := context.Background()
	headerA := create(t, store, "A")
	segA := headerA.ID
	aw := open(t, store, "A", false)

	// Not later than the tip, or unknown to the Ledger: refused, tip unmoved.
	for _, tc := range []struct {
		name    string
		version uint16
		code    session.ErrorCode
	}{{"same version", 1, session.ErrInvalid}, {"unknown version", 9, session.ErrUnsupportedVersion}} {
		if _, err := aw.Advance(ctx, session.AdvanceRequest{ProtocolVersion: tc.version}); !session.IsCode(err, tc.code) {
			t.Fatalf("%s: advance = %v, want %s", tc.name, err, tc.code)
		}
	}
	if h, _ := store.Header(ctx, "A"); h.ID != segA {
		t.Fatal("a refused advance moved the tip")
	}

	var a []session.Commit
	for i := 0; i < 2; i++ {
		a = append(a, appendCommit(t, aw, "a"+string(rune('0'+i)),
			batch(chatStream(), "twilight/x/a", `{"n":`+string(rune('0'+i))+`}`),
			batch(runStream("r1"), "twilight/x/r", `{"n":`+string(rune('0'+i))+`}`)))
	}
	meta := jsonstable.MustParse(`{"why":"upgrade"}`)
	headerB, err := aw.Advance(ctx, session.AdvanceRequest{ProtocolVersion: 2, CausationID: "upgrade", Metadata: meta})
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	segB := headerB.ID
	if headerB.ProtocolVersion != 2 || headerB.Parent == nil || headerB.Parent.Segment != segA || headerB.Parent.Seq != a[1].Seq {
		t.Fatalf("advanced header = %+v, want v2 with edge to %s@%d", headerB, segA, a[1].Seq)
	}
	if !headerB.Metadata.Equal(meta) || headerB.CausationID != "upgrade" {
		t.Fatalf("advanced header fields = %+v", headerB)
	}
	if rec, err := store.Record(ctx, "A"); err != nil || rec.Tip != segB {
		t.Fatalf("record after advance = %+v %v", rec, err)
	}
	// The handle continues on the empty v2 tip: history is inherited, own
	// streams start empty (SES-FRK-5), the next commit continues the v1
	// numbering.
	if aw.Head() != (session.Head{Next: a[1].Seq + 1}) || !aw.Committed("a0") || aw.Committed("b0") {
		t.Fatalf("handle after advance: head %+v", aw.Head())
	}
	if _, ok := aw.StreamHead(runStream("r1")); ok {
		t.Fatal("previous tip's run stream counted as the new tip's")
	}
	b0 := appendCommit(t, aw, "b0", batch(chatStream(), "twilight/x/b", `{"n":0}`))
	if b0.Seq != a[1].Seq+1 {
		t.Fatalf("append after advance = %+v", b0)
	}
	page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "A"})
	if err != nil || ids(page.Commits) != "a0,a1,b0" || page.Header.ProtocolVersion != 2 {
		t.Fatalf("read across versions = %s header v%d %v", ids(page.Commits), page.Header.ProtocolVersion, err)
	}
	sp, err := store.ReadStream(ctx, session.StreamReadRequest{SessionID: "A", Stream: chatStream(), Lineage: session.LineageSession})
	if err != nil || len(sp.Events) != 3 {
		t.Fatalf("chat stream across versions = %d events %v", len(sp.Events), err)
	}
	_ = aw.Close(ctx)

	// A fresh Open reads the v1 prefix and the v2 tip.
	aw2 := open(t, store, "A", false)
	if aw2.Head() != (session.Head{Next: b0.Seq + 1}) || !aw2.Committed("a1") || !aw2.Committed("b0") {
		t.Fatalf("reopen across versions: head %+v", aw2.Head())
	}
	// A superseded handle cannot advance.
	aw3 := open(t, store, "A", true)
	if _, err := aw2.Advance(ctx, session.AdvanceRequest{ProtocolVersion: 3}); !session.IsCode(err, session.ErrOwnershipLost) && !session.IsCode(err, session.ErrUnsupportedVersion) {
		t.Fatalf("superseded advance = %v", err)
	}
	_ = aw3.Close(ctx)

	// Forks cross versions both ways: a v2 child of a v1 commit, a v1 child
	// of a v2 commit.
	hf, err := store.Create(ctx, session.CreateRequest{ProtocolVersion: 2, SessionID: "F", CreatedAtUnixMilli: 3, Fork: &session.ForkOrigin{Session: "A", Seq: a[0].Seq}})
	if err != nil || hf.Parent.Segment != segA || hf.ProtocolVersion != 2 {
		t.Fatalf("v2 fork of a v1 commit = %+v %v", hf, err)
	}
	hg, err := forkAt(t, store, "G", "A", b0.Seq)
	if err != nil || hg.Parent.Segment != segB || hg.ProtocolVersion != 1 {
		t.Fatalf("v1 fork of a v2 commit = %+v %v", hg, err)
	}
	for _, sid := range []session.SessionID{"F", "G"} {
		w := open(t, store, sid, false)
		if !w.Committed("a0") {
			t.Fatalf("%s: inherited membership lost across versions", sid)
		}
		appendCommit(t, w, "child-"+string(sid), batch(chatStream(), "twilight/x/c", `{}`))
		_ = w.Close(ctx)
		if _, err := store.Open(ctx, sid, session.OpenOptions{}); err != nil {
			t.Fatalf("%s: reopen after cross-version fork: %v", sid, err)
		}
	}

	// An empty tip is replaced, not extended: the new segment carries the
	// tip's own edge and the old node is reclaimed.
	create(t, store, "E")
	ew := open(t, store, "E", false)
	he, err := ew.Advance(ctx, session.AdvanceRequest{ProtocolVersion: 2})
	if err != nil || he.Parent != nil || he.ProtocolVersion != 2 {
		t.Fatalf("advance of an empty root tip = %+v %v", he, err)
	}
	e0 := appendCommit(t, ew, "e0", batch(chatStream(), "twilight/x/e", `{}`))
	if e0.Seq != 0 || he.ID == "" {
		t.Fatalf("first commit after replacing an empty tip = %+v", e0)
	}
	_ = ew.Close(ctx)
	if report, err := store.Collect(ctx); err != nil || len(report.Removed) != 1 {
		t.Fatalf("collect after replacing an empty tip = %+v %v, want the old empty node removed", report, err)
	}
}
