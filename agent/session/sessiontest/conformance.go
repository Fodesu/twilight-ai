// Package sessiontest is the Store-parameterized conformance suite of the
// Session kernel (agent-session.md section 7). Memory and durable adapters run
// the same suite.
package sessiontest

import (
	"context"
	"testing"

	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/session"
)

// Fixture is one adapter under test.
type Fixture struct {
	Store session.Store
}

// Factory builds a fresh, empty Store for one subtest.
type Factory func(t *testing.T) Fixture

// Run executes the suite.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("wire", func(t *testing.T) { testWire(t, factory(t)) })
	t.Run("ownership", func(t *testing.T) { testOwnership(t, factory(t)) })
	t.Run("append", func(t *testing.T) { testAppend(t, factory(t)) })
	t.Run("crash", func(t *testing.T) { testCrashTail(t, factory(t)) })
	t.Run("read", func(t *testing.T) { testRead(t, factory(t)) })
	t.Run("scope", func(t *testing.T) { testScope(t, factory(t)) })
}

func create(t *testing.T, store session.Store, sid session.SessionID) session.SessionHeader {
	t.Helper()
	h, err := store.Create(context.Background(), session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: sid, CreatedAtUnixMilli: 1})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	return h
}

func open(t *testing.T, store session.Store, sid session.SessionID, takeover bool) session.Writer {
	t.Helper()
	w, err := store.Open(context.Background(), sid, session.OpenOptions{Takeover: takeover})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return w
}

func ev(typ, payload string) session.UncommittedEvent {
	return session.UncommittedEvent{Type: session.EventType(typ), Payload: jsonstable.MustParse(payload), RecordedAtUnixMilli: 1}
}

func appendGroup(t *testing.T, w session.Writer, id string, events ...session.UncommittedEvent) []session.SessionEvent {
	t.Helper()
	rows, err := w.Append(context.Background(), session.Group{CommitID: session.CommitID(id), Events: events})
	if err != nil {
		t.Fatalf("append %s: %v", id, err)
	}
	return rows
}

// SES-WIR-1/2/3: contiguous Seq, group Index/Last, unique CommitID, canonical
// payload, chain rooted at the header, one version per stream.
func testWire(t *testing.T, f Fixture) {
	ctx := context.Background()
	h := create(t, f.Store, "s")
	if h.ProtocolVersion != session.ProtocolVersion1 || h.HeaderDigest == "" {
		t.Fatalf("header = %+v", h)
	}
	if again, err := f.Store.Create(ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: "s", CreatedAtUnixMilli: 1}); err != nil || again.HeaderDigest != h.HeaderDigest {
		t.Fatalf("identical create is not idempotent: %+v %v", again, err)
	}
	if _, err := f.Store.Create(ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: "s", CreatedAtUnixMilli: 2}); !session.IsCode(err, session.ErrConflict) {
		t.Fatalf("different create = %v, want conflict", err)
	}
	if _, err := f.Store.Create(ctx, session.CreateRequest{ProtocolVersion: 9, SessionID: "v9"}); !session.IsCode(err, session.ErrUnsupportedProfile) {
		t.Fatalf("unsupported version = %v", err)
	}
	w := open(t, f.Store, "s", false)
	if head := w.Head(); head.Next != 0 || head.Digest != h.HeaderDigest {
		t.Fatalf("empty head = %+v", head)
	}
	g1 := appendGroup(t, w, "c1", ev("twilight/x/a", `{"a":1}`), ev("twilight/y/b", `{"b":2}`))
	g2 := appendGroup(t, w, "c2", ev("twilight/x/c", `{}`))
	if g1[0].Seq != 0 || g1[1].Seq != 1 || g2[0].Seq != 2 {
		t.Fatalf("seq not contiguous: %v %v", g1, g2)
	}
	if g1[0].Index != 0 || g1[0].Last || g1[1].Index != 1 || !g1[1].Last || !g2[0].Last {
		t.Fatalf("group markers wrong: %+v %+v", g1, g2)
	}
	if g1[0].CommitID != "c1" || g1[1].CommitID != "c1" {
		t.Fatal("rows of one append must share CommitID")
	}
	if _, err := w.Append(ctx, session.Group{CommitID: "c1", Events: []session.UncommittedEvent{ev("twilight/x/a", `{}`)}}); !session.IsCode(err, session.ErrConflict) {
		t.Fatalf("duplicate CommitID = %v, want conflict", err)
	}
	if head := w.Head(); head.Next != 3 || head.Digest != g2[0].Digest {
		t.Fatalf("head = %+v", head)
	}
	// Chain: every digest recomputes from the previous row and the header.
	page, err := f.Store.Read(ctx, session.ReadRequest{SessionID: "s"})
	if err != nil || len(page.Events) != 3 {
		t.Fatalf("read = %+v %v", page, err)
	}
	if err := session.ValidateChain(session.ProfileV1(), page.Header, page.Events); err != nil {
		t.Fatalf("chain: %v", err)
	}
	if page.Header.HeaderDigest != h.HeaderDigest || page.Head.Next != 3 {
		t.Fatalf("page header/head = %+v", page)
	}
}

// SES-OWN-1/2: second Open is ErrOwned; Close then Open bumps Epoch; an Open
// with Takeover supersedes a live owner; a superseded Writer's Append fails
// without writing.
func testOwnership(t *testing.T, f Fixture) {
	ctx := context.Background()
	create(t, f.Store, "s")
	w1 := open(t, f.Store, "s", false)
	if w1.Epoch() != 1 {
		t.Fatalf("first epoch = %d", w1.Epoch())
	}
	if _, err := f.Store.Open(ctx, "s", session.OpenOptions{}); !session.IsCode(err, session.ErrOwned) {
		t.Fatalf("second open = %v, want owned", err)
	}
	appendGroup(t, w1, "c1", ev("twilight/x/a", `{}`))
	if err := w1.Close(ctx); err != nil {
		t.Fatal(err)
	}
	w2 := open(t, f.Store, "s", false)
	if w2.Epoch() != 2 {
		t.Fatalf("epoch after reopen = %d, want 2", w2.Epoch())
	}
	if _, err := w1.Append(ctx, session.Group{CommitID: "c2", Events: []session.UncommittedEvent{ev("twilight/x/a", `{}`)}}); !session.IsCode(err, session.ErrOwnershipLost) {
		t.Fatalf("old writer append = %v, want ownership_lost", err)
	}
	page, _ := f.Store.Read(ctx, session.ReadRequest{SessionID: "s"})
	if len(page.Events) != 1 {
		t.Fatalf("fenced append wrote rows: %d", len(page.Events))
	}
	appendGroup(t, w2, "c2", ev("twilight/x/a", `{}`))
	if err := w1.Close(ctx); err != nil {
		t.Fatalf("closing a superseded writer must be a no-op: %v", err)
	}
	if _, err := f.Store.Open(ctx, "s", session.OpenOptions{}); !session.IsCode(err, session.ErrOwned) {
		t.Fatal("closing a superseded writer released the current owner")
	}
	// Takeover supersedes the live owner: the crashed-process recovery path.
	w3 := open(t, f.Store, "s", true)
	if w3.Epoch() != w2.Epoch()+1 {
		t.Fatalf("takeover epoch = %d, want %d", w3.Epoch(), w2.Epoch()+1)
	}
	if _, err := w2.Append(ctx, session.Group{CommitID: "late", Events: []session.UncommittedEvent{ev("twilight/x/a", `{}`)}}); !session.IsCode(err, session.ErrOwnershipLost) {
		t.Fatalf("superseded writer append = %v, want ownership_lost", err)
	}
	appendGroup(t, w3, "c3", ev("twilight/x/a", `{}`))
	page, _ = f.Store.Read(ctx, session.ReadRequest{SessionID: "s"})
	if len(page.Events) != 3 {
		t.Fatalf("stream after takeover = %d rows, want 3", len(page.Events))
	}
	// Read never needs ownership (SES-OWN-4): already exercised above while owned.
}

// SES-APP-1/3: whole-group visibility and the rejection list, none writing.
func testAppend(t *testing.T, f Fixture) {
	ctx := context.Background()
	create(t, f.Store, "s")
	w := open(t, f.Store, "s", false)
	rejects := []struct {
		name string
		g    session.Group
		code session.ErrorCode
	}{
		{"empty group", session.Group{CommitID: "c"}, session.ErrInvalid},
		{"empty commit id", session.Group{Events: []session.UncommittedEvent{ev("twilight/x/a", `{}`)}}, session.ErrInvalid},
		{"empty type", session.Group{CommitID: "c", Events: []session.UncommittedEvent{ev("", `{}`)}}, session.ErrInvalid},
		{"non-object payload", session.Group{CommitID: "c", Events: []session.UncommittedEvent{ev("twilight/x/a", `[1]`)}}, session.ErrInvalid},
		{"empty payload", session.Group{CommitID: "c", Events: []session.UncommittedEvent{{Type: "twilight/x/a"}}}, session.ErrInvalid},
	}
	for _, tc := range rejects {
		if _, err := w.Append(ctx, tc.g); !session.IsCode(err, tc.code) {
			t.Fatalf("%s: err = %v, want %s", tc.name, err, tc.code)
		}
	}
	if head := w.Head(); head.Next != 0 {
		t.Fatalf("rejections wrote rows: head %+v", head)
	}
	rows := appendGroup(t, w, "c1", ev("twilight/x/a", `{"i":0}`), ev("twilight/x/a", `{"i":1}`), ev("twilight/x/a", `{"i":2}`))
	page, _ := f.Store.Read(ctx, session.ReadRequest{SessionID: "s"})
	if len(page.Events) != 3 || page.Events[2].Digest != rows[2].Digest {
		t.Fatalf("group not visible as a whole: %+v", page.Events)
	}
	// SourceSeqs and Ignorable round-trip untouched; the kernel does not
	// interpret them.
	out := appendGroup(t, w, "c2", session.UncommittedEvent{Type: "twilight/x/b", Payload: jsonstable.MustParse(`{}`), SourceSeqs: []session.Seq{7, 1}, Ignorable: true})
	if len(out[0].SourceSeqs) != 2 || out[0].SourceSeqs[0] != 7 || !out[0].Ignorable {
		t.Fatalf("row metadata altered: %+v", out[0])
	}
}

// SES-REP-1/2: order, From, Limit at group boundaries, filter equivalence,
// tamper detection at Open.
func testRead(t *testing.T, f Fixture) {
	ctx := context.Background()
	create(t, f.Store, "s")
	w := open(t, f.Store, "s", false)
	appendGroup(t, w, "c1", ev("twilight/run/a", `{}`), ev("twilight/chat/a", `{}`))                             // 0,1
	appendGroup(t, w, "c2", ev("twilight/chat/b", `{}`))                                                         // 2
	appendGroup(t, w, "c3", ev("twilight/run/c", `{}`), ev("twilight/run/d", `{}`), ev("twilight/chat/e", `{}`)) // 3,4,5
	all, err := f.Store.Read(ctx, session.ReadRequest{SessionID: "s"})
	if err != nil || len(all.Events) != 6 || all.HasMore {
		t.Fatalf("read all = %d %v %v", len(all.Events), all.HasMore, err)
	}
	for i, e := range all.Events {
		if e.Seq != session.Seq(i) {
			t.Fatalf("order broken at %d: %+v", i, e)
		}
	}
	from, _ := f.Store.Read(ctx, session.ReadRequest{SessionID: "s", From: 4})
	if len(from.Events) != 2 || from.Events[0].Seq != 4 {
		t.Fatalf("from = %+v", from.Events)
	}
	beyond, _ := f.Store.Read(ctx, session.ReadRequest{SessionID: "s", From: 99})
	if len(beyond.Events) != 0 || beyond.Head.Next != 6 {
		t.Fatalf("beyond head = %+v", beyond)
	}
	limited, _ := f.Store.Read(ctx, session.ReadRequest{SessionID: "s", Limit: 2})
	if len(limited.Events) != 2 || !limited.HasMore || !limited.Events[1].Last {
		t.Fatalf("limit must cut at a group boundary: %+v more=%v", limited.Events, limited.HasMore)
	}
	tiny, _ := f.Store.Read(ctx, session.ReadRequest{SessionID: "s", Limit: 1})
	if len(tiny.Events) != 2 || !tiny.HasMore {
		t.Fatalf("a limit below one group still returns the whole first group: %d more=%v", len(tiny.Events), tiny.HasMore)
	}
	filtered, _ := f.Store.Read(ctx, session.ReadRequest{SessionID: "s", Types: []session.EventType{"twilight/run/"}})
	var want []session.SessionEvent
	for _, e := range all.Events {
		if session.HasTypePrefix(e.Type, []session.EventType{"twilight/run/"}) {
			want = append(want, e)
		}
	}
	if len(filtered.Events) != len(want) {
		t.Fatalf("filtered = %d, want %d", len(filtered.Events), len(want))
	}
	for i := range want {
		if filtered.Events[i].Digest != want[i].Digest {
			t.Fatalf("filtered row %d differs from unfiltered", i)
		}
	}
	if _, err := f.Store.Read(ctx, session.ReadRequest{SessionID: "nope"}); !session.IsCode(err, session.ErrNotFound) {
		t.Fatalf("unknown session = %v", err)
	}
	if tamper, ok := f.Store.(interface {
		Tamper(session.SessionID, session.Seq, func(*session.SessionEvent))
	}); ok {
		if err := w.Close(ctx); err != nil {
			t.Fatal(err)
		}
		tamper.Tamper("s", 2, func(e *session.SessionEvent) { e.Payload = jsonstable.MustParse(`{"x":1}`) })
		if _, err := f.Store.Open(ctx, "s", session.OpenOptions{}); !session.IsCode(err, session.ErrCorrupt) {
			t.Fatalf("open over a tampered stream = %v, want corrupt", err)
		}
	}
}

// TailCrasher is the optional adapter capability that simulates a crash inside
// Append by dropping every durable row after the first keep rows. An adapter
// whose writes cannot tear (a database transaction) need not implement it, and
// the crash case is then skipped rather than silently passing.
type TailCrasher interface {
	CrashTail(session.SessionID, int) error
}

// SES-APP-2: a crash leaves at most one incomplete tail group. Open must
// recover to the last complete group, so no reader ever sees a partial group
// and the next Append cannot extend a group that never got its Last row.
func testCrashTail(t *testing.T, f Fixture) {
	crash, ok := f.Store.(TailCrasher)
	if !ok {
		t.Skip("adapter has no tail to tear: writes are atomic by construction")
	}
	ctx := context.Background()
	header := create(t, f.Store, "s")
	w := open(t, f.Store, "s", false)
	g1 := appendGroup(t, w, "c1", ev("twilight/x/a", `{"a":1}`), ev("twilight/x/b", `{"b":2}`))
	appendGroup(t, w, "c2", ev("twilight/x/c", `{"c":3}`), ev("twilight/x/d", `{"d":4}`))
	if err := w.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Only c1 and the first row of c2 reached durable storage.
	if err := crash.CrashTail("s", 3); err != nil {
		t.Fatalf("crash injection: %v", err)
	}

	w2 := open(t, f.Store, "s", false)
	if got, want := w2.Head(), (session.Head{Next: 2, Digest: g1[1].Digest}); got != want {
		t.Fatalf("head after a torn tail = %+v, want %+v (the torn group must be dropped, not continued)", got, want)
	}

	// A reader never sees the partial group.
	page, err := f.Store.Read(ctx, session.ReadRequest{SessionID: "s"})
	if err != nil {
		t.Fatalf("read after crash: %v", err)
	}
	if len(page.Events) != 2 {
		t.Fatalf("rows after a torn tail = %d, want the 2 complete rows of c1", len(page.Events))
	}
	if err := session.ValidateChain(session.ProfileV1(), header, page.Events); err != nil {
		t.Fatalf("chain after recovery: %v", err)
	}

	// The group that never committed must be admissible again, and it must
	// start at the recovered head rather than inside the torn group.
	re := appendGroup(t, w2, "c2", ev("twilight/x/c", `{"c":3}`))
	if re[0].Seq != 2 {
		t.Fatalf("re-appended group starts at seq %d, want 2", re[0].Seq)
	}

	page, err = f.Store.Read(ctx, session.ReadRequest{SessionID: "s"})
	if err != nil {
		t.Fatalf("read after re-append: %v", err)
	}
	if len(page.Events) != 3 {
		t.Fatalf("rows after re-append = %d, want 3", len(page.Events))
	}
	if err := session.ValidateChain(session.ProfileV1(), header, page.Events); err != nil {
		t.Fatalf("chain after re-append: %v", err)
	}
	// No group may weld the torn row to the group that replaced it: every row
	// must arrive in a complete group whose CommitID is uniform.
	for i := 0; i < len(page.Events); {
		end := i
		for end < len(page.Events) && !page.Events[end].Last {
			end++
		}
		if end >= len(page.Events) {
			t.Fatalf("row %d: an incomplete group reached a reader", i)
		}
		for j := i; j <= end; j++ {
			if page.Events[j].Seq != session.Seq(j) {
				t.Fatalf("seq %d at row %d", page.Events[j].Seq, j)
			}
			if page.Events[j].CommitID != page.Events[i].CommitID {
				t.Fatalf("rows %d..%d mix CommitIDs %s and %s: a torn group was welded to the next one",
					i, end, page.Events[i].CommitID, page.Events[j].CommitID)
			}
		}
		i = end + 1
	}
}

// SES-SCP-3: an unknown Session is ErrNotFound, not an implicitly created
// stream.
func testScope(t *testing.T, f Fixture) {
	ctx := context.Background()
	if _, err := f.Store.Open(ctx, "missing", session.OpenOptions{}); !session.IsCode(err, session.ErrNotFound) {
		t.Fatalf("open unknown session = %v", err)
	}
	if _, err := f.Store.Header(ctx, "missing"); !session.IsCode(err, session.ErrNotFound) {
		t.Fatalf("header unknown session = %v", err)
	}
}
