// Package sessiontest is the Store-parameterized conformance suite of the
// Session kernel (agent-session.md section 7). Memory and durable adapters run
// the same suite.
package sessiontest

import (
	"context"
	"testing"
	"time"

	"github.com/memohai/twilight/agent/jsonstable"
	"github.com/memohai/twilight/agent/session"
)

// Fixture is one adapter under test. Advance moves the adapter's clock for
// TTL takeover checks; nil skips those checks (file adapters have no TTL).
type Fixture struct {
	Store   session.Store
	Advance func(time.Duration)
}

// Factory builds a fresh, empty Store for one subtest.
type Factory func(t *testing.T) Fixture

// Run executes the suite.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("wire", func(t *testing.T) { testWire(t, factory(t)) })
	t.Run("ownership", func(t *testing.T) { testOwnership(t, factory(t)) })
	t.Run("append", func(t *testing.T) { testAppend(t, factory(t)) })
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

func open(t *testing.T, store session.Store, sid session.SessionID, ttl time.Duration) session.Writer {
	t.Helper()
	w, err := store.Open(context.Background(), sid, session.OpenOptions{TTL: ttl})
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
	w := open(t, f.Store, "s", 0)
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

// SES-OWN-1/2: second Open is ErrOwned; Close then Open bumps Epoch; a
// superseded Writer's Append and Heartbeat fail without writing; TTL expiry
// allows takeover.
func testOwnership(t *testing.T, f Fixture) {
	ctx := context.Background()
	create(t, f.Store, "s")
	w1 := open(t, f.Store, "s", 0)
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
	w2 := open(t, f.Store, "s", 0)
	if w2.Epoch() != 2 {
		t.Fatalf("epoch after reopen = %d, want 2", w2.Epoch())
	}
	if _, err := w1.Append(ctx, session.Group{CommitID: "c2", Events: []session.UncommittedEvent{ev("twilight/x/a", `{}`)}}); !session.IsCode(err, session.ErrOwnershipLost) {
		t.Fatalf("old writer append = %v, want ownership_lost", err)
	}
	if err := w1.Heartbeat(ctx); !session.IsCode(err, session.ErrOwnershipLost) {
		t.Fatalf("old writer heartbeat = %v, want ownership_lost", err)
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
	if err := w2.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// Read never needs ownership (SES-OWN-4): already exercised above while owned.
	if f.Advance == nil {
		return
	}
	w3 := open(t, f.Store, "s", time.Minute)
	f.Advance(30 * time.Second)
	if err := w3.Heartbeat(ctx); err != nil {
		t.Fatalf("heartbeat inside TTL: %v", err)
	}
	f.Advance(45 * time.Second)
	if _, err := f.Store.Open(ctx, "s", session.OpenOptions{TTL: time.Minute}); !session.IsCode(err, session.ErrOwned) {
		t.Fatal("open succeeded while the heartbeat kept ownership alive")
	}
	f.Advance(time.Minute)
	w4 := open(t, f.Store, "s", time.Minute)
	if w4.Epoch() != w3.Epoch()+1 {
		t.Fatalf("takeover epoch = %d, want %d", w4.Epoch(), w3.Epoch()+1)
	}
	if _, err := w3.Append(ctx, session.Group{CommitID: "late", Events: []session.UncommittedEvent{ev("twilight/x/a", `{}`)}}); !session.IsCode(err, session.ErrOwnershipLost) {
		t.Fatalf("expired writer append = %v, want ownership_lost", err)
	}
	appendGroup(t, w4, "c3", ev("twilight/x/a", `{}`))
}

// SES-APP-1/3: whole-group visibility and the rejection list, none writing.
func testAppend(t *testing.T, f Fixture) {
	ctx := context.Background()
	create(t, f.Store, "s")
	w := open(t, f.Store, "s", 0)
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
// tamper detection.
func testRead(t *testing.T, f Fixture) {
	ctx := context.Background()
	create(t, f.Store, "s")
	w := open(t, f.Store, "s", 0)
	appendGroup(t, w, "c1", ev("twilight/run/a", `{}`), ev("twilight/chat/a", `{}`))       // 0,1
	appendGroup(t, w, "c2", ev("twilight/chat/b", `{}`))                                    // 2
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
		tamper.Tamper("s", 2, func(e *session.SessionEvent) { e.Payload = jsonstable.MustParse(`{"x":1}`) })
		if _, err := f.Store.Read(ctx, session.ReadRequest{SessionID: "s"}); !session.IsCode(err, session.ErrCorrupt) {
			t.Fatalf("tampered read = %v, want corrupt", err)
		}
	}
}

// SES-SCP-3: appendix A is out of v1.
func testScope(t *testing.T, f Fixture) {
	ctx := context.Background()
	if _, err := f.Store.Open(ctx, "missing", session.OpenOptions{}); !session.IsCode(err, session.ErrNotFound) {
		t.Fatalf("open unknown session = %v", err)
	}
	if _, err := f.Store.Header(ctx, "missing"); !session.IsCode(err, session.ErrNotFound) {
		t.Fatalf("header unknown session = %v", err)
	}
	h := session.SessionHeader{ProtocolVersion: session.ProtocolVersion1, SessionID: "f", ParentFork: &session.ForkPoint{ParentSessionID: "p"}}
	if err := session.ProfileV1().ValidateHeader(h); !session.IsCode(err, session.ErrUnsupported) {
		t.Fatalf("fork header = %v, want unsupported", err)
	}
}
