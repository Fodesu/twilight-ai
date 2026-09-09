package extension

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agent/artifact"
	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/session"
)

type notePayload struct {
	Text string   `json:"text"`
	Refs []string `json:"refs,omitempty"`
}

type noteState struct {
	Notes []string `json:"notes"`
}

var refsExtractor = BindingExtractorFunc(func(value any) ([]artifact.BindingID, error) {
	var out []artifact.BindingID
	for _, r := range value.(notePayload).Refs {
		out = append(out, artifact.BindingID(r))
	}
	return out, nil
})

func noteModule(id ModuleID, requires ...ModuleRequirement) ModuleDescriptor {
	typ := ModulePrefix(id) + "note"
	return ModuleDescriptor{ID: id, Requires: requires,
		Events: []EventDefinition{
			{Type: typ, Current: 1, Codecs: map[PayloadVersion]PayloadCodec{1: JSONCodec[notePayload]{}},
				Bindings: []BindingReferenceDefinition{{Extractor: refsExtractor, RequiredDurability: artifact.EventBound}}},
			{Type: ModulePrefix(id) + "hint", Current: 1, Codecs: map[PayloadVersion]PayloadCodec{1: JSONCodec[notePayload]{}}, Ignorable: true},
		},
		Projections: []ProjectionDefinition{{
			ID: ProjectionID(string(typ) + "s"), Version: 1, Consumes: []session.EventType{typ},
			Initial: func() (any, error) { return noteState{}, nil },
			Apply: func(state any, e DecodedEvent) (any, error) {
				s := state.(noteState)
				text := e.Value.(notePayload).Text
				if text == "reject" {
					return nil, errors.New("rejected by projection")
				}
				s.Notes = append(append([]string(nil), s.Notes...), text)
				return s, nil
			},
			StateCodec: JSONStateCodec[noteState]{},
		}},
	}
}

func TestBuildRegistryValidatesRequires(t *testing.T) {
	cases := map[string][]ModuleDescriptor{
		"unregistered dependency": {noteModule("a", ModuleRequirement{Module: "zzz"})},
		"cycle":                   {noteModule("a", ModuleRequirement{Module: "b"}), noteModule("b", ModuleRequirement{Module: "a"})},
		"unhandled version": {noteModule("a"), noteModule("b", ModuleRequirement{Module: "a",
			Events: map[session.EventType][]PayloadVersion{ModulePrefix("a") + "note": {2}}})},
		"event outside module": {{ID: "a", Events: []EventDefinition{{Type: "twilight/b/x", Current: 1, Codecs: map[PayloadVersion]PayloadCodec{1: JSONCodec[notePayload]{}}}}}},
		"projection outside scope": {noteModule("a"), {ID: "b", Projections: []ProjectionDefinition{{ID: "p", Version: 1, Consumes: []session.EventType{ModulePrefix("a") + "note"},
			Initial: func() (any, error) { return nil, nil }, Apply: func(s any, _ DecodedEvent) (any, error) { return s, nil }, StateCodec: JSONStateCodec[noteState]{}}}}},
	}
	for name, modules := range cases {
		if _, err := BuildRegistry(session.ProtocolVersion1, modules...); err == nil {
			t.Errorf("%s: registry built", name)
		}
	}
	if _, err := BuildRegistry(session.ProtocolVersion1, noteModule("a"), noteModule("b", ModuleRequirement{Module: "a",
		Events: map[session.EventType][]PayloadVersion{ModulePrefix("a") + "note": {1}}})); err != nil {
		t.Fatalf("valid registry: %v", err)
	}
}

// Encode adds v; Decode selects the codec by v and keeps unknown versions raw.
func TestRegistryPayloadVersion(t *testing.T) {
	r, err := BuildRegistry(session.ProtocolVersion1, noteModule("a"))
	if err != nil {
		t.Fatal(err)
	}
	typ := ModulePrefix("a") + "note"
	wire, v, err := r.Encode(typ, notePayload{Text: "hi"})
	if err != nil || v != 1 || wire.String() != `{"text":"hi","v":1}` {
		t.Fatalf("encode = %s v%d %v", wire, v, err)
	}
	decoded, err := r.Decode(session.SessionEvent{Type: typ, Payload: wire})
	if err != nil || decoded.Unknown || decoded.Value.(notePayload).Text != "hi" {
		t.Fatalf("decode = %+v %v", decoded, err)
	}
	future, err := r.Decode(session.SessionEvent{Type: typ, Payload: jsonstable.MustParse(`{"text":"hi","v":2}`)})
	if err != nil || !future.Unknown || future.Version != 2 {
		t.Fatalf("future version = %+v %v", future, err)
	}
	if _, _, err := r.Encode("twilight/a/other", notePayload{}); err == nil {
		t.Fatal("unknown type encoded")
	}
}

type fixture struct {
	store    *session.MemoryStore
	registry *Registry
	bindings *artifact.MemoryBindingStore
	ledger   *artifact.MemoryLedger
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{}
	f.store = session.NewMemoryStore()
	r, err := BuildRegistry(session.ProtocolVersion1, noteModule("a"))
	if err != nil {
		t.Fatal(err)
	}
	f.registry = r
	f.bindings = artifact.NewMemoryBindingStore()
	f.ledger = artifact.NewMemoryLedger(artifact.SetBuilder{Resolver: f.bindings})
	if _, err := f.store.Create(context.Background(), session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: "s"}); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *fixture) admission() Admission { return Admission{Bindings: f.bindings, Ledger: f.ledger} }

func (f *fixture) open(t *testing.T, takeover bool) Writer {
	t.Helper()
	w, err := OpenWriter(context.Background(), f.store, f.registry, f.admission(), "s", session.OpenOptions{Takeover: takeover})
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	return w
}

func noteGroup(id string, texts ...string) CommitFn {
	return func(View) (*SemanticGroup, error) {
		g := &SemanticGroup{CommitID: session.CommitID(id)}
		for _, tx := range texts {
			g.Events = append(g.Events, TypedEvent{Type: ModulePrefix("a") + "note", Value: notePayload{Text: tx}})
		}
		return g, nil
	}
}

func notes(t *testing.T, w Writer) []string {
	t.Helper()
	state, _, err := w.Projections().Load(context.Background(), "s", ProjectionID(string(ModulePrefix("a"))+"notes"), 1)
	if err != nil {
		t.Fatal(err)
	}
	return state.(noteState).Notes
}

// EXT-WRT-1/2: serial commits, in-memory idempotency, rebuild on reopen,
// projections visible through View and Projections().
func TestWriterCommitReplayAndRebuild(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	w := f.open(t, false)
	res, err := w.Commit(ctx, noteGroup("c1", "one", "two"))
	if err != nil || res.Outcome != CommitApplied || len(res.Events) != 2 || res.Events[0].Seq != 0 {
		t.Fatalf("commit = %+v %v", res, err)
	}
	if got := notes(t, w); len(got) != 2 || got[1] != "two" {
		t.Fatalf("projection after commit = %v", got)
	}
	replay, _ := w.Commit(ctx, noteGroup("c1", "one", "two"))
	if replay.Outcome != CommitAlreadyApplied || len(replay.Events) != 2 || replay.Events[1].Digest != res.Events[1].Digest {
		t.Fatalf("replay = %+v", replay)
	}
	conflict, _ := w.Commit(ctx, noteGroup("c1", "changed"))
	if conflict.Outcome != CommitConflict {
		t.Fatalf("conflict = %+v", conflict)
	}
	noop, _ := w.Commit(ctx, func(View) (*SemanticGroup, error) { return nil, nil })
	if noop.Outcome != CommitNoop {
		t.Fatalf("noop = %+v", noop)
	}
	invalid, _ := w.Commit(ctx, func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "c2", Events: []TypedEvent{{Type: "twilight/a/unknown", Value: notePayload{}}}}, nil
	})
	if invalid.Outcome != CommitInvalid {
		t.Fatalf("invalid = %+v", invalid)
	}
	rejected, _ := w.Commit(ctx, noteGroup("c3", "fine", "reject"))
	if rejected.Outcome != CommitInvalid {
		t.Fatalf("projection rejection must block the append: %+v", rejected)
	}
	if page, _ := f.store.Read(ctx, session.ReadRequest{SessionID: "s"}); len(page.Events) != 2 {
		t.Fatalf("rejected groups wrote rows: %d", len(page.Events))
	}
	// The View sees head, index and projection; fn may use them.
	_, err = w.Commit(ctx, func(v View) (*SemanticGroup, error) {
		if v.Head().Next != 2 || v.Epoch() != 1 {
			t.Fatalf("view head/epoch = %+v %d", v.Head(), v.Epoch())
		}
		if rows, ok := v.LookupCommit("c1"); !ok || len(rows) != 2 {
			t.Fatal("view lookup failed")
		}
		if s, err := v.Projection(ProjectionID(string(ModulePrefix("a"))+"notes"), 1); err != nil || len(s.(noteState).Notes) != 2 {
			t.Fatalf("view projection = %+v %v", s, err)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWriter(ctx, f.store, f.registry, f.admission(), "s", session.OpenOptions{}); !session.IsCode(err, session.ErrOwned) {
		t.Fatalf("second writer = %v, want owned", err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Commit(ctx, noteGroup("c4", "x")); err == nil {
		t.Fatal("closed writer accepted a commit")
	}
	w2 := f.open(t, false)
	if w2.Epoch() != 2 {
		t.Fatalf("epoch = %d", w2.Epoch())
	}
	if got := notes(t, w2); len(got) != 2 {
		t.Fatalf("rebuilt projection = %v", got)
	}
	if again, _ := w2.Commit(ctx, noteGroup("c1", "one", "two")); again.Outcome != CommitAlreadyApplied {
		t.Fatalf("index not rebuilt: %+v", again)
	}
	reader := NewProjectionReader(f.store, f.registry, nil)
	state, through, err := reader.Load(ctx, "s", ProjectionID(string(ModulePrefix("a"))+"notes"), 1)
	if err != nil || len(state.(noteState).Notes) != 2 || through.Next != 2 {
		t.Fatalf("store reader = %+v %+v %v", state, through, err)
	}
}

// EXT-WRT-4: a superseded writer fails closed.
func TestWriterOwnershipLost(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	w1 := f.open(t, false)
	if _, err := w1.Commit(ctx, noteGroup("c1", "one")); err != nil {
		t.Fatal(err)
	}
	w2 := f.open(t, true)
	if _, err := w1.Commit(ctx, noteGroup("c2", "late")); !errors.Is(err, &Error{Code: ErrOwnershipLost}) {
		t.Fatalf("stale writer commit = %v, want ownership_lost", err)
	}
	if _, err := w1.Commit(ctx, noteGroup("c3", "again")); !errors.Is(err, &Error{Code: ErrOwnershipLost}) {
		t.Fatal("writer did not stay failed")
	}
	if got := notes(t, w2); len(got) != 1 {
		t.Fatalf("fenced write leaked: %v", got)
	}
	ws := NewWriters(f.store, f.registry, f.admission(), session.OpenOptions{})
	if _, err := ws.Writer(ctx, "s"); !session.IsCode(err, session.ErrOwned) {
		t.Fatalf("writers while owned = %v", err)
	}
	_ = w2.Close(ctx)
	a, err := ws.Writer(ctx, "s")
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := ws.Writer(ctx, "s"); a != b {
		t.Fatal("Writers handed out two writers for one session")
	}
}

// EXT-PRJ-2: unknown events in scope fail the fold unless Ignorable; unknown
// events of other modules are skipped.
func TestProjectionUnknownEvents(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	w := f.open(t, false)
	if _, err := w.Commit(ctx, noteGroup("c1", "one")); err != nil {
		t.Fatal(err)
	}
	hint, _ := w.Commit(ctx, func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "c2", Events: []TypedEvent{{Type: ModulePrefix("a") + "hint", Value: notePayload{Text: "h"}}}}, nil
	})
	if hint.Outcome != CommitApplied || !hint.Events[0].Ignorable {
		t.Fatalf("ignorable definition not applied to the row: %+v", hint)
	}
	_ = w.Close(ctx)
	kw, _ := f.store.Open(ctx, "s", session.OpenOptions{})
	raw := func(id, typ string, ignorable bool) {
		if _, err := kw.Append(ctx, session.Group{CommitID: session.CommitID(id), Events: []session.UncommittedEvent{{Type: session.EventType(typ), Payload: jsonstable.MustParse(`{"v":1}`), Ignorable: ignorable}}}); err != nil {
			t.Fatal(err)
		}
	}
	raw("other", "twilight/zzz/thing", false) // out of scope: skipped
	raw("future", "twilight/a/future", true)  // in scope, ignorable: skipped
	_ = kw.Close(ctx)
	w = f.open(t, false)
	if got := notes(t, w); len(got) != 1 {
		t.Fatalf("notes = %v", got)
	}
	_ = w.Close(ctx)
	kw, _ = f.store.Open(ctx, "s", session.OpenOptions{})
	raw("strict", "twilight/a/strict", false) // in scope, not ignorable: fold fails
	_ = kw.Close(ctx)
	if _, err := OpenWriter(ctx, f.store, f.registry, f.admission(), "s", session.OpenOptions{}); !errors.Is(err, &Error{Code: ErrUnknownEvent}) {
		t.Fatalf("open with unknown strict event = %v", err)
	}
}

// EXT-WRT-3 and ART-RET-3: claims are Active before the rows exist; an
// orphan claim is released on the next OpenWriter; a live claim survives.
func TestWriterClaimsAndReconcile(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	b, _ := artifact.NewBinding("b1", artifact.Ref{Scheme: "spill", Authority: "local", Key: "k", Durability: artifact.EventBound})
	if _, err := f.bindings.CreateBinding(ctx, b); err != nil {
		t.Fatal(err)
	}
	w := f.open(t, false)
	res, err := w.Commit(ctx, func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "c1", Events: []TypedEvent{{Type: ModulePrefix("a") + "note", Value: notePayload{Text: "file", Refs: []string{"b1"}}}}}, nil
	})
	if err != nil || res.Outcome != CommitApplied || res.Claim == nil || res.Claim.State != artifact.ClaimActive {
		t.Fatalf("commit with binding = %+v %v", res, err)
	}
	missing, _ := w.Commit(ctx, func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "c2", Events: []TypedEvent{{Type: ModulePrefix("a") + "note", Value: notePayload{Text: "x", Refs: []string{"nope"}}}}}, nil
	})
	if missing.Outcome != CommitInvalid {
		t.Fatalf("unknown binding = %+v", missing)
	}
	// Simulate a crash between claim and append: an Active claim whose owner
	// commit never made it into the stream.
	set, _ := artifact.SetBuilder{Resolver: f.bindings}.Build(ctx, []artifact.BindingID{"b1"})
	orphanID := DeriveClaimID(session.ProtocolVersion1, "s", "never", set.RefSetDigest)
	if _, err := f.ledger.Activate(ctx, orphanID, CommitOwner("s", "never"), set); err != nil {
		t.Fatal(err)
	}
	_ = w.Close(ctx)
	w = f.open(t, false)
	defer w.Close(ctx)
	if c, ok, _ := f.ledger.LookupClaim(ctx, orphanID); !ok || c.State != artifact.ClaimReleased {
		t.Fatalf("orphan claim = %+v", c)
	}
	if c, ok, _ := f.ledger.LookupClaim(ctx, res.Claim.ID); !ok || c.State != artifact.ClaimActive {
		t.Fatalf("live claim = %+v", c)
	}
}

// EXT-PRJ-3/4: cache entry plus tail equals the full fold; a stale or missing
// entry falls back to a full fold; Writer and Store readers agree.
func TestProjectionCache(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	w := f.open(t, false)
	defer w.Close(ctx)
	id := ProjectionID(string(ModulePrefix("a")) + "notes")
	_, _ = w.Commit(ctx, noteGroup("c1", "one"))
	cache := NewMemoryProjectionCache()
	state, through, _ := w.Projections().Load(ctx, "s", id, 1)
	if err := SaveProjection(ctx, cache, f.registry, "s", id, 1, state, through); err != nil {
		t.Fatal(err)
	}
	_, _ = w.Commit(ctx, noteGroup("c2", "two"))
	reader := NewProjectionReader(f.store, f.registry, cache)
	got, head, err := reader.Load(ctx, "s", id, 1)
	if err != nil || len(got.(noteState).Notes) != 2 || head.Next != 2 {
		t.Fatalf("cache+tail = %+v %+v %v", got, head, err)
	}
	// A cache entry claiming a head the stream does not have is ignored.
	_ = cache.Save(ctx, "s", id, 1, jsonstable.MustParse(`{"notes":["bogus"]}`), session.Head{Next: 1, Digest: "sha256:wrong"})
	got, _, err = reader.Load(ctx, "s", id, 1)
	if err != nil || got.(noteState).Notes[0] != "one" {
		t.Fatalf("stale cache used: %+v %v", got, err)
	}
	cache.Delete("s", id, 1)
	got, _, _ = reader.Load(ctx, "s", id, 1)
	mem, _, _ := w.Projections().Load(ctx, "s", id, 1)
	if len(got.(noteState).Notes) != len(mem.(noteState).Notes) {
		t.Fatal("store reader and writer reader disagree")
	}
}
