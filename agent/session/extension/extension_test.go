package extension

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
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

// tpfx is the first-party prefix of a test module.
func tpfx(id ModuleID) session.EventType { return ModulePrefix(SourceTwilight, id) }

func noteModule(id ModuleID, requires ...ModuleRequirement) ModuleDescriptor {
	typ := tpfx(id) + "note"
	return ModuleDescriptor{Source: SourceTwilight, ID: id, Requires: requires,
		Events: []EventDefinition{
			{Type: typ, Current: 1, Codecs: map[PayloadVersion]PayloadCodec{1: JSONCodec[notePayload]{}},
				Bindings: []BindingReferenceDefinition{{Extractor: refsExtractor, RequiredDurability: artifact.EventBound}}},
			{Type: tpfx(id) + "hint", Current: 1, Codecs: map[PayloadVersion]PayloadCodec{1: JSONCodec[notePayload]{}}, Ignorable: true},
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
		"unregistered dependency": {noteModule("a", ModuleRequirement{Source: SourceTwilight, Module: "zzz"})},
		"cycle":                   {noteModule("a", ModuleRequirement{Source: SourceTwilight, Module: "b"}), noteModule("b", ModuleRequirement{Source: SourceTwilight, Module: "a"})},
		"unhandled version": {noteModule("a"), noteModule("b", ModuleRequirement{Source: SourceTwilight, Module: "a",
			Events: map[session.EventType][]PayloadVersion{tpfx("a") + "note": {2}}})},
		"event outside module": {{Source: SourceTwilight, ID: "a", Events: []EventDefinition{{Type: "twilight/b/x", Current: 1, Codecs: map[PayloadVersion]PayloadCodec{1: JSONCodec[notePayload]{}}}}}},
		"projection outside scope": {noteModule("a"), {Source: SourceTwilight, ID: "b", Projections: []ProjectionDefinition{{ID: "p", Version: 1, Consumes: []session.EventType{tpfx("a") + "note"},
			Initial: func() (any, error) { return nil, nil }, Apply: func(s any, _ DecodedEvent) (any, error) { return s, nil }, StateCodec: JSONStateCodec[noteState]{}}}}},
	}
	for name, modules := range cases {
		if _, err := BuildRegistry(session.ProtocolVersion1, modules...); err == nil {
			t.Errorf("%s: registry built", name)
		}
	}
	if _, err := BuildRegistry(session.ProtocolVersion1, noteModule("a"), noteModule("b", ModuleRequirement{Source: SourceTwilight, Module: "a",
		Events: map[session.EventType][]PayloadVersion{tpfx("a") + "note": {1}}})); err != nil {
		t.Fatalf("valid registry: %v", err)
	}
}

// srcModule is a minimal module under an arbitrary source.
func srcModule(source SourceID, id ModuleID) ModuleDescriptor {
	return ModuleDescriptor{Source: source, ID: id, Events: []EventDefinition{{
		Type: ModulePrefix(source, id) + "note", Current: 1,
		Codecs: map[PayloadVersion]PayloadCodec{1: JSONCodec[notePayload]{}},
	}}}
}

// EXT-REG-1: module identity is (Source, ID); sources are validated segments.
func TestBuildRegistryValidatesSource(t *testing.T) {
	rejects := map[string][]ModuleDescriptor{
		"empty source":           {srcModule("", "a")},
		"source with slash":      {srcModule("x/y", "a")},
		"source not utf8":        {srcModule(SourceID([]byte{0xff, 0xfe}), "a")},
		"duplicate (source, id)": {srcModule("app", "a"), srcModule("app", "a")},
		"app id colliding first-party under twilight": {noteModule("a"), srcModule(SourceTwilight, "a")},
		"requirement without source":                  {noteModule("a"), {Source: "app", ID: "b", Requires: []ModuleRequirement{{Module: "a"}}}},
	}
	for name, modules := range rejects {
		if _, err := BuildRegistry(session.ProtocolVersion1, modules...); err == nil {
			t.Errorf("%s: registry built", name)
		}
	}
	// The same ID under two sources coexists and both prefixes resolve.
	r, err := BuildRegistry(session.ProtocolVersion1, noteModule("a"), srcModule("app", "a"))
	if err != nil {
		t.Fatalf("two sources, one id: %v", err)
	}
	if key, ok := r.ModuleOf("app/a/note"); !ok || key != (ModuleKey{Source: "app", ID: "a"}) {
		t.Fatalf("ModuleOf app/a/note = %+v %v", key, ok)
	}
	if key, ok := r.ModuleOf(tpfx("a") + "note"); !ok || key != TwilightModule("a") {
		t.Fatalf("ModuleOf twilight/a/note = %+v %v", key, ok)
	}
	if _, ok := r.ModuleOf("ghost/a/note"); ok {
		t.Fatal("unregistered source resolved")
	}
}

// Encode adds v; Decode selects the codec by v and keeps unknown versions raw.
func TestRegistryPayloadVersion(t *testing.T) {
	r, err := BuildRegistry(session.ProtocolVersion1, noteModule("a"))
	if err != nil {
		t.Fatal(err)
	}
	typ := tpfx("a") + "note"
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

// EXT-REG-1: advancing Current obliges the module to supply that version's
// codec. Encode selects def.Codecs[def.Current], so a missing entry must be
// refused when the registry is built rather than discovered on first write.
func TestBuildRegistryRequiresCodecForCurrent(t *testing.T) {
	for _, tc := range []struct {
		name    string
		current PayloadVersion
	}{
		{"current version without a codec", 2},
		{"zero current version", 0},
	} {
		_, err := BuildRegistry(session.ProtocolVersion1, ModuleDescriptor{Source: SourceTwilight, ID: "a",
			Events: []EventDefinition{{
				Type: tpfx("a") + "note", Current: tc.current,
				Codecs: map[PayloadVersion]PayloadCodec{1: JSONCodec[notePayload]{}},
			}}})
		if err == nil {
			t.Fatalf("%s: registry built", tc.name)
		}
		if !strings.Contains(err.Error(), "no codec for the current payload version") {
			t.Fatalf("%s: error = %v", tc.name, err)
		}
	}
	// Retaining the older codec alongside the current one is the supported
	// shape, so it must keep building.
	if _, err := BuildRegistry(session.ProtocolVersion1, ModuleDescriptor{Source: SourceTwilight, ID: "a",
		Events: []EventDefinition{{
			Type: tpfx("a") + "note", Current: 2,
			Codecs: map[PayloadVersion]PayloadCodec{1: legacyCodec{}, 2: JSONCodec[notePayload]{}},
		}}}); err != nil {
		t.Fatalf("coexisting versions: %v", err)
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
			g.Events = append(g.Events, TypedEvent{Type: tpfx("a") + "note", Value: notePayload{Text: tx}})
		}
		return g, nil
	}
}

func notes(t *testing.T, w Writer) []string {
	t.Helper()
	state, _, err := w.Projections().Load(context.Background(), "s", ProjectionID(string(tpfx("a"))+"notes"), 1)
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
		if s, err := v.Projection(ProjectionID(string(tpfx("a"))+"notes"), 1); err != nil || len(s.(noteState).Notes) != 2 {
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
	state, through, err := reader.Load(ctx, "s", ProjectionID(string(tpfx("a"))+"notes"), 1)
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
		return &SemanticGroup{CommitID: "c2", Events: []TypedEvent{{Type: tpfx("a") + "hint", Value: notePayload{Text: "h"}}}}, nil
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
// EXT-REF-1/2, EXT-WRT-3: a missing resolver or ledger is a configuration
// error, so it must surface as an error rather than as a CommitInvalid outcome
// that reads like a verdict on the group. It must not be rejected earlier
// either: an event type declaring Bindings only means its payloads may carry
// references, so a deployment that never attaches an artifact needs neither.
func TestCommitWithoutAdmission(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	// The resolver case below must reach the ledger check, so the referenced
	// binding has to exist.
	b, err := artifact.NewBinding("b1", artifact.Ref{Scheme: "spill", Authority: "local", Key: "k", Durability: artifact.EventBound})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.bindings.CreateBinding(ctx, b); err != nil {
		t.Fatal(err)
	}
	empty, err := OpenWriter(ctx, f.store, f.registry, Admission{}, "s", session.OpenOptions{})
	if err != nil {
		t.Fatalf("open without admission: %v", err)
	}
	defer empty.Close(ctx)

	// A payload with no references never consults admission, so a text-only
	// deployment must keep working.
	res, err := empty.Commit(ctx, noteGroup("c1", "no refs here"))
	if err != nil || res.Outcome != CommitApplied {
		t.Fatalf("commit without references = %+v %v", res, err)
	}

	// A payload that does carry a reference against a nil resolver is a
	// configuration error, not an invalid group.
	withRef, err := OpenWriter(ctx, f.store, f.registry, Admission{}, "s", session.OpenOptions{Takeover: true})
	if err != nil {
		t.Fatal(err)
	}
	defer withRef.Close(ctx)
	res, err = withRef.Commit(ctx, func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "c2", Events: []TypedEvent{
			{Type: tpfx("a") + "note", Value: notePayload{Text: "file", Refs: []string{"b1"}}},
		}}, nil
	})
	if err == nil {
		t.Fatalf("commit with a reference and no resolver = %+v, want an error (got no error)", res)
	}
	if !strings.Contains(err.Error(), "no binding resolver") {
		t.Fatalf("missing resolver error = %v", err)
	}

	// A resolver without a ledger fails the same way, at claim time.
	noLedger, err := OpenWriter(ctx, f.store, f.registry, Admission{Bindings: f.bindings}, "s", session.OpenOptions{Takeover: true})
	if err != nil {
		t.Fatal(err)
	}
	defer noLedger.Close(ctx)
	res, err = noLedger.Commit(ctx, func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "c3", Events: []TypedEvent{
			{Type: tpfx("a") + "note", Value: notePayload{Text: "file", Refs: []string{"b1"}}},
		}}, nil
	})
	if err == nil {
		t.Fatalf("commit with a reference and no ledger = %+v, want an error (got no error)", res)
	}
	if !strings.Contains(err.Error(), "no retention ledger") {
		t.Fatalf("missing ledger error = %v", err)
	}

	// None of the rejected commits may have written anything.
	page, err := f.store.Read(ctx, session.ReadRequest{SessionID: "s"})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 || page.Events[0].CommitID != "c1" {
		t.Fatalf("rejected commits wrote rows: %+v", page.Events)
	}
}

// legacyCodec decodes the v1 wire of the note event, which predates the
// optional refs field. It deliberately produces a distinguishable value so a
// test can prove the version, not the current codec, selected it.
type legacyCodec struct{}

func (legacyCodec) Encode(v any) (jsonstable.Value, error) {
	return jsonstable.FromValue(struct {
		Text string `json:"text"`
	}{v.(notePayload).Text})
}
func (legacyCodec) Decode(w jsonstable.Value) (any, error) {
	var body struct {
		Text string `json:"text"`
	}
	if err := w.Decode(&body); err != nil {
		return nil, err
	}
	return notePayload{Text: "v1:" + body.Text}, nil
}
func (legacyCodec) Validate(v any) error {
	if v.(notePayload).Text == "" {
		return errors.New("text is required")
	}
	return nil
}

// EXT-REG-1..4, EXT-COD-1/2: payload versions coexist. Advancing Current must
// not orphan stored rows: an old version still selects its own codec, decodes
// to a value and folds, while a version no codec claims stays Unknown with its
// raw payload.
func TestRegistryMultiVersionCodecsCoexist(t *testing.T) {
	typ := tpfx("v") + "note"
	upgraded := ModuleDescriptor{Source: SourceTwilight, ID: "v", Events: []EventDefinition{{
		Type: typ, Current: 2,
		Codecs: map[PayloadVersion]PayloadCodec{1: legacyCodec{}, 2: JSONCodec[notePayload]{}},
	}}}
	r, err := BuildRegistry(session.ProtocolVersion1, upgraded)
	if err != nil {
		t.Fatal(err)
	}

	// Writing uses Current.
	wire, v, err := r.Encode(typ, notePayload{Text: "hi"})
	if err != nil || v != 2 {
		t.Fatalf("encode = %s v%d %v, want v2", wire, v, err)
	}
	if wire.String() != `{"text":"hi","v":2}` {
		t.Fatalf("current wire = %s", wire)
	}

	// A row written before the upgrade still decodes, through its own codec.
	old, err := r.Decode(session.SessionEvent{Type: typ, Payload: jsonstable.MustParse(`{"text":"old","v":1}`)})
	if err != nil {
		t.Fatalf("decode v1: %v", err)
	}
	if old.Unknown {
		t.Fatal("a retained older version decoded as Unknown")
	}
	if old.Version != 1 || old.Value.(notePayload).Text != "v1:old" {
		t.Fatalf("v1 row = version %d value %+v: the v1 codec did not run", old.Version, old.Value)
	}
	current, err := r.Decode(session.SessionEvent{Type: typ, Payload: wire})
	if err != nil || current.Unknown || current.Value.(notePayload).Text != "hi" {
		t.Fatalf("v2 row = %+v %v", current, err)
	}

	// A version no codec claims is preserved raw rather than reinterpreted.
	future, err := r.Decode(session.SessionEvent{Type: typ, Payload: jsonstable.MustParse(`{"text":"x","v":3}`)})
	if err != nil || !future.Unknown || future.Version != 3 {
		t.Fatalf("v3 row = %+v %v, want Unknown v3", future, err)
	}
	if future.Event.Payload.String() != `{"text":"x","v":3}` {
		t.Fatalf("unknown-version payload was not preserved: %s", future.Event.Payload)
	}
}

// EXT-WRT-1: the Writer is the single serialization point. Concurrent callers
// must each see the state left by the previous one, so every commit applies
// exactly once and the stream stays contiguous.
func TestWriterSerializesConcurrentCommits(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	w := f.open(t, false)
	defer w.Close(ctx)

	const n = 8
	texts := make([]string, n)
	results := make([]CommitResult, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		texts[i] = fmt.Sprintf("n%d", i)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = w.Commit(ctx, noteGroup(fmt.Sprintf("c%d", i), texts[i]))
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		if errs[i] != nil || results[i].Outcome != CommitApplied {
			t.Fatalf("commit %d = %+v %v", i, results[i], errs[i])
		}
	}
	// Every commit was serialized onto its own contiguous Seq range.
	seen := map[session.Seq]bool{}
	for _, res := range results {
		for _, e := range res.Events {
			if seen[e.Seq] {
				t.Fatalf("seq %d assigned twice: commits raced", e.Seq)
			}
			seen[e.Seq] = true
		}
	}
	if len(seen) != n {
		t.Fatalf("distinct seqs = %d, want %d", len(seen), n)
	}
	for i := 0; i < n; i++ {
		if !seen[session.Seq(i)] {
			t.Fatalf("seq %d missing; head is not contiguous", i)
		}
	}
	if got := notes(t, w); len(got) != n {
		t.Fatalf("folded notes = %v, want all %d: a commit did not observe its predecessor", got, n)
	}
}

// EXT-REF-1/2: the extractor returns every reference in appearance order,
// cardinality and scheme/durability admission bound what may commit, and a
// rejected group writes nothing.
func TestBindingAdmission(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	for _, b := range []struct {
		id  artifact.BindingID
		ref artifact.Ref
	}{
		{"ok1", artifact.Ref{Scheme: "spill", Authority: "local", Key: "k1", Durability: artifact.EventBound}},
		{"ok2", artifact.Ref{Scheme: "spill", Authority: "local", Key: "k2", Durability: artifact.Pinned}},
		{"other", artifact.Ref{Scheme: "other", Authority: "local", Key: "k3", Durability: artifact.EventBound}},
		{"weak", artifact.Ref{Scheme: "spill", Authority: "local", Key: "k4", Durability: artifact.Ephemeral}},
	} {
		binding, err := artifact.NewBinding(b.id, b.ref)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.bindings.CreateBinding(ctx, binding); err != nil {
			t.Fatal(err)
		}
	}

	maxTwo := uint32(2)
	typ := tpfx("r") + "ref"
	reg, err := BuildRegistry(session.ProtocolVersion1, ModuleDescriptor{Source: SourceTwilight, ID: "r",
		Events: []EventDefinition{{
			Type: typ, Current: 1, Codecs: map[PayloadVersion]PayloadCodec{1: JSONCodec[notePayload]{}},
			Bindings: []BindingReferenceDefinition{{
				Extractor: refsExtractor, Cardinality: Cardinality{Min: 1, Max: &maxTwo},
				AllowedSchemes:     []artifact.Scheme{"spill"},
				RequiredDurability: artifact.EventBound,
			}},
		}}})
	if err != nil {
		t.Fatal(err)
	}
	w, err := OpenWriter(ctx, f.store, reg, Admission{Bindings: f.bindings, Ledger: f.ledger}, "s", session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close(ctx)

	commit := func(id string, refs ...string) CommitResult {
		t.Helper()
		res, err := w.Commit(ctx, func(View) (*SemanticGroup, error) {
			return &SemanticGroup{CommitID: session.CommitID(id), Events: []TypedEvent{
				{Type: typ, Value: notePayload{Text: "r", Refs: refs}},
			}}, nil
		})
		if err != nil {
			t.Fatalf("commit %s: %v", id, err)
		}
		return res
	}

	// Admissible: the claim covers every extracted reference, in order.
	res := commit("c1", "ok1", "ok2")
	if res.Outcome != CommitApplied || res.Claim == nil {
		t.Fatalf("admissible group = %+v", res)
	}
	claim, ok, err := f.ledger.LookupClaim(ctx, res.Claim.ID)
	if err != nil || !ok {
		t.Fatalf("claim lookup = %v %v", ok, err)
	}
	if got := claim.BindingSet.BindingIDs; len(got) != 2 || got[0] != "ok1" || got[1] != "ok2" {
		t.Fatalf("claim set = %v, want every extracted reference in appearance order", got)
	}

	for i, tc := range []struct {
		name string
		refs []string
		want string
	}{
		{"below cardinality", nil, "cardinality"},
		{"above cardinality", []string{"ok1", "ok2", "ok1"}, "cardinality"},
		{"scheme not allowed", []string{"other"}, "scheme other not allowed"},
		{"durability below required", []string{"weak"}, "below required"},
	} {
		got := commit(fmt.Sprintf("r%d", i), tc.refs...)
		if got.Outcome != CommitInvalid {
			t.Fatalf("%s = %+v, want invalid", tc.name, got)
		}
		// Assert the reason so a group rejected for some other cause cannot
		// make this pass.
		if !strings.Contains(got.Detail, tc.want) {
			t.Fatalf("%s detail = %q, want mention of %q", tc.name, got.Detail, tc.want)
		}
	}

	// Only the admissible group may have landed.
	page, err := f.store.Read(ctx, session.ReadRequest{SessionID: "s"})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 || page.Events[0].CommitID != "c1" {
		t.Fatalf("rejected groups wrote rows: %+v", page.Events)
	}
}

func TestWriterClaimsAndReconcile(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	b, _ := artifact.NewBinding("b1", artifact.Ref{Scheme: "spill", Authority: "local", Key: "k", Durability: artifact.EventBound})
	if _, err := f.bindings.CreateBinding(ctx, b); err != nil {
		t.Fatal(err)
	}
	w := f.open(t, false)
	res, err := w.Commit(ctx, func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "c1", Events: []TypedEvent{{Type: tpfx("a") + "note", Value: notePayload{Text: "file", Refs: []string{"b1"}}}}}, nil
	})
	if err != nil || res.Outcome != CommitApplied || res.Claim == nil || res.Claim.State != artifact.ClaimActive {
		t.Fatalf("commit with binding = %+v %v", res, err)
	}
	missing, _ := w.Commit(ctx, func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "c2", Events: []TypedEvent{{Type: tpfx("a") + "note", Value: notePayload{Text: "x", Refs: []string{"nope"}}}}}, nil
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
	id := ProjectionID(string(tpfx("a")) + "notes")
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
