package writer

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
)

// This file covers Writer.Advance (EXT-WRT-10, SES-ADV-1): the segment
// transition that moves a Session's tip to another Schema. The note module
// here has codecs under two Schemas, and its Schema 1 codec marks what it
// decodes, so a projection state shows which Schema each event was read
// under.

type markPayload struct {
	Run  string `json:"run"`
	Text string `json:"text"`
}

// v1NoteCodec is the note event's codec under Schema 1. It decodes to a
// marked text: an inherited event keeps decoding under the Schema it was
// written under, whatever the tip declares.
type v1NoteCodec struct{}

func (v1NoteCodec) Encode(v any) (jsonstable.Value, error) {
	p, ok := v.(notePayload)
	if !ok {
		return jsonstable.Value{}, errors.New("not a note")
	}
	return jsonstable.FromValue(map[string]string{"text": p.Text})
}

func (v1NoteCodec) Decode(wire jsonstable.Value) (any, error) {
	var body struct {
		Text string `json:"text"`
	}
	if err := wire.Decode(&body); err != nil {
		return nil, err
	}
	return notePayload{Text: "v1:" + body.Text}, nil
}

func (v1NoteCodec) Validate(v any) error {
	if p, ok := v.(notePayload); !ok || p.Text == "" {
		return errors.New("text is required")
	}
	return nil
}

const (
	marksAllID      = extension.ProjectionID("twilight/a/marks-all")
	marksSemanticID = extension.ProjectionID("twilight/a/marks-semantic")
)

// twoSchemaModule is noteModule("a") with the note under Schemas 1 and 2, the
// hint under Schema 1 only, and a mark under a keyed, segment-lineage stream
// domain folded by two projections that differ only in their inheritance
// policy.
func twoSchemaModule() extension.ModuleDescriptor {
	m := noteModule("a")
	m.Events[0].Codecs = map[extension.SchemaVersion]extension.PayloadCodec{1: v1NoteCodec{}, 2: extension.JSONCodec[notePayload]{}}
	markType := tpfx("a") + "mark"
	m.Streams = append(m.Streams, extension.StreamDefinition{Domain: "mark", IDField: "run", Lineage: session.LineageSegment})
	m.Events = append(m.Events, extension.EventDefinition{Type: markType, Stream: "mark",
		Codecs: map[extension.SchemaVersion]extension.PayloadCodec{1: extension.JSONCodec[markPayload]{}, 2: extension.JSONCodec[markPayload]{}}})
	marks := func(id extension.ProjectionID, inherits extension.InheritPolicy) extension.ProjectionDefinition {
		return extension.ProjectionDefinition{
			ID: id, Version: 1, Consumes: []session.EventType{markType}, Inherits: inherits,
			Initial: func() (any, error) { return noteState{}, nil },
			Apply: func(state any, e extension.DecodedEvent) (any, error) {
				s := state.(noteState)
				s.Notes = append(append([]string(nil), s.Notes...), e.Value.(markPayload).Text)
				return s, nil
			},
			StateCodec: extension.JSONStateCodec[noteState]{},
		}
	}
	m.Projections = append(m.Projections, marks(marksAllID, extension.InheritAll), marks(marksSemanticID, nil))
	return m
}

func newAdvanceFixture(t *testing.T) *fixture {
	t.Helper()
	f := newFixture(t)
	r, err := extension.BuildRegistry(session.ProtocolVersion1, twoSchemaModule())
	if err != nil {
		t.Fatal(err)
	}
	f.registry = r
	return f
}

func (f *fixture) openWith(t *testing.T, cfg WritersConfig) Writer {
	t.Helper()
	w, err := openWriter(context.Background(), f.store, f.registry, f.admission(), "s", session.OpenOptions{}, cfg)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	return w
}

// bootstrap is one bootstrap group of note events.
func bootstrap(id string, texts ...string) SemanticGroup {
	g := SemanticGroup{CommitID: session.CommitID(id)}
	var events []TypedEvent
	for _, tx := range texts {
		events = append(events, TypedEvent{Type: tpfx("a") + "note", Value: notePayload{Text: tx}})
	}
	g.Batches = noteBatch(events...)
	return g
}

func advanceTo(target extension.SchemaVersion, groups ...SemanticGroup) AdvanceFn {
	return func(View) (*AdvanceRequest, error) {
		return &AdvanceRequest{Target: target, CausationID: "test", Bootstrap: groups}, nil
	}
}

func markGroup(id, runID string, texts ...string) CommitFn {
	return func(View) (*SemanticGroup, error) {
		g := &SemanticGroup{CommitID: session.CommitID(id)}
		var events []TypedEvent
		for _, tx := range texts {
			events = append(events, TypedEvent{Type: tpfx("a") + "mark", Value: markPayload{Run: runID, Text: tx}})
		}
		g.Batches = []TypedBatch{{Stream: session.StreamRef{Domain: "mark", ID: runID}, Events: events}}
		return g, nil
	}
}

// withIntent seals an operation digest into the group fn decides.
func withIntent(fn CommitFn, intent es.Digest) CommitFn {
	return func(v View) (*SemanticGroup, error) {
		g, err := fn(v)
		if g != nil {
			g.Intent = intent
		}
		return g, err
	}
}

func projection(t *testing.T, w Writer, id extension.ProjectionID) []string {
	t.Helper()
	state, _, err := w.Projections().Load(context.Background(), "s", id, 1)
	if err != nil {
		t.Fatalf("load %s: %v", id, err)
	}
	return state.(noteState).Notes
}

func mustCommit(t *testing.T, w Writer, fn CommitFn) session.Commit {
	t.Helper()
	res, err := w.Commit(context.Background(), fn)
	if err != nil || res.Outcome != CommitApplied {
		t.Fatalf("commit = %+v %v", res, err)
	}
	return res.Commit
}

type advanceObserver struct{ commits []session.Commit }

func (o *advanceObserver) Committed(_ context.Context, _ session.SessionID, c session.Commit) {
	o.commits = append(o.commits, c)
}

// TestWriterAdvancePublishesTheTargetSegment: the applied transition is the
// new tip everywhere at once -- the Writer, the Store and a reopened Writer
// -- and every commit after it is encoded under the target Schema while the
// inherited ones keep decoding under theirs.
func TestWriterAdvancePublishesTheTargetSegment(t *testing.T) {
	ctx := context.Background()
	f := newAdvanceFixture(t)
	obs := &advanceObserver{}
	w := f.openWith(t, WritersConfig{Observers: []CommitObserver{obs}})
	c1 := mustCommit(t, w, withIntent(noteGroup("c1", "one", "two"), "op-1"))
	before := w.Header()

	res, err := w.Advance(ctx, advanceTo(2, bootstrap("b0", "boot"), bootstrap("b1", "more")))
	if err != nil || res.Outcome != AdvanceApplied {
		t.Fatalf("advance = %+v %v", res, err)
	}
	if schema, err := extension.SchemaOf(res.Header); err != nil || schema != 2 {
		t.Fatalf("new segment declares schema %d %v, want 2", schema, err)
	}
	wantParent := &session.LedgerRef{Segment: session.SegmentIDOf(before), Seq: 0, Digest: c1.Digest}
	if !reflect.DeepEqual(res.Header.Parent, wantParent) || res.Header.CausationID != "test" {
		t.Fatalf("header = %+v, want parent %+v", res.Header, wantParent)
	}
	if len(res.Commits) != 2 || res.Commits[0].Seq != 1 || res.Commits[0].CommitID != "b0" || res.Commits[1].CommitID != "b1" {
		t.Fatalf("sealed bootstrap = %+v", res.Commits)
	}
	if w.Schema() != 2 || !reflect.DeepEqual(w.Header(), res.Header) {
		t.Fatalf("writer tip = schema %d header %+v", w.Schema(), w.Header())
	}
	if stored, err := f.store.Header(ctx, "s"); err != nil || !reflect.DeepEqual(stored, res.Header) {
		t.Fatalf("store header = %+v %v", stored, err)
	}
	// Observers saw the bootstrap commits, in order, and nothing else new.
	if len(obs.commits) != 3 || obs.commits[1].CommitID != "b0" || obs.commits[2].CommitID != "b1" {
		t.Fatalf("observed = %+v", obs.commits)
	}
	// The inherited notes were decoded under Schema 1, the bootstrap under 2.
	if got := notes(t, w); !sameNotes(got, []string{"v1:one", "v1:two", "boot", "more"}) {
		t.Fatalf("notes after advance = %v", got)
	}
	// A commit on the new tip is encoded under the target Schema.
	c2 := mustCommit(t, w, noteGroup("c2", "three"))
	decoded, err := f.registry.Decode(c2.Batches[0].Events[0])
	if err != nil || decoded.Version != 2 || decoded.Value.(notePayload).Text != "three" {
		t.Fatalf("decoded = %+v %v, want schema 2 payload", decoded, err)
	}
	if got := notes(t, w); !sameNotes(got, []string{"v1:one", "v1:two", "boot", "more", "three"}) {
		t.Fatalf("notes after a commit on the new tip = %v", got)
	}
	// A replay of an inherited CommitID is judged by intent (EXT-WRT-2); the
	// fingerprint of a retry is computed under the tip's Schema, so it can
	// only match a commit written under the same Schema.
	if replay, _ := w.Commit(ctx, withIntent(noteGroup("c1", "one", "two"), "op-1")); replay.Outcome != CommitAlreadyApplied {
		t.Fatalf("intent replay across the boundary = %+v", replay)
	}
	if replay, _ := w.Commit(ctx, noteGroup("c1", "one", "two")); replay.Outcome != CommitConflict {
		t.Fatalf("fingerprint replay across the boundary = %+v", replay)
	}
	// A reopened Writer starts on the new tip and folds the same state.
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	reopened := f.open(t, false)
	if reopened.Schema() != 2 {
		t.Fatalf("reopened schema = %d", reopened.Schema())
	}
	if got := notes(t, reopened); !sameNotes(got, []string{"v1:one", "v1:two", "boot", "more", "three"}) {
		t.Fatalf("notes after reopen = %v", got)
	}
}

// TestWriterAdvanceInheritsByPolicy: at the boundary a projection is refolded
// under its own inheritance policy (EXT-PRJ-8): the declared-lineage default
// keeps the session-lineage domain only, InheritAll keeps every stream.
func TestWriterAdvanceInheritsByPolicy(t *testing.T) {
	ctx := context.Background()
	f := newAdvanceFixture(t)
	w := f.open(t, false)
	mustCommit(t, w, noteGroup("c1", "one"))
	mustCommit(t, w, markGroup("m1", "r1", "alpha", "beta"))
	if got := projection(t, w, marksSemanticID); !sameNotes(got, []string{"alpha", "beta"}) {
		t.Fatalf("marks before advance = %v", got)
	}
	if res, err := w.Advance(ctx, advanceTo(2)); err != nil || res.Outcome != AdvanceApplied {
		t.Fatalf("advance = %+v %v", res, err)
	}
	mustCommit(t, w, markGroup("m2", "r2", "gamma"))
	want := map[extension.ProjectionID][]string{marksAllID: {"alpha", "beta", "gamma"}, marksSemanticID: {"gamma"}}
	for id, notes := range want {
		if got := projection(t, w, id); !sameNotes(got, notes) {
			t.Errorf("%s = %v, want %v", id, got, notes)
		}
	}
	if got := notes(t, w); !sameNotes(got, []string{"v1:one"}) {
		t.Errorf("notes = %v, want the inherited note stream", got)
	}
	// The mark stream the tip did not write is not the tip's: a fresh Writer
	// folds the same, and the old stream's head is still known to the kernel.
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	reopened := f.open(t, false)
	for id, notes := range want {
		if got := projection(t, reopened, id); !sameNotes(got, notes) {
			t.Errorf("%s after reopen = %v, want %v", id, got, notes)
		}
	}
}

// TestWriterAdvanceRefusals: every refusal leaves the tip as it was -- same
// segment, same Schema, still writable under it.
func TestWriterAdvanceRefusals(t *testing.T) {
	ctx := context.Background()
	other, err := extension.DeclareSchema(jsonstable.Value{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]struct {
		fn      AdvanceFn
		outcome AdvanceOutcome
		detail  string
		code    session.ErrorCode
	}{
		"nil request":  {fn: func(View) (*AdvanceRequest, error) { return nil, nil }, outcome: AdvanceNoop},
		"schema zero":  {fn: advanceTo(0), outcome: AdvanceInvalid, detail: "schema 0"},
		"no codec":     {fn: advanceTo(3), code: session.ErrUnsupported},
		"empty id":     {fn: advanceTo(2, bootstrap("", "x")), outcome: AdvanceInvalid, detail: "empty CommitID"},
		"duplicate id": {fn: advanceTo(2, bootstrap("b", "x"), bootstrap("b", "y")), outcome: AdvanceInvalid, detail: "already in the ledger"},
		"committed id": {fn: advanceTo(2, bootstrap("c1", "x")), outcome: AdvanceInvalid, detail: "already in the ledger"},
		"refused":      {fn: advanceTo(2, bootstrap("b", "reject")), outcome: AdvanceInvalid, detail: "rejected by projection"},
		"other schema": {fn: func(View) (*AdvanceRequest, error) { return &AdvanceRequest{Target: 2, Metadata: other}, nil }, outcome: AdvanceInvalid, detail: "declares schema 1"},
		"absent target": {fn: func(View) (*AdvanceRequest, error) {
			return &AdvanceRequest{Target: 2, Bootstrap: []SemanticGroup{{CommitID: "b", Batches: noteBatch(TypedEvent{Type: tpfx("a") + "hint", Value: notePayload{Text: "x"}})}}}, nil
		}, outcome: AdvanceInvalid, detail: "hint"},
		"fn error": {fn: func(View) (*AdvanceRequest, error) { return nil, errors.New("decide failed") }},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := newAdvanceFixture(t)
			w := f.open(t, false)
			mustCommit(t, w, noteGroup("c1", "one"))
			before := w.Header()
			res, err := w.Advance(ctx, tc.fn)
			switch {
			case tc.code != "":
				if !session.IsCode(err, tc.code) {
					t.Fatalf("advance = %+v %v, want %s", res, err, tc.code)
				}
			case tc.outcome == "":
				if err == nil {
					t.Fatalf("advance = %+v, want an error", res)
				}
			default:
				if err != nil || res.Outcome != tc.outcome {
					t.Fatalf("advance = %+v %v, want %s", res, err, tc.outcome)
				}
				if tc.detail != "" && !strings.Contains(res.Detail, tc.detail) {
					t.Fatalf("detail %q does not mention %q", res.Detail, tc.detail)
				}
			}
			if w.Schema() != 1 || !reflect.DeepEqual(w.Header(), before) {
				t.Fatalf("tip changed: schema %d header %+v", w.Schema(), w.Header())
			}
			if stored, err := f.store.Header(ctx, "s"); err != nil || !reflect.DeepEqual(stored, before) {
				t.Fatalf("store header = %+v %v, want unchanged", stored, err)
			}
			mustCommit(t, w, noteGroup("c2", "two"))
			if got := notes(t, w); !sameNotes(got, []string{"v1:one", "v1:two"}) {
				t.Fatalf("notes = %v", got)
			}
		})
	}
}

// TestWriterAdvanceNeedsACommit: a transition anchors on the tip's head
// commit, so an empty tip has nothing to advance from.
func TestWriterAdvanceNeedsACommit(t *testing.T) {
	f := newAdvanceFixture(t)
	w := f.open(t, false)
	res, err := w.Advance(context.Background(), advanceTo(2))
	if err != nil || res.Outcome != AdvanceInvalid || !strings.Contains(res.Detail, "no commit") {
		t.Fatalf("advance on an empty tip = %+v %v", res, err)
	}
}

// TestWriterAdvanceOwnershipLost: the transition runs under the same fence
// as Append. A Writer that lost the Session fails closed, whether it learns
// so from the log head or from the kernel.
func TestWriterAdvanceOwnershipLost(t *testing.T) {
	ctx := context.Background()
	cases := map[string]func(t *testing.T, f *fixture, taker Writer){
		"kernel refuses": func(*testing.T, *fixture, Writer) {},
		"head moved":     func(t *testing.T, _ *fixture, taker Writer) { mustCommit(t, taker, noteGroup("c2", "two")) },
	}
	for name, prepare := range cases {
		t.Run(name, func(t *testing.T) {
			f := newAdvanceFixture(t)
			w := f.open(t, false)
			mustCommit(t, w, noteGroup("c1", "one"))
			taker := f.open(t, true)
			prepare(t, f, taker)
			_, err := w.Advance(ctx, advanceTo(2))
			if !errors.Is(err, &extension.Error{Code: extension.ErrOwnershipLost}) {
				t.Fatalf("advance after takeover = %v, want ownership lost", err)
			}
			if _, err := w.Commit(ctx, noteGroup("c3", "three")); !errors.Is(err, &extension.Error{Code: extension.ErrOwnershipLost}) {
				t.Fatalf("commit after a lost advance = %v, want ownership lost", err)
			}
			if taker.Schema() != 1 {
				t.Fatalf("the owner's tip moved to schema %d", taker.Schema())
			}
		})
	}
}

// TestWriterAdvanceCacheBoundary: a cache entry at the boundary the new
// segment inherits is never started from (EXT-PRJ-3), by the Writer and by
// the store reader alike, and a tip without a commit of its own writes none.
func TestWriterAdvanceCacheBoundary(t *testing.T) {
	ctx := context.Background()
	f := newCacheFixture(t)
	m := cacheModule(f.counter)
	m.Events[0].Codecs[2] = extension.JSONCodec[notePayload]{}
	registry, err := extension.BuildRegistry(session.ProtocolVersion1, m)
	if err != nil {
		t.Fatal(err)
	}
	f.registry = registry
	cfg := WritersConfig{Cache: f.cache}

	w := f.open(t, cfg)
	f.commit(t, w, "c1", "n1")
	f.commit(t, w, "c2", "n2")
	f.commit(t, w, "c3", "n3")
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	w = f.open(t, cfg)
	if res, err := w.Advance(ctx, advanceTo(2)); err != nil || res.Outcome != AdvanceApplied {
		t.Fatalf("advance = %+v %v", res, err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// Close wrote nothing: the entry is still the old tip's, at the boundary.
	_, through, ok, err := f.cache.Load(ctx, "s", alphaID, 1)
	if err != nil || !ok || through.Next != 3 {
		t.Fatalf("entry after the transition: ok=%v through=%d err=%v, want the old entry at 3", ok, through.Next, err)
	}

	// Neither reader starts from it: both fold the inherited log in full.
	f.counter.reset()
	reader := extension.NewProjectionReader(f.store, f.registry, f.cache)
	state, _, err := reader.Load(ctx, "s", alphaID, 1)
	if err != nil || !sameNotes(state.(noteState).Notes, []string{"n1", "n2", "n3"}) {
		t.Fatalf("reader load = %+v %v", state, err)
	}
	if n := f.counter.get(alphaID); n != 3 {
		t.Errorf("reader folded %d events, want 3 (the entry sits at an inherited boundary)", n)
	}
	f.counter.reset()
	w = f.open(t, cfg)
	if n := f.counter.get(alphaID); n != 3 {
		t.Errorf("writer folded %d events, want 3 (the entry sits at an inherited boundary)", n)
	}
	// The tip's first own commit makes the boundary its own; Close refreshes.
	f.commit(t, w, "c4", "n4")
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, through, ok, err := f.cache.Load(ctx, "s", alphaID, 1); err != nil || !ok || through.Next != 4 {
		t.Fatalf("entry after the tip's own commit: ok=%v through=%d err=%v, want 4", ok, through.Next, err)
	}
	f.counter.reset()
	w = f.open(t, cfg)
	if n := f.counter.get(alphaID); n != 0 {
		t.Errorf("writer folded %d events, want 0 from the tip's own entry", n)
	}
	if got := f.notes(t, w, alphaID); !sameNotes(got, []string{"n1", "n2", "n3", "n4"}) {
		t.Errorf("alpha = %v", got)
	}
}
