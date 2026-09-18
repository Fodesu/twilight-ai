package migrate_test

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
	"github.com/felinics/twilight/agent/session/migrate"
	"github.com/felinics/twilight/agent/session/writer"
)

type notePayload struct {
	Text string `json:"text"`
}

type noteState struct {
	Notes []string `json:"notes"`
}

var (
	noteType = extension.ModulePrefix(extension.SourceTwilight, "n") + "note"
	notesID  = extension.ProjectionID(string(noteType) + "s")
)

// noteModule has the note under Schemas 1 and 2 and an authoritative
// projection that refuses the text "reject".
func noteModule() extension.ModuleDescriptor {
	codec := extension.JSONCodec[notePayload]{}
	return extension.ModuleDescriptor{Source: extension.SourceTwilight, ID: "n",
		Streams: []extension.StreamDefinition{{Domain: "n", Lineage: session.LineageSession}},
		Events: []extension.EventDefinition{{Type: noteType, Stream: "n",
			Codecs: map[extension.SchemaVersion]extension.PayloadCodec{1: codec, 2: codec}}},
		Projections: []extension.ProjectionDefinition{{
			ID: notesID, Version: 1, Consumes: []session.EventType{noteType}, Authoritative: true,
			Initial: func() (any, error) { return noteState{}, nil },
			Apply: func(state any, e extension.DecodedEvent) (any, error) {
				s := state.(noteState)
				text := e.Value.(notePayload).Text
				if text == "reject" {
					return nil, errors.New("rejected by projection")
				}
				s.Notes = append(append([]string(nil), s.Notes...), text)
				return s, nil
			},
			StateCodec: extension.JSONStateCodec[noteState]{},
		}},
	}
}

type fixture struct {
	store    *session.MemoryStore
	registry *extension.Registry
	writers  writer.Writers
}

// newFixture creates each Session under schema, all in one store behind one
// Writers, the way an authority holds them.
func newFixture(t *testing.T, schema extension.SchemaVersion, sids ...session.SessionID) *fixture {
	t.Helper()
	registry, err := extension.BuildRegistry(session.ProtocolVersion1, noteModule())
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{store: session.NewMemoryStore(), registry: registry}
	for _, sid := range sids {
		if _, err := f.store.Create(context.Background(), session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: sid, Metadata: extension.SchemaMetadata(schema)}); err != nil {
			t.Fatal(err)
		}
	}
	f.writers = writer.NewWriters(f.store, registry, writer.Admission{}, session.OpenOptions{}, writer.WritersConfig{})
	t.Cleanup(func() { _ = writer.CloseWriters(context.Background(), f.writers) })
	return f
}

func (f *fixture) writer(t *testing.T, sid session.SessionID) writer.Writer {
	t.Helper()
	w, err := f.writers.Writer(context.Background(), sid)
	if err != nil {
		t.Fatalf("writer %s: %v", sid, err)
	}
	return w
}

// reopen drops the Session's Writer and opens a fresh one from the store.
func (f *fixture) reopen(t *testing.T, sid session.SessionID) writer.Writer {
	t.Helper()
	if err := writer.CloseWriter(context.Background(), f.writers, sid); err != nil {
		t.Fatal(err)
	}
	return f.writer(t, sid)
}

func (f *fixture) commit(t *testing.T, w writer.Writer, id string, texts ...string) session.Commit {
	t.Helper()
	res, err := w.Commit(context.Background(), func(writer.View) (*writer.SemanticGroup, error) {
		g := &writer.SemanticGroup{CommitID: session.CommitID(id)}
		var events []writer.TypedEvent
		for _, tx := range texts {
			events = append(events, writer.TypedEvent{Type: noteType, Value: notePayload{Text: tx}})
		}
		g.Batches = []writer.TypedBatch{{Stream: session.StreamRef{Domain: "n"}, Events: events}}
		return g, nil
	})
	if err != nil || res.Outcome != writer.CommitApplied {
		t.Fatalf("commit %s = %+v %v", id, res, err)
	}
	return res.Commit
}

func (f *fixture) notes(t *testing.T, w writer.Writer) []string {
	t.Helper()
	state, _, err := w.Projections().Load(context.Background(), w.SessionID(), notesID, 1)
	if err != nil {
		t.Fatal(err)
	}
	return state.(noteState).Notes
}

func (f *fixture) page(t *testing.T, sid session.SessionID) session.CommitPage {
	t.Helper()
	page, err := f.store.ReadCommits(context.Background(), session.CommitReadRequest{SessionID: sid})
	if err != nil {
		t.Fatal(err)
	}
	return page
}

// stubMigrator is a Migrator whose bootstrap is one note summarizing the
// source's notes projection: derived from the folded source state, so the
// same source state yields the same bootstrap.
type stubMigrator struct {
	profile        migrate.Profile
	source, target extension.SchemaVersion
	bootstrap      func(writer.View) ([]writer.SemanticGroup, error)
}

func (m stubMigrator) Profile() migrate.Profile        { return m.profile }
func (m stubMigrator) Source() extension.SchemaVersion { return m.source }
func (m stubMigrator) Target() extension.SchemaVersion { return m.target }
func (m stubMigrator) Bootstrap(_ context.Context, view writer.View) ([]writer.SemanticGroup, error) {
	if m.bootstrap != nil {
		return m.bootstrap(view)
	}
	state, err := view.Projection(notesID, 1)
	if err != nil {
		return nil, err
	}
	summary := notePayload{Text: strings.Join(state.(noteState).Notes, "+")}
	return []writer.SemanticGroup{{Batches: []writer.TypedBatch{{Stream: session.StreamRef{Domain: "n"},
		Events: []writer.TypedEvent{{Type: noteType, Value: summary}}}}}}, nil
}

const profile migrate.Profile = "twilight/test-migration/v1-to-v2@1"

func oneToTwo() stubMigrator { return stubMigrator{profile: profile, source: 1, target: 2} }

// TestMigrateAppliesOnceAndRecordsProvenance: the transition happens once,
// under the identity of its source head, and is answered as AlreadyApplied
// from then on -- by the same Writer and by one opened on the new tip.
func TestMigrateAppliesOnceAndRecordsProvenance(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 1, "s")
	w := f.writer(t, "s")
	c1 := f.commit(t, w, "c1", "one", "two")
	before := f.page(t, "s")
	want := migrate.IDOf("s", session.SegmentIDOf(before.Header), before.Head, 1, 2)

	res, err := migrate.Migrate(ctx, w, oneToTwo())
	if err != nil || res.Outcome != migrate.Applied || res.ID != want {
		t.Fatalf("migrate = %+v %v, want applied under %s", res, err, want)
	}
	if schema, err := extension.SchemaOf(res.Header); err != nil || schema != 2 {
		t.Fatalf("new segment schema = %d %v", schema, err)
	}
	if res.Header.CausationID != es.CausationID(want) {
		t.Fatalf("header causation = %s, want the migration id", res.Header.CausationID)
	}
	prov, ok, err := migrate.Record(res.Header)
	if err != nil || !ok {
		t.Fatalf("record = %+v %v %v", prov, ok, err)
	}
	wantProv := migrate.Provenance{ID: want, PreviousSegment: session.SegmentIDOf(before.Header), PreviousSeq: 0, PreviousDigest: c1.Digest,
		Source: 1, Target: 2, Profile: profile}
	if prov != wantProv {
		t.Fatalf("provenance = %+v, want %+v", prov, wantProv)
	}
	if len(res.Commits) != 1 || res.Commits[0].Seq != 1 || res.Commits[0].CommitID != migrate.BootstrapCommitID(want, 0) {
		t.Fatalf("bootstrap = %+v", res.Commits)
	}
	if w.Schema() != 2 || !reflect.DeepEqual(w.Header(), res.Header) {
		t.Fatalf("writer tip = schema %d %+v", w.Schema(), w.Header())
	}
	if got := f.notes(t, w); !reflect.DeepEqual(got, []string{"one", "two", "one+two"}) {
		t.Fatalf("notes = %v", got)
	}

	again, err := migrate.Migrate(ctx, w, oneToTwo())
	if err != nil || again.Outcome != migrate.AlreadyApplied || again.ID != want {
		t.Fatalf("rerun = %+v %v, want already applied under %s", again, err, want)
	}
	fresh := f.reopen(t, "s")
	if fresh.Schema() != 2 {
		t.Fatalf("reopened schema = %d", fresh.Schema())
	}
	again, err = migrate.Migrate(ctx, fresh, oneToTwo())
	if err != nil || again.Outcome != migrate.AlreadyApplied || again.ID != want {
		t.Fatalf("rerun on a fresh writer = %+v %v", again, err)
	}
	if got := f.notes(t, fresh); !reflect.DeepEqual(got, []string{"one", "two", "one+two"}) {
		t.Fatalf("notes after reopen = %v", got)
	}
}

// TestMigrateVerdicts: every refusal leaves the tip where it was; a tip
// already on the target is judged by its record.
func TestMigrateVerdicts(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("bootstrap failed")
	pending := errors.New("effect pending")
	reject := func(writer.View) ([]writer.SemanticGroup, error) {
		return []writer.SemanticGroup{{Batches: []writer.TypedBatch{{Stream: session.StreamRef{Domain: "n"},
			Events: []writer.TypedEvent{{Type: noteType, Value: notePayload{Text: "reject"}}}}}}}, nil
	}
	cases := map[string]struct {
		schema   extension.SchemaVersion // the Session's Schema
		setup    func(t *testing.T, w writer.Writer)
		migrator stubMigrator
		guards   []migrate.Guard
		want     func(t *testing.T, res migrate.Result, err error)
		after    extension.SchemaVersion // the tip's Schema afterwards
	}{
		"guard refuses": {schema: 1, migrator: oneToTwo(), guards: []migrate.Guard{func(writer.View) error { return pending }},
			want: func(t *testing.T, _ migrate.Result, err error) {
				if !errors.Is(err, migrate.ErrNotQuiescent) || !errors.Is(err, pending) {
					t.Fatalf("err = %v, want ErrNotQuiescent wrapping the guard's", err)
				}
			}, after: 1},
		"tip on another schema": {schema: 1, migrator: stubMigrator{profile: profile, source: 3, target: 2},
			want: func(t *testing.T, _ migrate.Result, err error) {
				if !session.IsCode(err, session.ErrInvalid) {
					t.Fatalf("err = %v, want ErrInvalid", err)
				}
			}, after: 1},
		"same schema": {schema: 1, migrator: stubMigrator{profile: profile, source: 1, target: 1},
			want: func(t *testing.T, _ migrate.Result, err error) {
				if err == nil {
					t.Fatal("a migrator between one schema and itself was accepted")
				}
			}, after: 1},
		"no profile": {schema: 1, migrator: stubMigrator{source: 1, target: 2},
			want: func(t *testing.T, _ migrate.Result, err error) {
				if err == nil {
					t.Fatal("a migrator without a profile was accepted")
				}
			}, after: 1},
		"target without codec": {schema: 1, migrator: stubMigrator{profile: profile, source: 1, target: 3},
			want: func(t *testing.T, _ migrate.Result, err error) {
				if !session.IsCode(err, session.ErrUnsupported) {
					t.Fatalf("err = %v, want ErrUnsupported", err)
				}
			}, after: 1},
		"bootstrap fails": {schema: 1, migrator: stubMigrator{profile: profile, source: 1, target: 2, bootstrap: func(writer.View) ([]writer.SemanticGroup, error) { return nil, boom }},
			want: func(t *testing.T, _ migrate.Result, err error) {
				if !errors.Is(err, boom) {
					t.Fatalf("err = %v, want the bootstrap's", err)
				}
			}, after: 1},
		"bootstrap refused by projection": {schema: 1, migrator: stubMigrator{profile: profile, source: 1, target: 2, bootstrap: reject},
			want: func(t *testing.T, _ migrate.Result, err error) {
				if !session.IsCode(err, session.ErrInvalid) {
					t.Fatalf("err = %v, want ErrInvalid", err)
				}
			}, after: 1},
		"another profile after migration": {schema: 1,
			setup: func(t *testing.T, w writer.Writer) {
				if res, err := migrate.Migrate(ctx, w, oneToTwo()); err != nil || res.Outcome != migrate.Applied {
					t.Fatalf("setup migrate = %+v %v", res, err)
				}
			},
			migrator: stubMigrator{profile: "twilight/test-migration/other@1", source: 1, target: 2},
			want: func(t *testing.T, _ migrate.Result, err error) {
				if !session.IsCode(err, session.ErrConflict) {
					t.Fatalf("err = %v, want ErrConflict", err)
				}
			}, after: 2},
		"tip on the target without a record": {schema: 2, migrator: oneToTwo(),
			want: func(t *testing.T, res migrate.Result, err error) {
				if err != nil || res.Outcome != migrate.AlreadyApplied || res.ID != "" {
					t.Fatalf("migrate = %+v %v, want already applied without an id", res, err)
				}
			}, after: 2},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, tc.schema, "s")
			w := f.writer(t, "s")
			f.commit(t, w, "c1", "one")
			if tc.setup != nil {
				tc.setup(t, w)
			}
			before := w.Header()
			res, err := migrate.Migrate(ctx, w, tc.migrator, tc.guards...)
			tc.want(t, res, err)
			if w.Schema() != tc.after || !reflect.DeepEqual(w.Header(), before) {
				t.Fatalf("tip after = schema %d header %+v, want schema %d and the same header", w.Schema(), w.Header(), tc.after)
			}
			if stored, err := f.store.Header(ctx, "s"); err != nil || !reflect.DeepEqual(stored, before) {
				t.Fatalf("store header = %+v %v, want unchanged", stored, err)
			}
			f.commit(t, w, "c2", "two")
		})
	}
}

// TestMigrateIsDeterministic: the same source state under the same profile
// yields the same bootstrap facts, whatever Session they belong to.
func TestMigrateIsDeterministic(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 1, "s1", "s2")
	var results []migrate.Result
	for _, sid := range []session.SessionID{"s1", "s2"} {
		w := f.writer(t, sid)
		f.commit(t, w, "c1", "one", "two")
		res, err := migrate.Migrate(ctx, w, oneToTwo())
		if err != nil || res.Outcome != migrate.Applied {
			t.Fatalf("%s: migrate = %+v %v", sid, res, err)
		}
		results = append(results, res)
	}
	if results[0].ID == results[1].ID {
		t.Fatal("two Sessions share one migration id")
	}
	if !reflect.DeepEqual(results[0].Commits[0].Batches, results[1].Commits[0].Batches) {
		t.Fatalf("bootstrap differs:\n%+v\n%+v", results[0].Commits[0].Batches, results[1].Commits[0].Batches)
	}
}

// TestIDOfNamesTheSourceHead: the identity changes with anything that names
// another source or target, and with nothing else.
func TestIDOfNamesTheSourceHead(t *testing.T) {
	base := migrate.IDOf("s", "seg", session.Head{Next: 3, Digest: "d2"}, 1, 2)
	if base == "" || base != migrate.IDOf("s", "seg", session.Head{Next: 3, Digest: "d2"}, 1, 2) {
		t.Fatalf("id is not a deterministic digest: %q", base)
	}
	others := map[string]es.Digest{
		"session": migrate.IDOf("t", "seg", session.Head{Next: 3, Digest: "d2"}, 1, 2),
		"segment": migrate.IDOf("s", "other", session.Head{Next: 3, Digest: "d2"}, 1, 2),
		"seq":     migrate.IDOf("s", "seg", session.Head{Next: 4, Digest: "d2"}, 1, 2),
		"digest":  migrate.IDOf("s", "seg", session.Head{Next: 3, Digest: "d3"}, 1, 2),
		"source":  migrate.IDOf("s", "seg", session.Head{Next: 3, Digest: "d2"}, 3, 2),
		"target":  migrate.IDOf("s", "seg", session.Head{Next: 3, Digest: "d2"}, 1, 3),
	}
	for name, id := range others {
		if id == base {
			t.Errorf("a different %s yields the same id", name)
		}
	}
}

// TestDeclareAndRecord pins the metadata record: one per segment, read back
// only when it names the segment's own parent.
func TestDeclareAndRecord(t *testing.T) {
	prov := migrate.Provenance{ID: "sha256:m", PreviousSegment: "seg", PreviousSeq: 4, PreviousDigest: "d4", Source: 1, Target: 2, Profile: profile}
	declared, err := migrate.Declare(extension.SchemaMetadata(2), prov)
	if err != nil {
		t.Fatal(err)
	}
	if schema, err := extension.SchemaOf(session.SegmentHeader{Metadata: declared}); err != nil || schema != 2 {
		t.Fatalf("declaring provenance disturbed the schema: %d %v", schema, err)
	}
	if _, err := migrate.Declare(declared, prov); err == nil {
		t.Error("a second record on one segment was accepted")
	}
	if _, err := migrate.Declare(jsonstable.MustParse(`[]`), prov); err == nil {
		t.Error("a non-object metadata was accepted")
	}
	parent := &session.LedgerRef{Segment: "seg", Seq: 4, Digest: "d4"}
	cases := map[string]struct {
		header session.SegmentHeader
		want   *migrate.Provenance
		err    bool
	}{
		"round trip":       {header: session.SegmentHeader{Parent: parent, Metadata: declared}, want: &prov},
		"no metadata":      {header: session.SegmentHeader{Parent: parent}},
		"schema only":      {header: session.SegmentHeader{Parent: parent, Metadata: extension.SchemaMetadata(2)}},
		"non-object":       {header: session.SegmentHeader{Parent: parent, Metadata: jsonstable.MustParse(`[]`)}},
		"root segment":     {header: session.SegmentHeader{Metadata: declared}, err: true},
		"other parent":     {header: session.SegmentHeader{Parent: &session.LedgerRef{Segment: "seg", Seq: 5, Digest: "d5"}, Metadata: declared}, err: true},
		"malformed record": {header: session.SegmentHeader{Parent: parent, Metadata: jsonstable.MustParse(`{"twilight/migration": {"id": 7}}`)}, err: true},
		"unknown field":    {header: session.SegmentHeader{Parent: parent, Metadata: jsonstable.MustParse(`{"twilight/migration": {"id": "x", "extra": 1}}`)}, err: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, ok, err := migrate.Record(tc.header)
			if tc.err {
				if err == nil {
					t.Fatalf("record = %+v %v, want an error", got, ok)
				}
				return
			}
			if err != nil || ok != (tc.want != nil) {
				t.Fatalf("record = %+v %v %v", got, ok, err)
			}
			if tc.want != nil && got != *tc.want {
				t.Fatalf("record = %+v, want %+v", got, *tc.want)
			}
		})
	}
}
