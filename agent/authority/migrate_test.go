package authority_test

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agent/authority"
	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/extension"
	"github.com/felinics/twilight/agent/session/migrate"
	"github.com/felinics/twilight/agent/session/writer"
	"github.com/felinics/twilight/agent/turn"
)

// The application module of these tests has codecs under Schemas 1 and 2, so
// the registry supports Schema 2 while the first-party modules stay on 1.
type markPayload struct {
	Text string `json:"text"`
}

type marksState struct {
	Marks []string `json:"marks"`
}

var (
	markType = extension.ModulePrefix("example", "mig") + "mark"
	marksID  = extension.ProjectionID("example/mig/marks")
)

var migModule = extension.ModuleDescriptor{Source: "example", ID: "mig",
	Streams: []extension.StreamDefinition{{Domain: "mig", Lineage: session.LineageSession}},
	Events: []extension.EventDefinition{{Type: markType, Stream: "mig",
		Codecs: map[extension.SchemaVersion]extension.PayloadCodec{1: extension.JSONCodec[markPayload]{}, 2: extension.JSONCodec[markPayload]{}}}},
	Projections: []extension.ProjectionDefinition{{
		ID: marksID, Version: 1, Consumes: []session.EventType{markType}, Inherits: extension.InheritAll,
		Initial: func() (any, error) { return marksState{}, nil },
		Apply: func(state any, e extension.DecodedEvent) (any, error) {
			s := state.(marksState)
			s.Marks = append(append([]string(nil), s.Marks...), e.Value.(markPayload).Text)
			return s, nil
		},
		StateCodec: extension.JSONStateCodec[marksState]{},
	}},
}

// stubMigrator moves a Session from source to target with one mark as its
// bootstrap.
type stubMigrator struct {
	profile        migrate.Profile
	source, target extension.SchemaVersion
}

func (m stubMigrator) Profile() migrate.Profile        { return m.profile }
func (m stubMigrator) Source() extension.SchemaVersion { return m.source }
func (m stubMigrator) Target() extension.SchemaVersion { return m.target }
func (m stubMigrator) Bootstrap(context.Context, writer.View) ([]writer.SemanticGroup, error) {
	return []writer.SemanticGroup{{Batches: []writer.TypedBatch{{Stream: session.StreamRef{Domain: "mig"},
		Events: []writer.TypedEvent{{Type: markType, Value: markPayload{Text: "migrated"}}}}}}}, nil
}

var oneToTwo = stubMigrator{profile: "example/mig/v1-to-v2@1", source: 1, target: 2}

func newMigratingAuthority(t *testing.T, migrators ...migrate.Migrator) *authority.Authority {
	t.Helper()
	p := basePorts(t)
	p.Modules = []extension.ModuleDescriptor{migModule}
	p.Migrators = migrators
	return newAuthorityFrom(t, &p)
}

// submitOnly creates the Session and commits one chatlog input without
// starting a Turn, then closes it.
func submitOnly(t *testing.T, a *authority.Authority, sid session.SessionID) {
	t.Helper()
	ctx := context.Background()
	if err := a.CreateSession(ctx, sid, jsonstable.Value{}); err != nil {
		t.Fatal(err)
	}
	h, err := a.Open(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Chatlog.Submit(ctx, h.Writer(), "in-1", "hello"); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// AUTH-MIG-1: MigrateSession is the one explicit path to a new Schema. It
// moves the tip once, is idempotent from then on, and the migrated Session
// reopens under the target with its history intact.
func TestMigrateSessionMovesTheTip(t *testing.T) {
	ctx := context.Background()
	a := newMigratingAuthority(t, oneToTwo)
	const sid session.SessionID = "s-mig"
	submitOnly(t, a, sid)

	res, err := a.MigrateSession(ctx, sid, 2)
	if err != nil || res.Outcome != migrate.Applied || res.ID == "" || len(res.Commits) != 1 {
		t.Fatalf("migrate = %+v %v", res, err)
	}
	header, err := a.Store.Header(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	if schema, err := extension.SchemaOf(header); err != nil || schema != 2 || header.HeaderDigest != res.Header.HeaderDigest {
		t.Fatalf("tip = schema %d %v header %+v, want the published segment under 2", schema, err, header)
	}
	prov, ok, err := migrate.Record(header)
	if err != nil || !ok || prov.ID != res.ID || prov.Profile != oneToTwo.profile {
		t.Fatalf("record = %+v %v %v", prov, ok, err)
	}
	again, err := a.MigrateSession(ctx, sid, 2)
	if err != nil || again.Outcome != migrate.AlreadyApplied || again.ID != res.ID {
		t.Fatalf("rerun = %+v %v", again, err)
	}
	// The migration held the Session like an Open and released it: the
	// Session opens again, on the new tip, with the inherited chatlog.
	h, err := a.Open(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	if h.Writer().Schema() != 2 {
		t.Fatalf("reopened schema = %d", h.Writer().Schema())
	}
	if chat, err := chatlog.ReadSurface(ctx, a.Projections, sid); err != nil || chat.Inputs.Len() != 1 {
		t.Fatalf("chatlog after migration = %d %v", chat.Inputs.Len(), err)
	}
	state, _, err := a.Projection(ctx, sid, marksID, 1)
	if err != nil || len(state.(marksState).Marks) != 1 || state.(marksState).Marks[0] != "migrated" {
		t.Fatalf("marks after migration = %+v %v", state, err)
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// AUTH-MIG-1: the refusals. None of them moves the tip, and each releases the
// Session again.
func TestMigrateSessionRefusals(t *testing.T) {
	ctx := context.Background()
	cases := map[string]struct {
		migrators []migrate.Migrator
		target    extension.SchemaVersion
		prepare   func(t *testing.T, a *authority.Authority, sid session.SessionID) (cleanup func())
		want      func(t *testing.T, err error)
	}{
		"while open": {migrators: []migrate.Migrator{oneToTwo}, target: 2,
			prepare: func(t *testing.T, a *authority.Authority, sid session.SessionID) func() {
				h, err := a.Open(ctx, sid)
				if err != nil {
					t.Fatal(err)
				}
				return func() { _ = h.Close(ctx) }
			},
			want: func(t *testing.T, err error) {
				if !errors.Is(err, authority.ErrSessionOpen) {
					t.Fatalf("err = %v, want ErrSessionOpen", err)
				}
			}},
		"schema zero": {migrators: []migrate.Migrator{oneToTwo}, target: 0,
			want: func(t *testing.T, err error) {
				if !session.IsCode(err, session.ErrUnsupported) {
					t.Fatalf("err = %v, want ErrUnsupported", err)
				}
			}},
		"no codec under target": {migrators: []migrate.Migrator{oneToTwo}, target: 3,
			want: func(t *testing.T, err error) {
				if !session.IsCode(err, session.ErrUnsupported) {
					t.Fatalf("err = %v, want ErrUnsupported", err)
				}
			}},
		"no migrator": {target: 2,
			want: func(t *testing.T, err error) {
				if !session.IsCode(err, session.ErrUnsupported) {
					t.Fatalf("err = %v, want ErrUnsupported", err)
				}
			}},
		"active turn": {migrators: []migrate.Migrator{oneToTwo}, target: 2,
			prepare: func(t *testing.T, a *authority.Authority, sid session.SessionID) func() {
				h, err := a.Open(ctx, sid)
				if err != nil {
					t.Fatal(err)
				}
				in, err := a.Chatlog.Submit(ctx, h.Writer(), "in-2", "again")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := a.Turns.Start(ctx, h.Writer(), turn.StartRequest{Ref: turn.TurnRef{SessionID: sid, TurnID: "t1"},
					Inputs: []run.AgentInput{in}, Preset: turn.PresetRef{ID: "p-1", Digest: "sha256:p-1"}}); err != nil {
					t.Fatal(err)
				}
				if err := h.Close(ctx); err != nil {
					t.Fatal(err)
				}
				return nil
			},
			want: func(t *testing.T, err error) {
				if !errors.Is(err, migrate.ErrNotQuiescent) {
					t.Fatalf("err = %v, want ErrNotQuiescent", err)
				}
			}},
		"owned by another process": {migrators: []migrate.Migrator{oneToTwo}, target: 2,
			prepare: func(t *testing.T, a *authority.Authority, sid session.SessionID) func() {
				raw, err := a.Store.Open(ctx, sid, session.OpenOptions{})
				if err != nil {
					t.Fatal(err)
				}
				return func() { _ = raw.Close(ctx) }
			},
			want: func(t *testing.T, err error) {
				if !session.IsCode(err, session.ErrOwned) {
					t.Fatalf("err = %v, want ErrOwned", err)
				}
			}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			a := newMigratingAuthority(t, tc.migrators...)
			const sid session.SessionID = "s-refused"
			submitOnly(t, a, sid)
			before, err := a.Store.Header(ctx, sid)
			if err != nil {
				t.Fatal(err)
			}
			if tc.prepare != nil {
				if cleanup := tc.prepare(t, a, sid); cleanup != nil {
					defer cleanup()
				}
			}
			_, err = a.MigrateSession(ctx, sid, tc.target)
			tc.want(t, err)
			if after, err := a.Store.Header(ctx, sid); err != nil || after.HeaderDigest != before.HeaderDigest {
				t.Fatalf("tip moved: %+v %v", after, err)
			}
		})
	}
	// A refused migration releases the Session: it opens afterwards.
	a := newMigratingAuthority(t)
	const sid session.SessionID = "s-released"
	submitOnly(t, a, sid)
	if _, err := a.MigrateSession(ctx, sid, 2); err == nil {
		t.Fatal("migration without a migrator succeeded")
	}
	h, err := a.Open(ctx, sid)
	if err != nil {
		t.Fatalf("open after a refused migration: %v", err)
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// AUTH-SCH-1: a new Session's root declares Ports.Schema, which the registry
// must have codecs under; creation metadata declaring another Schema is
// refused.
func TestNewSchemaSelection(t *testing.T) {
	ctx := context.Background()
	cases := map[string]struct {
		schema extension.SchemaVersion
		want   extension.SchemaVersion // zero: New fails
	}{
		"default":  {schema: 0, want: extension.SchemaVersion1},
		"declared": {schema: 2, want: 2},
		"no codec": {schema: 3},
	}
	for name, tc := range cases {
		p := basePorts(t)
		p.Modules = []extension.ModuleDescriptor{migModule}
		p.Schema = tc.schema
		a, err := authority.New(p)
		if tc.want == 0 {
			if err == nil {
				_ = a.Close(ctx)
				t.Errorf("%s: New accepted schema %d", name, tc.schema)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: New: %v", name, err)
		}
		if err := a.CreateSession(ctx, "s", jsonstable.Value{}); err != nil {
			t.Fatalf("%s: create: %v", name, err)
		}
		header, err := a.Store.Header(ctx, "s")
		if err != nil {
			t.Fatal(err)
		}
		if got, err := extension.SchemaOf(header); err != nil || got != tc.want {
			t.Errorf("%s: root declares schema %d %v, want %d", name, got, err, tc.want)
		}
		if err := a.CreateSession(ctx, "other", extension.SchemaMetadata(tc.want+1)); !session.IsCode(err, session.ErrInvalid) {
			t.Errorf("%s: create under another declared schema = %v, want ErrInvalid", name, err)
		}
		_ = a.Close(ctx)
	}
}

// AUTH-MIG-1: New refuses a migrator set it could not select from or apply.
func TestNewRefusesUnusableMigrators(t *testing.T) {
	cases := map[string][]migrate.Migrator{
		"nil":               {nil},
		"zero source":       {stubMigrator{profile: "p", source: 0, target: 2}},
		"same schema":       {stubMigrator{profile: "p", source: 1, target: 1}},
		"no profile":        {stubMigrator{source: 1, target: 2}},
		"target has codec?": {stubMigrator{profile: "p", source: 1, target: 3}},
		"duplicate pair":    {oneToTwo, stubMigrator{profile: "other", source: 1, target: 2}},
	}
	for name, migrators := range cases {
		p := basePorts(t)
		p.Modules = []extension.ModuleDescriptor{migModule}
		p.Migrators = migrators
		if a, err := authority.New(p); err == nil {
			_ = a.Close(context.Background())
			t.Errorf("%s: New accepted %+v", name, migrators)
		}
	}
}
