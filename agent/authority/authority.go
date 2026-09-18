// Package authority composes the agent core -- the fact layer (Store,
// Writers, SessionRunStore, Coordinator), the decision layer (preset registry and
// prompt builder catalog) and the effect layer (an Executor port) -- into
// one authority process (AUTH). Its exported fields are the core services a
// caller drives a Session with; Open hands out the ownership capability
// those services act under. It is deployment-neutral and carries no product
// policy: what to send, when to drain a backlog, whether to drive in the
// background and when to compact are the application's decisions.
package authority

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/felinics/twilight/agent/artifact"
	"github.com/felinics/twilight/agent/decision"
	"github.com/felinics/twilight/agent/driver"
	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/preset"
	"github.com/felinics/twilight/agent/run/effect"
	"github.com/felinics/twilight/agent/run/frozen"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/extension"
	"github.com/felinics/twilight/agent/session/migrate"
	runmod "github.com/felinics/twilight/agent/session/run"
	"github.com/felinics/twilight/agent/session/writer"
	"github.com/felinics/twilight/agent/turn"
)

// Artifacts groups the artifact ports. Durability is one bundle
// (AUTH-PRT-3): a durable Session Store needs a durable Content Store for
// the frozen bodies its facts name, a durable binding index and a durable
// retention ledger, or the facts outlive the bodies and the claims after a
// restart. Every port defaults to memory, but only an all-memory deployment
// may take those defaults; New refuses a mix unless Ephemeral says it is
// intended.
type Artifacts struct {
	Bindings artifact.BindingStore
	Ledger   artifact.RetentionLedger
	// Ephemeral accepts memory-only content, bindings or ledger next to a
	// durable Session Store: frozen bodies, the binding index and the
	// retention claims are lost with the process while the facts naming them
	// survive. Meant for tests and local runs, not production.
	Ephemeral bool
}

// Durability is the capability a store declares about surviving the
// process. New consults it on the Session Store, the Content Store, the
// BindingStore and the RetentionLedger instead of guessing from concrete
// types: a store that does not declare is taken as durable, so a memory
// implementation that stays silent cannot slip into a durable bundle, and
// an explicitly passed memory store says so itself.
type Durability interface {
	Durable() bool
}

// durable reports a port's declared durability; nil is not a store.
func durable(port any) bool {
	if port == nil {
		return false
	}
	if d, ok := port.(Durability); ok {
		return d.Durable()
	}
	return true
}

// ErrEphemeralArtifacts reports a durable Store mixed with a memory-only
// Content Store, binding store or retention ledger without the Ephemeral
// opt-in.
var ErrEphemeralArtifacts = errors.New("authority: durable session store with memory-only content store, binding store or retention ledger; provide the durable bundle or set Artifacts.Ephemeral")

// Ports are the roles an Authority is composed from (AUTH-PRT-1). Every field
// is an interface or a core value; nil fields take the defaults documented
// on each, all of them in-process.
type Ports struct {
	// Store is the Session kernel; nil selects an in-memory store.
	Store session.Store
	// Content is the cas ContentStore the frozen bodies live in under
	// runmod.FrozenAuthority (RUN-WIR-4). The run store writes them; the
	// materializer reads them for prompts, replies and transcripts. Nil
	// selects an in-memory store.
	Content artifact.ContentStore
	// Artifacts are the binding store and retention ledger.
	Artifacts Artifacts
	// Presets is the registry of decision identities; nil selects an
	// in-memory registry.
	Presets preset.Registry
	// Decisions resolve each preset's PromptBuilderRef (DEC-CAT); nil selects
	// decision.DefaultPromptBuilders().
	Decisions *decision.PromptBuilders
	// Executor is the effect layer port (RUN-EXE-3): required.
	Executor effect.ExecutionPort
	// TargetResolver supplies opaque per-Run resource targets.
	TargetResolver loop.TargetResolver
	// Observers are notified of every group the Writers apply (EXT-WRT-7).
	Observers []writer.CommitObserver
	// Modules are application modules registered after the first-party three
	// (EXT-APP).
	Modules []extension.ModuleDescriptor
	// Schema is the SchemaVersion new Sessions are created under
	// (AUTH-SCH-1); zero selects extension.SchemaVersion1. Existing Sessions
	// keep the Schema their tip segment declares until migrated.
	Schema extension.SchemaVersion
	// Migrators are the Schema migrations this authority can apply
	// (AUTH-MIG-1), at most one per (source, target) pair: MigrateSession
	// selects by the tip's Schema and the requested target. Each target
	// must be a Schema the registry has codecs under.
	Migrators []migrate.Migrator
	// Guards are the deployment's own quiescence preconditions for a
	// migration -- effects it tracks outside the Session -- evaluated after
	// the turn and run guards; nil adds none.
	Guards []migrate.Guard
	// Clock stamps event times; nil selects time.Now.
	Clock func() time.Time
	// Cache stores folded projection states; nil asks the Store for a durable
	// cache and falls back to an in-memory one (APP-MEM-2).
	Cache extension.ProjectionCache
	// CacheEvery bounds how far a cached projection may fall behind the head;
	// zero takes extension.DefaultCacheEvery (EXT-PRJ-7).
	CacheEvery session.CommitSeq
	// Ownership configures how Writers open Sessions.
	Ownership session.OpenOptions
	// Fail receives failures of work the authority does outside any caller's
	// call, such as settling a reattached Outcome; nil discards them.
	Fail func(session.SessionID, error)
}

// Authority is the composed core (AUTH-PRT-2). Exported fields are the ports
// and core services; none is a product facade.
type Authority struct {
	Store     session.Store
	Writers   writer.Writers
	Registry  *extension.Registry
	Admission writer.Admission
	// Runs is the Run module's Session adapter: the Run core's store bound
	// per Writer, Run reads by SessionID and the Run Parts of Turn units.
	Runs *runmod.SessionRunStore
	// Turns commits the Turn protocol and reads Turn status.
	Turns    *turn.Coordinator
	Driver   *driver.Driver
	Presets  preset.Registry
	Executor effect.ExecutionPort
	Frozen   frozen.Store
	// Projections reads every projection through the Session's Writer.
	Projections extension.ProjectionReader
	// Content materializes the frozen bodies projections name (CHT-MAT-1).
	Content chatlog.ContentResolver
	// Chatlog commits the chatlog's own facts (APP-INP-1, APP-CKP-1).
	Chatlog *chatlog.Commands
	// History answers fork-boundary questions (AUTH-FRK-2, SPN-5).
	History turn.History
	Clock   func() time.Time
	// Schema is the SchemaVersion CreateSession declares on new Sessions.
	Schema extension.SchemaVersion
	// Migrators and Guards are MigrateSession's procedures and the
	// deployment's quiescence preconditions (AUTH-MIG-1).
	Migrators []migrate.Migrator
	Guards    []migrate.Guard

	mu   sync.Mutex
	open map[session.SessionID]*openSession
}

// New composes an Authority from its ports (AUTH-PRT-1).
func New(p Ports) (*Authority, error) { //nolint:gocritic // hugeParam: Ports is a by-value options struct read once
	if p.Executor == nil {
		return nil, errors.New("authority: an Executor port is required")
	}
	store := p.Store
	if store == nil {
		store = session.NewMemoryStore()
	}
	// The first-party three are trusted core; Ports.Modules are extensions
	// and cannot declare authoritative projections (EXT-PRJ-9).
	registry, err := extension.BuildRegistryWithExtensions(session.ProtocolVersion1,
		[]extension.ModuleDescriptor{chatlog.Module, runmod.Module, turn.Module}, p.Modules)
	if err != nil {
		return nil, err
	}
	schema := p.Schema
	if schema == 0 {
		schema = extension.SchemaVersion1
	}
	if !registry.SupportsSchema(schema) {
		return nil, fmt.Errorf("authority: no registered module has a codec under schema %d", schema)
	}
	if err := checkMigrators(registry, p.Migrators); err != nil {
		return nil, err
	}
	// AUTH-PRT-3: durability is one bundle. Each port declares its own
	// durability (Durability); nil ports are the memory defaults below.
	anyDurable := durable(store) || durable(p.Content) || durable(p.Artifacts.Bindings) || durable(p.Artifacts.Ledger)
	allDurable := durable(store) && durable(p.Content) && durable(p.Artifacts.Bindings) && durable(p.Artifacts.Ledger)
	if anyDurable && !allDurable && !p.Artifacts.Ephemeral {
		return nil, ErrEphemeralArtifacts
	}
	bindings := p.Artifacts.Bindings
	if bindings == nil {
		bindings = artifact.NewMemoryBindingStore()
	}
	ledger := p.Artifacts.Ledger
	if ledger == nil {
		ledger = artifact.NewMemoryLedger(artifact.SetBuilder{Resolver: bindings})
	}
	// A projection cache lets a reopened Session start folding instead of
	// refolding the whole log (EXT-PRJ-3). An adapter that can store entries
	// durably provides its own; otherwise they live as long as the process.
	cache := p.Cache
	if cache == nil {
		if provider, ok := store.(extension.ProjectionCacheProvider); ok {
			cache = provider.ProjectionCache()
		} else {
			cache = extension.NewMemoryProjectionCache()
		}
	}
	// Frozen bodies live in the content store and are admitted through the
	// same binding store the Writers resolve against (RUN-WIR-4).
	var fz frozen.Store
	if p.Content == nil {
		fz = runmod.FrozenValuesInMemory(bindings)
	} else {
		fz, err = runmod.FrozenValues(p.Content, bindings)
		if err != nil {
			return nil, err
		}
	}
	now := p.Clock
	if now == nil {
		now = time.Now
	}
	admission := writer.Admission{Bindings: bindings, Ledger: ledger}
	writers := writer.NewWriters(store, registry, admission, p.Ownership,
		writer.WritersConfig{Cache: cache, CachePolicy: runmod.WriterCachePolicy(p.CacheEvery), Observers: p.Observers})
	runs, err := runmod.NewSessionRunStore(runmod.Config{Registry: registry, Store: store, Frozen: fz, Cache: cache, Now: now})
	if err != nil {
		return nil, err
	}
	presets := p.Presets
	if presets == nil {
		presets = preset.NewMemory()
	}
	decisions := p.Decisions
	if decisions == nil {
		decisions = decision.DefaultPromptBuilders()
	}
	// The read model folds from the Store through the cache: reading a Session
	// takes no ownership (AUTH-OWN-2). The Writer keeps its own transactional
	// projections for the commit critical section.
	projections := extension.NewProjectionReader(store, registry, cache)
	content := runmod.NewContent(fz)
	a := &Authority{
		Store: store, Writers: writers, Registry: registry, Admission: admission, Runs: runs,
		Turns:   &turn.Coordinator{Projections: projections, Runs: runs, Now: now},
		Presets: presets, Executor: p.Executor, Frozen: fz, Projections: projections, Content: content,
		Chatlog:   &chatlog.Commands{Now: now},
		History:   turn.History{Store: store, Registry: registry, Projections: projections},
		Clock:     now,
		Schema:    schema,
		Migrators: p.Migrators, Guards: p.Guards,
		open: make(map[session.SessionID]*openSession),
	}
	a.Driver = driver.New()
	a.Driver.Runs, a.Driver.Turns, a.Driver.Executor = runs, a.Turns, p.Executor
	a.Driver.Presets, a.Driver.Decisions, a.Driver.Targets = presets, decisions, p.TargetResolver
	a.Driver.Sources = decision.Sources{Projections: projections, Content: content}
	a.Driver.Fail = p.Fail
	return a, nil
}

// checkMigrators refuses a migrator set MigrateSession could not select
// from unambiguously or encode for: a nil or ill-formed migrator, two
// migrators for one (source, target) pair, or a target Schema the registry
// has no codec under.
func checkMigrators(registry *extension.Registry, migrators []migrate.Migrator) error {
	for i, m := range migrators {
		if m == nil {
			return errors.New("authority: nil migrator")
		}
		if m.Source() == 0 || m.Target() == 0 || m.Source() == m.Target() || m.Profile() == "" {
			return fmt.Errorf("authority: migrator %q must name a profile and two distinct schemas", m.Profile())
		}
		if !registry.SupportsSchema(m.Target()) {
			return fmt.Errorf("authority: migrator %s targets schema %d, which no registered module has a codec under", m.Profile(), m.Target())
		}
		for _, other := range migrators[:i] {
			if other != nil && other.Source() == m.Source() && other.Target() == m.Target() {
				return fmt.Errorf("authority: migrators %s and %s both migrate schema %d to %d", other.Profile(), m.Profile(), m.Source(), m.Target())
			}
		}
	}
	return nil
}

// Close releases every generation this authority holds -- recovery
// listeners, then Writers -- and every outstanding Handle is stale
// afterwards. Generations still opening or closing on another goroutine
// finish their own release.
func (a *Authority) Close(ctx context.Context) error {
	a.mu.Lock()
	var owned []session.SessionID
	for sid, gen := range a.open {
		if gen.state == open {
			gen.state = closing
			owned = append(owned, sid)
		}
	}
	a.mu.Unlock()
	a.Driver.Close()
	err := writer.CloseWriters(ctx, a.Writers)
	a.mu.Lock()
	for _, sid := range owned {
		delete(a.open, sid)
	}
	a.mu.Unlock()
	return err
}

// --- session lifecycle -------------------------------------------------------------

// CreateSession creates the Session under the authority's Schema
// (AUTH-SCH-1); meta is the segment's creation metadata (zero for none), and
// one declaring another Schema is refused.
func (a *Authority) CreateSession(ctx context.Context, sid session.SessionID, meta jsonstable.Value) error {
	meta, err := extension.DeclareSchema(meta, a.Schema)
	if err != nil {
		return &session.Error{Code: session.ErrInvalid, Operation: "create", SessionID: sid, Detail: err.Error()}
	}
	_, err = a.Store.Create(ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: sid, CreatedAtUnixMilli: a.Clock().UnixMilli(), Metadata: meta})
	return err
}

// EnsureSession creates the stream when it does not exist yet. Create's
// idempotency needs field-identical requests, so existence is probed first.
func (a *Authority) EnsureSession(ctx context.Context, sid session.SessionID) error {
	if _, err := a.Store.Header(ctx, sid); err == nil {
		return nil
	} else if !session.IsCode(err, session.ErrNotFound) {
		return err
	}
	if err := a.CreateSession(ctx, sid, jsonstable.Value{}); err != nil {
		// A concurrent creator winning the race is still "exists".
		if _, herr := a.Store.Header(ctx, sid); herr == nil {
			return nil
		}
		return err
	}
	return nil
}

// ForkRequest forks a Session at one commit of its ledger (AUTH-FRK-1): the
// child inherits every commit of Parent up to and including At and continues
// from there under its own identity.
type ForkRequest struct {
	Parent   session.SessionID
	At       session.CommitSeq
	Child    session.SessionID
	Metadata jsonstable.Value
}

// Fork creates the child Session (SES-FRK-1) and claims the artifacts its
// inherited prefix references (EXT-WRT-8). The child is not opened.
func (a *Authority) Fork(ctx context.Context, req ForkRequest) (session.SegmentHeader, error) {
	if req.Parent == "" || req.Child == "" {
		return session.SegmentHeader{}, errors.New("authority: fork requires parent and child session ids")
	}
	if req.Parent == req.Child {
		return session.SegmentHeader{}, errors.New("authority: a session cannot fork itself")
	}
	// A fork point inside a Turn would hand the child a Turn whose Run is
	// the parent's execution (SES-FRK-5): semantic history branches only at
	// quiescent points (AUTH-FRK-1).
	if active, ok, err := a.History.ActiveAt(ctx, req.Parent, req.At); err != nil {
		return session.SegmentHeader{}, err
	} else if ok {
		return session.SegmentHeader{}, &session.Error{Code: session.ErrInvalid, Operation: "fork", SessionID: req.Child,
			Detail: fmt.Sprintf("turn %s of %s is active at commit %d; fork at a quiescent point", active, req.Parent, req.At)}
	}
	return writer.Fork(ctx, a.Store, a.Registry, a.Admission, writer.ForkRequest{
		Parent: req.Parent, At: req.At, Child: req.Child, CreatedAtUnixMilli: a.Clock().UnixMilli(), Metadata: req.Metadata,
	})
}

// ForkBeforeTurn forks Parent at the commit just before turnID started
// (AUTH-FRK-2): the child holds the conversation as it was when that Turn's
// inputs were still submitted and undelivered.
func (a *Authority) ForkBeforeTurn(ctx context.Context, parent session.SessionID, turnID turn.TurnID, child session.SessionID) (session.SegmentHeader, error) {
	seq, err := a.History.StartCommit(ctx, parent, turnID)
	if err != nil {
		return session.SegmentHeader{}, err
	}
	if seq == 0 {
		return session.SegmentHeader{}, &session.Error{Code: session.ErrInvalid, Operation: "fork", SessionID: child,
			Detail: fmt.Sprintf("turn %s started in the first commit of %s; there is no prefix to fork", turnID, parent)}
	}
	return a.Fork(ctx, ForkRequest{Parent: parent, At: seq - 1, Child: child})
}

// DeleteSession drops a Session's root and releases its claims (AUTH-FRK-3,
// SES-GC-1). A Session this authority holds open is closed first; one owned
// by another process is ErrOwned.
func (a *Authority) DeleteSession(ctx context.Context, sid session.SessionID) error {
	if gen := a.beginClose(sid, nil); gen != nil {
		if err := a.release(ctx, sid, gen, true); err != nil {
			return err
		}
	} else {
		a.mu.Lock()
		_, transition := a.open[sid]
		a.mu.Unlock()
		if transition {
			return fmt.Errorf("%w: %s is opening or closing", ErrSessionOpen, sid)
		}
		if err := writer.CloseWriter(ctx, a.Writers, sid); err != nil {
			return err
		}
	}
	return writer.Delete(ctx, a.Store, a.Admission, sid)
}

const opMigrate = "migrate"

// MigrateSession moves a Session to the target Schema (AUTH-MIG-1,
// SES-MIG-1): the explicit management operation, never a Run's or a Turn's
// side effect, and the one way a Session leaves the Schema it was created
// under. It holds the Session exclusively for the duration -- one this
// authority already holds open is ErrSessionOpen, one another process owns
// is ErrOwned -- evaluates the quiescence guards inside the Writer's
// critical section (no active Turn, no active Run, then Ports.Guards), and
// publishes the migrator's bootstrap as the new tip segment in one atomic
// root transition (SES-ADV-2). A Session already on the target is
// AlreadyApplied: with the migration's ID when a registered migrator of the
// same Profile created the tip, ErrConflict when one of another Profile
// did, and without an ID when no recorded migration did. The Writer is
// closed afterwards, so the next Open reads the new tip.
func (a *Authority) MigrateSession(ctx context.Context, sid session.SessionID, target extension.SchemaVersion) (migrate.Result, error) {
	if target == 0 || !a.Registry.SupportsSchema(target) {
		return migrate.Result{}, &session.Error{Code: session.ErrUnsupported, Operation: opMigrate, SessionID: sid,
			Detail: fmt.Sprintf("no registered module has a codec under schema %d", target)}
	}
	// The generation table is the exclusion: a migration is one more owner
	// of the Session, held like an Open but never driven.
	gen := &openSession{state: opening}
	a.mu.Lock()
	if _, held := a.open[sid]; held {
		a.mu.Unlock()
		return migrate.Result{}, fmt.Errorf("%w: %s", ErrSessionOpen, sid)
	}
	a.open[sid] = gen
	a.mu.Unlock()
	w, err := a.Writers.Writer(ctx, sid)
	if err != nil {
		_ = a.release(context.WithoutCancel(ctx), sid, gen, false)
		return migrate.Result{}, err
	}
	gen.w = w
	result, err := a.migrate(ctx, w, target)
	if cerr := a.release(context.WithoutCancel(ctx), sid, gen, true); err == nil {
		err = cerr
	}
	if err != nil {
		return migrate.Result{}, err
	}
	return result, nil
}

// migrate selects the migrator for the tip's Schema and runs it under the
// authority's guards.
func (a *Authority) migrate(ctx context.Context, w writer.Writer, target extension.SchemaVersion) (migrate.Result, error) {
	sid := w.SessionID()
	source := w.Schema()
	if source == target {
		// Already on the target. A registered migrator judges the tip's
		// record -- the same Profile is AlreadyApplied, another Profile a
		// conflict -- and a tip no registered migration created is
		// AlreadyApplied as it stands.
		prov, ok, err := migrate.Record(w.Header())
		if err != nil {
			return migrate.Result{}, &session.Error{Code: session.ErrCorrupt, Operation: opMigrate, SessionID: sid, Detail: err.Error()}
		}
		if !ok {
			return migrate.Result{Outcome: migrate.AlreadyApplied}, nil
		}
		if m := a.migrator(prov.Source, target); m != nil {
			return migrate.Migrate(ctx, w, m)
		}
		return migrate.Result{Outcome: migrate.AlreadyApplied, ID: prov.ID}, nil
	}
	m := a.migrator(source, target)
	if m == nil {
		return migrate.Result{}, &session.Error{Code: session.ErrUnsupported, Operation: opMigrate, SessionID: sid,
			Detail: fmt.Sprintf("no migrator from schema %d to %d", source, target)}
	}
	guards := make([]migrate.Guard, 0, 2+len(a.Guards))
	guards = append(guards, turn.RequireNoActiveTurn, runmod.RequireNoActiveRun)
	guards = append(guards, a.Guards...)
	return migrate.Migrate(ctx, w, m, guards...)
}

// migrator returns the registered migration from source to target, or nil.
func (a *Authority) migrator(source, target extension.SchemaVersion) migrate.Migrator {
	for _, m := range a.Migrators {
		if m.Source() == source && m.Target() == target {
			return m
		}
	}
	return nil
}

// Collect reclaims the storage of deleted Sessions no live Session reaches
// (SES-GC-2).
func (a *Authority) Collect(ctx context.Context) (session.CollectReport, error) {
	return writer.Collect(ctx, a.Store)
}

// --- reads by SessionID ----------------------------------------------------------------

// Reading a Session needs no ownership: projections are queried by identity.
// Commands take the Writer of an open Handle (APP-SES-1).

// Projection reads any registered projection through the Session's Writer
// (APP-MEM-1).
func (a *Authority) Projection(ctx context.Context, sid session.SessionID, id extension.ProjectionID, v extension.ProjectionVersion) (any, session.Head, error) {
	return a.Projections.Load(ctx, sid, id, v)
}

// Reply is the Turn's last assistant text (CHT-MAT-1); empty when the Turn
// produced none.
func (a *Authority) Reply(ctx context.Context, ref turn.TurnRef) (string, error) {
	return chatlog.LastAssistantText(ctx, a.Projections, a.Content, ref.SessionID, chatlog.TurnID(ref.TurnID))
}
