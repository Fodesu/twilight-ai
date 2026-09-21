// Package app is the application layer over the agent core: it composes an
// Authority from deployment choices (Build), registers presets, and offers
// the conversation policies a product needs on top of an owned Session --
// Send and Submit, Deliver-or-Start routing, backlog draining, background
// driving, automatic compaction, the event stream and the subagent effect.
// The core packages remain independent of this package.
package app

import (
	"context"
	"errors"
	"fmt"
	stdhttp "net/http"
	"sync"
	"time"

	"github.com/felinics/twilight/agent/artifact"
	"github.com/felinics/twilight/agent/owner"
	"github.com/felinics/twilight/agent/context/compaction"
	"github.com/felinics/twilight/agent/decision"
	"github.com/felinics/twilight/agent/driver"
	"github.com/felinics/twilight/agent/executor"
	"github.com/felinics/twilight/agent/executor/http"
	executorlocal "github.com/felinics/twilight/agent/executor/local"
	executionstore "github.com/felinics/twilight/agent/executor/store"
	"github.com/felinics/twilight/agent/observe"
	"github.com/felinics/twilight/agent/preset"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/effect"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/run/plan"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
	runmod "github.com/felinics/twilight/agent/session/run"
	"github.com/felinics/twilight/agent/session/writer"
	"github.com/felinics/twilight/agent/spawn"
	"github.com/felinics/twilight/agent/turn"
)

// ExecutorMode selects the effect implementation built by Build.
type ExecutorMode string

const (
	// ExecutorLocal runs model and tool effects in this process.
	ExecutorLocal ExecutorMode = "local"
	// ExecutorRemote forwards effects to an executor/http Server.
	ExecutorRemote ExecutorMode = "remote"
)

// ExecutorConfig describes how the application obtains its effect Port. Port
// takes precedence when supplied, which is useful for tests and custom
// transports. Local and Remote are the standard deployment profiles.
type ExecutorConfig struct {
	Mode ExecutorMode
	Port effect.ExecutionPort

	// Models and Tools are required for ExecutorLocal. They are ignored by
	// ExecutorRemote because implementations live in the worker process.
	Models map[run.ModelRef]loop.ModelInvoker
	Tools  []loop.ExecutableTool

	// Endpoint and HTTP configure ExecutorRemote.
	Endpoint string
	HTTP     *stdhttp.Client
}

// Preset is an authority-side decision identity to register during Build.
type Preset struct {
	ID    turn.PresetID
	Value turn.AgentPreset
}

// Config contains application assembly choices. It deliberately contains
// concrete ports rather than a file format: YAML, environment variables and
// command-line flags can be decoded into this type by an outer deployment
// package later.
type Config struct {
	// Store is the Session kernel (required). A memory Store is a test
	// double, passed by name; Build never falls back to one.
	Store     session.Store
	Content   artifact.ContentStore
	Artifacts owner.Artifacts

	Executor ExecutorConfig
	// Executions is the record store of the Worker Build composes
	// (RUN-EXE-8): required whenever a Worker is composed (the local mode
	// always; a supplied Port or the remote mode when Spawn is set), unused
	// otherwise. It belongs to the durability bundle (OWN-PRT-3): a durable
	// Store with a memory record store manufactures missing executions after
	// a restart, so Build refuses the mix unless Artifacts.Ephemeral opts in.
	Executions executionstore.Store
	Presets    []Preset
	// Registry is the preset registry; nil selects an in-memory one.
	Registry preset.Registry
	// Decisions resolve each preset's PromptBuilderRef; nil selects the
	// default catalog.
	Decisions *decision.PromptBuilders

	Ownership session.OpenOptions
	// TargetResolver resolves the opaque resource target of each effect
	// (RUN-LOP-9). The application's resource layer owns it; nil gives every
	// effect no target (APP-TGT-1).
	TargetResolver loop.TargetResolver
	// Modules are application modules registered after the first-party four
	// (EXT-APP).
	Modules []extension.ModuleDescriptor
	// Observers are notified of every applied group besides the event stream.
	Observers []writer.CommitObserver
	Clock     func() time.Time
	// Cache and CacheEvery configure the projection cache (APP-MEM-2).
	Cache      extension.ProjectionCache
	CacheEvery session.CommitSeq
	// Warn receives failures of background work; nil discards them.
	Warn func(error)
	// Spawn enables the subagent tool (SPN); nil leaves it unavailable.
	Spawn *spawn.Options
	// Worker configures the Worker that owns execution records (lease, id,
	// reconcile loop, clock). It applies whenever Build composes a Worker:
	// the local mode always does; a supplied Port or the remote mode do when
	// Spawn is set, so the spawn Backend can be routed beside them.
	Worker executor.WorkerOptions
}

// ErrEphemeralExecutions reports a durable session Store mixed with a
// memory-only execution record store, or the reverse, without the Ephemeral
// opt-in (OWN-PRT-3): the Worker's records would not survive the restart the
// Session facts survive, and every Executing target would come back missing.
var ErrEphemeralExecutions = errors.New("app: durable session store with memory-only execution record store (or the reverse); provide the durable bundle or set Artifacts.Ephemeral")

// durable reports a port's declared durability (owner.Durability); nil is
// not a store, and a port that does not declare is taken as durable.
func durable(port any) bool {
	if port == nil {
		return false
	}
	if d, ok := port.(owner.Durability); ok {
		return d.Durable()
	}
	return true
}

// CompactorSystemPrompt is kept here for deterministic model test doubles and
// applications that need to recognize the built-in compaction request.
const CompactorSystemPrompt = compaction.CompactorSystemPrompt

// Application is the composition root: the Authority plus the application's
// own services -- the preset table, the event stream and the spawn effect.
type Application struct {
	Owner *owner.Owner
	bus       *observe.Bus
	spawn     *spawn.Responder
	// worker is the Worker Build composed, if any; Close stops it after the
	// Authority (RUN-EXE-8).
	worker *executor.Worker
	warn   func(error)

	mu   sync.RWMutex
	refs map[turn.PresetID]turn.PresetRef
	// sessions are the Sessions this process has open, by id: the Planner
	// finds a Session's compaction policy here while its Run is driven
	// (APP-CKP-1).
	sessions map[session.SessionID]*Session
}

// BeforePrepare is driver.Planner (RUN-LOP-10, APP-CKP-1): between two steps
// of a Run, while it is Open, the Session's automatic compaction policy runs
// against the context the next model request will read. Failures reach
// CompactWarn and never stop the drive.
func (app *Application) BeforePrepare(ctx context.Context, w writer.Writer, _ plan.PromptInput) error {
	app.mu.RLock()
	s := app.sessions[w.SessionID()]
	app.mu.RUnlock()
	if s == nil || s.opts.CompactAfterEntries <= 0 {
		return nil
	}
	s.maybeCompact(ctx)
	return nil
}

func (app *Application) track(s *Session) {
	app.mu.Lock()
	defer app.mu.Unlock()
	app.sessions[s.sid] = s
}

func (app *Application) untrack(s *Session) {
	app.mu.Lock()
	defer app.mu.Unlock()
	if app.sessions[s.sid] == s {
		delete(app.sessions, s.sid)
	}
}

// Build assembles an application from typed dependencies and a
// deployment-neutral executor profile.
func Build(c Config) (*Application, error) { //nolint:gocritic // hugeParam: Config is a by-value options struct read once
	if c.Store == nil {
		return nil, errors.New("app: a session Store is required")
	}
	content := c.Content
	if content == nil {
		var err error
		content, err = artifact.NewMemoryContentStore(runmod.FrozenAuthority, artifact.MemoryContentStoreOptions{})
		if err != nil {
			return nil, fmt.Errorf("app: create default content store: %w", err)
		}
	}
	warn := c.Warn
	if warn == nil {
		warn = func(error) {}
	}
	app := &Application{warn: warn, refs: make(map[turn.PresetID]turn.PresetRef, len(c.Presets)), sessions: make(map[session.SessionID]*Session)}
	if c.Worker.Warn == nil {
		c.Worker.Warn = warn
	}
	// The subagent tool is answered by a Responder on the Driver (SPN-1,
	// DRV-4), not executed: it drives children through the Authority, so it
	// is bound after New. No Worker route is involved.
	var routes []executor.Route
	if c.Spawn != nil {
		app.spawn = spawn.NewResponder(*c.Spawn)
	}
	// One progress hub serves the Worker and every local backend it routes
	// to (RUN-EXE-12); the driver's sink relays its frames onto the Bus.
	if c.Worker.Progress == nil {
		c.Worker.Progress = executor.NewProgressHub(0)
	}
	port, worker, err := buildExecutor(&c, routes)
	if err != nil {
		return nil, err
	}
	app.worker = worker
	// The event stream is a CommitObserver on the Writers (EXT-WRT-7); it
	// needs the Registry, which the Authority builds, so the bus is wired
	// through a forwarding observer bound after New.
	var bus *observe.Bus
	observers := append([]writer.CommitObserver{forwardingObserver{&bus}}, c.Observers...)
	a, err := owner.New(owner.Ports{
		Store: c.Store, Content: content, Artifacts: c.Artifacts, Presets: c.Registry, Decisions: c.Decisions,
		Executor: port, TargetResolver: c.TargetResolver, Observers: observers, Modules: c.Modules,
		Clock: c.Clock, Cache: c.Cache, CacheEvery: c.CacheEvery, Ownership: c.Ownership, Fail: app.fail,
	})
	if err != nil {
		return nil, err
	}
	bus = observe.NewBus(a.Registry)
	app.Owner, app.bus = a, bus
	// The Sessions' compaction policy runs between the steps of a Turn
	// through the driver's planner seam (APP-CKP-1, RUN-LOP-10).
	a.Driver.Planner = app
	// Provisional observations of effects in flight reach the same stream
	// as the committed facts (OBS-1, RUN-LOP-6).
	a.Driver.Sink = busSink{bus}
	if app.spawn != nil {
		// The subagent tool waits for an external response the Responder
		// gives (SPN-1, DRV-4).
		app.spawn.Bind(a)
		a.Driver.Responders = map[run.ToolRef]driver.Responder{c.Spawn.ToolRef(): app.spawn}
	}
	for i := range c.Presets {
		p := &c.Presets[i]
		if p.ID == "" {
			return nil, errors.New("app: preset requires an id")
		}
		if _, err := app.RegisterPreset(p.ID, p.Value); err != nil {
			return nil, fmt.Errorf("app: register preset %q: %w", p.ID, err)
		}
	}
	return app, nil
}

// busSink is the drive's loop.EventSink: provisional observations become
// transient Bus events; committed observations are already on the Bus from
// the Writer, so they are dropped here.
type busSink struct{ bus *observe.Bus }

func (s busSink) Emit(_ context.Context, e loop.Event) error { //nolint:gocritic // hugeParam: EventSink contract takes the Event by value
	if s.bus == nil || e.Durability != loop.EventProvisional {
		return nil
	}
	s.bus.Publish(session.SessionID(e.Session), observe.Progress{RunID: e.RunID, Effect: e.Effect, Generation: e.Generation,
		Sequence: e.Sequence, Kind: string(e.Kind), Payload: e.Payload})
	return nil
}

// forwardingObserver forwards to a Bus that is bound after the Writers are
// built; commits before binding (none: Build binds before any Session opens)
// are dropped.
type forwardingObserver struct{ bus **observe.Bus }

func (o forwardingObserver) Committed(ctx context.Context, sid session.SessionID, c session.Commit) {
	if b := *o.bus; b != nil {
		b.Committed(ctx, sid, c)
	}
}

// fail reports a failure of background work to Warn and, as an Event, to the
// Session's subscribers (OBS-1).
func (app *Application) fail(sid session.SessionID, err error) {
	app.warn(err)
	app.bus.Failed(sid, err)
}

// RegisterPreset adds or replaces a decision identity after Build.
func (app *Application) RegisterPreset(id turn.PresetID, p turn.AgentPreset) (turn.PresetRef, error) {
	ref, err := app.Owner.Presets.Register(id, p)
	if err != nil {
		return turn.PresetRef{}, err
	}
	app.mu.Lock()
	app.refs[id] = ref
	app.mu.Unlock()
	return ref, nil
}

// PresetRef returns the digest-checked reference for a registered preset.
func (app *Application) PresetRef(id turn.PresetID) (turn.PresetRef, error) {
	app.mu.RLock()
	ref, ok := app.refs[id]
	app.mu.RUnlock()
	if !ok {
		return turn.PresetRef{}, fmt.Errorf("app: unknown preset %q", id)
	}
	return ref, nil
}

// Events subscribes to one Session's event stream from this moment on
// (OBS-1): every event of every group applied by this application's
// Writers, in commit order, plus failures of background drives.
func (app *Application) Events(ctx context.Context, sid session.SessionID) <-chan Event {
	return app.bus.Subscribe(ctx, sid)
}

// Close cancels the spawn effect's child drives, stops recovery listeners
// and releases every Session this application owns.
func (app *Application) Close(ctx context.Context) error {
	if app.spawn != nil {
		app.spawn.Close()
	}
	err := app.Owner.Close(ctx)
	// The Worker goes last: the Authority's drives may still be settling
	// outcomes through it. Records keep their leases until they expire and
	// the next incarnation adopts them (RUN-EXE-8, SPN-4).
	if app.worker != nil {
		app.worker.Close()
	}
	return err
}

// buildExecutor is the effect Port the Authority drives against. The local
// mode is a Worker over the colocated Backend (RUN-EXE-8). A supplied Port or
// the remote client is used as is unless extra routes (spawn) are configured,
// in which case a Worker routes to them and to the Port as its default
// Backend; the remote Worker keeps its own record of the physical execution.
func buildExecutor(c *Config, extra []executor.Route) (effect.ExecutionPort, *executor.Worker, error) {
	worker := func(routes ...executor.Route) (effect.ExecutionPort, *executor.Worker, error) {
		if c.Executions == nil {
			return nil, nil, errors.New("app: an execution record store (Config.Executions) is required when Build composes a Worker")
		}
		if durable(c.Store) != durable(c.Executions) && !c.Artifacts.Ephemeral {
			return nil, nil, ErrEphemeralExecutions
		}
		w, err := executor.NewWorker(context.Background(), c.Executions, append(extra, routes...), c.Worker)
		if err != nil {
			return nil, nil, err
		}
		return w, w, nil
	}
	if c.Executor.Port != nil {
		if len(extra) == 0 {
			return c.Executor.Port, nil, nil
		}
		return worker(executor.Default("port", executor.PortBackend(c.Executor.Port)))
	}
	switch c.Executor.Mode {
	case "", ExecutorLocal:
		catalog, err := executorlocal.NewCatalog(c.Executor.Models, c.Executor.Tools...)
		if err != nil {
			return nil, nil, err
		}
		backend, err := executorlocal.NewLocalExecutor(catalog, c.Worker.Progress, true)
		if err != nil {
			return nil, nil, err
		}
		return worker(executorlocal.Route(backend))
	case ExecutorRemote:
		if c.Executor.Endpoint == "" {
			return nil, nil, errors.New("app: remote executor requires an endpoint")
		}
		client := &http.Client{BaseURL: c.Executor.Endpoint, HTTP: c.Executor.HTTP}
		if len(extra) == 0 {
			return client, nil, nil
		}
		return worker(executor.Default("remote", executor.PortBackend(client)))
	default:
		return nil, nil, fmt.Errorf("app: unknown executor mode %q", c.Executor.Mode)
	}
}

// Event is one item of a Session's event stream (OBS-1).
type Event = observe.Event

// ForkRequest forks a Session at one commit of its ledger (OWN-FRK-1).
type ForkRequest = owner.ForkRequest
