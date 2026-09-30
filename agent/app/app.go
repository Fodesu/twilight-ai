// Package app is the reference agent's application layer over agentcore: it composes an
// Authority from deployment choices (Build), registers presets, and offers
// the conversation policies a product needs on top of an owned Session --
// replies, workspace binding and snapshots, automatic compaction, the event
// stream and the subagent effect. Input admission, routing and driving are
// the runtime's; the core packages remain independent of this package.
package app

import (
	"context"
	"errors"
	"fmt"
	"github.com/felinics/twilight/agent/context/compaction"
	"github.com/felinics/twilight/agent/environment"
	"github.com/felinics/twilight/agent/executor/http"
	"github.com/felinics/twilight/agent/executor/local"
	"github.com/felinics/twilight/agent/executor/sandbox"
	"github.com/felinics/twilight/agent/prompt"
	"github.com/felinics/twilight/agent/spawn"
	"github.com/felinics/twilight/agent/tools"
	"github.com/felinics/twilight/agent/workspace"
	"github.com/felinics/twilight/agentcore/decision"
	"github.com/felinics/twilight/agentcore/driver"
	"github.com/felinics/twilight/agentcore/executor"
	executionstore "github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/inbox"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/observe"
	"github.com/felinics/twilight/agentcore/owner"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	rt "github.com/felinics/twilight/agentcore/runtime"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/sessionkernel"
	stdhttp "net/http"
	"sync"
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
	Models map[run.ModelRef]local.ModelInvoker
	Tools  []local.ExecutableTool

	// Endpoint and HTTP configure ExecutorRemote.
	Endpoint string
	HTTP     *stdhttp.Client
}

// Preset is an authority-side decision identity to register during Build.
type Preset struct {
	ID    preset.PresetID
	Value preset.AgentPreset
}

// Config contains application assembly choices. Kernel is the session
// kernel's assembly and Execution the execution side's; the two compose
// independently (sessionkernel.New and runtime.NewExecution) and Build
// pairs them. The remaining fields are the deployment profile and the
// product policies. The Config deliberately contains concrete ports rather
// than a file format: YAML, environment variables and command-line flags
// can be decoded into this type by an outer deployment package later.
type Config struct {
	// Kernel is the session kernel's assembly: the Session store, the
	// content and artifact stores, the module extensions, the projection
	// cache and the other ports and policies sessionkernel.New composes.
	Kernel sessionkernel.Ports
	// Execution is the execution side's assembly: the decision catalog,
	// the preset registry, the takeover policy and the other drive-chain
	// policies runtime.NewExecution composes over the Kernel. Build fills
	// the effect port, the Planner, the Responders and Fail.
	Execution rt.ExecutionConfig

	Executor ExecutorConfig
	// Executions is the record store of the Worker Build composes
	// (RUN-EXE-8): required whenever a Worker is composed (the local mode
	// always; a supplied Port or the remote mode when Spawn is set), unused
	// otherwise. Like every store it is durable (OWN-PRT-3); a record that
	// did not survive a restart would let the Owner dispose an execution
	// that is still running.
	Executions executionstore.Store
	// Worker configures the Worker that owns execution records (lease, id,
	// reconcile loop, clock). It applies whenever Build composes a Worker:
	// the local mode always does; a supplied Port or the remote mode do when
	// Spawn is set, so the spawn Backend can be routed beside them.
	Worker executor.WorkerOptions
	Presets []Preset
	// Warn receives failures of background work; nil discards them.
	Warn func(error)
	// Spawn enables the subagent tool (SPN); nil leaves it unavailable.
	Spawn *spawn.Options
	// Inbox is the durable command inbox of the Sessions (APP-INB-1): the
	// way a caller that does not hold a Session reaches its owner. Nil
	// leaves Enqueue and ApplyPending unavailable (ErrNoInbox); like every
	// store it is durable (OWN-PRT-3).
	Inbox inbox.Store
	// Activation holds Sessions for active work only and releases them when
	// quiescent (APP-ACT); nil keeps every Session open until Close. It
	// requires Inbox.
	Activation *Activation
	// Workspaces enables the workspace layer (APP-WSP): the Session binding
	// module, its target resolver, the workspace backend of the composed
	// Worker and the prompt's workspace preface. Nil leaves every
	// workspace-placed tool call without a target.
	Workspaces *WorkspaceConfig
}

// WorkspaceConfig composes the workspace layer (APP-WSP-3).
type WorkspaceConfig struct {
	// Store holds the Workspace records and RuntimeBindings (required).
	Store workspace.Store
	// Provider materializes and attaches environments; required whenever
	// Build composes a Worker (the workspace backend runs beside it).
	Provider environment.Provider
	// Backend is the Provider's identity in RuntimeBindings (required with
	// Provider).
	Backend environment.Backend
	// Tools are the workspace-placed tools the backend serves; nil selects
	// tools.Default().
	Tools []tools.Tool
	// Snapshots takes the Snapshots (APP-WSP-7): nil with Provider set
	// selects the composed backend; a process without the backend (the
	// remote executor mode) hands in the tool backend's client
	// (agent/workspace/http.Client).
	Snapshots workspace.Snapshotter
	// SnapshotAfterTurn takes a Snapshot of a Session's bound Workspace
	// after every quiescent settlement and records it on the
	// Session (APP-WSP-7), so a fork at a Turn boundary can restore the
	// files as they were. It needs Snapshots.
	SnapshotAfterTurn bool
}

func (c *WorkspaceConfig) tools() []tools.Tool {
	if c.Tools == nil {
		return tools.Default()
	}
	return c.Tools
}

// CompactorSystemPrompt is kept here for deterministic model test doubles and
// applications that need to recognize the built-in compaction request.
const CompactorSystemPrompt = compaction.CompactorSystemPrompt

// Application is the composition root: the Session kernel, the execution
// side over it, the ownership authority and the Worker that owns the
// execution records -- plus the application's own services, the preset
// table, the event stream and the spawn effect.
type Application struct {
	// Owner holds the Sessions this process owns; Kernel is their durable
	// state, Execution is what advances them, and Worker owns the execution
	// records of the effects dispatched here.
	Owner     *owner.Owner
	Kernel    *sessionkernel.Kernel
	Execution *rt.Execution
	Worker    *executor.Worker
	spawn     *spawn.Responder
	// warn receives failures of background work.
	warn func(error)
	inbox      inbox.Store
	activation *Activation
	bg         context.Context
	bgCancel   context.CancelFunc
	loops      sync.WaitGroup
	releases   group
	actMu      sync.Mutex
	activating map[session.SessionID]chan struct{}
	// workspaces is the workspace layer's configuration, nil when absent.
	workspaces *WorkspaceConfig
	// sandbox is the workspace backend this process composed, if any;
	// snapshots is where snapshots are taken, here or remotely.
	sandbox   *sandbox.Backend
	snapshots workspace.Snapshotter
	// bindings writes the Session's workspace binding facts.
	bindings workspace.Commands
	// resolved is closed and replaced at each inbox resolution of this
	// process; AwaitCommand waits on it (APP-INB-1).
	resolvedMu sync.Mutex
	resolved   chan struct{}

	mu   sync.RWMutex
	refs map[preset.PresetID]preset.PresetRef
	// sessions are the Sessions this process has open, by id: the Planner
	// finds a Session's compaction policy here while its Run is driven
	// (APP-CKP-1).
	sessions map[session.SessionID]*Session
}

// BeforePrepare is driver.Planner (RUN-LOP-10, APP-CKP-1): between two steps
// of a Run, while it is Open, the Session's automatic compaction policy runs
// against the context the next model request will read. Failures reach
// CompactWarn and never stop the drive.
func (app *Application) BeforePrepare(ctx context.Context, w writer.Writer, _ decision.Input) error {
	app.mu.RLock()
	s := app.sessions[w.SessionID()]
	app.mu.RUnlock()
	if s == nil || s.opts.CompactAfterEntries <= 0 {
		return nil
	}
	s.maybeCompact(ctx)
	return nil
}

// Opened returns the Session this process holds open under sid, if any: the
// command face wakes and closes Sessions through it (APP-INB-3).
func (app *Application) Opened(sid session.SessionID) (*Session, bool) {
	app.mu.RLock()
	defer app.mu.RUnlock()
	s, ok := app.sessions[sid]
	return s, ok
}

// Lease is the Session's current writer lease, read without ownership
// (SES-OWN-5): what a gateway routes by and a controller judges expiry by.
func (app *Application) Lease(ctx context.Context, sid session.SessionID) (session.Lease, bool, error) {
	return app.Kernel.Store.LeaseOf(ctx, sid)
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
	warn := c.Warn
	if warn == nil {
		warn = func(error) {}
	}
	app := &Application{warn: warn, inbox: c.Inbox, workspaces: c.Workspaces, bindings: workspace.Commands{Now: c.Kernel.Clock},
		refs: make(map[preset.PresetID]preset.PresetRef, len(c.Presets)), sessions: make(map[session.SessionID]*Session),
		activating: make(map[session.SessionID]chan struct{})}
	app.bg, app.bgCancel = context.WithCancel(context.Background())
	// Build is transactional: a failure after a resource with a lifetime
	// was created releases what was created, in reverse order.
	built := false
	defer func() {
		if !built {
			app.rollback()
		}
	}()
	if c.Activation != nil {
		if c.Inbox == nil {
			return nil, errors.New("app: Activation requires an inbox Store")
		}
		act := *c.Activation
		app.activation = &act
	}
	// The subagent tool is answered by a Responder on the Driver (SPN-1,
	// DRV-4), not executed: it drives children through the Authority, so it
	// is bound after New. No Worker route is involved.
	var routes []executor.Route
	if c.Spawn != nil {
		app.spawn = spawn.NewResponder(*c.Spawn)
	}
	// One progress hub serves the Worker and every local backend it routes
	// to (RUN-EXE-12); the Execution relays its frames onto the event stream.
	if c.Worker.Progress == nil {
		c.Worker.Progress = executor.NewProgressHub(0)
	}
	// The workspace layer (APP-WSP-3): the binding module joins the
	// registry, the resolver answers RUN-LOP-9 from the binding projection
	// once the Authority exists, the prompt gets its workspace preface, and
	// the workspace backend takes the workspace-placed tool calls of the
	// Worker Build composes.
	var resolver *workspace.Resolver
	if c.Workspaces != nil {
		if c.Workspaces.Store == nil {
			return nil, errors.New("app: Workspaces requires a workspace Store")
		}
		c.Kernel.Modules = append([]module.ModuleDescriptor{workspace.Module}, c.Kernel.Modules...)
		if c.Execution.TargetResolver == nil {
			resolver = &workspace.Resolver{}
			c.Execution.TargetResolver = resolver
		}
		if c.Execution.Decisions == nil {
			c.Execution.Decisions = prompt.CatalogWith(prompt.WorkspacePreface)
		}
		if c.Workspaces.Provider != nil {
			backend, err := sandbox.New(sandbox.Options{Workspaces: c.Workspaces.Store, Provider: c.Workspaces.Provider,
				Backend: c.Workspaces.Backend, Tools: c.Workspaces.tools(), Progress: c.Worker.Progress})
			if err != nil {
				return nil, err
			}
			app.sandbox = backend
			routes = append(routes, sandbox.Route(backend))
		}
		app.snapshots = c.Workspaces.Snapshots
		if app.snapshots == nil && app.sandbox != nil {
			app.snapshots = app.sandbox
		}
		if c.Workspaces.SnapshotAfterTurn && app.snapshots == nil {
			return nil, errors.New("app: Workspaces.SnapshotAfterTurn requires Snapshots or Provider")
		}
	}
	port, workerRoutes, composeWorker, err := buildExecutor(&c, routes)
	if err != nil {
		return nil, err
	}
	if c.Execution.Decisions == nil {
		c.Execution.Decisions = prompt.DefaultCatalog()
	}
	kernel, err := sessionkernel.New(c.Kernel)
	if err != nil {
		return nil, err
	}
	app.Kernel = kernel
	// The Worker owns the execution records of the effects dispatched here
	// (RUN-EXE-8); it is a deployment component, so it composes at this
	// root, not inside the core subsystems.
	if composeWorker {
		w, err := executor.NewWorker(context.Background(), c.Executions, workerRoutes, c.Worker)
		if err != nil {
			return nil, err
		}
		app.Worker = w
		port = w
	}
	// The subagent tool waits for an external response the Responder gives
	// (SPN-1, DRV-4); the Responder opens children through the Owner, which
	// it is bound to once the Owner exists, before any Respond can run.
	var responders map[run.ToolRef]driver.Responder
	if app.spawn != nil {
		responders = map[run.ToolRef]driver.Responder{c.Spawn.ToolRef(): app.spawn}
	}
	// The Sessions' compaction policy runs between the steps of a Turn
	// (APP-CKP-1, RUN-LOP-10); provisional observations of effects in
	// flight reach the event stream (OBS-1).
	c.Execution.Executor = port
	c.Execution.Planner = app
	c.Execution.Responders = responders
	c.Execution.Fail = app.fail
	exec, err := rt.NewExecution(c.Execution, kernel)
	if err != nil {
		return nil, err
	}
	app.Execution = exec
	app.Owner = owner.New(kernel, exec)
	if resolver != nil {
		resolver.Projections = kernel.Projections
	}
	if app.spawn != nil {
		app.spawn.Bind(app.Owner)
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
	if app.activation != nil && app.activation.Scan > 0 {
		app.loops.Add(1)
		go app.scanLoop()
	}
	built = true
	return app, nil
}

// rollback releases what a failed Build created, in reverse order of
// creation: the execution side, the Worker, the Kernel (its Writers) and
// the sandbox. Nothing was opened yet, so there is no ownership to release.
func (app *Application) rollback() {
	app.bgCancel()
	ctx := context.Background()
	if app.Execution != nil {
		app.Execution.Close()
	}
	if app.Worker != nil {
		app.Worker.Close()
	}
	if app.Kernel != nil {
		_ = app.Kernel.Close(ctx)
	}
	if app.sandbox != nil {
		_ = app.sandbox.Close(ctx)
	}
}

// fail reports a failure of background work to Warn and, as an Event, to the
// Session's subscribers (OBS-1).
func (app *Application) fail(sid session.SessionID, err error) {
	app.warn(err)
	app.Kernel.Bus.Failed(sid, err)
}

// RegisterPreset adds or replaces a decision identity after Build.
func (app *Application) RegisterPreset(id preset.PresetID, p preset.AgentPreset) (preset.PresetRef, error) {
	ref, err := app.Execution.Presets.Register(id, p)
	if err != nil {
		return preset.PresetRef{}, err
	}
	app.mu.Lock()
	app.refs[id] = ref
	app.mu.Unlock()
	return ref, nil
}

// PresetRef returns the digest-checked reference for a registered preset.
func (app *Application) PresetRef(id preset.PresetID) (preset.PresetRef, error) {
	app.mu.RLock()
	ref, ok := app.refs[id]
	app.mu.RUnlock()
	if !ok {
		return preset.PresetRef{}, fmt.Errorf("app: unknown preset %q", id)
	}
	return ref, nil
}

// Events subscribes to one Session's event stream from this moment on
// (OBS-1): every event of every group applied by this application's
// Writers, in commit order, plus failures of background drives.
func (app *Application) Events(ctx context.Context, sid session.SessionID) <-chan Event {
	return app.Kernel.Bus.Subscribe(ctx, sid)
}

// EventsFrom is the catch-up form of Events: the Session's committed events
// from CommitSeq from, then the live stream. A client that keeps the last
// Position it handled resumes here after a disconnect without a gap.
func (app *Application) EventsFrom(ctx context.Context, sid session.SessionID, from ledger.CommitSeq) (<-chan Event, error) {
	return app.Kernel.Bus.SubscribeFrom(ctx, sid, from)
}

// Close cancels the spawn effect's child drives, stops recovery listeners
// and releases every Session this application owns.
func (app *Application) Close(ctx context.Context) error {
	if app.spawn != nil {
		app.spawn.Close()
	}
	// The activation scan stops first, so it opens nothing more; then the
	// Sessions this process holds close: their background drives, appliers
	// and snapshots end before the Writers they commit through.
	app.bgCancel()
	app.loops.Wait()
	app.mu.RLock()
	open := make([]*Session, 0, len(app.sessions))
	for _, s := range app.sessions {
		open = append(open, s)
	}
	app.mu.RUnlock()
	var err error
	for _, s := range open {
		if cerr := s.Close(ctx); cerr != nil && err == nil {
			err = cerr
		}
	}
	if werr := app.releases.wait(ctx); werr != nil && err == nil {
		err = werr
	}
	// Ownership is released first, then the execution side, then the
	// Writers and, last, the Worker the drives may still be settling
	// outcomes through (RUN-EXE-8, SPN-4). Records keep their leases until
	// they expire and the next incarnation adopts them.
	if oerr := app.Owner.Close(ctx); oerr != nil && err == nil {
		err = oerr
	}
	if app.Execution != nil {
		app.Execution.Close()
	}
	if app.Kernel != nil {
		if cerr := app.Kernel.Close(ctx); cerr != nil && err == nil {
			err = cerr
		}
	}
	if app.Worker != nil {
		app.Worker.Close()
	}
	if app.sandbox != nil {
		if cerr := app.sandbox.Close(ctx); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

// buildExecutor decides the effect port the Execution drives against and,
// when this process owns execution records, the routes of the Worker Build
// composes over it (RUN-EXE-8). The local mode routes to the colocated
// Backend; a supplied Port or the remote client is used as is unless extra
// routes (spawn, the workspace backend) are configured, in which case the
// Worker routes to them and to the Port as its default Backend; the remote
// Worker keeps its own record of the physical execution.
func buildExecutor(c *Config, extra []executor.Route) (effect.ExecutionPort, []executor.Route, bool, error) {
	worker := func(routes ...executor.Route) (effect.ExecutionPort, []executor.Route, bool, error) {
		if c.Executions == nil {
			return nil, nil, false, errors.New("app: an execution record store (Config.Executions) is required when Build composes a Worker")
		}
		return nil, append(extra, routes...), true, nil
	}
	if c.Executor.Port != nil {
		if len(extra) == 0 {
			return c.Executor.Port, nil, false, nil
		}
		return worker(executor.Default("port", executor.PortBackend(c.Executor.Port)))
	}
	switch c.Executor.Mode {
	case "", ExecutorLocal:
		catalog, err := local.NewCatalog(c.Executor.Models, c.Executor.Tools...)
		if err != nil {
			return nil, nil, false, err
		}
		backend, err := local.NewLocalExecutor(catalog, catalog, c.Worker.Progress, true)
		if err != nil {
			return nil, nil, false, err
		}
		return worker(local.Route(backend))
	case ExecutorRemote:
		if c.Executor.Endpoint == "" {
			return nil, nil, false, errors.New("app: remote executor requires an endpoint")
		}
		client := &http.Client{BaseURL: c.Executor.Endpoint, HTTP: c.Executor.HTTP}
		if len(extra) == 0 {
			return client, nil, false, nil
		}
		return worker(executor.Default("remote", executor.PortBackend(client)))
	default:
		return nil, nil, false, fmt.Errorf("app: unknown executor mode %q", c.Executor.Mode)
	}
}

// Event is one item of a Session's event stream (OBS-1).
type Event = observe.Event

// ForkRequest forks a Session at one commit of its ledger (OWN-FRK-1).
type ForkRequest = sessionkernel.ForkRequest
