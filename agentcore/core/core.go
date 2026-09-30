// Package core composes the agent core -- the session kernel (fact layer,
// projections, Turn protocol, content) and the execution side over it (the
// settlement subscription, the drive chain, the execution port, the decision
// identities) -- into one Core for hosts that run both sides in one process.
// The composition is a convenience for single-process deployments; the two
// sides assemble independently (sessionkernel.New and runtime.NewExecution)
// and which process hosts which is a deployment choice.
package core

import (
	"context"
	"errors"
	"time"

	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/decision"
	"github.com/felinics/twilight/agentcore/driver"
	"github.com/felinics/twilight/agentcore/executor"
	executionstore "github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/observe"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/reconcile"
	"github.com/felinics/twilight/agentcore/run/redispatch"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/sessionkernel"
)

// Artifacts groups the artifact ports (OWN-PRT-3). It is the session
// kernel's port group, re-exported for hosts that compose a whole Core.
type Artifacts = sessionkernel.Artifacts

// ForkRequest forks a Session at one commit of its ledger (OWN-FRK-1).
type ForkRequest = sessionkernel.ForkRequest

// Ports are the roles a Core is composed from (OWN-PRT-1): the session
// kernel's ports plus the execution side's. Every field is an interface or
// a core value. The Store, the Executor, the Content store and both
// Artifacts are required and durable (OWN-PRT-3); the other nil fields take
// the in-process defaults documented on each.
type Ports struct {
	// Store is the Session kernel (required).
	Store session.Stores
	// Content is the cas ContentStore the frozen bodies live in under
	// sessionstore.FrozenAuthority (RUN-WIR-4). Required.
	Content artifact.ContentStore
	// Artifacts are the binding store and retention ledger (required).
	Artifacts Artifacts
	// Presets is the registry of decision identities; nil selects an
	// in-memory registry.
	Presets preset.Registry
	// Decisions resolve each preset's PromptBuilderRef (DEC-CAT): required.
	// The core ships no builder; the agent built on it supplies its catalog
	// (agent/prompt.DefaultPromptBuilders for the reference agent).
	Decisions *decision.Catalog
	// MissingEffects is the takeover policy for an Executing effect the
	// Executor holds nothing for (RUN-CMT-7): the zero value disposes it,
	// reconcile.RedispatchMissing hands it to the Executor again within a
	// budget (RUN-EXE-15) and requires Redispatches.
	MissingEffects reconcile.MissingPolicy
	// Redispatches is the dispatch ledger RedispatchMissing writes; durable
	// like every store (OWN-PRT-3). Unused under DisposeMissing.
	Redispatches redispatch.Store
	// Executor is the effect layer port (RUN-EXE-3): required unless a Worker
	// is composed, which replaces it as the port the Core drives.
	Executor effect.ExecutionPort
	// Worker composes the execution Worker this Core drives; nil drives
	// Executor directly. The Worker owns the execution records (takeover
	// disposition, RUN-EXE-8) and closes with the Core, after its services.
	Worker *WorkerConfig
	// Planner, when set, is consulted between the steps of every Run with
	// the Writer of the Session being driven: the application's in-turn
	// context policy.
	Planner driver.Planner
	// Responders answer ExternalResponse waits by the ToolRef whose calls
	// they answer; nil answers none.
	Responders map[run.ToolRef]driver.Responder
	// TargetResolver supplies the opaque resource target of each effect
	// (RUN-LOP-9). It belongs to the application's resource layer; nil
	// gives every effect no target (APP-TGT-1).
	TargetResolver loop.TargetResolver
	// Observers are notified of every group the Writers apply (EXT-WRT-7)
	// besides the event stream.
	Observers []writer.CommitObserver
	// Registry is the module registry every Writer, projection and event
	// stream of this Core decodes through. When nil, New builds one from
	// the first-party modules and Modules; a host that needs the Registry
	// before the Core exists builds it with NewRegistry and passes it here.
	Registry *module.Registry
	// Modules are application modules registered after the first-party
	// ones (EXT-APP); ignored when Registry is given.
	Modules []module.ModuleDescriptor
	// Clock stamps event times; nil selects time.Now.
	Clock func() time.Time
	// Cache stores folded projection states; nil asks the Store for a durable
	// cache and falls back to an in-memory one (APP-MEM-2).
	Cache session.ProjectionCache
	// CacheEvery bounds how far a cached projection may fall behind the head;
	// zero takes module.DefaultCacheEvery (EXT-PRJ-7).
	CacheEvery ledger.CommitSeq
	// Ownership configures how Writers open Sessions.
	Ownership session.OpenOptions
	// Fail receives failures of work the Core's components do outside any
	// caller's call, such as settling a reattached Outcome; nil discards
	// them.
	Fail func(session.SessionID, error)
	// OrphanProbe is how often an effect still waiting is attached and, when
	// orphaned, handed to RecoverExecution: by the Watcher of a live drive
	// and by the Reconciler of a takeover. Zero selects the defaults.
	OrphanProbe time.Duration
}

// WorkerConfig composes the execution Worker of this Core (RUN-EXE-8): the
// record store it owns its records in (durable, OWN-PRT-3), the Routes it
// routes effects to and its tuning options. Routes name the Executor port's
// backend as one of their Default routes when the port is supplied or
// remote. A nil Worker drives Ports.Executor directly.
type WorkerConfig struct {
	Executions executionstore.Store
	Routes     []executor.Route
	Options    executor.WorkerOptions
}

// Core is the composed core for a single-process host: the session kernel
// plus the execution side. Exported fields are the ports and core services;
// none is a product facade.
type Core struct {
	*sessionkernel.Kernel
	// Worker is the composed execution Worker, nil when the Core drives the
	// Executor port directly; it closes with the Core, after its services.
	Worker *executor.Worker
	// Watcher is the settlement subscription every Loop and Reconciler of
	// this Core waits on; a host component that waits for an effect of its
	// own (compaction's summary) shares it instead of subscribing again.
	Watcher *effect.Watcher
	// Driver drives an active Turn; Recovery runs the takeover disposition
	// when a Session opens and ends its listeners when it closes. Both sit
	// on the Loops and the Watcher the Core built and closes.
	Driver   *driver.Driver
	Recovery *driver.Recovery
	// Presets is the registry of decision identities the Loops resolve and
	// the Turn protocol starts Turns under.
	Presets  preset.Registry
	Executor effect.ExecutionPort
}

// New composes a Core from its ports: the session kernel assembles the
// durable semantic mechanism; the execution side assembles over it.
func New(p Ports) (*Core, error) { //nolint:gocritic // hugeParam: Ports is a by-value options struct read once
	if p.Executor == nil && p.Worker == nil {
		return nil, errors.New("core: an Executor port or a Worker is required")
	}
	if p.Decisions == nil {
		return nil, errors.New("core: a prompt builder catalog is required (Ports.Decisions); the core ships no default")
	}
	if p.MissingEffects == reconcile.RedispatchMissing && p.Redispatches == nil {
		return nil, errors.New("core: MissingEffects=redispatch requires a dispatch ledger (Ports.Redispatches, RUN-EXE-15)")
	}
	kernel, err := sessionkernel.New(sessionkernel.Ports{
		Store: p.Store, Content: p.Content, Artifacts: p.Artifacts,
		Observers: p.Observers, Registry: p.Registry, Modules: p.Modules,
		Clock: p.Clock, Cache: p.Cache, CacheEvery: p.CacheEvery, Ownership: p.Ownership,
	})
	if err != nil {
		return nil, err
	}
	executorPort := p.Executor
	var worker *executor.Worker
	if p.Worker != nil {
		if p.Worker.Executions == nil {
			return nil, errors.New("core: a composed Worker requires an execution record store (Ports.Worker.Executions)")
		}
		w, err := executor.NewWorker(context.Background(), p.Worker.Executions, p.Worker.Routes, p.Worker.Options)
		if err != nil {
			return nil, err
		}
		worker, executorPort = w, w
	}
	presets := p.Presets
	if presets == nil {
		presets = preset.NewMemory()
	}
	sink := busSink{kernel.Bus}
	a := &Core{Kernel: kernel, Worker: worker, Presets: presets, Executor: executorPort}
	a.Watcher = &effect.Watcher{Port: executorPort, Probe: p.OrphanProbe}
	// A nil resolver gives every effect no target (APP-TGT-1).
	loops := &driver.Loops{Executor: executorPort, Presets: presets, Decisions: p.Decisions, Targets: p.TargetResolver,
		Sources: decision.Sources{Projections: kernel.Projections, Content: kernel.Content}, Watcher: a.Watcher, Planner: p.Planner}
	a.Recovery = &driver.Recovery{Runs: kernel.Runs, Executor: executorPort, Loops: loops, Watcher: a.Watcher, Fail: p.Fail,
		MissingEffects: p.MissingEffects, Redispatches: p.Redispatches, OrphanProbe: p.OrphanProbe, Sink: sink}
	var responders *driver.Responders
	if len(p.Responders) > 0 {
		responders = &driver.Responders{Runs: kernel.Runs, Tools: p.Responders, Fail: p.Fail}
	}
	a.Driver = &driver.Driver{Runs: kernel.Runs, Loops: loops, Recovery: a.Recovery, Responders: responders, Sink: sink}
	return a, nil
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

// NewRegistry builds the module registry of a Core: the first-party
// modules as trusted core, extensions after them. Extensions cannot declare
// authoritative projections.
func NewRegistry(extensions []module.ModuleDescriptor) (*module.Registry, error) {
	return sessionkernel.NewRegistry(extensions)
}

// Close ends the execution side, then the session kernel: the recovery
// listeners, the settlement subscription, the Writers and, last, the
// Worker the drives may still be settling outcomes through. The owner
// releases the Sessions it holds before this.
func (a *Core) Close(ctx context.Context) error {
	a.Recovery.Close()
	a.Watcher.Close()
	err := a.Kernel.Close(ctx)
	if a.Worker != nil {
		a.Worker.Close()
	}
	return err
}
