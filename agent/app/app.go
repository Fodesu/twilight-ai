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
	"github.com/felinics/twilight/agent/authority"
	"github.com/felinics/twilight/agent/context/compaction"
	"github.com/felinics/twilight/agent/decision"
	"github.com/felinics/twilight/agent/executor/http"
	executorlocal "github.com/felinics/twilight/agent/executor/local"
	"github.com/felinics/twilight/agent/observe"
	"github.com/felinics/twilight/agent/preset"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/effect"
	"github.com/felinics/twilight/agent/run/loop"
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
	Port effect.Port

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
	Store     session.Store
	Content   artifact.ContentStore
	Artifacts authority.Artifacts

	Executor ExecutorConfig
	Presets  []Preset
	// Registry is the preset registry; nil selects an in-memory one.
	Registry preset.Registry
	// Decisions resolve each preset's PromptBuilderRef; nil selects the
	// default catalog.
	Decisions *decision.PromptBuilders

	Ownership      session.OpenOptions
	TargetResolver loop.TargetResolver
	// Modules are application modules registered after the first-party three
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
}

// CompactorSystemPrompt is kept here for deterministic model test doubles and
// applications that need to recognize the built-in compaction request.
const CompactorSystemPrompt = compaction.CompactorSystemPrompt

// ResumeAlreadyDriving reports that another driver already carries the
// inputs forward; the Result names the Turn that took them.
const ResumeAlreadyDriving = authority.ResumeAlreadyDriving

// Application is the composition root: the Authority plus the application's
// own services -- the preset table, the event stream and the spawn effect.
type Application struct {
	Authority *authority.Authority
	bus       *observe.Bus
	spawn     *spawn.Executor
	warn      func(error)

	mu   sync.RWMutex
	refs map[turn.PresetID]turn.PresetRef
}

// Build assembles an application from typed dependencies and a
// deployment-neutral executor profile.
func Build(c Config) (*Application, error) {
	content := c.Content
	if content == nil {
		var err error
		content, err = artifact.NewMemoryContentStore(runmod.FrozenAuthority, artifact.MemoryContentStoreOptions{})
		if err != nil {
			return nil, fmt.Errorf("app: create default content store: %w", err)
		}
	}
	port, err := buildExecutor(c.Executor)
	if err != nil {
		return nil, err
	}
	warn := c.Warn
	if warn == nil {
		warn = func(error) {}
	}
	app := &Application{warn: warn, refs: make(map[turn.PresetID]turn.PresetRef, len(c.Presets))}
	// The event stream is a CommitObserver on the Writers (EXT-WRT-7); it
	// needs the Registry, which the Authority builds, so the bus is wired
	// through a forwarding observer bound after New.
	var bus *observe.Bus
	observers := append([]writer.CommitObserver{forwardingObserver{&bus}}, c.Observers...)
	// The spawn effect claims its tool's Assignments before the deployment's
	// Executor sees them (SPN-1); it drives children through the
	// Authority, so it is bound after New as well.
	executor := port
	if c.Spawn != nil {
		app.spawn = spawn.NewExecutor(*c.Spawn)
		executor = spawn.Intercept(app.spawn, port)
	}
	a, err := authority.New(authority.Ports{
		Store: c.Store, Content: content, Artifacts: c.Artifacts, Presets: c.Registry, Decisions: c.Decisions,
		Executor: executor, TargetResolver: c.TargetResolver, Observers: observers, Modules: c.Modules,
		Clock: c.Clock, Cache: c.Cache, CacheEvery: c.CacheEvery, Ownership: c.Ownership, Fail: app.fail,
	})
	if err != nil {
		return nil, err
	}
	bus = observe.NewBus(a.Registry)
	app.Authority, app.bus = a, bus
	if app.spawn != nil {
		app.spawn.Bind(a)
	}
	for _, p := range c.Presets {
		if p.ID == "" {
			return nil, errors.New("app: preset requires an id")
		}
		if _, err := app.RegisterPreset(p.ID, p.Value); err != nil {
			return nil, fmt.Errorf("app: register preset %q: %w", p.ID, err)
		}
	}
	return app, nil
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
	ref, err := app.Authority.Presets.Register(id, p)
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
	return app.Authority.Close(ctx)
}

func buildExecutor(c ExecutorConfig) (effect.Port, error) {
	if c.Port != nil {
		return c.Port, nil
	}
	switch c.Mode {
	case "", ExecutorLocal:
		catalog, err := executorlocal.NewCatalog(c.Models, c.Tools...)
		if err != nil {
			return nil, err
		}
		return executorlocal.NewLocalExecutor(catalog, nil, false)
	case ExecutorRemote:
		if c.Endpoint == "" {
			return nil, errors.New("app: remote executor requires an endpoint")
		}
		return &http.Client{BaseURL: c.Endpoint, HTTP: c.HTTP}, nil
	default:
		return nil, fmt.Errorf("app: unknown executor mode %q", c.Mode)
	}
}

// Event is one item of a Session's event stream (OBS-1).
type Event = observe.Event

// ForkRequest forks a Session at one commit of its ledger (AUTH-FRK-1).
type ForkRequest = authority.ForkRequest
