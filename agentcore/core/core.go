// Package core composes the agent core -- the fact layer (Store, Writers,
// SessionRunStore, Coordinator), the decision layer (preset registry and
// prompt builder catalog) and the effect layer (an Executor port, the drive
// chain over it) -- into one Core: the services a caller drives a Session
// with, and the Session lifecycle over the Store that needs no ownership
// (create, fork, collect, read). Ownership of a Session -- opening it for
// commands and releasing it -- is the owner package's. The Core is
// deployment-neutral and carries no product policy: what to send, whether
// to drive in the background and when to compact are the application's
// decisions.
package core

import (
	"context"
	"errors"
	"fmt"
	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/decision"
	"github.com/felinics/twilight/agentcore/driver"
	"github.com/felinics/twilight/agentcore/history"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/frozen"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/reconcile"
	"github.com/felinics/twilight/agentcore/run/redispatch"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	rt "github.com/felinics/twilight/agentcore/runtime"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
	"time"
)

// Artifacts groups the artifact ports (OWN-PRT-3): the binding index the
// Writers resolve against and the retention ledger their claims live in.
// Both are required and durable; the facts of a Session name frozen bodies
// through them, so they must survive every restart the facts survive. There
// is no memory implementation of either.
type Artifacts struct {
	Bindings artifact.BindingStore
	Ledger   artifact.RetentionLedger
}

// Ports are the roles a Core is composed from (OWN-PRT-1). Every field
// is an interface or a core value. The Store, the Executor, the Content
// store and both Artifacts are required and durable (OWN-PRT-3); the other
// nil fields take the in-process defaults documented on each.
type Ports struct {
	// Store is the Session kernel (required).
	Store session.Stores
	// Content is the cas ContentStore the frozen bodies live in under
	// sessionstore.FrozenAuthority (RUN-WIR-4). The run store writes them; the
	// materializer reads them for prompts, replies and transcripts.
	// Required.
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
	// Executor is the effect layer port (RUN-EXE-3): required.
	Executor effect.ExecutionPort
	// Planner, when set, is consulted between the steps of every Run with
	// the Writer of the Session being driven: the application's in-turn
	// context policy.
	Planner driver.Planner
	// Sink receives the drives' provisional observations (progress frames);
	// nil discards them.
	Sink loop.EventSink
	// Responders answer ExternalResponse waits by the ToolRef whose calls
	// they answer; nil answers none.
	Responders map[run.ToolRef]driver.Responder
	// TargetResolver supplies the opaque resource target of each effect
	// (RUN-LOP-9). It belongs to the application's resource layer; nil
	// gives every effect no target (APP-TGT-1).
	TargetResolver loop.TargetResolver
	// Observers are notified of every group the Writers apply (EXT-WRT-7).
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

// Core is the composed core. Exported fields are the ports
// and core services; none is a product facade.
type Core struct {
	Store     session.Stores
	Writers   writer.Writers
	Registry  *module.Registry
	Admission writer.Admission
	// Runs is the Run module's Session adapter: the Run core's store bound
	// per Writer, Run reads by SessionID and the Run Parts of Turn units.
	Runs *sessionstore.SessionRunStore
	// Turns commits the Turn protocol and reads Turn status.
	Turns *rt.Coordinator
	// Driver drives an active Turn; Recovery runs the takeover disposition
	// when a Session opens and ends its listeners when it closes. Both sit
	// on the Loops and the Watcher the Core built and closes.
	Driver   *driver.Driver
	Recovery *driver.Recovery
	// Watcher is the settlement subscription every Loop and Reconciler of
	// this Core waits on; a host component that waits for an effect of its
	// own (compaction's summary) shares it instead of subscribing again.
	Watcher  *effect.Watcher
	Presets  preset.Registry
	Executor effect.ExecutionPort
	Frozen   frozen.Store
	// Projections reads every projection through the Session's Writer.
	Projections session.ProjectionReader
	// Content materializes the frozen bodies projections name (CHT-MAT-1).
	Content chatlog.ContentResolver
	// Chatlog commits the chatlog's own facts (APP-INP-1, APP-CKP-1).
	Chatlog *chatlog.Commands
	// History answers fork-boundary questions.
	History history.History
	Clock   func() time.Time
}

// New composes a Core from its ports.
func New(p Ports) (*Core, error) { //nolint:gocritic // hugeParam: Ports is a by-value options struct read once
	if p.Executor == nil {
		return nil, errors.New("core: an Executor port is required")
	}
	if p.Store == nil {
		return nil, errors.New("core: a session Store is required")
	}
	if p.Content == nil {
		return nil, errors.New("core: a content Store is required (OWN-PRT-3)")
	}
	if p.Artifacts.Bindings == nil || p.Artifacts.Ledger == nil {
		return nil, errors.New("core: a binding store and a retention ledger are required (OWN-PRT-3)")
	}
	if p.MissingEffects == reconcile.RedispatchMissing && p.Redispatches == nil {
		return nil, errors.New("core: MissingEffects=redispatch requires a dispatch ledger (Ports.Redispatches, RUN-EXE-15)")
	}
	store := p.Store
	registry := p.Registry
	if registry == nil {
		var err error
		if registry, err = NewRegistry(p.Modules); err != nil {
			return nil, err
		}
	}
	bindings, retention := p.Artifacts.Bindings, p.Artifacts.Ledger
	// A projection cache lets a reopened Session start folding instead of
	// refolding the whole log (EXT-PRJ-3). An adapter that can store entries
	// durably provides its own; otherwise they live as long as the process.
	cache := p.Cache
	if cache == nil {
		if provider, ok := store.(session.ProjectionCacheProvider); ok {
			cache = provider.ProjectionCache()
		} else {
			cache = session.NewMemoryProjectionCache()
		}
	}
	// Frozen bodies live in the content store and are admitted through the
	// same binding store the Writers resolve against (RUN-WIR-4).
	fz, err := sessionstore.FrozenValues(p.Content, bindings)
	if err != nil {
		return nil, err
	}
	now := p.Clock
	if now == nil {
		now = time.Now
	}
	admission := writer.Admission{Bindings: bindings, Ledger: retention}
	writers := writer.NewWriters(store, registry, admission, p.Ownership,
		writer.WritersConfig{Cache: cache, CachePolicy: sessionstore.WriterCachePolicy(p.CacheEvery), Observers: p.Observers})
	runs, err := sessionstore.NewSessionRunStore(sessionstore.Config{Registry: registry, Store: store, Frozen: fz, Cache: cache, Now: now})
	if err != nil {
		return nil, err
	}
	presets := p.Presets
	if presets == nil {
		presets = preset.NewMemory()
	}
	decisions := p.Decisions
	if decisions == nil {
		return nil, errors.New("core: a prompt builder catalog is required (Ports.Decisions); the core ships no default")
	}
	// The read model folds from the Store through the cache: reading a Session
	// takes no ownership (OWN-HDL-2). The Writer keeps its own transactional
	// projections for the commit critical section.
	projections := session.NewProjectionReader(store, registry, cache)
	content := sessionstore.NewContent(fz)
	a := &Core{
		Store: store, Writers: writers, Registry: registry, Admission: admission, Runs: runs,
		Turns:   &rt.Coordinator{Projections: projections, Runs: runs, Now: now},
		Presets: presets, Executor: p.Executor, Frozen: fz, Projections: projections, Content: content,
		Chatlog: &chatlog.Commands{Now: now},
		History: history.History{Store: store, Registry: registry, Projections: projections},
		Clock:   now,
	}
	a.Watcher = &effect.Watcher{Port: p.Executor, Probe: p.OrphanProbe}
	// A nil resolver gives every effect no target (APP-TGT-1).
	loops := &driver.Loops{Executor: p.Executor, Presets: presets, Decisions: decisions, Targets: p.TargetResolver,
		Sources: decision.Sources{Projections: projections, Content: content}, Watcher: a.Watcher, Planner: p.Planner}
	a.Recovery = &driver.Recovery{Runs: runs, Executor: p.Executor, Loops: loops, Watcher: a.Watcher, Fail: p.Fail,
		MissingEffects: p.MissingEffects, Redispatches: p.Redispatches, OrphanProbe: p.OrphanProbe, Sink: p.Sink}
	var responders *driver.Responders
	if len(p.Responders) > 0 {
		responders = &driver.Responders{Runs: runs, Tools: p.Responders, Fail: p.Fail}
	}
	a.Driver = &driver.Driver{Runs: runs, Loops: loops, Recovery: a.Recovery, Responders: responders, Sink: p.Sink}
	return a, nil
}

// NewRegistry builds the module registry of a Core: the first-party
// modules as trusted core, extensions after them. Extensions cannot declare
// authoritative projections.
func NewRegistry(extensions []module.ModuleDescriptor) (*module.Registry, error) {
	return module.BuildRegistryWithExtensions(
		[]module.ModuleDescriptor{chatlog.Module, sessionstore.Module, turn.Module}, extensions)
}

// Close ends every Session's recovery listeners, the settlement
// subscription and the Writers. The owner releases the Sessions it holds
// before this.
func (a *Core) Close(ctx context.Context) error {
	a.Recovery.Close()
	a.Watcher.Close()
	return writer.CloseWriters(ctx, a.Writers)
}

// CreateSession creates the Session; ext are the segment's module extension
// slots (nil for none), carried opaquely by the kernel (SES-WIR-5).
func (a *Core) CreateSession(ctx context.Context, sid session.SessionID, ext module.Extensions) error {
	_, err := a.Store.Create(ctx, session.CreateRequest{SessionID: sid, CreatedAtUnixMilli: a.Clock().UnixMilli(), Ext: ext})
	return err
}

// EnsureSession makes sure the Session exists, whatever record created it:
// a root made here, a fork or a spawned child all count. Create alone would
// refuse a Session whose segment fields differ (SES-CRT-1), so existence is
// probed first.
func (a *Core) EnsureSession(ctx context.Context, sid session.SessionID) error {
	if _, err := a.Store.Header(ctx, sid); err == nil {
		return nil
	} else if !session.IsCode(err, session.ErrNotFound) {
		return err
	}
	if err := a.CreateSession(ctx, sid, nil); err != nil {
		// A concurrent creator winning the race is still "exists".
		if _, herr := a.Store.Header(ctx, sid); herr == nil {
			return nil
		}
		return err
	}
	return nil
}

// ForkRequest forks a Session at one commit of its ledger (OWN-FRK-1): the
// child inherits every commit of Parent up to and including At and continues
// from there under its own identity.
type ForkRequest struct {
	Parent session.SessionID
	At     ledger.CommitSeq
	Child  session.SessionID
	// Ext are the child segment's module extension slots (SES-WIR-5).
	Ext module.Extensions
}

// Fork creates the child Session (SES-FRK-1) and claims the artifacts its
// inherited prefix references (EXT-WRT-8). The child is not opened.
func (a *Core) Fork(ctx context.Context, req ForkRequest) (session.SegmentHeader, error) {
	if req.Parent == "" || req.Child == "" {
		return session.SegmentHeader{}, errors.New("core: fork requires parent and child session ids")
	}
	if req.Parent == req.Child {
		return session.SegmentHeader{}, errors.New("core: a session cannot fork itself")
	}
	// A fork point inside a Turn would hand the child a Turn whose Run is
	// the parent's execution (SES-FRK-5): semantic history branches only at
	// quiescent points (OWN-FRK-1).
	if active, ok, err := a.History.ActiveAt(ctx, req.Parent, req.At); err != nil {
		return session.SegmentHeader{}, err
	} else if ok {
		return session.SegmentHeader{}, &session.Error{Code: session.ErrInvalid, Operation: "fork", SessionID: req.Child,
			Detail: fmt.Sprintf("turn %s of %s is active at commit %d; fork at a quiescent point", active, req.Parent, req.At)}
	}
	return writer.Fork(ctx, a.Store, a.Registry, writer.ForkRequest{
		Parent: req.Parent, At: req.At, Child: req.Child, CreatedAtUnixMilli: a.Clock().UnixMilli(), Ext: req.Ext,
	})
}

// ForkBeforeTurn forks Parent at the commit just before turnID started
// (OWN-FRK-2): the child holds the conversation as it was when that Turn's
// inputs were still submitted and undelivered.
func (a *Core) ForkBeforeTurn(ctx context.Context, parent session.SessionID, turnID turn.TurnID, child session.SessionID) (session.SegmentHeader, error) {
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

// Collect reclaims the storage of deleted Sessions no live Session reaches
// (SES-GC-2) and releases the claims of the commits it reclaimed (SES-GC-3).
func (a *Core) Collect(ctx context.Context) (session.CollectReport, error) {
	return writer.Collect(ctx, a.Store, a.Admission)
}

// --- reads by SessionID ----------------------------------------------------------------

// Reading a Session needs no ownership: projections are queried by identity.
// Commands take the Writer of an open owner.Handle.

// Projection reads any registered projection through the Session's Writer
// (APP-MEM-1).
func (a *Core) Projection(ctx context.Context, sid session.SessionID, id module.ProjectionID, v module.ProjectionVersion) (any, ledger.Head, error) {
	return a.Projections.Load(ctx, sid, id, v)
}
