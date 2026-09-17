// Package host is the deployment-neutral host layer over the agent core
// (docs/design/agent-host.md). It composes the fact layer (Store, Writers,
// Runtime, Coordinator), the decision layer (Presets, prompt builder and policy
// catalogs) and the effect layer (an Executor port) into one Host, and offers
// the Session facade on top. Whether the Executor runs effects in this process
// or forwards them to remote workers, whether a Store is memory, files or a
// database, and how a caller drives Turns are all choices made through Ports;
// the Host itself holds no model client, tool implementation or environment.
package host

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/felinics/twilight/agent/artifact"
	"github.com/felinics/twilight/agent/decision"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/extension"
	runmod "github.com/felinics/twilight/agent/session/run"
	"github.com/felinics/twilight/agent/session/writer"
	"github.com/felinics/twilight/agent/orchestration"
	"github.com/felinics/twilight/agent/turn"
)

// Artifacts groups the artifact ports; both may be nil while no committed
// group references an artifact (EXT-REF-2).
type Artifacts struct {
	Bindings artifact.BindingStore
	Ledger   artifact.RetentionLedger
}

// Ports are the roles a Host is composed from (HST-PRT-1). Every field is an
// interface or a core value: the Host never learns which implementation it
// got. Nil fields take the defaults documented on each.
type Ports struct {
	// Store is the Session kernel; nil selects an in-memory store.
	Store session.Store
	// Content is the cas ContentStore the frozen bodies live in under
	// runmod.FrozenAuthority (RUN-WIR-4): model requests, model results, tool
	// outputs and external responses. The Runtime writes them; the Host's
	// materializer reads them for prompts, replies and transcripts. Nil
	// selects an in-memory store.
	Content artifact.ContentStore
	// Artifacts are the binding store and retention ledger; nil fields select
	// in-memory implementations.
	Artifacts Artifacts
	// Presets is the authority-side registry of decision identities; nil
	// selects an in-memory registry. It holds no effect implementation.
	Presets PresetRegistry
	// Decisions resolve each preset's PromptBuilderRef (DEC-CAT); nil selects
	// decision.DefaultPromptBuilders().
	Decisions *decision.PromptBuilders
	// Executor is the effect layer port (RUN-EXE-3): required. A colocated
	// host passes NewLocalExecutor; a cloud host passes a remote client.
	Executor loop.Executor
	// TargetResolver supplies opaque per-Run resource targets. Workspace and
	// runtime semantics remain outside Agent Core.
	TargetResolver loop.TargetResolver
	// Observers are notified of every group the Host's Writers apply
	// (EXT-WRT-7); the Host's own event stream (Events) is one of them.
	Observers []writer.CommitObserver
	// Modules are application modules registered after the first-party three
	// (EXT-APP); each carries its own non-twilight Source.
	Modules []extension.ModuleDescriptor
	// Clock stamps event times; nil selects time.Now.
	Clock func() time.Time
	// Cache stores folded projection states; nil asks the Store for a durable
	// cache and falls back to an in-memory one (HST-MEM-2).
	Cache extension.ProjectionCache
	// CacheEvery bounds how far a cached projection may fall behind the head;
	// zero takes extension.DefaultCacheEvery (EXT-PRJ-7).
	CacheEvery session.CommitSeq
	// Ownership configures how Writers open Sessions: Takeover supersedes a
	// previous owner, whose writer is then fenced by its Epoch.
	Ownership session.OpenOptions
	// Warn receives failures of work the Host does outside any caller's call,
	// such as settling a reattached Outcome; nil discards them.
	Warn func(error)
	// Spawn enables the subagent effect (HST-SPN): the Host intercepts the
	// spawn tool's Assignments before Executor sees them and runs each as a
	// child Session it drives itself. Nil leaves the tool unavailable.
	Spawn *SpawnOptions
}

// Host is the composed authority process (HST-PRT-2). Exported fields are the
// ports and core interfaces callers may need; none is a concrete
// implementation.
type Host struct {
	Store       session.Store
	Writers     writer.Writers
	Runtime     run.Runtime
	Coordinator turn.Service
	Presets     PresetRegistry
	Executor    loop.Executor
	Decisions   *decision.PromptBuilders

	registry       *extension.Registry
	admission      writer.Admission
	frozen         run.FrozenValueStore
	content        *runmod.Content
	chatlog        *chatlog.Service
	bus            *eventBus
	now            func() time.Time
	warn           func(error)
	targetResolver loop.TargetResolver
	driver         *orchestration.Driver
	history        turn.History
}

// New composes a Host from its ports (HST-PRT-1).
func New(p Ports) (*Host, error) {
	if p.Executor == nil {
		return nil, errors.New("host: an Executor port is required")
	}
	store := p.Store
	if store == nil {
		store = session.NewMemoryStore()
	}
	modules := append([]extension.ModuleDescriptor{chatlog.Module, runmod.Module, turn.Module}, p.Modules...)
	registry, err := extension.BuildRegistry(session.ProtocolVersion1, modules...)
	if err != nil {
		return nil, err
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
	frozen, err := frozenValues(p.Content)
	if err != nil {
		return nil, err
	}
	now := p.Clock
	if now == nil {
		now = time.Now
	}
	bus := newEventBus(registry)
	observers := append([]writer.CommitObserver{bus}, p.Observers...)
	admission := writer.Admission{Bindings: bindings, Ledger: ledger}
	writers := writer.NewWriters(store, registry, admission, p.Ownership,
		writer.WritersConfig{Cache: cache, CachePolicy: runmod.WriterCachePolicy(p.CacheEvery), Observers: observers})
	runtime, err := runmod.NewRuntime(runmod.Config{
		Writers: writers, Registry: registry, Store: store,
		Frozen: frozen, Bindings: bindings, Cache: cache, Now: now,
	})
	if err != nil {
		return nil, err
	}
	presets := p.Presets
	if presets == nil {
		presets = NewPresets()
	}
	decisions := p.Decisions
	if decisions == nil {
		decisions = decision.DefaultPromptBuilders()
	}
	warn := p.Warn
	if warn == nil {
		warn = func(error) {}
	}
	h := &Host{
		Store: store, Writers: writers, Runtime: runtime, Presets: presets, Executor: p.Executor, Decisions: decisions,
		registry: registry, admission: admission, frozen: frozen, content: runmod.NewContent(frozen), chatlog: &chatlog.Service{Writers: writers, Now: now}, bus: bus, now: now, warn: warn,
		targetResolver: p.TargetResolver,
	}
	h.Coordinator = &turn.Coordinator{Writers: writers, Runtime: runtime, Now: now}
	h.history = turn.History{Store: store, Registry: registry, Surfaces: h.TurnSurface}
	h.driver = orchestration.New()
	executor := p.Executor
	if p.Spawn != nil {
		// The spawn effect intercepts its tool's Assignments before the
		// deployment's Executor sees them; Runs drive against the wrapped
		// port so a model's spawn call reaches the interceptor. Children
		// drive through the same Driver as any Turn, over narrow ports.
		executor = orchestration.NewSpawnExecutor(p.Executor, *p.Spawn, orchestration.SpawnPorts{
			Store: store, Registry: registry, Admission: admission, History: h.history,
			Surfaces: h, Chatlog: h.chatlog, Coordinator: h.Coordinator, Driver: h.driver,
			Writers: writers, Content: h.content, Now: now, NewTurnID: NewTurnID, NewInputID: NewInputID,
			Warn: warn,
		})
		h.Executor = executor
	}
	h.driver.Runtime, h.driver.Coordinator, h.driver.Executor = runtime, h.Coordinator, executor
	h.driver.Presets, h.driver.Decisions, h.driver.Sources = presets, decisions, h.sources()
	h.driver.Targets, h.driver.Surfaces, h.driver.Fail = p.TargetResolver, h.TurnSurface, h.fail
	return h, nil
}

// frozenValues is the run layer's view of the content store: nil selects an
// in-memory cas store under runmod.FrozenAuthority.
func frozenValues(content artifact.ContentStore) (run.FrozenValueStore, error) {
	if content == nil {
		return runmod.FrozenValuesInMemory(), nil
	}
	return runmod.FrozenValues(content), nil
}

// --- presets --------------------------------------------------------------------

// PresetRegistry is the authority-side registry of decision identities
// (HST-PST-1): an AgentPreset in, a digest-checked PresetRef out. It never holds a
// model client or a tool implementation; those live behind the Executor.
type PresetRegistry interface {
	Register(turn.PresetID, turn.AgentPreset) (turn.PresetRef, error)
	Resolve(turn.PresetRef) (turn.AgentPreset, error)
}

// ErrPresetUnavailable reports a PresetRef this process cannot resolve
// (HST-PST-2).
var ErrPresetUnavailable = errors.New("host: profile_unavailable")

// Presets is the in-memory PresetRegistry.
type Presets struct {
	mu    sync.RWMutex
	byRef map[turn.PresetRef]turn.AgentPreset
}

func NewPresets() *Presets { return &Presets{byRef: make(map[turn.PresetRef]turn.AgentPreset)} }

// Register validates and retains an immutable preset version (TRN-PST-2).
// Re-registration under the same ID retains previous digest-addressed versions.
func (r *Presets) Register(id turn.PresetID, p turn.AgentPreset) (turn.PresetRef, error) {
	if id == "" {
		return turn.PresetRef{}, errors.New("host: register requires a preset id")
	}
	if err := turn.ValidatePreset(&p); err != nil {
		return turn.PresetRef{}, err
	}
	digest, err := turn.DigestPreset(&p)
	if err != nil {
		return turn.PresetRef{}, err
	}
	ref := turn.PresetRef{ID: id, Digest: digest}
	r.mu.Lock()
	r.byRef[ref] = clonePreset(p)
	r.mu.Unlock()
	return ref, nil
}

// Resolve returns the AgentPreset when the ref's digest matches the registered
// one (TRN-PST-2).
func (r *Presets) Resolve(ref turn.PresetRef) (turn.AgentPreset, error) {
	r.mu.RLock()
	p, ok := r.byRef[ref]
	r.mu.RUnlock()
	if !ok {
		return turn.AgentPreset{}, fmt.Errorf("%w: unknown preset %s", ErrPresetUnavailable, ref.ID)
	}
	digest, err := turn.DigestPreset(&p)
	if err != nil {
		return turn.AgentPreset{}, err
	}
	if digest != ref.Digest {
		return turn.AgentPreset{}, fmt.Errorf("%w: preset %s digest mismatch", ErrPresetUnavailable, ref.ID)
	}
	return clonePreset(p), nil
}

func clonePreset(p turn.AgentPreset) turn.AgentPreset {
	p.Tools = slices.Clone(p.Tools)
	for i := range p.Tools {
		if cache := p.Tools[i].Definition.CacheControl; cache != nil {
			copy := *cache
			p.Tools[i].Definition.CacheControl = &copy
		}
	}
	return p
}

// --- driving ---------------------------------------------------------------------

// ResumeAlreadyDriving extends the turn disposition vocabulary for hosts:
// the inputs (if any) are committed and another local driver of the same
// Run carries them forward. The Coordinator itself never produces it.
const ResumeAlreadyDriving = orchestration.ResumeAlreadyDriving

// Drive is HST-DRV-1: while the Turn is active, drive the active attempt to
// the next quiescent point, then read the committed Status. It is the
// orchestration Driver's method with the Host's surfaces and ports.
func (h *Host) Drive(ctx context.Context, ref turn.TurnRef) (turn.TurnResponse, error) {
	return h.driver.Drive(ctx, ref)
}

// fail reports a failure of work the Host does outside any caller's call:
// to Ports.Warn and, as a host-level Event, to the Session's subscribers
// (HST-EVT-1).
func (h *Host) fail(sid session.SessionID, err error) {
	h.warn(err)
	h.bus.failed(sid, err)
}

// Open takes ownership of the Session and runs the takeover disposition
// (RUN-CMT-7). It returns the number of recovery commands issued.
func (h *Host) Open(ctx context.Context, sid session.SessionID) (int, error) {
	if _, err := h.Writers.Writer(ctx, sid); err != nil {
		return 0, err
	}
	return h.driver.Open(ctx, sid)
}

func (h *Host) stopRecovery(sid session.SessionID) { h.driver.Stop(sid) }

// Close stops recovery listeners, cancels the child drives of the spawn
// effect and releases every Session this Host owns.
func (h *Host) Close(ctx context.Context) error {
	h.driver.Close()
	if se, ok := h.Executor.(*orchestration.SpawnExecutor); ok {
		se.Close()
	}
	return writer.CloseWriters(ctx, h.Writers)
}

// --- sessions ---------------------------------------------------------------------

// CreateSession creates the Session stream.
func (h *Host) CreateSession(ctx context.Context, sid session.SessionID) error {
	_, err := h.Store.Create(ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: sid, CreatedAtUnixMilli: h.now().UnixMilli()})
	return err
}

// EnsureSession creates the stream when it does not exist yet. Create's
// idempotency needs field-identical requests, so existence is probed first.
func (h *Host) EnsureSession(ctx context.Context, sid session.SessionID) error {
	if _, err := h.Store.Header(ctx, sid); err == nil {
		return nil
	} else if !session.IsCode(err, session.ErrNotFound) {
		return err
	}
	if err := h.CreateSession(ctx, sid); err != nil {
		// A concurrent creator winning the race is still "exists".
		if _, herr := h.Store.Header(ctx, sid); herr == nil {
			return nil
		}
		return err
	}
	return nil
}

// SubmitInput writes twilight/chatlog/input_submitted for one user text and
// returns the AgentInput a Start or Deliver hands to the Turn (HST-INP-1).
func (h *Host) SubmitInput(ctx context.Context, sid session.SessionID, id run.InputID, text string) (run.AgentInput, error) {
	return h.chatlog.SubmitInput(ctx, sid, id, text)
}

// SubmitText submits one user text under a fresh InputID.
func (h *Host) SubmitText(ctx context.Context, sid session.SessionID, text string) (run.AgentInput, error) {
	return h.SubmitInput(ctx, sid, NewInputID(), text)
}

// Projection reads any registered projection through the Session's Writer;
// application modules read theirs here (HST-MEM-1).
func (h *Host) Projection(ctx context.Context, sid session.SessionID, id extension.ProjectionID, v extension.ProjectionVersion) (any, session.Head, error) {
	return h.projections().Load(ctx, sid, id, v)
}

// ChatlogSurface reads the chatlog surface projection.
func (h *Host) ChatlogSurface(ctx context.Context, sid session.SessionID) (chatlog.Surface, error) {
	state, _, err := h.Projection(ctx, sid, chatlog.SurfaceProjectionID, chatlog.SurfaceProjection.Version)
	if err != nil {
		return chatlog.Surface{}, err
	}
	return state.(chatlog.Surface), nil
}

// TurnSurface reads the turn surface projection.
func (h *Host) TurnSurface(ctx context.Context, sid session.SessionID) (turn.TurnSurface, error) {
	state, _, err := h.Projection(ctx, sid, turn.SurfaceProjectionID, turn.SurfaceProjection.Version)
	if err != nil {
		return turn.TurnSurface{}, err
	}
	return state.(turn.TurnSurface), nil
}

func (h *Host) projections() decision.ProjectionSource { return writersProjections{h.Writers} }

// sources are the prompt builder's read ports: projections through the
// Writer and frozen bodies through the content store (DEC-PMT-1).
func (h *Host) sources() decision.Sources {
	return decision.Sources{Projections: h.projections(), Content: h.content}
}

// Content is the materializer over the Host's frozen bodies (CHT-MAT-1):
// what renders a structural projection into text.
func (h *Host) Content() chatlog.ContentResolver { return h.content }

// writersProjections reads projections through the Session's Writer.
type writersProjections struct{ writers writer.Writers }

func (p writersProjections) Load(ctx context.Context, sid session.SessionID, id extension.ProjectionID, v extension.ProjectionVersion) (any, session.Head, error) {
	w, err := p.writers.Writer(ctx, sid)
	if err != nil {
		return nil, session.Head{}, err
	}
	return w.Projections().Load(ctx, sid, id, v)
}

// --- ids and sinks ---------------------------------------------------------------

// NewTurnID mints a collision-free TurnID.
func NewTurnID() turn.TurnID { return turn.TurnID("turn-" + randomHex(8)) }

// NewInputID mints a collision-free InputID; chatlog requires session-global
// uniqueness across restarts.
func NewInputID() run.InputID { return run.InputID("in-" + randomHex(8)) }

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("host: rand: " + err.Error())
	}
	return hex.EncodeToString(b)
}
