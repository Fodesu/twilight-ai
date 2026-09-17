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
	bus            *eventBus
	now            func() time.Time
	warn           func(error)
	targetResolver loop.TargetResolver

	mu       sync.Mutex
	loops    map[turn.PresetRef]*loop.Loop
	recovery map[session.SessionID]*recoveryLifetime
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
		registry: registry, admission: admission, frozen: frozen, content: runmod.NewContent(frozen), bus: bus, now: now, warn: warn,
		targetResolver: p.TargetResolver, loops: make(map[turn.PresetRef]*loop.Loop),
		recovery: make(map[session.SessionID]*recoveryLifetime),
	}
	h.Coordinator = &turn.Coordinator{Writers: writers, Runtime: runtime, Now: now}
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

// ResumeAlreadyDriving extends the turn disposition vocabulary for hosts: the
// inputs (if any) are committed and another local driver of the same Run
// carries them forward. The Coordinator itself never produces it.
const ResumeAlreadyDriving turn.ResumeDisposition = "already_driving"

// loopFor returns the Loop that drives Runs of one AgentPreset. A Loop binds
// the preset's prompt builder and settings to the shared Executor; it is
// built once per PresetRef so every drive of a Run meets the same
// already-driving guard (HST-DRV-2).
func (h *Host) loopFor(ref turn.PresetRef) (*loop.Loop, turn.AgentPreset, error) {
	preset, err := h.Presets.Resolve(ref)
	if err != nil {
		return nil, turn.AgentPreset{}, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if l, ok := h.loops[ref]; ok {
		return l, preset, nil
	}
	builder, err := h.Decisions.Resolve(preset, h.sources())
	if err != nil {
		return nil, turn.AgentPreset{}, err
	}
	l, err := loop.New(h.Executor, builder, loop.Settings{
		Scheduling:       preset.Scheduling,
		MalformedRetries: preset.MalformedRetries,
		TargetResolver:   h.targetResolver,
	})
	if err != nil {
		return nil, turn.AgentPreset{}, err
	}
	h.loops[ref] = l
	return l, preset, nil
}

// Drive is HST-DRV-1: while the Turn is active, resolve its recorded preset
// and drive the active attempt to the next quiescent point, then read the
// committed Status. The caller's ctx bounds the drive, so cancellation is a
// host decision. A concurrent local driver of the same Run yields
// ResumeAlreadyDriving.
func (h *Host) Drive(ctx context.Context, ref turn.TurnRef) (turn.TurnResponse, error) {
	surface, err := h.TurnSurface(ctx, ref.SessionID)
	if err != nil {
		return turn.TurnResponse{}, err
	}
	view, ok := surface.Turns[ref.TurnID]
	if !ok {
		return turn.TurnResponse{}, fmt.Errorf("%w: unknown turn %s", turn.ErrConflict, ref.TurnID)
	}
	if view.Status == turn.TurnActive {
		l, _, err := h.loopFor(view.Preset)
		if err != nil {
			return turn.TurnResponse{}, err
		}
		res, err := l.Run(ctx, h.Runtime, ref.SessionID, view.ActiveRun, nil)
		if err != nil {
			if errors.Is(err, loop.ErrRunAlreadyRunning) {
				resp, rerr := h.Coordinator.Status(ctx, ref)
				if rerr != nil {
					return turn.TurnResponse{}, rerr
				}
				resp.Disposition = ResumeAlreadyDriving
				return resp, nil
			}
			return turn.TurnResponse{}, err
		}
		if res.ExecutionRecovery {
			// The drive quiesced with executions in flight and no local
			// waiter. Offer every Executing target reattachment and dispose
			// what no executor answers, instead of leaving the Turn to a
			// driver that already returned (RUN-CMT-7).
			if _, err := h.recoverInterrupted(context.WithoutCancel(ctx), ref.SessionID); err != nil {
				h.fail(ref.SessionID, fmt.Errorf("host: recovering a quiesced drive: %w", err))
			}
		}
	}
	return h.Coordinator.Status(ctx, ref)
}

// fail reports a failure of work the Host does outside any caller's call:
// to Ports.Warn and, as a host-level Event, to the Session's subscribers
// (HST-EVT-1).
func (h *Host) fail(sid session.SessionID, err error) {
	h.warn(err)
	h.bus.failed(sid, err)
}

// reattachDeliver is the glue a takeover hands the Executor (RUN-CMT-7): an
// Outcome of an attempt that survived the previous owner is settled through
// the Loop of the Turn that owns its Run, and the Run is driven on from there.
func (h *Host) reattachDeliver(ctx context.Context, sid session.SessionID) loop.Deliver {
	return func(out loop.Outcome) {
		if ctx.Err() != nil {
			return
		}
		surface, err := h.TurnSurface(ctx, sid)
		if err != nil {
			h.fail(sid, fmt.Errorf("host: reattached outcome for run %s: %w", out.Key.RunID, err))
			return
		}
		turnID, ok := surface.RunOwner[out.Key.RunID]
		if !ok {
			h.fail(sid, fmt.Errorf("host: reattached outcome for run %s: no owning turn", out.Key.RunID))
			return
		}
		l, _, err := h.loopFor(surface.Turns[turnID].Preset)
		if err != nil {
			h.fail(sid, fmt.Errorf("host: reattached outcome for run %s: %w", out.Key.RunID, err))
			return
		}
		res, err := l.Deliver(ctx, h.Runtime, sid, out, nil)
		if err != nil {
			h.fail(sid, fmt.Errorf("host: settling reattached outcome for run %s: %w", out.Key.RunID, err))
			return
		}
		if res.Disposition != loop.LoopDelivered {
			return
		}
		if _, err := l.Run(ctx, h.Runtime, sid, out.Key.RunID, nil); err != nil && !errors.Is(err, loop.ErrRunAlreadyRunning) {
			h.fail(sid, fmt.Errorf("host: driving run %s after a reattached outcome: %w", out.Key.RunID, err))
		}
	}
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

// recoveryLifetime is the detached context the Session's recovery goroutines
// -- reattached outcome reads -- live under, and the cancel that stops them.
type recoveryLifetime struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// installRecoveryLifetimeLocked replaces the Session's recovery lifetime with
// one derived from parent, stopping the previous listeners. Callers hold h.mu.
func (h *Host) installRecoveryLifetimeLocked(sid session.SessionID, parent context.Context) *recoveryLifetime {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	if previous := h.recovery[sid]; previous != nil {
		previous.cancel()
	}
	lt := &recoveryLifetime{ctx: ctx, cancel: cancel}
	h.recovery[sid] = lt
	return lt
}

// ensureRecoveryLifetime returns the Session's recovery lifetime, installing a
// detached one when absent. Open replaces it instead: a takeover supersedes
// the previous owner's listeners.
func (h *Host) ensureRecoveryLifetime(sid session.SessionID) *recoveryLifetime {
	h.mu.Lock()
	defer h.mu.Unlock()
	if lt, ok := h.recovery[sid]; ok {
		return lt
	}
	return h.installRecoveryLifetimeLocked(sid, context.Background())
}

// recoverInterrupted runs the takeover disposition (RUN-CMT-7) for the
// Session: every Executing target is offered to the Executor for reattachment
// and disposed only when no running attempt answers. It is the explicit
// recovery behind an unknown dispatch boundary -- the drive kept the call
// Executing, so the durable record, not a duplicate dispatch, decides the
// settlement.
func (h *Host) recoverInterrupted(ctx context.Context, sid session.SessionID) (int, error) {
	lt := h.ensureRecoveryLifetime(sid)
	n, err := h.Runtime.RecoverInterrupted(ctx, sid, loop.Reattach(lt.ctx, h.Executor, sid, h.reattachDeliver(lt.ctx, sid)))
	return n, err
}

// Open takes ownership of the Session and runs the takeover disposition
// (RUN-CMT-7): every Executing target is first offered to the Executor for
// reattachment and disposed only when no running attempt answers. It returns
// the number of recovery commands issued.
func (h *Host) Open(ctx context.Context, sid session.SessionID) (int, error) {
	if _, err := h.Writers.Writer(ctx, sid); err != nil {
		return 0, err
	}
	h.mu.Lock()
	h.installRecoveryLifetimeLocked(sid, ctx)
	h.mu.Unlock()
	n, err := h.recoverInterrupted(ctx, sid)
	if err != nil {
		h.stopRecovery(sid)
	}
	return n, err
}

func (h *Host) stopRecovery(sid session.SessionID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if lt := h.recovery[sid]; lt != nil {
		lt.cancel()
		delete(h.recovery, sid)
	}
}

// Close stops recovery listeners and releases every Session this Host owns.
func (h *Host) Close(ctx context.Context) error {
	h.mu.Lock()
	for sid, lt := range h.recovery {
		lt.cancel()
		delete(h.recovery, sid)
	}
	h.mu.Unlock()
	return writer.CloseWriters(ctx, h.Writers)
}

// SubmitInput writes twilight/chatlog/input_submitted for one user text and
// returns the AgentInput a Start or Deliver hands to the Turn (HST-INP-1).
func (h *Host) SubmitInput(ctx context.Context, sid session.SessionID, id run.InputID, text string) (run.AgentInput, error) {
	content := decision.InputContent(text)
	w, err := h.Writers.Writer(ctx, sid)
	if err != nil {
		return run.AgentInput{}, err
	}
	res, err := w.Commit(ctx, func(writer.View) (*writer.SemanticGroup, error) {
		return &writer.SemanticGroup{CommitID: session.CommitID("input-submitted/" + string(id)),
			Batches: []writer.TypedBatch{{Stream: session.StreamRef{Kind: session.StreamKindSession}, Events: []writer.TypedEvent{{
				Type: chatlog.TypeInputSubmitted, RecordedAtUnixMilli: h.now().UnixMilli(),
				Value: chatlog.InputSubmittedPayload{InputID: chatlog.InputID(id), Content: content, SubmittedAtUnixMilli: h.now().UnixMilli()},
			}}}}}, nil
	})
	if err != nil {
		return run.AgentInput{}, err
	}
	switch res.Outcome {
	case writer.CommitApplied, writer.CommitAlreadyApplied:
		return run.AgentInput{ID: id, Payload: content}, nil
	default:
		return run.AgentInput{}, fmt.Errorf("host: submit input: %s: %s", res.Outcome, res.Detail)
	}
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
