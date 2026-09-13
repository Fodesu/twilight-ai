// Package host is the deployment-neutral host layer over the agent core
// (docs/design/agent-host.md). It composes the fact layer (Store, Writers,
// Runtime, Coordinator), the decision layer (Profiles, planner and policy
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
	// Frozen holds model request bodies by digest (RUN-WIR-4); nil selects an
	// in-memory store. Executors read it; the Runtime writes it.
	Frozen run.FrozenValueStore
	// Artifacts are the binding store and retention ledger; nil fields select
	// in-memory implementations.
	Artifacts Artifacts
	// Profiles is the authority-side registry of decision identities; nil
	// selects an in-memory registry. It holds no effect implementation.
	Profiles ProfileRegistry
	// Decisions resolve PlannerRef and PolicyRef (DEC-CAT); the zero value
	// selects decision.DefaultCatalogs().
	Decisions decision.Catalogs
	// Executor is the effect layer port (RUN-EXE-3): required. A colocated
	// host passes NewLocalExecutor; a cloud host passes a remote client.
	Executor loop.Executor
	// Observers receive Loop observations; nil discards them.
	Observers []loop.EventSink
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
	CacheEvery session.Seq
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
	Profiles    ProfileRegistry
	Executor    loop.Executor
	Decisions   decision.Catalogs

	registry *extension.Registry
	frozen   run.FrozenValueStore
	sink     loop.EventSink
	now      func() time.Time
	warn     func(error)

	mu    sync.Mutex
	loops map[turn.ProfileRef]*loop.Loop
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
	frozen := p.Frozen
	if frozen == nil {
		frozen = run.NewMemoryFrozenValues()
	}
	now := p.Clock
	if now == nil {
		now = time.Now
	}
	writers := writer.NewWriters(store, registry, writer.Admission{Bindings: bindings, Ledger: ledger}, p.Ownership,
		writer.WritersConfig{Cache: cache, CachePolicy: runmod.WriterCachePolicy(p.CacheEvery)})
	runtime, err := runmod.NewRuntime(runmod.Config{
		Writers: writers, Registry: registry, Store: store,
		Frozen: frozen, Companion: turn.CompanionV1{}, Cache: cache, Now: now,
	})
	if err != nil {
		return nil, err
	}
	profiles := p.Profiles
	if profiles == nil {
		profiles = NewProfiles()
	}
	decisions := p.Decisions
	if decisions.Planners == nil && decisions.Policies == nil {
		decisions = decision.DefaultCatalogs()
	}
	warn := p.Warn
	if warn == nil {
		warn = func(error) {}
	}
	h := &Host{
		Store: store, Writers: writers, Runtime: runtime, Profiles: profiles, Executor: p.Executor, Decisions: decisions,
		registry: registry, frozen: frozen, sink: fanOut(p.Observers), now: now, warn: warn, loops: make(map[turn.ProfileRef]*loop.Loop),
	}
	h.Coordinator = &turn.Coordinator{Writers: writers, Runtime: runtime, Now: now}
	return h, nil
}

// --- profiles --------------------------------------------------------------------

// ProfileRegistry is the authority-side registry of decision identities
// (HST-PRF-1): a Profile in, a digest-checked ProfileRef out. It never holds a
// model client or a tool implementation; those live behind the Executor.
type ProfileRegistry interface {
	Register(turn.ProfileID, turn.Profile) (turn.ProfileRef, error)
	Resolve(turn.ProfileRef) (turn.Profile, error)
}

// ErrProfileUnavailable reports a ProfileRef this process cannot resolve: not
// registered, or registered with a different digest (HST-PRF-2).
var ErrProfileUnavailable = errors.New("host: profile_unavailable")

// Profiles is the in-memory ProfileRegistry.
type Profiles struct {
	mu   sync.RWMutex
	byID map[turn.ProfileID]turn.Profile
}

func NewProfiles() *Profiles { return &Profiles{byID: make(map[turn.ProfileID]turn.Profile)} }

// Register validates the Profile (TRN-PRF-2) and records it under id. A
// re-registration replaces the Profile; refs recorded under the previous
// digest stop resolving.
func (r *Profiles) Register(id turn.ProfileID, p turn.Profile) (turn.ProfileRef, error) {
	if id == "" {
		return turn.ProfileRef{}, errors.New("host: register requires a profile id")
	}
	if err := turn.ValidateProfile(&p); err != nil {
		return turn.ProfileRef{}, err
	}
	digest, err := turn.DigestProfile(&p)
	if err != nil {
		return turn.ProfileRef{}, err
	}
	r.mu.Lock()
	r.byID[id] = p
	r.mu.Unlock()
	return turn.ProfileRef{ID: id, Digest: digest}, nil
}

// Resolve returns the Profile when the ref's digest matches the registered
// one (TRN-PRF-2).
func (r *Profiles) Resolve(ref turn.ProfileRef) (turn.Profile, error) {
	r.mu.RLock()
	p, ok := r.byID[ref.ID]
	r.mu.RUnlock()
	if !ok {
		return turn.Profile{}, fmt.Errorf("%w: unknown profile %s", ErrProfileUnavailable, ref.ID)
	}
	digest, err := turn.DigestProfile(&p)
	if err != nil {
		return turn.Profile{}, err
	}
	if digest != ref.Digest {
		return turn.Profile{}, fmt.Errorf("%w: profile %s digest mismatch", ErrProfileUnavailable, ref.ID)
	}
	return p, nil
}

// --- driving ---------------------------------------------------------------------

// ResumeAlreadyDriving extends the turn disposition vocabulary for hosts: the
// inputs (if any) are committed and another local driver of the same Run
// carries them forward. The Coordinator itself never produces it.
const ResumeAlreadyDriving turn.ResumeDisposition = "already_driving"

// loopFor returns the Loop that drives Runs of one Profile. A Loop binds the
// Profile's planner and policy to the shared Executor; it is built once per
// ProfileRef so every drive of a Run meets the same already-driving guard
// (HST-DRV-2).
func (h *Host) loopFor(ref turn.ProfileRef) (*loop.Loop, turn.Profile, error) {
	profile, err := h.Profiles.Resolve(ref)
	if err != nil {
		return nil, turn.Profile{}, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if l, ok := h.loops[ref]; ok {
		return l, profile, nil
	}
	planner, policy, err := h.Decisions.Resolve(profile, h.projections())
	if err != nil {
		return nil, turn.Profile{}, err
	}
	l, err := loop.New(h.Executor, planner, policy)
	if err != nil {
		return nil, turn.Profile{}, err
	}
	h.loops[ref] = l
	return l, profile, nil
}

// Drive is HST-DRV-1: while the Turn is active, resolve its recorded profile
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
		l, _, err := h.loopFor(view.Profile)
		if err != nil {
			return turn.TurnResponse{}, err
		}
		if _, err := l.Run(ctx, h.Runtime, ref.SessionID, view.ActiveRun, h.sink); err != nil {
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
	}
	return h.Coordinator.Status(ctx, ref)
}

// reattachDeliver is the glue a takeover hands the Executor (RUN-CMT-7): an
// Outcome of an attempt that survived the previous owner is settled through
// the Loop of the Turn that owns its Run, and the Run is driven on from there.
func (h *Host) reattachDeliver(sid session.SessionID) loop.Deliver {
	return func(out loop.Outcome) {
		ctx := context.Background()
		surface, err := h.TurnSurface(ctx, sid)
		if err != nil {
			h.warn(fmt.Errorf("host: reattached outcome for run %s: %w", out.Key.RunID, err))
			return
		}
		turnID, ok := surface.RunOwner[out.Key.RunID]
		if !ok {
			h.warn(fmt.Errorf("host: reattached outcome for run %s: no owning turn", out.Key.RunID))
			return
		}
		l, _, err := h.loopFor(surface.Turns[turnID].Profile)
		if err != nil {
			h.warn(fmt.Errorf("host: reattached outcome for run %s: %w", out.Key.RunID, err))
			return
		}
		res, err := l.Deliver(ctx, h.Runtime, sid, out, h.sink)
		if err != nil {
			h.warn(fmt.Errorf("host: settling reattached outcome for run %s: %w", out.Key.RunID, err))
			return
		}
		if res.Disposition != loop.LoopDelivered {
			return
		}
		if _, err := l.Run(ctx, h.Runtime, sid, out.Key.RunID, h.sink); err != nil && !errors.Is(err, loop.ErrRunAlreadyRunning) {
			h.warn(fmt.Errorf("host: driving run %s after a reattached outcome: %w", out.Key.RunID, err))
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

// Open takes ownership of the Session and runs the takeover disposition
// (RUN-CMT-7): every Executing target is first offered to the Executor for
// reattachment and disposed only when no running attempt answers. It returns
// the number of recovery commands issued.
func (h *Host) Open(ctx context.Context, sid session.SessionID) (int, error) {
	if _, err := h.Writers.Writer(ctx, sid); err != nil {
		return 0, err
	}
	return h.Runtime.RecoverInterrupted(ctx, sid, loop.Reattach(h.Executor, sid, h.reattachDeliver(sid)))
}

// Close releases every Session this Host owns.
func (h *Host) Close(ctx context.Context) error { return writer.CloseWriters(ctx, h.Writers) }

// SubmitInput writes twilight/chatlog/input_submitted for one user text and
// returns the AgentInput a Start or Deliver hands to the Turn (HST-INP-1).
func (h *Host) SubmitInput(ctx context.Context, sid session.SessionID, id run.InputID, text string) (run.AgentInput, error) {
	content := decision.InputContent(text)
	w, err := h.Writers.Writer(ctx, sid)
	if err != nil {
		return run.AgentInput{}, err
	}
	res, err := w.Commit(ctx, func(writer.View) (*writer.SemanticGroup, error) {
		return &writer.SemanticGroup{CommitID: session.CommitID("input-submitted/" + string(id)), Events: []writer.TypedEvent{{
			Type: chatlog.TypeInputSubmitted, RecordedAtUnixMilli: h.now().UnixMilli(),
			Value: chatlog.InputSubmittedPayload{InputID: chatlog.InputID(id), Content: content, SubmittedAtUnixMilli: h.now().UnixMilli()},
		}}}, nil
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

// fanOut delivers every observation to each sink; nil when there is none.
func fanOut(sinks []loop.EventSink) loop.EventSink {
	var live []loop.EventSink
	for _, s := range sinks {
		if s != nil {
			live = append(live, s)
		}
	}
	switch len(live) {
	case 0:
		return nil
	case 1:
		return live[0]
	}
	return multiSink(live)
}

type multiSink []loop.EventSink

func (m multiSink) Emit(ctx context.Context, e loop.Event) error {
	var first error
	for _, s := range m {
		if err := s.Emit(ctx, e); err != nil && first == nil {
			first = err
		}
	}
	return first
}
