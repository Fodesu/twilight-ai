package ref

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/felinics/twilight/agent/artifact"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/extension"
	runmod "github.com/felinics/twilight/agent/session/run"
	"github.com/felinics/twilight/agent/turn"
)

// Options tunes the Memory assembly.
type Options struct {
	// Ownership configures the Session Writer: Takeover lets this assembly
	// supersede a previous owner, whose writer is then fenced by its Epoch.
	Ownership session.OpenOptions
	Now       func() time.Time
	// Frozen shares request bodies between "processes" in tests; nil creates one.
	Frozen run.FrozenValueStore
	// Store shares the Session store between assemblies; nil creates one.
	Store session.Store
	// Ledger shares the retention ledger between assemblies; nil creates one.
	Ledger artifact.RetentionLedger
	// BindingStore shares bindings between assemblies; nil creates one.
	BindingStore *artifact.MemoryBindingStore
	// Sink receives Loop observations; nil discards them.
	Sink loop.EventSink
	// Modules are application modules registered after the first-party three;
	// each must carry its own non-twilight Source (EXT-REG-1).
	Modules []extension.ModuleDescriptor
	// ProjectionCache overrides where folded projection states are stored; nil
	// asks the Store for a durable cache and falls back to an in-memory one.
	ProjectionCache extension.ProjectionCache
	// CacheEvery is how far behind the head a cached projection state may fall,
	// in rows; zero takes extension.DefaultCacheEvery. It bounds what a reopened
	// Session folds again after an abrupt end, and a clean Close refreshes
	// regardless, so a larger value trades a longer repeat fold for fewer writes
	// (EXT-PRJ-7). The run machine projection is never written this way: its
	// checkpoints belong to SnapshotPolicy (REF-MEM-2).
	CacheEvery session.Seq
}

// Memory is the fully wired in-process agent (REF 5). One Memory is one
// owner process: its Writers hold the Session ownership.
type Memory struct {
	Store        session.Store
	Registry     *extension.Registry
	Writers      extension.Writers
	Agents       *Agents
	Runtime      *runmod.Runtime
	Coordinator  *turn.Coordinator
	BindingStore *artifact.MemoryBindingStore
	Ledger       artifact.RetentionLedger
	now          func() time.Time
}

// New assembles store, registry, writers, runtime and coordinator over the
// three first-party modules.
func New(opts Options) (*Memory, error) {
	store := opts.Store
	if store == nil {
		store = session.NewMemoryStore()
	}
	modules := append([]extension.ModuleDescriptor{chatlog.Module, runmod.Module, turn.Module}, opts.Modules...)
	registry, err := extension.BuildRegistry(session.ProtocolVersion1, modules...)
	if err != nil {
		return nil, err
	}
	bindings := opts.BindingStore
	if bindings == nil {
		bindings = artifact.NewMemoryBindingStore()
	}
	ledger := opts.Ledger
	if ledger == nil {
		ledger = artifact.NewMemoryLedger(artifact.SetBuilder{Resolver: bindings})
	}
	// A projection cache lets a reopened Session start folding instead of
	// refolding the whole log (EXT-PRJ-3). An adapter that can store them
	// durably provides its own; otherwise the entries live as long as the
	// process.
	cache := opts.ProjectionCache
	if cache == nil {
		if provider, ok := store.(extension.ProjectionCacheProvider); ok {
			cache = provider.ProjectionCache()
		} else {
			cache = extension.NewMemoryProjectionCache()
		}
	}
	writers := extension.NewWriters(store, registry, extension.Admission{Bindings: bindings, Ledger: ledger}, opts.Ownership,
		extension.WritersConfig{Cache: cache, CachePolicy: runmod.WriterCachePolicy(opts.CacheEvery)})
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	runtime, err := runmod.NewRuntime(runmod.Config{
		Writers: writers, Registry: registry, Store: store,
		Frozen: opts.Frozen, Companion: turn.CompanionV1{}, Cache: cache, Now: now,
	})
	if err != nil {
		return nil, err
	}
	m := &Memory{Store: store, Registry: registry, Writers: writers, Runtime: runtime, BindingStore: bindings, Ledger: ledger, now: now}
	m.Agents = NewAgents(runtime, writersProjections{writers}, opts.Sink)
	m.Coordinator = &turn.Coordinator{Writers: writers, Runtime: runtime, Now: now}
	return m, nil
}

// Drive is REF-DRV-1, the host side the Coordinator no longer carries: while
// the Turn is active, resolve its recorded profile and drive the active
// attempt to the next quiescent point, then read the committed Status. The
// caller's ctx bounds the drive, so cancellation is a host decision. A
// concurrent local driver of the same Run yields ResumeAlreadyDriving.
func (m *Memory) Drive(ctx context.Context, ref turn.TurnRef) (turn.TurnResponse, error) {
	surface, err := m.TurnSurface(ctx, ref.SessionID)
	if err != nil {
		return turn.TurnResponse{}, err
	}
	view, ok := surface.Turns[ref.TurnID]
	if !ok {
		return turn.TurnResponse{}, fmt.Errorf("%w: unknown turn %s", turn.ErrConflict, ref.TurnID)
	}
	if view.Status == turn.TurnActive {
		driver, err := m.Agents.Resolve(view.Profile)
		if err != nil {
			return turn.TurnResponse{}, fmt.Errorf("%w: %v", ErrProfileUnavailable, err)
		}
		if err := driver.Drive(ctx, DriveRequest{Ref: ref, RunID: view.ActiveRun}); err != nil {
			if errors.Is(err, ErrAlreadyDriving) {
				resp, rerr := m.Coordinator.Status(ctx, ref)
				if rerr != nil {
					return turn.TurnResponse{}, rerr
				}
				resp.Disposition = ResumeAlreadyDriving
				return resp, nil
			}
			return turn.TurnResponse{}, err
		}
	}
	return m.Coordinator.Status(ctx, ref)
}

// writersProjections reads projections through the Session's Writer.
type writersProjections struct{ writers extension.Writers }

func (p writersProjections) Load(ctx context.Context, sid session.SessionID, id extensionProjectionID, v extensionProjectionVersion) (any, session.Head, error) {
	w, err := p.writers.Writer(ctx, sid)
	if err != nil {
		return nil, session.Head{}, err
	}
	return w.Projections().Load(ctx, sid, id, v)
}

// CreateSession creates the Session stream.
func (m *Memory) CreateSession(ctx context.Context, sid session.SessionID) error {
	_, err := m.Store.Create(ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: sid, CreatedAtUnixMilli: m.now().UnixMilli()})
	return err
}

// EnsureSession creates the stream when it does not exist yet. Create's
// idempotency needs field-identical requests, so existence is probed first.
func (m *Memory) EnsureSession(ctx context.Context, sid session.SessionID) error {
	if _, err := m.Store.Header(ctx, sid); err == nil {
		return nil
	} else if !session.IsCode(err, session.ErrNotFound) {
		return err
	}
	if err := m.CreateSession(ctx, sid); err != nil {
		// A concurrent creator winning the race is still "exists".
		if _, herr := m.Store.Header(ctx, sid); herr == nil {
			return nil
		}
		return err
	}
	return nil
}

// Open takes ownership of the Session and runs the takeover disposition
// (REF-DRV-5, RUN-CMT-7). It returns the number of recovery commands issued.
func (m *Memory) Open(ctx context.Context, sid session.SessionID) (int, error) {
	if _, err := m.Writers.Writer(ctx, sid); err != nil {
		return 0, err
	}
	return m.Runtime.RecoverInterrupted(ctx, sid)
}

// Close releases every Session this assembly owns.
func (m *Memory) Close(ctx context.Context) error { return extension.CloseWriters(ctx, m.Writers) }

// SubmitInput writes twilight/chatlog/input_submitted for one user text and
// returns the AgentInput a Start or Deliver hands to the Turn (REF-INP-2).
func (m *Memory) SubmitInput(ctx context.Context, sid session.SessionID, id run.InputID, text string) (run.AgentInput, error) {
	content := InputContent(text)
	w, err := m.Writers.Writer(ctx, sid)
	if err != nil {
		return run.AgentInput{}, err
	}
	res, err := w.Commit(ctx, func(extension.View) (*extension.SemanticGroup, error) {
		return &extension.SemanticGroup{CommitID: session.CommitID("input-submitted/" + string(id)), Events: []extension.TypedEvent{{
			Type: chatlog.TypeInputSubmitted, RecordedAtUnixMilli: m.now().UnixMilli(),
			Value: chatlog.InputSubmittedPayload{InputID: chatlog.InputID(id), Content: content, SubmittedAtUnixMilli: m.now().UnixMilli()},
		}}}, nil
	})
	if err != nil {
		return run.AgentInput{}, err
	}
	switch res.Outcome {
	case extension.CommitApplied, extension.CommitAlreadyApplied:
		return run.AgentInput{ID: id, Payload: content}, nil
	default:
		return run.AgentInput{}, fmt.Errorf("ref: submit input: %s: %s", res.Outcome, res.Detail)
	}
}

// SubmitText submits one user text under a fresh InputID (REF-INP-2).
func (m *Memory) SubmitText(ctx context.Context, sid session.SessionID, text string) (run.AgentInput, error) {
	return m.SubmitInput(ctx, sid, NewInputID(), text)
}

// Projection reads any registered projection through the Session's Writer —
// application modules read theirs here.
func (m *Memory) Projection(ctx context.Context, sid session.SessionID, id extension.ProjectionID, v extension.ProjectionVersion) (any, session.Head, error) {
	return writersProjections{m.Writers}.Load(ctx, sid, id, v)
}

// ChatlogSurface reads the chatlog surface projection.
func (m *Memory) ChatlogSurface(ctx context.Context, sid session.SessionID) (chatlog.Surface, error) {
	state, _, err := m.Projection(ctx, sid, chatlog.SurfaceProjectionID, chatlog.SurfaceProjection.Version)
	if err != nil {
		return chatlog.Surface{}, err
	}
	return state.(chatlog.Surface), nil
}

// TurnSurface reads the turn surface projection.
func (m *Memory) TurnSurface(ctx context.Context, sid session.SessionID) (turn.TurnSurface, error) {
	state, _, err := m.Projection(ctx, sid, turn.SurfaceProjectionID, turn.SurfaceProjection.Version)
	if err != nil {
		return turn.TurnSurface{}, err
	}
	return state.(turn.TurnSurface), nil
}

// SessionDriver routes user input to Deliver or Start and opens the next Turn
// after settlement (REF 4). It keeps no state of its own.
type SessionDriver struct {
	Coordinator turn.Service
	Memory      *Memory
	Profile     turn.ProfileRef
	Companion   turn.CompanionVersion
	// NewTurnID mints the next TurnID; nil selects the random default.
	NewTurnID func() turn.TurnID
}

// Send is REF-DRV-2: commit the input's route (Deliver into the active Turn,
// or Start a new one), then drive the Turn to its next quiescent point.
func (d *SessionDriver) Send(ctx context.Context, sid session.SessionID, inputs []run.AgentInput) (turn.TurnResponse, error) {
	newTurnID := d.NewTurnID
	if newTurnID == nil {
		newTurnID = NewTurnID
	}
	surface, err := d.Memory.TurnSurface(ctx, sid)
	if err != nil {
		return turn.TurnResponse{}, err
	}
	var ref turn.TurnRef
	if active, ok := surface.Active(); ok {
		ref = turn.TurnRef{SessionID: sid, TurnID: active.TurnID}
		if _, err := d.Coordinator.Deliver(ctx, turn.DeliverRequest{Ref: ref, Inputs: inputs}); err != nil {
			return turn.TurnResponse{}, err
		}
	} else {
		for _, v := range surface.Turns {
			if v.Status == turn.TurnAttemptFailed {
				return turn.TurnResponse{}, fmt.Errorf("%w: turn %s awaits Retry or Settle", turn.ErrConflict, v.TurnID)
			}
		}
		ref = turn.TurnRef{SessionID: sid, TurnID: newTurnID()}
		if _, err := d.Coordinator.Start(ctx, turn.StartRequest{Ref: ref, Inputs: inputs,
			Profile: d.Profile, Companion: d.Companion}); err != nil {
			return turn.TurnResponse{}, err
		}
	}
	return d.Memory.Drive(ctx, ref)
}

// OnTurnSettled is REF-DRV-3: start the next Turn from the backlog of
// submitted, undelivered inputs.
func (d *SessionDriver) OnTurnSettled(ctx context.Context, sid session.SessionID) (turn.TurnResponse, bool, error) {
	surface, err := d.Memory.ChatlogSurface(ctx, sid)
	if err != nil {
		return turn.TurnResponse{}, false, err
	}
	pending := surface.SubmittedInputs()
	if len(pending) == 0 {
		return turn.TurnResponse{}, false, nil
	}
	inputs := make([]run.AgentInput, len(pending))
	for i, in := range pending {
		inputs[i] = run.AgentInput{ID: run.InputID(in.ID), Payload: in.Content}
	}
	resp, err := d.Send(ctx, sid, inputs)
	return resp, err == nil, err
}
