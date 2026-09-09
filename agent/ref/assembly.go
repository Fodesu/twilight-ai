package ref

import (
	"context"
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
	registry, err := extension.BuildRegistry(session.ProtocolVersion1, chatlog.Module, runmod.Module, turn.Module)
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
	writers := extension.NewWriters(store, registry, extension.Admission{Bindings: bindings, Ledger: ledger}, opts.Ownership)
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	runtime, err := runmod.NewRuntime(runmod.Config{
		Writers: writers, Registry: registry, Store: store,
		Frozen: opts.Frozen, Companion: turn.CompanionV1{}, Now: now,
	})
	if err != nil {
		return nil, err
	}
	m := &Memory{Store: store, Registry: registry, Writers: writers, Runtime: runtime, BindingStore: bindings, Ledger: ledger, now: now}
	m.Agents = NewAgents(runtime, writersProjections{writers}, opts.Sink)
	m.Coordinator = &turn.Coordinator{Writers: writers, Runtime: runtime, Profiles: m.Agents, Now: now}
	return m, nil
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
// (REF-DRV-4, RUN-CMT-7). It returns the number of recovery commands issued.
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

// ChatlogSurface reads the chatlog surface projection.
func (m *Memory) ChatlogSurface(ctx context.Context, sid session.SessionID) (chatlog.Surface, error) {
	state, _, err := writersProjections{m.Writers}.Load(ctx, sid, chatlog.SurfaceProjectionID, chatlog.SurfaceProjection.Version)
	if err != nil {
		return chatlog.Surface{}, err
	}
	return state.(chatlog.Surface), nil
}

// TurnSurface reads the turn surface projection.
func (m *Memory) TurnSurface(ctx context.Context, sid session.SessionID) (turn.TurnSurface, error) {
	state, _, err := writersProjections{m.Writers}.Load(ctx, sid, turn.SurfaceProjectionID, turn.SurfaceProjection.Version)
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

// Send is REF-DRV-1: Deliver into the active Turn, or Start a new one.
func (d *SessionDriver) Send(ctx context.Context, sid session.SessionID, inputs []run.AgentInput) (turn.TurnResponse, error) {
	newTurnID := d.NewTurnID
	if newTurnID == nil {
		newTurnID = NewTurnID
	}
	surface, err := d.Memory.TurnSurface(ctx, sid)
	if err != nil {
		return turn.TurnResponse{}, err
	}
	if active, ok := surface.Active(); ok {
		return d.Coordinator.Deliver(ctx, turn.DeliverRequest{Ref: turn.TurnRef{SessionID: sid, TurnID: active.TurnID}, Inputs: inputs})
	}
	for _, v := range surface.Turns {
		if v.Status == turn.TurnAttemptFailed {
			return turn.TurnResponse{}, fmt.Errorf("%w: turn %s awaits Retry or Settle", turn.ErrConflict, v.TurnID)
		}
	}
	return d.Coordinator.Start(ctx, turn.StartRequest{Ref: turn.TurnRef{SessionID: sid, TurnID: newTurnID()}, Inputs: inputs,
		Profile: d.Profile, Companion: d.Companion})
}

// OnTurnSettled is REF-DRV-2: start the next Turn from the backlog of
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
