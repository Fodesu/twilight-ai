package ref

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/memohai/twilight/agent/artifact"
	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/run/loop"
	"github.com/memohai/twilight/agent/session"
	"github.com/memohai/twilight/agent/session/chatlog"
	"github.com/memohai/twilight/agent/session/extension"
	runmod "github.com/memohai/twilight/agent/session/run"
	"github.com/memohai/twilight/agent/turn"
)

// Options tunes the Memory assembly.
type Options struct {
	// LeaseTTL zero means execution leases never expire.
	LeaseTTL time.Duration
	Now      func() time.Time
	// Frozen shares request bodies between "processes" in tests; nil creates one.
	Frozen run.FrozenValueStore
	// Store shares the Session store between assemblies; nil creates one.
	Store session.Store
	// Sink receives Loop observations; nil discards them.
	Sink loop.EventSink
}

// Memory is the fully wired in-process agent (REF 5).
type Memory struct {
	Store        session.Store
	Registry     *extension.Registry
	Projections  extension.ProjectionReader
	Bindings     *Bindings
	Appender     extension.SemanticAppender
	Runtime      *runmod.Runtime
	Coordinator  *turn.Coordinator
	BindingStore *artifact.MemoryBindingStore
	now          func() time.Time
}

// New assembles store, registry, appender, projections, runtime and
// coordinator over the three first-party modules.
func New(opts Options) (*Memory, error) {
	store := opts.Store
	if store == nil {
		store = session.NewMemoryStore()
	}
	registry, err := extension.BuildRegistry(session.ProfileV1(), chatlog.Module, runmod.Module, turn.Module)
	if err != nil {
		return nil, err
	}
	bindings := artifact.NewMemoryBindingStore()
	ledger := artifact.KVLedger{Builder: artifact.SetBuilder{Resolver: bindings}}
	appender, err := extension.NewSemanticAppender(store, registry, artifact.SetBuilder{Resolver: bindings}, ledger)
	if err != nil {
		return nil, err
	}
	projections := extension.NewProjectionReader(store, registry)
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	runtime, err := runmod.NewRuntime(runmod.Config{
		Store: store, Registry: registry, Appender: appender, Projections: projections,
		Frozen: opts.Frozen, Companion: turn.CompanionV1{}, LeaseTTL: opts.LeaseTTL, Now: now,
	})
	if err != nil {
		return nil, err
	}
	m := &Memory{Store: store, Registry: registry, Projections: projections, Appender: appender, Runtime: runtime, BindingStore: bindings, now: now}
	m.Bindings = NewBindings(runtime, projections, opts.Sink)
	m.Coordinator = &turn.Coordinator{Projections: projections, Appender: appender, Runtime: runtime, Bindings: m.Bindings, Now: now}
	return m, nil
}

// CreateSession creates the Session stream.
func (m *Memory) CreateSession(ctx context.Context, sid session.SessionID) error {
	_, err := m.Store.Create(ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: sid})
	return err
}

// SubmitInput writes twilight/chatlog/input_submitted for one user text and
// returns the AgentInput a Start or Deliver hands to the Turn (REF-INP-2).
func (m *Memory) SubmitInput(ctx context.Context, sid session.SessionID, id run.InputID, text string) (run.AgentInput, error) {
	content := InputContent(text)
	head, err := m.Store.Head(ctx, sid)
	if err != nil {
		return run.AgentInput{}, err
	}
	res, err := m.Appender.AppendSemantic(ctx, extension.SemanticAppendRequest{
		SessionID: sid, ExpectedHead: head,
		Group: extension.SemanticGroup{CommitID: session.CommitID("input-submitted/" + string(id)), Events: []extension.TypedEvent{{
			Type: chatlog.TypeInputSubmitted, RecordedAtUnixMilli: m.now().UnixMilli(),
			Value: chatlog.InputSubmittedPayload{InputID: chatlog.InputID(id), Content: content, SubmittedAtUnixMilli: m.now().UnixMilli()},
		}}},
	})
	if err != nil {
		return run.AgentInput{}, err
	}
	switch res.Outcome {
	case extension.SemanticApplied, extension.SemanticAlreadyApplied:
		return run.AgentInput{ID: id, Payload: content}, nil
	case extension.SemanticHeadConflict:
		// Another writer moved the head; the caller retries with a fresh head.
		return m.SubmitInput(ctx, sid, id, text)
	default:
		return run.AgentInput{}, fmt.Errorf("ref: submit input: %s: %s", res.Outcome, res.Detail)
	}
}

// ChatlogSurface reads the chatlog surface projection.
func (m *Memory) ChatlogSurface(ctx context.Context, sid session.SessionID) (chatlog.Surface, error) {
	state, _, err := m.Projections.Load(ctx, sid, chatlog.SurfaceProjectionID, chatlog.SurfaceProjection.Version)
	if err != nil {
		return chatlog.Surface{}, err
	}
	return state.(chatlog.Surface), nil
}

// TurnSurface reads the turn surface projection.
func (m *Memory) TurnSurface(ctx context.Context, sid session.SessionID) (turn.TurnSurface, error) {
	state, _, err := m.Projections.Load(ctx, sid, turn.SurfaceProjectionID, turn.SurfaceProjection.Version)
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
	Binding     turn.ExecutionBindingRef
	Companion   turn.CompanionVersion
	NewTurnID   func() turn.TurnID
}

// Send is REF-DRV-1: Deliver into the active Turn, or Start a new one.
func (d *SessionDriver) Send(ctx context.Context, sid session.SessionID, inputs []run.AgentInput) (turn.TurnResponse, error) {
	if d.NewTurnID == nil {
		return turn.TurnResponse{}, errors.New("ref: session driver requires NewTurnID")
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
	return d.Coordinator.Start(ctx, turn.StartRequest{Ref: turn.TurnRef{SessionID: sid, TurnID: d.NewTurnID()}, Inputs: inputs,
		ExecutionBinding: d.Binding, Companion: d.Companion})
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
