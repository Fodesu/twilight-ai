package turn

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/memohai/twilight/agent/es"
	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/session"
	"github.com/memohai/twilight/agent/session/chatlog"
	"github.com/memohai/twilight/agent/session/extension"
	runmod "github.com/memohai/twilight/agent/session/run"
)

// ErrConflict reports a Turn in a state that does not admit the operation.
var ErrConflict = errors.New("turn: conflict")

// ErrBindingUnavailable reports that the persisted binding cannot be resolved.
var ErrBindingUnavailable = errors.New("turn: binding_unavailable")

type DriveRequest struct {
	Ref   TurnRef
	RunID run.RunID
}

// RunDriver drives one Run to its next quiescent point; the reference driver
// wraps loop.Run (TRN-DRV-1).
type RunDriver interface {
	Drive(context.Context, DriveRequest) error
}

type ExecutionBindingRegistry interface {
	Resolve(ExecutionBindingRef) (RunDriver, error)
}

type StartRequest struct {
	Ref              TurnRef
	Inputs           []run.AgentInput
	ExecutionBinding ExecutionBindingRef
	Companion        CompanionVersion
}
type DeliverRequest struct {
	Ref    TurnRef
	Inputs []run.AgentInput
}
type TurnRequest struct{ Ref TurnRef }
type RetryRequest struct {
	Ref    TurnRef
	Reason string
}
type StopRequest struct {
	Ref    TurnRef
	Reason string
}
type SettleRequest struct {
	Ref          TurnRef
	FailureClass string
}

type ResumeDisposition string

const (
	ResumeWaitingForResponse ResumeDisposition = "waiting_for_response"
	ResumeWaitingForRecovery ResumeDisposition = "waiting_for_recovery"
	ResumeFinished           ResumeDisposition = "finished"
)

type TurnResponse struct {
	Ref         TurnRef
	RunID       run.RunID
	Attempt     uint32
	Status      TurnStatus
	Disposition ResumeDisposition
	End         *run.RunEnd
	Waiting     []run.ResponseRequest
}

// Service is the Turn API (TRN 3).
type Service interface {
	Start(context.Context, StartRequest) (TurnResponse, error)
	Deliver(context.Context, DeliverRequest) (TurnResponse, error)
	Resume(context.Context, TurnRequest) (TurnResponse, error)
	Retry(context.Context, RetryRequest) (TurnResponse, error)
	Stop(context.Context, StopRequest) (TurnResponse, error)
	Settle(context.Context, SettleRequest) (TurnResponse, error)
}

// Coordinator has no hidden state (TRN-SCP-3): every method reads the turn
// surface and the machine projection first.
type Coordinator struct {
	Projections extension.ProjectionReader
	Appender    extension.SemanticAppender
	Runtime     run.Runtime
	Bindings    ExecutionBindingRegistry
	// Now stamps event times; nil selects time.Now.
	Now func() time.Time
}

func (c *Coordinator) now() int64 {
	if c.Now != nil {
		return c.Now().UnixMilli()
	}
	return time.Now().UnixMilli()
}

func (c *Coordinator) surface(ctx context.Context, sid session.SessionID) (TurnSurface, error) {
	state, _, err := c.Projections.Load(ctx, sid, SurfaceProjectionID, SurfaceProjection.Version)
	if err != nil {
		return TurnSurface{}, err
	}
	return state.(TurnSurface), nil
}

// --- Start ------------------------------------------------------------------------

func (c *Coordinator) Start(ctx context.Context, req StartRequest) (TurnResponse, error) {
	if req.Ref.SessionID == "" || req.Ref.TurnID == "" || req.ExecutionBinding.ID == "" || req.ExecutionBinding.Digest == "" || req.Companion == "" {
		return TurnResponse{}, errors.New("turn: start requires ref, binding and companion")
	}
	inputIDs := make([]chatlog.InputID, len(req.Inputs))
	seen := map[run.InputID]struct{}{}
	for i, in := range req.Inputs {
		if _, dup := seen[in.ID]; dup || in.ID == "" {
			return TurnResponse{}, errors.New("turn: start inputs must have unique non-empty IDs")
		}
		seen[in.ID] = struct{}{}
		inputIDs[i] = chatlog.InputID(in.ID)
	}
	sid, turnID := req.Ref.SessionID, req.Ref.TurnID
	plan := PlanDigest(turnID, req.ExecutionBinding.Digest, req.Companion, inputIDs)
	commitID := session.CommitID(StartOperationDigest(sid, turnID, plan))
	runID := DeriveRunID(sid, turnID, 1)
	newRun, err := run.BuildNewRunFor(runID, run.OwnerID(turnID), 1, es.CausationID(commitID))
	if err != nil {
		return TurnResponse{}, err
	}
	facts, err := run.ProtocolV1().BuildCreateGroup(newRun, req.Inputs)
	if err != nil {
		return TurnResponse{}, err
	}
	now := c.now()
	res, err := c.Appender.AppendSemanticIn(ctx, sid, func(tx extension.SemanticTx) (*extension.SemanticGroup, error) {
		if _, found, err := tx.LookupCommit(commitID); err != nil {
			return nil, err
		} else if found {
			group := c.startGroup(commitID, turnID, inputIDs, req, plan, facts, now)
			return &group, nil // exact replay: the Appender compares fingerprints
		}
		surface, err := loadSurface(tx)
		if err != nil {
			return nil, err
		}
		if _, exists := surface.Turns[turnID]; exists {
			return nil, fmt.Errorf("%w: turn %s already started", ErrConflict, turnID)
		}
		if _, active := surface.Active(); active {
			return nil, fmt.Errorf("%w: session already has an active turn", ErrConflict)
		}
		if err := checkSubmitted(tx, req.Inputs); err != nil {
			return nil, err
		}
		group := c.startGroup(commitID, turnID, inputIDs, req, plan, facts, now)
		return &group, nil
	})
	if err != nil {
		return TurnResponse{}, err
	}
	if res.Outcome != extension.SemanticApplied && res.Outcome != extension.SemanticAlreadyApplied {
		return TurnResponse{}, fmt.Errorf("turn: start: %s: %s", res.Outcome, res.Detail)
	}
	return c.drive(ctx, req.Ref, runID)
}

func (c *Coordinator) startGroup(commitID session.CommitID, turnID TurnID, inputIDs []chatlog.InputID, req StartRequest, plan es.Digest, facts []run.Fact, now int64) extension.SemanticGroup {
	group := extension.SemanticGroup{CommitID: commitID}
	group.Events = append(group.Events, extension.TypedEvent{Type: TypeStarted, RecordedAtUnixMilli: now,
		Value: StartedPayload{TurnID: turnID, InputIDs: inputIDs, ExecutionBinding: req.ExecutionBinding, Companion: req.Companion, PlanDigest: plan}})
	for _, id := range inputIDs {
		group.Events = append(group.Events, extension.TypedEvent{Type: chatlog.TypeInputDelivered, RecordedAtUnixMilli: now,
			Value: chatlog.InputDeliveredPayload{InputID: id, TurnID: chatlog.TurnID(turnID)}})
	}
	runID := DeriveRunID(req.Ref.SessionID, turnID, 1)
	for _, f := range facts {
		group.Events = append(group.Events, extension.TypedEvent{Type: runmod.EventType(f), RecordedAtUnixMilli: now, Value: runmod.Event{RunID: runID, Fact: f}})
	}
	return group
}

func loadSurface(tx extension.SemanticTx) (TurnSurface, error) {
	state, _, err := extension.LoadIn(tx, &SurfaceProjection)
	if err != nil {
		return TurnSurface{}, err
	}
	return state.(TurnSurface), nil
}

// checkSubmitted enforces TRN-STR-1 (2): each input is a submitted chatlog
// Input whose Content equals the payload.
func checkSubmitted(tx extension.SemanticTx, inputs []run.AgentInput) error {
	if len(inputs) == 0 {
		return nil
	}
	state, _, err := extension.LoadIn(tx, &chatlog.SurfaceProjection)
	if err != nil {
		return err
	}
	surface := state.(chatlog.Surface)
	for _, in := range inputs {
		view, ok := surface.Inputs[chatlog.InputID(in.ID)]
		if !ok || view.Status != chatlog.InputSubmitted {
			return fmt.Errorf("%w: input %s is not a submitted input", ErrConflict, in.ID)
		}
		if !view.Input.Content.Equal(in.Payload) {
			return fmt.Errorf("%w: input %s payload differs from its submitted content", ErrConflict, in.ID)
		}
	}
	return nil
}

// --- Deliver ----------------------------------------------------------------------

func (c *Coordinator) Deliver(ctx context.Context, req DeliverRequest) (TurnResponse, error) {
	sid := req.Ref.SessionID
	surface, err := c.surface(ctx, sid)
	if err != nil {
		return TurnResponse{}, err
	}
	view, ok := surface.Turns[req.Ref.TurnID]
	if !ok || view.Status != TurnActive {
		return TurnResponse{}, fmt.Errorf("%w: turn %s is not active", ErrConflict, req.Ref.TurnID)
	}
	// AcceptInput is not a hard-CAS command (RUN-CMT-4): no Base is needed, and
	// the attempt's SchemaVersion comes from the surface, so Deliver does not
	// read the machine projection.
	att := view.ActiveAttempt()
	if att == nil {
		return TurnResponse{}, fmt.Errorf("%w: turn %s has no active attempt", ErrConflict, req.Ref.TurnID)
	}
	runID := att.RunID
	proto, err := run.ProtocolFor(att.SchemaVersion)
	if err != nil {
		return TurnResponse{}, err
	}
	for _, in := range req.Inputs {
		env, err := proto.BuildEnvelope(sid, runID, run.DeriveInputCommandID(runID, in.ID), run.AcceptInput{Input: in})
		if err != nil {
			return TurnResponse{}, err
		}
		_, err = c.Runtime.Commit(ctx, sid, run.CommitRequest{Command: env,
			Attach: []run.ModuleEvent{{Type: chatlog.TypeInputDelivered, Value: chatlog.InputDeliveredPayload{InputID: chatlog.InputID(in.ID), TurnID: chatlog.TurnID(req.Ref.TurnID)}}}})
		if err != nil {
			if errors.Is(err, run.ErrRunTerminal) {
				// The last step settled first (TRN-DLV-3): the input stays submitted.
				return c.respond(ctx, req.Ref, runID)
			}
			return TurnResponse{}, err
		}
	}
	return c.drive(ctx, req.Ref, runID)
}

// --- Resume / Retry / Stop / Settle ------------------------------------------------------

func (c *Coordinator) Resume(ctx context.Context, req TurnRequest) (TurnResponse, error) {
	surface, err := c.surface(ctx, req.Ref.SessionID)
	if err != nil {
		return TurnResponse{}, err
	}
	view, ok := surface.Turns[req.Ref.TurnID]
	if !ok {
		return TurnResponse{}, fmt.Errorf("%w: unknown turn %s", ErrConflict, req.Ref.TurnID)
	}
	if view.Status != TurnActive {
		return c.responseFor(ctx, req.Ref, &view)
	}
	return c.drive(ctx, req.Ref, view.ActiveRun)
}

func (c *Coordinator) Retry(ctx context.Context, req RetryRequest) (TurnResponse, error) {
	sid, turnID := req.Ref.SessionID, req.Ref.TurnID
	var runID run.RunID
	now := c.now()
	res, err := c.Appender.AppendSemanticIn(ctx, sid, func(tx extension.SemanticTx) (*extension.SemanticGroup, error) {
		surface, err := loadSurface(tx)
		if err != nil {
			return nil, err
		}
		view, ok := surface.Turns[turnID]
		if !ok || view.Status != TurnAttemptFailed {
			return nil, fmt.Errorf("%w: turn %s is not attempt_failed", ErrConflict, turnID)
		}
		attempt := uint32(len(view.Attempts)) + 1
		runID = DeriveRunID(sid, turnID, attempt)
		commitID := RetryCommitID(sid, turnID, attempt)
		newRun, err := run.BuildNewRunFor(runID, run.OwnerID(turnID), attempt, es.CausationID(commitID))
		if err != nil {
			return nil, err
		}
		inputs, err := deliveredInputs(tx, view.InputIDs)
		if err != nil {
			return nil, err
		}
		facts, err := run.ProtocolV1().BuildCreateGroup(newRun, inputs)
		if err != nil {
			return nil, err
		}
		group := &extension.SemanticGroup{CommitID: commitID}
		for _, f := range facts {
			group.Events = append(group.Events, extension.TypedEvent{Type: runmod.EventType(f), RecordedAtUnixMilli: now, Value: runmod.Event{RunID: runID, Fact: f}})
		}
		return group, nil
	})
	if err != nil {
		return TurnResponse{}, err
	}
	if res.Outcome != extension.SemanticApplied && res.Outcome != extension.SemanticAlreadyApplied {
		return TurnResponse{}, fmt.Errorf("turn: retry: %s: %s", res.Outcome, res.Detail)
	}
	return c.drive(ctx, req.Ref, runID)
}

// deliveredInputs rebuilds the AgentInputs of a Turn from the chatlog surface,
// in TurnView.InputIDs order (TRN-RTY-1).
func deliveredInputs(tx extension.SemanticTx, ids []chatlog.InputID) ([]run.AgentInput, error) {
	state, _, err := extension.LoadIn(tx, &chatlog.SurfaceProjection)
	if err != nil {
		return nil, err
	}
	surface := state.(chatlog.Surface)
	out := make([]run.AgentInput, 0, len(ids))
	for _, id := range ids {
		view, ok := surface.Inputs[id]
		if !ok {
			return nil, fmt.Errorf("turn: retry: delivered input %s missing from chatlog", id)
		}
		out = append(out, run.AgentInput{ID: run.InputID(id), Payload: view.Input.Content})
	}
	return out, nil
}

func (c *Coordinator) Stop(ctx context.Context, req StopRequest) (TurnResponse, error) {
	sid, turnID := req.Ref.SessionID, req.Ref.TurnID
	surface, err := c.surface(ctx, sid)
	if err != nil {
		return TurnResponse{}, err
	}
	view, ok := surface.Turns[turnID]
	if !ok || view.Status != TurnActive {
		return TurnResponse{}, fmt.Errorf("%w: turn %s is not active", ErrConflict, turnID)
	}
	att := view.ActiveAttempt()
	if att == nil {
		return TurnResponse{}, fmt.Errorf("%w: turn %s has no active attempt", ErrConflict, turnID)
	}
	runID := att.RunID
	proto, err := run.ProtocolFor(att.SchemaVersion)
	if err != nil {
		return TurnResponse{}, err
	}
	env, err := proto.BuildEnvelope(sid, runID, CancelCommandID(sid, turnID, runID), run.CancelRun{})
	if err != nil {
		return TurnResponse{}, err
	}
	// CancelRun rebases on the current state; no Base and no machine read.
	_, err = c.Runtime.Commit(ctx, sid, run.CommitRequest{Command: env,
		Attach: []run.ModuleEvent{{Type: TypeFailed, Value: FailedPayload{TurnID: turnID, RunID: runID, Settlement: SettlementStopped, FailureClass: "cancelled"}}}})
	if err != nil && !errors.Is(err, run.ErrRunTerminal) {
		return TurnResponse{}, err
	}
	return c.respond(ctx, req.Ref, runID)
}

func (c *Coordinator) Settle(ctx context.Context, req SettleRequest) (TurnResponse, error) {
	sid, turnID := req.Ref.SessionID, req.Ref.TurnID
	var runID run.RunID
	now := c.now()
	res, err := c.Appender.AppendSemanticIn(ctx, sid, func(tx extension.SemanticTx) (*extension.SemanticGroup, error) {
		surface, err := loadSurface(tx)
		if err != nil {
			return nil, err
		}
		view, ok := surface.Turns[turnID]
		if !ok || view.Status != TurnAttemptFailed {
			return nil, fmt.Errorf("%w: turn %s is not attempt_failed", ErrConflict, turnID)
		}
		runID = view.LastAttempt().RunID
		return &extension.SemanticGroup{CommitID: SettleCommitID(sid, turnID, runID), Events: []extension.TypedEvent{{
			Type: TypeFailed, RecordedAtUnixMilli: now,
			Value: FailedPayload{TurnID: turnID, RunID: runID, Settlement: SettlementFailed, FailureClass: req.FailureClass}}}}, nil
	})
	if err != nil {
		return TurnResponse{}, err
	}
	if res.Outcome != extension.SemanticApplied && res.Outcome != extension.SemanticAlreadyApplied {
		return TurnResponse{}, fmt.Errorf("turn: settle: %s: %s", res.Outcome, res.Detail)
	}
	return c.respond(ctx, req.Ref, runID)
}

// --- Drive ----------------------------------------------------------------------------

func (c *Coordinator) drive(ctx context.Context, ref TurnRef, runID run.RunID) (TurnResponse, error) {
	surface, err := c.surface(ctx, ref.SessionID)
	if err != nil {
		return TurnResponse{}, err
	}
	view := surface.Turns[ref.TurnID]
	if view.Status == TurnActive {
		driver, err := c.Bindings.Resolve(view.ExecutionBinding)
		if err != nil {
			return TurnResponse{}, fmt.Errorf("%w: %v", ErrBindingUnavailable, err)
		}
		if err := driver.Drive(ctx, DriveRequest{Ref: ref, RunID: runID}); err != nil {
			return TurnResponse{}, err
		}
	}
	return c.respond(ctx, ref, runID)
}

// respond reads the projections and fills the disposition (TRN-DRV-1).
func (c *Coordinator) respond(ctx context.Context, ref TurnRef, runID run.RunID) (TurnResponse, error) {
	surface, err := c.surface(ctx, ref.SessionID)
	if err != nil {
		return TurnResponse{}, err
	}
	view, ok := surface.Turns[ref.TurnID]
	if !ok {
		return TurnResponse{}, fmt.Errorf("%w: unknown turn %s", ErrConflict, ref.TurnID)
	}
	if runID == "" {
		if last := view.LastAttempt(); last != nil {
			runID = last.RunID
		}
	}
	return c.responseFor(ctx, ref, &view, runID)
}

func (c *Coordinator) responseFor(ctx context.Context, ref TurnRef, view *TurnView, runIDs ...run.RunID) (TurnResponse, error) {
	resp := TurnResponse{Ref: ref, Status: view.Status}
	var att *AttemptView
	if len(runIDs) > 0 && runIDs[0] != "" {
		for i := range view.Attempts {
			if view.Attempts[i].RunID == runIDs[0] {
				att = &view.Attempts[i]
			}
		}
	}
	if att == nil {
		att = view.LastAttempt()
	}
	if att == nil {
		return resp, nil
	}
	resp.RunID, resp.Attempt, resp.End = att.RunID, att.Attempt, att.Ended()
	if att.End != nil {
		resp.Disposition = ResumeFinished
		return resp, nil
	}
	snapshot, err := c.Runtime.Load(ctx, ref.SessionID, att.RunID)
	if err != nil {
		return TurnResponse{}, err
	}
	switch {
	case snapshot.State.Status.Terminal():
		resp.Disposition = ResumeFinished
	case run.NeedsRecovery(snapshot.State):
		resp.Disposition = ResumeWaitingForRecovery
	default:
		resp.Waiting = run.WaitingCalls(snapshot.State)
		if len(resp.Waiting) > 0 {
			resp.Disposition = ResumeWaitingForResponse
		}
	}
	return resp, nil
}

var _ Service = (*Coordinator)(nil)
