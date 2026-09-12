package turn

import (
	"context"
	"errors"
	"fmt"
	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/extension"
	runmod "github.com/felinics/twilight/agent/session/run"
	"github.com/felinics/twilight/agent/session/writer"
	"time"
)

// ErrConflict reports a Turn in a state that does not admit the operation.
var ErrConflict = errors.New("turn: conflict")

type StartRequest struct {
	Ref       TurnRef
	Inputs    []run.AgentInput
	Profile   ProfileRef
	Companion CompanionVersion
}
type DeliverRequest struct {
	Ref    TurnRef
	Inputs []run.AgentInput
}
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

// Service is the Turn API (TRN 3): protocol commits plus the Status read.
// Driving a Run belongs to the host (REF-DRV): every method returns as soon
// as its commit landed, with the response reflecting the committed state.
type Service interface {
	Start(context.Context, StartRequest) (TurnResponse, error)
	Deliver(context.Context, DeliverRequest) (TurnResponse, error)
	Retry(context.Context, RetryRequest) (TurnResponse, error)
	Stop(context.Context, StopRequest) (TurnResponse, error)
	Settle(context.Context, SettleRequest) (TurnResponse, error)
	Status(context.Context, TurnRef) (TurnResponse, error)
}

// Coordinator has no hidden state (TRN-SCP-3): every method reads the turn
// surface and the machine projection first. Writes and projection reads go
// through the Session's Writer (TRN-SCP-4, TRN-API-1). It never drives a
// Run: it commits protocol transitions and computes dispositions.
type Coordinator struct {
	Writers writer.Writers
	Runtime run.Runtime
	// Now stamps event times; nil selects time.Now.
	Now func() time.Time
}

func (c *Coordinator) now() int64 {
	if c.Now != nil {
		return c.Now().UnixMilli()
	}
	return time.Now().UnixMilli()
}

func (c *Coordinator) writer(ctx context.Context, sid session.SessionID) (writer.Writer, error) {
	w, err := c.Writers.Writer(ctx, sid)
	if err != nil {
		if errors.Is(err, &extension.Error{Code: extension.ErrOwnershipLost}) {
			return nil, fmt.Errorf("%w: %v", run.ErrOwnershipLost, err)
		}
		return nil, err
	}
	return w, nil
}

func (c *Coordinator) surface(ctx context.Context, sid session.SessionID) (TurnSurface, error) {
	w, err := c.writer(ctx, sid)
	if err != nil {
		return TurnSurface{}, err
	}
	state, _, err := w.Projections().Load(ctx, sid, SurfaceProjectionID, SurfaceProjection.Version)
	if err != nil {
		return TurnSurface{}, err
	}
	return state.(TurnSurface), nil
}

// commit runs fn in the Session Writer and maps the outcome (TRN-STR-3).
func (c *Coordinator) commit(ctx context.Context, sid session.SessionID, op string, fn writer.CommitFn) error {
	w, err := c.writer(ctx, sid)
	if err != nil {
		return err
	}
	res, err := w.Commit(ctx, fn)
	if err != nil {
		if errors.Is(err, &extension.Error{Code: extension.ErrOwnershipLost}) {
			return fmt.Errorf("%w: %v", run.ErrOwnershipLost, err)
		}
		return err
	}
	switch res.Outcome {
	case writer.CommitApplied, writer.CommitAlreadyApplied:
		return nil
	case writer.CommitConflict:
		return fmt.Errorf("%w: %s replayed with different content", ErrConflict, op)
	default:
		return fmt.Errorf("turn: %s: %s: %s", op, res.Outcome, res.Detail)
	}
}

// --- Start ------------------------------------------------------------------------

func (c *Coordinator) Start(ctx context.Context, req StartRequest) (TurnResponse, error) {
	if req.Ref.SessionID == "" || req.Ref.TurnID == "" || req.Profile.ID == "" || req.Profile.Digest == "" || req.Companion == "" {
		return TurnResponse{}, errors.New("turn: start requires ref, profile and companion")
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
	plan := PlanDigest(turnID, req.Profile.Digest, req.Companion, inputIDs)
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
	err = c.commit(ctx, sid, "start", func(view writer.View) (*writer.SemanticGroup, error) {
		if view.Committed(commitID) {
			group := c.startGroup(commitID, turnID, inputIDs, req, facts, now)
			return &group, nil // exact replay: the Writer compares fingerprints
		}
		surface, err := loadSurface(view)
		if err != nil {
			return nil, err
		}
		if _, exists := surface.Turns[turnID]; exists {
			return nil, fmt.Errorf("%w: turn %s already started", ErrConflict, turnID)
		}
		if _, active := surface.Active(); active {
			return nil, fmt.Errorf("%w: session already has an active turn", ErrConflict)
		}
		if err := checkSubmitted(view, req.Inputs); err != nil {
			return nil, err
		}
		group := c.startGroup(commitID, turnID, inputIDs, req, facts, now)
		return &group, nil
	})
	if err != nil {
		return TurnResponse{}, err
	}
	return c.respond(ctx, req.Ref, runID)
}

func (c *Coordinator) startGroup(commitID session.CommitID, turnID TurnID, inputIDs []chatlog.InputID, req StartRequest, facts []run.Fact, now int64) writer.SemanticGroup {
	group := writer.SemanticGroup{CommitID: commitID}
	group.Events = append(group.Events, writer.TypedEvent{Type: TypeStarted, RecordedAtUnixMilli: now,
		Value: StartedPayload{TurnID: turnID, InputIDs: inputIDs, Profile: req.Profile, Companion: req.Companion}})
	for _, id := range inputIDs {
		group.Events = append(group.Events, writer.TypedEvent{Type: chatlog.TypeInputDelivered, RecordedAtUnixMilli: now,
			Value: chatlog.InputDeliveredPayload{InputID: id, TurnID: chatlog.TurnID(turnID)}})
	}
	runID := DeriveRunID(req.Ref.SessionID, turnID, 1)
	for _, f := range facts {
		group.Events = append(group.Events, writer.TypedEvent{Type: runmod.EventType(f), RecordedAtUnixMilli: now, Value: runmod.Event{RunID: runID, Fact: f}})
	}
	return group
}

func loadSurface(view writer.View) (TurnSurface, error) {
	state, err := view.Projection(SurfaceProjectionID, SurfaceProjection.Version)
	if err != nil {
		return TurnSurface{}, err
	}
	return state.(TurnSurface), nil
}

// checkSubmitted enforces TRN-STR-1 (2): each input is a submitted chatlog
// Input whose Content equals the payload.
func checkSubmitted(view writer.View, inputs []run.AgentInput) error {
	if len(inputs) == 0 {
		return nil
	}
	state, err := view.Projection(chatlog.SurfaceProjectionID, chatlog.SurfaceProjection.Version)
	if err != nil {
		return err
	}
	return checkSubmittedIn(state.(chatlog.Surface), inputs)
}

// checkSubmittedIn is TRN-STR-1(2) against a loaded chatlog surface: every
// input is a submitted chatlog Input whose content equals the payload.
func checkSubmittedIn(surface chatlog.Surface, inputs []run.AgentInput) error {
	for _, in := range inputs {
		view, ok := surface.Inputs.Get(chatlog.InputID(in.ID))
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
	if len(req.Inputs) == 0 {
		return TurnResponse{}, fmt.Errorf("%w: deliver without inputs", ErrConflict)
	}
	proto, err := run.ProtocolFor(att.SchemaVersion)
	if err != nil {
		return TurnResponse{}, err
	}
	// TRN-DLV-2: one command carries the whole batch, so the Run accepts every
	// input and the chatlog delivers every input in one group, or nothing is
	// written. The CommandID derives from the ordered InputIDs; a replay of
	// the same batch is AlreadyApplied.
	cmd := run.AcceptInput{Inputs: req.Inputs}
	env, err := proto.BuildEnvelope(sid, runID, run.DeriveInputCommandID(runID, cmd.InputIDs()...), cmd)
	if err != nil {
		return TurnResponse{}, err
	}
	w, err := c.writer(ctx, sid)
	if err != nil {
		return TurnResponse{}, err
	}
	// TRN-DLV-1: the same check as Start, skipped for a replay of a batch the
	// stream already holds (its inputs are delivered by then; Runtime.Commit
	// answers AlreadyApplied or Conflict). The check runs before Runtime.Commit,
	// so a withdrawal landing in between is not caught here; the chatlog
	// projection pre-fold then rejects input_delivered for a non-submitted
	// input and the whole group is refused, which keeps the outcome
	// all-or-nothing.
	replayed, err := w.OwnerExists(ctx, writer.CommitOwner(sid, session.CommitID(env.ID)))
	if err != nil {
		return TurnResponse{}, err
	}
	if !replayed {
		state, _, err := w.Projections().Load(ctx, sid, chatlog.SurfaceProjectionID, chatlog.SurfaceProjection.Version)
		if err != nil {
			return TurnResponse{}, err
		}
		if err := checkSubmittedIn(state.(chatlog.Surface), req.Inputs); err != nil {
			return TurnResponse{}, err
		}
	}
	attach := make([]run.ModuleEvent, len(req.Inputs))
	for i, in := range req.Inputs {
		attach[i] = run.ModuleEvent{Type: chatlog.TypeInputDelivered, Value: chatlog.InputDeliveredPayload{InputID: chatlog.InputID(in.ID), TurnID: chatlog.TurnID(req.Ref.TurnID)}}
	}
	if _, err := c.Runtime.Commit(ctx, sid, run.CommitRequest{Command: env, Attach: attach}); err != nil {
		if errors.Is(err, run.ErrRunTerminal) {
			// The last step settled first (TRN-DLV-3): the inputs stay submitted.
			return c.respond(ctx, req.Ref, runID)
		}
		return TurnResponse{}, err
	}
	return c.respond(ctx, req.Ref, runID)
}

// --- Retry / Stop / Settle ------------------------------------------------------

func (c *Coordinator) Retry(ctx context.Context, req RetryRequest) (TurnResponse, error) {
	sid, turnID := req.Ref.SessionID, req.Ref.TurnID
	var runID run.RunID
	now := c.now()
	err := c.commit(ctx, sid, "retry", func(v writer.View) (*writer.SemanticGroup, error) {
		surface, err := loadSurface(v)
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
		inputs, err := deliveredInputs(v, view.InputIDs)
		if err != nil {
			return nil, err
		}
		facts, err := run.ProtocolV1().BuildCreateGroup(newRun, inputs)
		if err != nil {
			return nil, err
		}
		group := &writer.SemanticGroup{CommitID: commitID}
		for _, f := range facts {
			group.Events = append(group.Events, writer.TypedEvent{Type: runmod.EventType(f), RecordedAtUnixMilli: now, Value: runmod.Event{RunID: runID, Fact: f}})
		}
		return group, nil
	})
	if err != nil {
		return TurnResponse{}, err
	}
	return c.respond(ctx, req.Ref, runID)
}

// deliveredInputs rebuilds the AgentInputs of a Turn from the chatlog surface,
// in TurnView.InputIDs order (TRN-RTY-1).
func deliveredInputs(view writer.View, ids []chatlog.InputID) ([]run.AgentInput, error) {
	state, err := view.Projection(chatlog.SurfaceProjectionID, chatlog.SurfaceProjection.Version)
	if err != nil {
		return nil, err
	}
	surface := state.(chatlog.Surface)
	out := make([]run.AgentInput, 0, len(ids))
	for _, id := range ids {
		view, ok := surface.Inputs.Get(id)
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
	err := c.commit(ctx, sid, "settle", func(v writer.View) (*writer.SemanticGroup, error) {
		surface, err := loadSurface(v)
		if err != nil {
			return nil, err
		}
		view, ok := surface.Turns[turnID]
		if !ok || view.Status != TurnAttemptFailed {
			return nil, fmt.Errorf("%w: turn %s is not attempt_failed", ErrConflict, turnID)
		}
		runID = view.LastAttempt().RunID
		return &writer.SemanticGroup{CommitID: SettleCommitID(sid, turnID, runID), Events: []writer.TypedEvent{{
			Type: TypeFailed, RecordedAtUnixMilli: now,
			Value: FailedPayload{TurnID: turnID, RunID: runID, Settlement: SettlementFailed, FailureClass: req.FailureClass}}}}, nil
	})
	if err != nil {
		return TurnResponse{}, err
	}
	return c.respond(ctx, req.Ref, runID)
}

// --- Status ----------------------------------------------------------------------------

// Status is the pure read: the Turn's committed state and the disposition of
// its last attempt (TRN-STA-1). Hosts call it after driving to assemble the
// conversational result; the disposition logic has this single source.
func (c *Coordinator) Status(ctx context.Context, ref TurnRef) (TurnResponse, error) {
	return c.respond(ctx, ref, "")
}

// respond reads the projections and fills the disposition (TRN-STA-1).
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
