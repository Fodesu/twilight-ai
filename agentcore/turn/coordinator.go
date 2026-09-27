package turn

import (
	"context"
	"errors"
	"fmt"
	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/plan"
	"github.com/felinics/twilight/agentcore/run/runtime"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/session/extension"
	runmod "github.com/felinics/twilight/agentcore/session/run"
	"github.com/felinics/twilight/agentcore/session/unit"
	"github.com/felinics/twilight/agentcore/session/writer"
	"time"
)

// ErrConflict reports a Turn in a state that does not admit the operation.
var ErrConflict = errors.New("turn: conflict")

type StartRequest struct {
	Ref    TurnRef
	Inputs []run.AgentInput
	Preset PresetRef
}
type DeliverRequest struct {
	Ref    TurnRef
	Inputs []run.AgentInput
}
type StopRequest struct {
	Ref    TurnRef
	Reason string
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
	Status      TurnStatus
	Disposition ResumeDisposition
	End         run.RunEnd
	Waiting     []run.ResponseRequest
}

// Commands are the Turn protocol commits (TRN 3). Each takes the Writer of
// the Session it commits to: the caller's ownership capability, so every
// command lands on the same Writer, epoch and projection view as the other
// domains' commands, and a stale owner is fenced by the Writer itself.
// Driving a Run belongs to the driver (DRV): every method returns as
// soon as its commit landed, with the response reflecting the committed
// state.
type Commands interface {
	Start(context.Context, writer.Writer, StartRequest) (TurnResponse, error)
	Deliver(context.Context, writer.Writer, DeliverRequest) (TurnResponse, error)
	Stop(context.Context, writer.Writer, StopRequest) (TurnResponse, error)
}

// Reader is the Turn status read (TRN-STA-1); it needs no ownership.
type Reader interface {
	Status(context.Context, TurnRef) (TurnResponse, error)
}

// Coordinator has no hidden state (TRN-SCP-3): every method reads the turn
// surface first. Every command is one unit of work (SES-ATM): the Turn's own
// Part beside the chatlog's and the Run module's, prepared against one View
// and appended as one commit. The Coordinator never encodes another module's
// events and never drives a Run: it commits protocol transitions and computes
// dispositions.
type Coordinator struct {
	// Projections is the lease-free read side for Status (OWN-HDL-2).
	Projections extension.ProjectionReader
	// Runs is the Run module's Session adapter: it reads Runs for Status and
	// contributes the Run Parts of every Turn unit.
	Runs *runmod.SessionRunStore
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
	return ReadSurface(ctx, c.Projections, sid)
}

// owned checks that the request addresses the Session the Writer owns.
func owned(w writer.Writer, ref TurnRef) error {
	if w == nil {
		return errors.New("turn: command requires the session's writer")
	}
	if ref.SessionID != w.SessionID() {
		return fmt.Errorf("%w: request for %s through the writer of %s", ErrConflict, ref.SessionID, w.SessionID())
	}
	return nil
}

// commit appends one unit of work and maps the outcome (TRN-STR-3). The
// CommitID names the operation, so a replay is already applied (EXT-WRT-2).
// A chatlog refusal (an input no longer submitted) is the Turn's conflict.
func (c *Coordinator) commit(ctx context.Context, w writer.Writer, op string, work unit.Work) error {
	res, err := unit.Commit(ctx, w, c.now(), work)
	if err != nil {
		switch {
		case errors.Is(err, &extension.Error{Code: extension.ErrOwnershipLost}):
			return fmt.Errorf("%w: %w", runtime.ErrOwnershipLost, err)
		case errors.Is(err, chatlog.ErrNotSubmitted), errors.Is(err, runmod.ErrRunExists):
			return fmt.Errorf("%w: %w", ErrConflict, err)
		}
		return err
	}
	switch res.Outcome {
	case writer.CommitApplied, writer.CommitAlreadyApplied, writer.CommitNoop:
		return nil
	case writer.CommitConflict:
		return fmt.Errorf("%w: %s replayed with different content", ErrConflict, op)
	default:
		return fmt.Errorf("turn: %s: %s: %s", op, res.Outcome, res.Detail)
	}
}

func loadSurface(view writer.View) (TurnSurface, error) {
	state, err := view.Projection(SurfaceProjectionID, SurfaceProjection.Version)
	if err != nil {
		return TurnSurface{}, err
	}
	surface, ok := state.(TurnSurface)
	if !ok {
		return TurnSurface{}, fmt.Errorf("turn: surface projection is %T", state)
	}
	return surface, nil
}

// turnBatch is one batch of events in the Turn's stream.
func turnBatch(turnID TurnID, now int64, events ...writer.TypedEvent) []writer.TypedBatch {
	for i := range events {
		events[i].RecordedAtUnixMilli = now
	}
	return []writer.TypedBatch{{Stream: Stream(turnID), Events: events}}
}

// --- Start ------------------------------------------------------------------------

func (c *Coordinator) Start(ctx context.Context, w writer.Writer, req StartRequest) (TurnResponse, error) {
	if req.Ref.SessionID == "" || req.Ref.TurnID == "" || req.Preset.ID == "" || req.Preset.Digest == "" {
		return TurnResponse{}, errors.New("turn: start requires ref and preset")
	}
	if err := owned(w, req.Ref); err != nil {
		return TurnResponse{}, err
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
	p := PlanDigest(turnID, req.Preset.Digest, inputIDs)
	commitID := session.CommitID(StartOperationDigest(sid, turnID, p))
	runID := DeriveRunID(sid, turnID)
	newRun, err := run.BuildNewRun(runID, es.CausationID(commitID))
	if err != nil {
		return TurnResponse{}, err
	}
	// TRN-STR-2: the Turn's facts, the chatlog's deliveries and the Run's
	// creation are one unit; the chatlog Part enforces TRN-STR-1 (2) on the
	// same View.
	work := unit.Work{CommitID: commitID, Parts: []unit.Part{
		unit.PartFunc(func(_ context.Context, view writer.View, now int64) ([]writer.TypedBatch, error) {
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
			// The Turn's fact names the one Run that executes it (TRN-SCP-2).
			return turnBatch(turnID, now,
				writer.TypedEvent{Type: TypeStarted, Value: StartedPayload{TurnID: turnID, RunID: runID, InputIDs: inputIDs, Preset: req.Preset}}), nil
		}),
		chatlog.DeliverInputs(chatlog.TurnID(turnID), runID, req.Inputs),
		runmod.CreateRun(newRun, req.Inputs),
	}}
	if err := c.commit(ctx, w, "start", work); err != nil {
		return TurnResponse{}, err
	}
	return c.respond(ctx, req.Ref)
}

// --- Deliver ----------------------------------------------------------------------

func (c *Coordinator) Deliver(ctx context.Context, w writer.Writer, req DeliverRequest) (TurnResponse, error) {
	if err := owned(w, req.Ref); err != nil {
		return TurnResponse{}, err
	}
	sid := req.Ref.SessionID
	surface, err := ReadSurface(ctx, w.Projections(), sid)
	if err != nil {
		return TurnResponse{}, err
	}
	view, ok := surface.Turns[req.Ref.TurnID]
	if !ok || view.Status != TurnActive {
		return TurnResponse{}, fmt.Errorf("%w: turn %s is not active", ErrConflict, req.Ref.TurnID)
	}
	// AcceptInput is not a hard-CAS command (RUN-CMT-4): no Base is needed;
	// the machine projection is read only for the Run's protocol version
	// (RUN-CMT-8).
	runID := view.RunID
	if len(req.Inputs) == 0 {
		return TurnResponse{}, fmt.Errorf("%w: deliver without inputs", ErrConflict)
	}
	// TRN-DLV-2: one command carries the whole batch, so the Run accepts every
	// input and the chatlog delivers every input in one unit, or nothing is
	// written. The CommandID derives from the ordered InputIDs; a replay of
	// the same batch is AlreadyApplied. The chatlog Part enforces TRN-DLV-1 on
	// the unit's View, so a withdrawal landing between this read and the
	// commit refuses the unit.
	cmd := run.AcceptInput{Inputs: req.Inputs}
	env, err := schema.Wire().Envelope(runID, schema.Identity().DeriveInputCommandID(runID, cmd.InputIDs()...), cmd)
	if err != nil {
		return TurnResponse{}, err
	}
	accept, err := c.Runs.Command(ctx, runtime.CommitRequest{Command: env})
	if err != nil {
		return TurnResponse{}, err
	}
	work := unit.Work{CommitID: session.CommitID(env.ID), Parts: []unit.Part{accept, chatlog.DeliverInputs(chatlog.TurnID(req.Ref.TurnID), runID, req.Inputs)}}
	if err := c.commit(ctx, w, "deliver", work); err != nil {
		if errors.Is(err, run.ErrRunTerminal) {
			// The last step settled first (TRN-DLV-3): the inputs stay submitted.
			return c.respond(ctx, req.Ref)
		}
		return TurnResponse{}, err
	}
	return c.respond(ctx, req.Ref)
}

// --- Stop ---------------------------------------------------------------------------

func (c *Coordinator) Stop(ctx context.Context, w writer.Writer, req StopRequest) (TurnResponse, error) {
	if err := owned(w, req.Ref); err != nil {
		return TurnResponse{}, err
	}
	sid, turnID := req.Ref.SessionID, req.Ref.TurnID
	surface, err := ReadSurface(ctx, w.Projections(), sid)
	if err != nil {
		return TurnResponse{}, err
	}
	view, ok := surface.Turns[turnID]
	if !ok || view.Status != TurnActive {
		return TurnResponse{}, fmt.Errorf("%w: turn %s is not active", ErrConflict, turnID)
	}
	runID := view.RunID
	env, err := schema.Wire().Envelope(runID, CancelCommandID(sid, turnID, runID), run.CancelRun{})
	if err != nil {
		return TurnResponse{}, err
	}
	// CancelRun rebases on the current state; no Base and no machine read.
	// The Run's cancellation and the Turn's failed settlement are one unit.
	cancel, err := c.Runs.Command(ctx, runtime.CommitRequest{Command: env})
	if err != nil {
		return TurnResponse{}, err
	}
	work := unit.Work{CommitID: session.CommitID(env.ID), Parts: []unit.Part{cancel,
		unit.PartFunc(func(_ context.Context, _ writer.View, now int64) ([]writer.TypedBatch, error) {
			return turnBatch(turnID, now, writer.TypedEvent{Type: TypeFailed,
				Value: FailedPayload{TurnID: turnID, RunID: runID, Settlement: SettlementStopped, FailureClass: "cancelled"}}), nil
		}),
	}}
	if err := c.commit(ctx, w, "stop", work); err != nil && !errors.Is(err, run.ErrRunTerminal) {
		return TurnResponse{}, err
	}
	return c.respond(ctx, req.Ref)
}

func (c *Coordinator) Status(ctx context.Context, ref TurnRef) (TurnResponse, error) {
	return c.respond(ctx, ref)
}

// respond reads the projections and fills the disposition (TRN-STA-1).
func (c *Coordinator) respond(ctx context.Context, ref TurnRef) (TurnResponse, error) {
	surface, err := c.surface(ctx, ref.SessionID)
	if err != nil {
		return TurnResponse{}, err
	}
	view, ok := surface.Turns[ref.TurnID]
	if !ok {
		return TurnResponse{}, fmt.Errorf("%w: unknown turn %s", ErrConflict, ref.TurnID)
	}
	return c.responseFor(ctx, ref, &view)
}

func (c *Coordinator) responseFor(ctx context.Context, ref TurnRef, view *TurnView) (TurnResponse, error) {
	resp := TurnResponse{Ref: ref, Status: view.Status, RunID: view.RunID, End: view.Ended()}
	if view.End != nil {
		resp.Disposition = ResumeFinished
		return resp, nil
	}
	record, err := c.Runs.Record(ctx, ref.SessionID, view.RunID)
	if err != nil {
		return TurnResponse{}, err
	}
	snapshot := record.Snapshot
	switch {
	case snapshot.State.Status.Terminal():
		resp.Disposition = ResumeFinished
	case plan.NeedsRecovery(snapshot.State):
		resp.Disposition = ResumeWaitingForRecovery
	default:
		resp.Waiting = plan.WaitingCalls(snapshot.State)
		if len(resp.Waiting) > 0 {
			resp.Disposition = ResumeWaitingForResponse
		}
	}
	return resp, nil
}

var (
	_ Commands = (*Coordinator)(nil)
	_ Reader   = (*Coordinator)(nil)
)
