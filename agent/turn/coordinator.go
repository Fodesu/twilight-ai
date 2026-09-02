package turn

import (
	"context"
	"errors"
	"fmt"

	"github.com/memohai/twilight/agent/es"
	"github.com/memohai/twilight/agent/run"
)

// RunDriver executes one Run until it finishes or has nothing executable.
// The reference driver wraps loop.Loop.Run; the coordinator reads the
// resulting disposition from the Runtime rather than from the driver.
type RunDriver interface {
	Drive(ctx context.Context, runID run.RunID) error
}

// Coordinator ties one Turn to one primary Run (TRN-SCP-1). It keeps no
// state of its own: every operation replays the session log and reads the
// Runtime (TRN-SCP-3).
type Coordinator struct {
	Log     Log
	Runtime run.Runtime
	Driver  RunDriver
}

var (
	ErrTurnNotFound = errors.New("agent: turn: no started turn")
	ErrTurnConflict = errors.New("agent: turn: turn already started with a different run")
)

type Disposition string

const (
	// DispositionFinished: the Run is terminal and the Turn is settled.
	DispositionFinished Disposition = "finished"
	// DispositionWaitingForResponse: a tool call waits for approval or an
	// external answer; Application submits it and calls Resume again.
	DispositionWaitingForResponse Disposition = "waiting_for_response"
	// DispositionWaitingForRecovery: an execution is in flight with no local
	// owner; Application runs Runtime.RecoverExpired and calls Resume again.
	DispositionWaitingForRecovery Disposition = "waiting_for_recovery"
	// DispositionActive: the driver returned before an idle point (for
	// example its context was cancelled); Resume continues.
	DispositionActive Disposition = "active"
)

type StartRequest struct {
	Ref    Ref
	RunID  run.RunID
	Inputs []run.AgentInput
}

type Result struct {
	Ref         Ref
	RunID       run.RunID
	Disposition Disposition
	Settlement  Settlement
	Waiting     []run.ResponseRequest
	Result      *run.RunResult
}

// linkage is the replayed view of one Turn.
type linkage struct {
	started *StartedPayload
	settled bool
	covered uint64 // highest Run revision already materialized
	inputs  map[run.InputID]run.CanonicalJSON
}

func (c *Coordinator) replay(ctx context.Context, ref Ref) (linkage, error) {
	events, err := c.Log.Replay(ctx, ref.Session)
	if err != nil {
		return linkage{}, err
	}
	l := linkage{inputs: make(map[run.InputID]run.CanonicalJSON)}
	for i := range events {
		ev := &events[i]
		if ev.Turn != ref.Turn {
			continue
		}
		switch ev.Type {
		case EventTurnStarted:
			var p StartedPayload
			if err := ev.Payload.Decode(&p); err != nil {
				return linkage{}, err
			}
			l.started = &p
		case EventTurnCompleted, EventTurnFailed:
			l.settled = true
		case EventInputDelivered:
			var p InputDeliveredPayload
			if err := ev.Payload.Decode(&p); err != nil {
				return linkage{}, err
			}
			l.inputs[p.InputID] = p.Content
		default:
			if ev.Revision > l.covered {
				l.covered = ev.Revision
			}
		}
	}
	return l, nil
}

// Start appends the Turn's started group, creates the Run and resumes
// (TRN-STR-3..6). A repeated Start with the same RunID is idempotent; a
// different RunID for a started Turn is a conflict.
func (c *Coordinator) Start(ctx context.Context, req StartRequest) (Result, error) {
	if req.Ref.Session == "" || req.Ref.Turn == "" || req.RunID == "" {
		return Result{}, errors.New("agent: turn: start requires session, turn and run ids")
	}
	l, err := c.replay(ctx, req.Ref)
	if err != nil {
		return Result{}, err
	}
	if l.started != nil {
		if l.started.RunID != req.RunID {
			return Result{}, ErrTurnConflict
		}
		return c.Resume(ctx, req.Ref)
	}
	ids := make([]run.InputID, len(req.Inputs))
	group := make([]Event, 0, len(req.Inputs)+1)
	for i, in := range req.Inputs {
		if in.ID == "" {
			return Result{}, fmt.Errorf("agent: turn: input %d has empty id", i)
		}
		ids[i] = in.ID
	}
	started, err := run.CanonicalJSONFromValue(StartedPayload{TurnID: req.Ref.Turn, RunID: req.RunID, InputIDs: ids})
	if err != nil {
		return Result{}, err
	}
	group = append(group, Event{Type: EventTurnStarted, Turn: req.Ref.Turn, Payload: started})
	for _, in := range req.Inputs {
		p, err := run.CanonicalJSONFromValue(InputDeliveredPayload{InputID: in.ID, TurnID: req.Ref.Turn, Content: in.Payload})
		if err != nil {
			return Result{}, err
		}
		group = append(group, Event{Type: EventInputDelivered, Turn: req.Ref.Turn, Payload: p})
	}
	if err := c.Log.Append(ctx, req.Ref.Session, group); err != nil {
		return Result{}, err
	}
	return c.Resume(ctx, req.Ref)
}

// Resume re-derives the Turn from the log, ensures the Run exists and holds
// every delivered input, materializes what is committed, drives, then
// materializes and settles (TRN-RSM-1..5).
func (c *Coordinator) Resume(ctx context.Context, ref Ref) (Result, error) {
	l, err := c.replay(ctx, ref)
	if err != nil {
		return Result{}, err
	}
	if l.started == nil {
		return Result{}, ErrTurnNotFound
	}
	runID := l.started.RunID
	if !l.settled {
		if err := c.ensureRun(ctx, ref, runID, &l); err != nil {
			return Result{}, err
		}
		if _, err := c.materialize(ctx, ref, runID, &l); err != nil {
			return Result{}, err
		}
		snap, err := c.Runtime.Load(ctx, runID)
		if err != nil {
			return Result{}, err
		}
		if !snap.State.Status.Terminal() {
			if err := c.Driver.Drive(ctx, runID); err != nil {
				return Result{}, err
			}
		}
	}
	return c.settle(ctx, ref, runID, &l)
}

// Stop cancels an active Run under a Turn-derived CommandID and settles
// (TRN-STP-1..3).
func (c *Coordinator) Stop(ctx context.Context, ref Ref) (Result, error) {
	l, err := c.replay(ctx, ref)
	if err != nil {
		return Result{}, err
	}
	if l.started == nil {
		return Result{}, ErrTurnNotFound
	}
	runID := l.started.RunID
	if !l.settled {
		snap, err := c.Runtime.Load(ctx, runID)
		if err != nil {
			return Result{}, err
		}
		if !snap.State.Status.Terminal() {
			proto, err := snap.Protocol()
			if err != nil {
				return Result{}, err
			}
			id := run.CommandID(es.DigestBytes([]byte("twilight/turn/cancel-run:" + string(ref.Session) + ":" + string(ref.Turn) + ":" + string(runID))))
			env, err := proto.BuildEnvelope(runID, id, run.CancelRun{Reason: run.ReasonCancelled})
			if err != nil {
				return Result{}, err
			}
			if _, err := c.Runtime.Commit(ctx, run.CommitRequest{BaseRevision: snap.Revision, Command: env}); err != nil && !errors.Is(err, run.ErrRunTerminal) {
				return Result{}, err
			}
		}
	}
	return c.settle(ctx, ref, runID, &l)
}

// ensureRun creates the Run (idempotent) and accepts every delivered input
// through its derived CommandID; Accepted and AlreadyApplied both advance,
// a terminal Run absorbs the rest (TRN-RSM-2..3).
func (c *Coordinator) ensureRun(ctx context.Context, ref Ref, runID run.RunID, l *linkage) error {
	newRun, err := run.BuildNewRun(runID, es.CausationID(string(ref.Session)+"/"+string(ref.Turn)))
	if err != nil {
		return err
	}
	if _, err := c.Runtime.Create(ctx, newRun); err != nil {
		return err
	}
	for _, id := range l.started.InputIDs {
		content, ok := l.inputs[id]
		if !ok {
			return fmt.Errorf("agent: turn: input %q named by started has no input_delivered", id)
		}
		snap, err := c.Runtime.Load(ctx, runID)
		if err != nil {
			return err
		}
		if snap.State.Status.Terminal() {
			return nil
		}
		proto, err := snap.Protocol()
		if err != nil {
			return err
		}
		env, err := proto.BuildEnvelope(runID, run.DeriveInputCommandID(runID, id), run.AcceptInput{Input: run.AgentInput{ID: id, Payload: content}})
		if err != nil {
			return err
		}
		if _, err := c.Runtime.Commit(ctx, run.CommitRequest{BaseRevision: snap.Revision, Command: env}); err != nil {
			// Not at Open (a step is in progress) means the input was already
			// consumed by an earlier prepare; the derived id would have
			// replayed otherwise.
			if errors.Is(err, run.ErrStaleRuntime) {
				continue
			}
			return err
		}
	}
	return nil
}

// materialize maps every transition above the coverage watermark into
// chatlog events and appends them as one group (TRN-MAT-1). It returns the
// verified record so callers settle from the same consistent read.
func (c *Coordinator) materialize(ctx context.Context, ref Ref, runID run.RunID, l *linkage) (run.RunRecord, error) {
	record, err := c.Runtime.Record(ctx, runID)
	if err != nil {
		return run.RunRecord{}, err
	}
	var group []Event
	for i := range record.Transitions {
		tr := &record.Transitions[i]
		if tr.Revision <= l.covered {
			continue
		}
		mapped, err := MapTransition(ref.Turn, tr)
		if err != nil {
			return run.RunRecord{}, err
		}
		group = append(group, mapped...)
	}
	if len(group) > 0 {
		if err := c.Log.Append(ctx, ref.Session, group); err != nil {
			return run.RunRecord{}, err
		}
		l.covered = group[len(group)-1].Revision
	}
	return record, nil
}

// settle materializes the tail and, when the Run is terminal and the Turn
// not yet settled, appends completed/failed from RunEnded (TRN-SET-3). The
// result reports the disposition read from the Runtime.
func (c *Coordinator) settle(ctx context.Context, ref Ref, runID run.RunID, l *linkage) (Result, error) {
	record, err := c.materialize(ctx, ref, runID, l)
	if err != nil {
		return Result{}, err
	}
	state := &record.Snapshot.State
	res := Result{Ref: ref, RunID: runID}
	if !state.Status.Terminal() {
		switch {
		case run.NeedsRecovery(*state):
			res.Disposition = DispositionWaitingForRecovery
		case len(run.WaitingCalls(*state)) > 0:
			res.Disposition = DispositionWaitingForResponse
			res.Waiting = run.WaitingCalls(*state)
		default:
			res.Disposition = DispositionActive
		}
		return res, nil
	}
	res.Disposition = DispositionFinished
	res.Result = state.Result
	settlement, typ := settlementOf(state.Result)
	res.Settlement = settlement
	if !l.settled {
		p := SettledPayload{TurnID: ref.Turn, RunID: runID, Settlement: settlement, Revision: record.Snapshot.Revision, Reason: string(state.Result.Reason)}
		if state.Result.Failure != nil {
			p.FailureClass = state.Result.Failure.Class
		} else if settlement == SettlementStopped {
			p.FailureClass = string(run.ReasonCancelled)
		}
		raw, err := run.CanonicalJSONFromValue(p)
		if err != nil {
			return Result{}, err
		}
		if err := c.Log.Append(ctx, ref.Session, []Event{{Type: typ, Turn: ref.Turn, Revision: record.Snapshot.Revision, Payload: raw}}); err != nil {
			return Result{}, err
		}
		l.settled = true
	}
	return res, nil
}

func settlementOf(r *run.RunResult) (settlement Settlement, eventType string) {
	switch r.Status {
	case run.RunCompleted:
		return SettlementCompleted, EventTurnCompleted
	case run.RunStopped:
		return SettlementStopped, EventTurnFailed
	default:
		return SettlementFailed, EventTurnFailed
	}
}
