package execution

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/run/store"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
)

// driver advances an active Turn by one step. An effect the step dispatches
// is awaited under the Session's lifetime and its Outcome settles through
// notify; a wait a Responder can answer is answered and the step goes on
// from the answer; a step that leaves effects in flight nobody here awaits
// hands them to the recovery.
type driver struct {
	// runs is the Run module's Session adapter; every drive binds it to the
	// caller's Writer.
	runs     *sessionstore.SessionRunStore
	loops    *loops
	recovery *recovery
	// responders answer the ExternalResponse waits a drive leaves; nil
	// answers none.
	responders *responders
	// sink receives the drives' provisional observations: the executor's
	// progress frames relayed from the Loop. nil discards them.
	sink loop.EventSink
}

// Drive advances the Turn while it is active: resolve its recorded preset
// and step its Run once. w is the caller's ownership capability over the
// Session: the decision whether to drive reads w's own projections, and the
// Loop commits through w, so a superseded owner plans against its own
// epoch's view and is fenced at commit instead of adopting the new owner's
// state. The caller's ctx bounds the step; what the step dispatched is
// awaited beyond it. The Turn's committed status is the Turn protocol's
// read, not this method's.
func (d *driver) Drive(ctx context.Context, w writer.Writer, turnID turn.TurnID) (DriveResult, error) {
	ref := turn.TurnRef{SessionID: w.SessionID(), TurnID: turnID}
	surface, err := turn.ReadSurface(ctx, w.Projections(), ref.SessionID)
	if err != nil {
		return DriveResult{}, err
	}
	view, ok := surface.Turns[ref.TurnID]
	if !ok {
		return DriveResult{}, fmt.Errorf("%w: unknown turn %s", turn.ErrConflict, ref.TurnID)
	}
	if view.Status != turn.TurnActive {
		return DriveResult{Finished: true}, nil
	}
	l, err := d.loops.For(view.Preset)
	if err != nil {
		return DriveResult{}, err
	}
	lt := d.recovery.lifetimeOf(w)
	for {
		if err := lt.fenced(); err != nil {
			return DriveResult{}, err
		}
		res, err := l.Advance(ctx, d.runs.Bind(w), view.RunID, d.sink)
		if err != nil {
			if errors.Is(err, loop.ErrRunAlreadyRunning) {
				return DriveResult{AlreadyDriving: true, InFlight: d.inFlight(lt, view.RunID)}, nil
			}
			return DriveResult{}, err
		}
		switch res.Disposition {
		case loop.LoopFinished:
			return DriveResult{Finished: true}, nil
		case loop.LoopDispatched:
			for _, key := range res.Dispatched {
				d.recovery.awaitOutcome(lt, key)
			}
			return DriveResult{Dispatched: len(res.Dispatched), InFlight: d.inFlight(lt, view.RunID)}, nil
		}
		// The Run waits. Effects in flight that this process awaits settle
		// through their Outcomes; any other -- an execution a previous owner
		// started, a dispatch whose answer was lost -- is reconciled with
		// the executor now rather than left to a drive that already returned.
		for _, key := range res.Executing {
			if !lt.awaiting(key) {
				if _, err := d.recovery.Recover(context.WithoutCancel(ctx), w); err != nil {
					if errors.Is(err, store.ErrOwnershipLost) {
						d.recovery.lost(lt)
						return DriveResult{}, err
					}
					d.recovery.report(ref.SessionID, fmt.Errorf("execution: recovering a waiting drive: %w", err))
				}
				break
			}
		}
		if d.responders == nil {
			return DriveResult{Waiting: true, InFlight: d.inFlight(lt, view.RunID)}, nil
		}
		settled, err := d.responders.answerWaiting(ctx, w, view.RunID)
		if err != nil {
			return DriveResult{}, err
		}
		if !settled {
			return DriveResult{Waiting: true, InFlight: d.inFlight(lt, view.RunID)}, nil
		}
		// An answered wait is not a quiescent point: the Run moves on from
		// the answer in this same step.
	}
}

// inFlight counts what this process carries for the Run after a step: the
// effects whose Outcomes it awaits and the waits a Responder is answering.
func (d *driver) inFlight(lt *lifetime, runID run.RunID) int {
	n := lt.inFlight(runID)
	if d.responders != nil {
		n += d.responders.answeringFor(runID)
	}
	return n
}

// ResumeWaiting answers, under the Session's lifetime, every
// ExternalResponse wait a previous owner left with a Responder. A Session
// that opens runs it after the takeover disposition: the Responder
// continues from its durable state. It returns at once; each answer is
// committed in the background and reported through notify, so the host
// drives the owning Turn on. Failures reach the recovery's fail.
func (d *driver) ResumeWaiting(ctx context.Context, w writer.Writer, notify func(*lifetime)) {
	if d.responders == nil {
		return
	}
	lt := d.recovery.lifetimeOf(w)
	d.responders.answerAll(ctx, w, lt.ctx, func(*answer) { notify(lt) })
}
