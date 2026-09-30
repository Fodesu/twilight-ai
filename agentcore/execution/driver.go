package execution

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
)

// driver drives an active Turn to its next quiescent point. A wait a
// Responder can answer is not a quiescent point: the answer is committed
// and the drive continues from it. A drive that quiesces with executions in
// flight and no local waiter hands them to the recovery.
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
	// progress frames relayed by the Loop. nil discards them.
	sink loop.EventSink
}

// Drive drives the Turn while it is active: resolve its recorded preset and
// drive its Run to the next quiescent point. alreadyDriving is true when a
// concurrent local driver of the same Run was already carrying it, in which
// case this call drove nothing. w is the caller's ownership capability over
// the Session: the decision whether to drive reads w's own projections, and
// the Loop commits through w, so a superseded owner plans against its own
// epoch's view and is fenced at commit instead of adopting the new owner's
// state. The caller's ctx bounds the drive, so cancellation is the caller's
// decision. The Turn's committed status is the Turn protocol's read, not
// this method's.
func (d *driver) Drive(ctx context.Context, w writer.Writer, turnID turn.TurnID) (alreadyDriving bool, err error) {
	ref := turn.TurnRef{SessionID: w.SessionID(), TurnID: turnID}
	surface, err := turn.ReadSurface(ctx, w.Projections(), ref.SessionID)
	if err != nil {
		return false, err
	}
	view, ok := surface.Turns[ref.TurnID]
	if !ok {
		return false, fmt.Errorf("%w: unknown turn %s", turn.ErrConflict, ref.TurnID)
	}
	if view.Status != turn.TurnActive {
		return false, nil
	}
	l, err := d.loops.For(view.Preset)
	if err != nil {
		return false, err
	}
	for {
		res, err := l.Run(ctx, d.runs.Bind(w), view.RunID, d.sink)
		if err != nil {
			if errors.Is(err, loop.ErrRunAlreadyRunning) {
				return true, nil
			}
			return false, err
		}
		if res.ExecutionRecovery {
			// The drive quiesced with executions in flight and no local
			// waiter. Offer every Executing target reattachment and dispose
			// what no executor answers, instead of leaving the Turn to a
			// driver that already returned.
			if _, err := d.recovery.Recover(context.WithoutCancel(ctx), w); err != nil {
				d.recovery.fail(ref.SessionID, fmt.Errorf("execution: recovering a quiesced drive: %w", err))
			}
		}
		if res.Disposition == loop.LoopWaiting && d.responders != nil {
			settled, err := d.responders.answerWaiting(ctx, w, view.RunID)
			if err != nil {
				return false, err
			}
			if settled {
				continue
			}
		}
		return false, nil
	}
}

// ResumeWaiting answers, under the Session's recovery lifetime, every
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
