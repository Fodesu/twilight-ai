package app

import (
	"context"

	"github.com/felinics/twilight/agentcore/owner"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/turn"
)

// Owned is one acquired Session without a conversation over it: the
// ownership Handle and the outcome of the takeover that ran when it was
// acquired. It is what a caller that drives through the Handle itself holds.
type Owned struct {
	// Handle is the ownership capability every command commits through.
	Handle *owner.Handle
	// Recovered is the number of recovery commands the takeover issued.
	Recovered int
	app       *Application
}

// Acquire takes ownership of the Session and runs the takeover disposition;
// the waits a previous owner left are answered in the background and
// reported through the engine's notice. Close releases it.
func (app *Application) Acquire(ctx context.Context, sid session.SessionID) (*Owned, error) {
	h, err := app.Owner.Open(ctx, sid)
	if err != nil {
		return nil, err
	}
	n, err := app.Execution.Takeover(ctx, h.Writer())
	if err != nil {
		app.Execution.Detach(sid)
		_ = h.Close(context.WithoutCancel(ctx))
		return nil, err
	}
	return &Owned{Handle: h, Recovered: n, app: app}, nil
}

// resumeWaiting answers, under the engine, the waits a previous owner left
// open. It runs after the Session is reachable by the notice its answers
// send.
func (o *Owned) resumeWaiting(ctx context.Context) {
	o.app.Execution.ResumeWaiting(ctx, o.Handle.Writer())
}

// Drive steps the Turn on until it ends or waits on something this process
// does not carry, waiting between steps for the Outcomes the engine awaits:
// what a Session's Settle does for a Session held open, for a holder that
// drives through the Handle itself.
func (o *Owned) Drive(ctx context.Context, turnID turn.TurnID) (turn.TurnResult, error) {
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	events := o.app.Events(subCtx, o.Handle.ID())
	ref := turn.TurnRef{SessionID: o.Handle.ID(), TurnID: turnID}
	for {
		step, err := o.app.Execution.Drive(ctx, o.Handle.Writer(), turnID)
		if err != nil {
			return turn.TurnResult{}, err
		}
		res, err := o.app.Turns.Status(ctx, ref)
		if err != nil {
			return turn.TurnResult{}, err
		}
		if res.Status != turn.TurnActive || (step.InFlight == 0 && !step.AlreadyDriving) {
			return res, nil
		}
		if err := awaitProgress(ctx, events, res.RunID, turnID, step.AlreadyDriving); err != nil {
			return res, err
		}
	}
}

// Close ends the engine's listeners for the Session and releases the
// ownership.
func (o *Owned) Close(ctx context.Context) error {
	o.app.Execution.Detach(o.Handle.ID())
	return o.Handle.Close(ctx)
}
