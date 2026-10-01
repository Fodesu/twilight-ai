// Package conversation is the orchestration over one owned Session: the
// Controller admits inputs into Turns and advances the Session by one step
// through the Engine it is given. The Turn protocol itself -- the
// Coordinator, the quiescence guards, the request and result vocabulary --
// is the Turn module's. Every call runs on the caller's goroutine and ctx
// and returns once the Session has nothing to do on it: an effect in flight
// settles in the Engine and reaches the host as a notice, and the host
// advances again. Which calls run in the background, what a reply is and
// which policies run at quiescence are the host's decisions, taken on the
// Settlement each call returns.
package conversation

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/execution"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
)

// Turns is the Turn protocol the Controller routes inputs into and reads
// status back from: the Turn module's Commands and Reader.
type Turns interface {
	turn.Commands
	turn.Reader
}

// DriveResult is what one step of a Turn reports: the Turn's committed
// answer and what the step left behind. InFlight counts the effects of the
// Run this process awaits after the step, whose Outcomes reach the host as
// notices; an active Turn with none in flight waits on something this
// process does not carry. AlreadyDriving reports another step of the same
// Run in progress in this process, in which case this call did nothing and
// the answer is the status as read. Both are facts about this process, not
// about the Turn, so they are not Turn dispositions: the Turn's durable
// vocabulary stays the Turn module's.
type DriveResult struct {
	turn.TurnResult
	InFlight       int
	AlreadyDriving bool
}

// Settlement is what one advance of the Session reports: the Turns it
// stepped, in order -- the Turn it began with and, when that Turn ended,
// every Turn it started from inputs that were submitted but not yet
// delivered. Quiescent reports that no Turn is active and no such input
// remains: the point at which the host's quiescence policies apply. It is
// false while a Turn is active, when another step of the same Run carries
// it (AlreadyDriving), when the advance stopped at the TurnBudget and when a
// concurrent advance took the pending inputs.
type Settlement struct {
	Turns     []DriveResult
	Quiescent bool
}

// Config composes one Controller. Writer is the ownership capability
// every command commits through; Engine, Turns, Chatlog and Projections are
// the composed core services of the same Session.
type Config struct {
	Writer      writer.Writer
	Engine      execution.Engine
	Turns       Turns
	Chatlog     *chatlog.Commands
	Projections session.ProjectionReader
	// Preset is the decision identity every Turn the Controller starts runs
	// under; required.
	Preset preset.PresetRef
	// NewTurnID mints TurnIDs for new Turns; nil selects the random default.
	NewTurnID func() turn.TurnID
	// RouteRetries bounds how many times one input's route is re-committed
	// after a conflict with a concurrent route before the last conflict is
	// returned; the input stays submitted and the next Submit, Advance or
	// Resume routes it. Zero selects DefaultRouteRetries.
	RouteRetries int
	// TurnBudget bounds how many Turns one advance starts from submitted,
	// undelivered inputs before returning ErrTurnBudget with the Turns so
	// far; the remaining inputs stay submitted for the next call. Zero
	// selects DefaultTurnBudget.
	TurnBudget int
}

// DefaultRouteRetries and DefaultTurnBudget are the liveness bounds a
// Config with zero values takes.
const (
	DefaultRouteRetries = 4
	DefaultTurnBudget   = 64
)

var (
	// ErrRouteContended reports a route that lost to concurrent routes
	// RouteRetries times. The input is submitted and stays undelivered; the
	// next Submit, Advance or Resume routes it. It is a transient answer,
	// unlike the turn.ErrConflict of a Turn that admits no route.
	ErrRouteContended = errors.New("conversation: route contended")
	// ErrTurnBudget reports an advance that stopped at the TurnBudget with
	// inputs still submitted.
	ErrTurnBudget = errors.New("conversation: turn budget exhausted with inputs still submitted")
)

// Controller is the orchestration over one owned Session. Submit admits an
// input into a Turn, Advance steps a Turn once and, when it ended, on
// through the Turns the remaining inputs start, Resume does the same for a
// Session as found after a restart or a notice, and Stop settles the active
// Turn. Every command runs through the Config's Writer; reads go by
// SessionID. Concurrent calls are safe: writes serialize in the Writer, and
// a call that meets a step in progress reports AlreadyDriving. It starts no
// goroutine of its own and waits on no effect.
type Controller struct {
	w      writer.Writer
	engine execution.Engine
	turns  Turns
	chat   *chatlog.Commands
	proj   session.ProjectionReader
	sid    session.SessionID
	preset preset.PresetRef
	newID  func() turn.TurnID

	routeRetries int
	turnBudget   int
}

// New returns the Controller of one owned Session.
func New(cfg Config) (*Controller, error) { //nolint:gocritic // hugeParam: Config is a by-value options struct read once
	if cfg.Writer == nil {
		return nil, errors.New("conversation: a writer is required")
	}
	if cfg.Engine == nil {
		return nil, errors.New("conversation: an engine is required")
	}
	if cfg.Turns == nil {
		return nil, errors.New("conversation: turn commands are required")
	}
	if cfg.Chatlog == nil {
		return nil, errors.New("conversation: chatlog commands are required")
	}
	if cfg.Projections == nil {
		return nil, errors.New("conversation: a projection reader is required")
	}
	if cfg.Preset.ID == "" || cfg.Preset.Digest == "" {
		return nil, errors.New("conversation: a preset ref is required")
	}
	newID := cfg.NewTurnID
	if newID == nil {
		newID = turn.NewTurnID
	}
	retry := cfg.RouteRetries
	if retry <= 0 {
		retry = DefaultRouteRetries
	}
	budget := cfg.TurnBudget
	if budget <= 0 {
		budget = DefaultTurnBudget
	}
	return &Controller{w: cfg.Writer, engine: cfg.Engine, turns: cfg.Turns, chat: cfg.Chatlog, proj: cfg.Projections,
		sid: cfg.Writer.SessionID(), preset: cfg.Preset, newID: newID, routeRetries: retry, turnBudget: budget}, nil
}

func (r *Controller) ref(turnID turn.TurnID) turn.TurnRef {
	return turn.TurnRef{SessionID: r.sid, TurnID: turnID}
}

// Submitted is what Submit reports: the Turn the input landed in, and
// whether another driver of this process is already carrying that Turn, in
// which case it settles and reports there and the caller has nothing to
// advance.
type Submitted struct {
	Ref            turn.TurnRef
	AlreadyDriving bool
}

// Submit records one input body under id and commits its route: into the
// active Turn when there is one, into a new Turn otherwise. The body is
// opaque to the Controller; the idempotency key is the id, so a retried
// submission replays. Nothing is driven: the caller advances the Turn,
// here or on another goroutine, with Advance.
func (r *Controller) Submit(ctx context.Context, id run.InputID, content run.CanonicalJSON) (Submitted, error) {
	in, err := r.chat.Submit(ctx, r.w, id, content)
	if err != nil {
		return Submitted{}, err
	}
	var lastErr error
	for attempt := 0; attempt < r.routeRetries; attempt++ {
		ref, err := r.route(ctx, []run.AgentInput{in})
		if err == nil {
			return Submitted{Ref: ref}, nil
		}
		if !errors.Is(err, turn.ErrConflict) {
			return Submitted{}, err
		}
		lastErr = err
		if taken, ok := r.absorbed(ctx, in); ok {
			return Submitted{Ref: taken.Ref, AlreadyDriving: true}, nil
		}
	}
	return Submitted{}, fmt.Errorf("%w after %d attempts: %s", ErrRouteContended, r.routeRetries, lastErr.Error())
}

// Send submits the input and advances the Session once: the first Turn of
// the Settlement is the one the input landed in, the rest are the Turns the
// remaining inputs started once it ended. When another step of this process
// took the input, the single Turn reports AlreadyDriving and that step
// settles.
func (r *Controller) Send(ctx context.Context, id run.InputID, content run.CanonicalJSON) (Settlement, error) {
	sub, err := r.Submit(ctx, id, content)
	if err != nil {
		return Settlement{}, err
	}
	if sub.AlreadyDriving {
		resp, _ := r.absorbedStatus(ctx, sub.Ref)
		return Settlement{Turns: []DriveResult{resp}}, nil
	}
	return r.Advance(ctx, sub.Ref.TurnID)
}

// Advance steps the Turn once. A Turn still active afterwards -- effects
// dispatched, a response awaited -- is the Settlement: its Outcomes reach
// the host as notices and the host advances again. A Turn that ended lets
// the inputs submitted but undelivered start the next Turn, stepped once in
// turn, until a Turn stays active or no input remains and no Turn is
// active: the Settlement is then Quiescent. An advance that stops at the
// TurnBudget returns the Turns so far with ErrTurnBudget. The caller's ctx
// bounds the step: a cancelled step leaves the Turn active for the next
// Resume.
func (r *Controller) Advance(ctx context.Context, turnID turn.TurnID) (Settlement, error) {
	resp, err := r.drive(ctx, turnID)
	if err != nil {
		return Settlement{}, err
	}
	return r.settle(ctx, &resp)
}

// Resume advances a Session as found after a restart or a notice: the
// still-active Turn when there is one, otherwise the Turn the submitted,
// undelivered inputs start. ok is false when there is neither; the
// Settlement is then Quiescent.
func (r *Controller) Resume(ctx context.Context) (Settlement, bool, error) {
	if active, ok, err := r.active(ctx); err != nil {
		return Settlement{}, false, err
	} else if ok {
		out, err := r.Advance(ctx, active)
		return out, true, err
	}
	resp, ok, err := r.next(ctx)
	if err != nil {
		return Settlement{}, false, err
	}
	if !ok {
		return Settlement{Quiescent: true}, false, nil
	}
	out, err := r.settle(ctx, &resp)
	return out, true, err
}

// Stop stops the active Turn; ok is false when no Turn is active. The
// stopped Turn's drive observes the cancellation and returns.
func (r *Controller) Stop(ctx context.Context, reason string) (turn.TurnResult, bool, error) {
	active, ok, err := r.active(ctx)
	if err != nil || !ok {
		return turn.TurnResult{}, false, err
	}
	resp, err := r.turns.Stop(ctx, r.w, turn.StopRequest{Ref: r.ref(active), Reason: reason})
	return resp, true, err
}

func (r *Controller) active(ctx context.Context) (turn.TurnID, bool, error) {
	surface, err := turn.ReadSurface(ctx, r.proj, r.sid)
	if err != nil {
		return "", false, err
	}
	if a, ok := surface.Active(); ok {
		return a.TurnID, true, nil
	}
	return "", false, nil
}

// drive steps the Turn once and reads its committed answer.
func (r *Controller) drive(ctx context.Context, turnID turn.TurnID) (DriveResult, error) {
	step, err := r.engine.Drive(ctx, r.w, turnID)
	if err != nil {
		return DriveResult{}, err
	}
	resp, err := r.turns.Status(ctx, r.ref(turnID))
	if err != nil {
		return DriveResult{}, err
	}
	return DriveResult{TurnResult: resp, InFlight: step.InFlight, AlreadyDriving: step.AlreadyDriving}, nil
}

// route commits the inputs' route: Deliver into the active Turn when there
// is one, Start a new one when there is none.
func (r *Controller) route(ctx context.Context, inputs []run.AgentInput) (turn.TurnRef, error) {
	surface, err := turn.ReadSurface(ctx, r.proj, r.sid)
	if err != nil {
		return turn.TurnRef{}, err
	}
	if active, ok := surface.Active(); ok {
		ref := r.ref(active.TurnID)
		if _, err := r.turns.Deliver(ctx, r.w, turn.DeliverRequest{Ref: ref, Inputs: inputs}); err != nil {
			return turn.TurnRef{}, err
		}
		return ref, nil
	}
	ref := r.ref(r.newID())
	if _, err := r.turns.Start(ctx, r.w, turn.StartRequest{Ref: ref, Inputs: inputs, Preset: r.preset}); err != nil {
		return turn.TurnRef{}, err
	}
	return ref, nil
}

// absorbed reports whether another driver already delivered the input; the
// Turn that took it settles and reports there.
func (r *Controller) absorbed(ctx context.Context, in run.AgentInput) (DriveResult, bool) {
	chat, err := chatlog.ReadSurface(ctx, r.proj, r.sid)
	if err != nil {
		return DriveResult{}, false
	}
	v, ok := chat.Inputs.Get(chatlog.InputID(in.ID))
	if !ok || v.Status == chatlog.InputSubmitted {
		return DriveResult{}, false
	}
	resp, _ := r.absorbedStatus(ctx, r.ref(turn.TurnID(v.Input.TurnID)))
	return resp, true
}

// absorbedStatus is the AlreadyDriving answer for a Turn another driver
// carries: its status as read, or its Ref alone when the read fails.
func (r *Controller) absorbedStatus(ctx context.Context, ref turn.TurnRef) (DriveResult, error) {
	out := DriveResult{TurnResult: turn.TurnResult{Ref: ref}, AlreadyDriving: true}
	resp, err := r.turns.Status(ctx, ref)
	if err == nil {
		out.TurnResult = resp
	}
	return out, err
}

// next starts a Turn from the submitted, undelivered inputs and drives it;
// ok is false when there is none.
func (r *Controller) next(ctx context.Context) (DriveResult, bool, error) {
	chat, err := chatlog.ReadSurface(ctx, r.proj, r.sid)
	if err != nil {
		return DriveResult{}, false, err
	}
	pending := chat.SubmittedInputs()
	if len(pending) == 0 {
		return DriveResult{}, false, nil
	}
	inputs := make([]run.AgentInput, len(pending))
	for i, in := range pending {
		inputs[i] = run.AgentInput{ID: run.InputID(in.ID), Digest: in.Digest}
	}
	ref, err := r.route(ctx, inputs)
	if err != nil {
		return DriveResult{}, false, err
	}
	resp, err := r.drive(ctx, ref.TurnID)
	return resp, err == nil, err
}

// settle is what follows one step: a Turn still active is the Settlement,
// its next step waits on a notice; a Turn that ended lets the submitted,
// undelivered inputs start the next Turn; when none remains and no Turn is
// active, the Settlement is Quiescent.
func (r *Controller) settle(ctx context.Context, resp *DriveResult) (Settlement, error) {
	out := Settlement{Turns: []DriveResult{*resp}}
	last := *resp
	for range r.turnBudget {
		if last.AlreadyDriving || last.Status == turn.TurnActive {
			// The step in progress, or the notice of what this step
			// dispatched, carries the Turn on.
			return out, nil
		}
		next, ok, err := r.next(ctx)
		if err != nil {
			if errors.Is(err, turn.ErrConflict) {
				// A concurrent advance took the inputs; it reports there.
				return out, nil
			}
			return out, err
		}
		if !ok {
			out.Quiescent = true
			return out, nil
		}
		out.Turns = append(out.Turns, next)
		last = next
	}
	return out, ErrTurnBudget
}
