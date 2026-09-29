// Package runtime is the conversation's execution over the fact and effect
// layers: the Coordinator commits the Turn protocol's cross-module commands
// (Turn facts, chatlog deliveries and Run commands in one unit), the
// quiescence guards judge when a Session admits work that moves the
// context, and the SessionRuntime admits inputs, routes them into Turns,
// drives the Turns to settlement and drains the submitted backlog until the
// Session is quiescent. Every call runs on the caller's goroutine and ctx;
// which calls run in the background, what a reply is and which policies
// run at quiescence are the host's decisions, taken on the Settlement each
// call returns.
package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/driver"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
)

// Turns is the Turn protocol the runtime routes inputs into and reads status
// back from.
type Turns interface {
	Commands
	Reader
}

// DriveResult is what one drive of a Turn reports: the Turn's committed
// answer, and whether another local driver of the same Run was already
// carrying it, in which case this call drove nothing and the answer is the
// status as read. AlreadyDriving is a fact about this process, not about
// the Turn, so it is not a Turn disposition: the Turn's durable vocabulary
// stays the Turn module's.
type DriveResult struct {
	TurnResult
	AlreadyDriving bool
}

// Settlement is what follows one drive: the Turn that was driven and the
// backlog Turns drained after it, in order. Quiescent reports that the
// backlog is drained and no Turn is active: the point at which the host's
// quiescence policies apply. It is false when another driver carries the
// settlement (AlreadyDriving), when the drain stopped at the DrainBudget
// and when a concurrent drain took the backlog.
type Settlement struct {
	Turns     []DriveResult
	Quiescent bool
}

// Config composes one SessionRuntime. Writer is the ownership capability
// every command commits through; Driver, Turns, Chatlog and Projections are
// the composed core services of the same Session.
type Config struct {
	Writer      writer.Writer
	Driver      *driver.Driver
	Turns       Turns
	Chatlog     *chatlog.Commands
	Projections session.ProjectionReader
	// Preset is the decision identity every Turn the runtime starts runs
	// under; required.
	Preset preset.PresetRef
	// NewTurnID mints TurnIDs for new Turns; nil selects the random default.
	NewTurnID func() turn.TurnID
	// RouteRetries bounds how many times one input's route is re-committed
	// after a conflict with a concurrent route before the last conflict is
	// returned; the input stays submitted and the next Send, Drain or
	// Resume routes it. Zero selects DefaultRouteRetries.
	RouteRetries int
	// DrainBudget bounds how many Turns one settlement drains from the
	// backlog before returning ErrDrainBudget with the results so far; the
	// remaining backlog stays submitted for the next call. Zero selects
	// DefaultDrainBudget.
	DrainBudget int
}

// DefaultRouteRetries and DefaultDrainBudget are the liveness bounds a
// Config with zero values takes.
const (
	DefaultRouteRetries = 4
	DefaultDrainBudget  = 64
)

var (
	// ErrRouteContended reports a route that lost to concurrent routes
	// RouteRetries times. The input is submitted and stays in the backlog;
	// the next Send, Drain or Resume routes it. It is a transient answer,
	// unlike the turn.ErrConflict of a Turn that admits no route.
	ErrRouteContended = errors.New("runtime: route contended")
	// ErrDrainBudget reports a settlement that stopped draining the backlog
	// at the DrainBudget with inputs still submitted.
	ErrDrainBudget = errors.New("runtime: drain budget exhausted with inputs still submitted")
)

// SessionRuntime is the conversation process over one owned Session: the
// input admission, the deliver-or-start routing, the driving and the
// backlog draining. Every command runs through the Config's Writer; reads
// go by SessionID. Concurrent calls are safe: writes serialize in the
// Writer, and a call whose input lands in a running Turn reports
// AlreadyDriving. It starts no goroutine of its own.
type SessionRuntime struct {
	w      writer.Writer
	driver *driver.Driver
	turns  Turns
	chat   *chatlog.Commands
	proj   session.ProjectionReader
	sid    session.SessionID
	preset preset.PresetRef
	newID  func() turn.TurnID

	routeRetries int
	drainBudget  int
}

// New returns the conversation process for one owned Session.
func New(cfg Config) (*SessionRuntime, error) { //nolint:gocritic // hugeParam: Config is a by-value options struct read once
	if cfg.Writer == nil {
		return nil, errors.New("runtime: a writer is required")
	}
	if cfg.Driver == nil {
		return nil, errors.New("runtime: a driver is required")
	}
	if cfg.Turns == nil {
		return nil, errors.New("runtime: turn commands are required")
	}
	if cfg.Chatlog == nil {
		return nil, errors.New("runtime: chatlog commands are required")
	}
	if cfg.Projections == nil {
		return nil, errors.New("runtime: a projection reader is required")
	}
	if cfg.Preset.ID == "" || cfg.Preset.Digest == "" {
		return nil, errors.New("runtime: a preset ref is required")
	}
	newID := cfg.NewTurnID
	if newID == nil {
		newID = turn.NewTurnID
	}
	retry := cfg.RouteRetries
	if retry <= 0 {
		retry = DefaultRouteRetries
	}
	budget := cfg.DrainBudget
	if budget <= 0 {
		budget = DefaultDrainBudget
	}
	return &SessionRuntime{w: cfg.Writer, driver: cfg.Driver, turns: cfg.Turns, chat: cfg.Chatlog, proj: cfg.Projections,
		sid: cfg.Writer.SessionID(), preset: cfg.Preset, newID: newID, routeRetries: retry, drainBudget: budget}, nil
}

func (r *SessionRuntime) ref(turnID turn.TurnID) turn.TurnRef {
	return turn.TurnRef{SessionID: r.sid, TurnID: turnID}
}

// Submit records one input body under id and returns the AgentInput routing
// names it by. The body is opaque to the runtime; the idempotency key is the
// id, so a retried submission replays.
func (r *SessionRuntime) Submit(ctx context.Context, id run.InputID, content run.CanonicalJSON) (run.AgentInput, error) {
	return r.chat.Submit(ctx, r.w, id, content)
}

// Send submits the input and drives the Session to quiescence: the first
// Turn of the Settlement is the one the input landed in, the rest are
// backlog Turns this call drained after its settlement. When another driver
// of this process took the input, the single Turn reports AlreadyDriving
// and that driver settles and drains.
func (r *SessionRuntime) Send(ctx context.Context, id run.InputID, content run.CanonicalJSON) (Settlement, error) {
	in, err := r.Submit(ctx, id, content)
	if err != nil {
		return Settlement{}, err
	}
	resp, err := r.route(ctx, in)
	if err != nil {
		return Settlement{}, err
	}
	return r.Settle(ctx, resp)
}

// RouteInput commits one submitted input's route -- Deliver into the active
// Turn, or Start a new one -- with the conflict retry of Config, and
// returns the Turn it landed in without driving it. absorbed reports that
// another driver delivered the input first; ref then names the Turn that
// took it, and that driver settles and drains. A caller that drives later,
// or elsewhere, continues with Drive and Settle.
func (r *SessionRuntime) RouteInput(ctx context.Context, in run.AgentInput) (ref turn.TurnRef, absorbed bool, err error) {
	var lastErr error
	for attempt := 0; attempt < r.routeRetries; attempt++ {
		ref, err := r.commitRoute(ctx, []run.AgentInput{in})
		if err == nil {
			return ref, false, nil
		}
		if !errors.Is(err, turn.ErrConflict) {
			return turn.TurnRef{}, false, err
		}
		lastErr = err
		if taken, ok := r.absorbed(ctx, in); ok {
			return taken.Ref, true, nil
		}
	}
	return turn.TurnRef{}, false, fmt.Errorf("%w after %d attempts: %s", ErrRouteContended, r.routeRetries, lastErr.Error())
}

// Route commits the route of the already-submitted inputs -- Deliver into
// the active Turn, or Start a new one -- and drives the Turn to its next
// quiescent point.
func (r *SessionRuntime) Route(ctx context.Context, inputs []run.AgentInput) (DriveResult, error) {
	ref, err := r.commitRoute(ctx, inputs)
	if err != nil {
		return DriveResult{}, err
	}
	return r.Drive(ctx, ref.TurnID)
}

// Drain starts the next Turn from the backlog of submitted, undelivered
// inputs and drives it; ok is false when there is none.
func (r *SessionRuntime) Drain(ctx context.Context) (DriveResult, bool, error) {
	resp, ok, err := r.drain(ctx)
	return resp, ok, err
}

// Resume drives a still-active Turn (after a restart) to settlement and
// drains the backlog; ok is false when no Turn is active.
func (r *SessionRuntime) Resume(ctx context.Context) (Settlement, bool, error) {
	active, ok, err := r.active(ctx)
	if err != nil || !ok {
		return Settlement{}, false, err
	}
	resp, err := r.Drive(ctx, active)
	if err != nil {
		return Settlement{}, false, err
	}
	out, err := r.Settle(ctx, resp)
	return out, true, err
}

// Stop stops the active Turn; ok is false when no Turn is active. The
// stopped Turn's drive observes the cancellation and returns.
func (r *SessionRuntime) Stop(ctx context.Context, reason string) (TurnResult, bool, error) {
	active, ok, err := r.active(ctx)
	if err != nil || !ok {
		return TurnResult{}, false, err
	}
	resp, err := r.turns.Stop(ctx, r.w, StopRequest{Ref: r.ref(active), Reason: reason})
	return resp, true, err
}

func (r *SessionRuntime) active(ctx context.Context) (turn.TurnID, bool, error) {
	surface, err := turn.ReadSurface(ctx, r.proj, r.sid)
	if err != nil {
		return "", false, err
	}
	if a, ok := surface.Active(); ok {
		return a.TurnID, true, nil
	}
	return "", false, nil
}

// route commits one input's route with the conflict retry of Config and
// drives the Turn the input landed in. The AlreadyDriving result reports
// that another driver of this process delivered the input first.
func (r *SessionRuntime) route(ctx context.Context, in run.AgentInput) (DriveResult, error) {
	var lastErr error
	for attempt := 0; attempt < r.routeRetries; attempt++ {
		ref, err := r.commitRoute(ctx, []run.AgentInput{in})
		if err == nil {
			return r.Drive(ctx, ref.TurnID)
		}
		if !errors.Is(err, turn.ErrConflict) {
			return DriveResult{}, err
		}
		lastErr = err
		if absorbed, taken := r.absorbed(ctx, in); taken {
			return absorbed, nil
		}
	}
	return DriveResult{}, fmt.Errorf("%w after %d attempts: %s", ErrRouteContended, r.routeRetries, lastErr.Error())
}

// Drive runs the Turn to its next quiescent point and reads its committed
// answer; AlreadyDriving reports a concurrent local driver of the same Run
// carried it, in which case the answer is the status as read. The caller's
// ctx bounds the drive: a cancelled drive leaves the Turn active for the
// next Resume.
func (r *SessionRuntime) Drive(ctx context.Context, turnID turn.TurnID) (DriveResult, error) {
	taken, err := r.driver.Drive(ctx, r.w, turnID)
	if err != nil {
		return DriveResult{}, err
	}
	resp, err := r.turns.Status(ctx, r.ref(turnID))
	if err != nil {
		return DriveResult{}, err
	}
	return DriveResult{TurnResult: resp, AlreadyDriving: taken}, nil
}

// commitRoute is the deliver-or-start half of routing: Deliver into the
// active Turn when there is one, Start a new one when there is none.
func (r *SessionRuntime) commitRoute(ctx context.Context, inputs []run.AgentInput) (turn.TurnRef, error) {
	surface, err := turn.ReadSurface(ctx, r.proj, r.sid)
	if err != nil {
		return turn.TurnRef{}, err
	}
	if active, ok := surface.Active(); ok {
		ref := r.ref(active.TurnID)
		if _, err := r.turns.Deliver(ctx, r.w, DeliverRequest{Ref: ref, Inputs: inputs}); err != nil {
			return turn.TurnRef{}, err
		}
		return ref, nil
	}
	ref := r.ref(r.newID())
	if _, err := r.turns.Start(ctx, r.w, StartRequest{Ref: ref, Inputs: inputs, Preset: r.preset}); err != nil {
		return turn.TurnRef{}, err
	}
	return ref, nil
}

// absorbed reports whether another driver already delivered the input; the
// Turn that took it settles and reports there.
func (r *SessionRuntime) absorbed(ctx context.Context, in run.AgentInput) (DriveResult, bool) {
	chat, err := chatlog.ReadSurface(ctx, r.proj, r.sid)
	if err != nil {
		return DriveResult{}, false
	}
	v, ok := chat.Inputs.Get(chatlog.InputID(in.ID))
	if !ok || v.Status == chatlog.InputSubmitted {
		return DriveResult{}, false
	}
	ref := r.ref(turn.TurnID(v.Input.TurnID))
	out := DriveResult{TurnResult: TurnResult{Ref: ref}, AlreadyDriving: true}
	if resp, err := r.turns.Status(ctx, ref); err == nil {
		out.TurnResult = resp
		out.AlreadyDriving = true
	}
	return out, true
}

// drain starts the next Turn from the backlog of submitted, undelivered
// inputs and drives it; ok is false when there is none.
func (r *SessionRuntime) drain(ctx context.Context) (DriveResult, bool, error) {
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
	resp, err := r.Route(ctx, inputs)
	return resp, err == nil, err
}

// Settle is what follows one drive: while the settlement leaves submitted,
// undelivered inputs, the next Turn starts from them; when the backlog is
// drained and no Turn is active, the Settlement is Quiescent. A drain that
// stops at the DrainBudget returns the Turns so far with ErrDrainBudget.
func (r *SessionRuntime) Settle(ctx context.Context, resp DriveResult) (Settlement, error) {
	out := Settlement{Turns: []DriveResult{resp}}
	if resp.AlreadyDriving {
		// The running driver settles the Turn and drains in its own call.
		return out, nil
	}
	for range r.drainBudget {
		next, ok, err := r.drain(ctx)
		if err != nil {
			if errors.Is(err, turn.ErrConflict) {
				// A concurrent Send or drain took the backlog; it reports there.
				return out, nil
			}
			return out, err
		}
		if !ok {
			out.Quiescent = true
			return out, nil
		}
		out.Turns = append(out.Turns, next)
		if next.AlreadyDriving {
			return out, nil
		}
	}
	return out, ErrDrainBudget
}
