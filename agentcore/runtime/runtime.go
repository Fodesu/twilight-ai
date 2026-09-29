// Package runtime is the conversation's execution over the fact and effect
// layers: the Coordinator commits the Turn protocol's cross-module commands
// (Turn facts, chatlog deliveries and Run commands in one unit), the
// quiescence guards judge when a Session admits work that moves the
// context, and the SessionRuntime admits inputs, routes them into Turns,
// drives the Turns to settlement and drains the submitted backlog until the
// Session is quiescent. What a reply is and which policies run at
// quiescence are the host's decisions, hooked in Config.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/felinics/twilight/agentcore/driver"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/session/extension"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
)

// Turns is the Turn protocol the runtime routes inputs into and reads status
// back from.
type Turns interface {
	turn.Commands
	turn.Reader
}

// Config composes one SessionRuntime. Writer is the ownership capability
// every command commits through; Driver, Turns, Chatlog and Projections are
// the composed core services of the same Session.
type Config struct {
	Writer      writer.Writer
	Driver      *driver.Driver
	Turns       Turns
	Chatlog     *chatlog.Commands
	Projections extension.ProjectionReader
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
	// OnQuiescent runs after a settlement drained the backlog: the
	// quiescence point for the host's policies. Nil skips it.
	OnQuiescent func(ctx context.Context)
	// OnBackgroundFailure receives failures of background drives; nil
	// discards them.
	OnBackgroundFailure func(ref turn.TurnRef, err error)
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
// input admission, the deliver-or-start routing, the driving, the backlog
// draining and the background execution that carries submitted work. Every
// command runs through the Config's Writer; reads go by SessionID.
// Concurrent calls are safe: writes serialize in the Writer, and a call
// whose input lands in a running Turn reports AlreadyDriving.
type SessionRuntime struct {
	w      writer.Writer
	driver *driver.Driver
	turns  Turns
	chat   *chatlog.Commands
	proj   extension.ProjectionReader
	sid    session.SessionID
	preset preset.PresetRef
	newID  func() turn.TurnID

	routeRetries int
	drainBudget  int
	onQuiescent  func(ctx context.Context)
	onFailure    func(ref turn.TurnRef, err error)

	// bg bounds the background tasks Go and the asynchronous submits
	// start; Close cancels it and waits for them. mu guards n, idle and
	// lastActive: n counts the tasks in flight, idle is closed when the
	// count returns to zero, and lastActive drives IdleFor.
	bg         context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	n          int
	idle       chan struct{}
	lastActive time.Time
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
	r := &SessionRuntime{w: cfg.Writer, driver: cfg.Driver, turns: cfg.Turns, chat: cfg.Chatlog, proj: cfg.Projections,
		sid: cfg.Writer.SessionID(), preset: cfg.Preset, newID: newID, routeRetries: retry, drainBudget: budget,
		onQuiescent: cfg.OnQuiescent, onFailure: cfg.OnBackgroundFailure, lastActive: time.Now()}
	r.bg, r.cancel = context.WithCancel(context.Background())
	return r, nil
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
// result is the Turn the input landed in, the rest are backlog Turns this
// call drained after its settlement. When another driver of this process
// took the input, the single result reports AlreadyDriving and that driver
// settles and drains.
func (r *SessionRuntime) Send(ctx context.Context, id run.InputID, content run.CanonicalJSON) ([]driver.DriveResult, error) {
	in, err := r.Submit(ctx, id, content)
	if err != nil {
		return nil, err
	}
	resp, err := r.route(ctx, in)
	if err != nil {
		return nil, err
	}
	return r.settle(ctx, resp)
}

// SubmitAsync submits the input and returns the Turn it landed in without
// waiting; the Turn is driven to settlement -- and the backlog drained -- in
// the background, with failures reported to Config.OnBackgroundFailure.
// Close cancels the background drive; a cancelled Turn stays active and
// resumes on the next Resume.
func (r *SessionRuntime) SubmitAsync(ctx context.Context, id run.InputID, content run.CanonicalJSON) (turn.TurnRef, error) {
	in, err := r.Submit(ctx, id, content)
	if err != nil {
		return turn.TurnRef{}, err
	}
	ref, absorbed, err := r.routeCommit(ctx, in)
	if err != nil {
		return turn.TurnRef{}, err
	}
	if absorbed {
		// A running driver carries the input; nothing to drive here.
		return ref, nil
	}
	r.driveAsync(ref)
	return ref, nil
}

// Route commits the route of the already-submitted inputs -- Deliver into
// the active Turn, or Start a new one -- and drives the Turn to its next
// quiescent point.
func (r *SessionRuntime) Route(ctx context.Context, inputs []run.AgentInput) (driver.DriveResult, error) {
	ref, err := r.commitRoute(ctx, inputs)
	if err != nil {
		return driver.DriveResult{}, err
	}
	return r.driver.Drive(ctx, r.w, ref.TurnID)
}

// Drain starts the next Turn from the backlog of submitted, undelivered
// inputs and drives it; ok is false when there is none.
func (r *SessionRuntime) Drain(ctx context.Context) (driver.DriveResult, bool, error) {
	resp, ok, err := r.drain(ctx)
	return resp, ok, err
}

// Resume drives a still-active Turn (after a restart) to settlement and
// drains the backlog; ok is false when no Turn is active.
func (r *SessionRuntime) Resume(ctx context.Context) ([]driver.DriveResult, bool, error) {
	active, ok, err := r.active(ctx)
	if err != nil || !ok {
		return nil, false, err
	}
	resp, err := r.driver.Drive(ctx, r.w, active)
	if err != nil {
		return nil, false, err
	}
	out, err := r.settle(ctx, resp)
	return out, true, err
}

// Stop stops the active Turn; ok is false when no Turn is active. The
// stopped Turn's drive observes the cancellation and returns.
func (r *SessionRuntime) Stop(ctx context.Context, reason string) (turn.TurnResponse, bool, error) {
	active, ok, err := r.active(ctx)
	if err != nil || !ok {
		return turn.TurnResponse{}, false, err
	}
	resp, err := r.turns.Stop(ctx, r.w, turn.StopRequest{Ref: r.ref(active), Reason: reason})
	return resp, true, err
}

// Wait blocks until every background task started so far has finished, or
// ctx ends. It does not cancel anything; Close does.
func (r *SessionRuntime) Wait(ctx context.Context) error {
	r.mu.Lock()
	idle, n := r.idle, r.n
	r.mu.Unlock()
	if n == 0 {
		return nil
	}
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Go runs fn as a background task: Close cancels the runtime's context and
// Wait covers fn.
func (r *SessionRuntime) Go(fn func(ctx context.Context)) {
	r.track()
	go func() {
		defer r.untrack()
		fn(r.bg)
	}()
}

// Touch records activity: the idle clock restarts.
func (r *SessionRuntime) Touch() {
	r.mu.Lock()
	r.lastActive = time.Now()
	r.mu.Unlock()
}

// IdleFor reports no background task has run for at least d since the last
// recorded activity.
func (r *SessionRuntime) IdleFor(d time.Duration) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n == 0 && time.Since(r.lastActive) >= d
}

// Close cancels the background tasks and waits for them to return.
func (r *SessionRuntime) Close() {
	r.cancel()
	_ = r.Wait(context.Background()) // the tasks observe the cancelled ctx and return
}

func (r *SessionRuntime) track() {
	r.mu.Lock()
	if r.n == 0 {
		r.idle = make(chan struct{})
	}
	r.n++
	r.lastActive = time.Now()
	r.mu.Unlock()
}

func (r *SessionRuntime) untrack() {
	r.mu.Lock()
	r.n--
	if r.n == 0 {
		close(r.idle)
	}
	r.lastActive = time.Now()
	r.mu.Unlock()
}

func (r *SessionRuntime) fail(ref turn.TurnRef, err error) {
	if r.onFailure != nil {
		r.onFailure(ref, err)
	}
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
func (r *SessionRuntime) route(ctx context.Context, in run.AgentInput) (driver.DriveResult, error) {
	var lastErr error
	for attempt := 0; attempt < r.routeRetries; attempt++ {
		ref, err := r.commitRoute(ctx, []run.AgentInput{in})
		if err == nil {
			return r.driver.Drive(ctx, r.w, ref.TurnID)
		}
		if !errors.Is(err, turn.ErrConflict) {
			return driver.DriveResult{}, err
		}
		lastErr = err
		if absorbed, taken := r.absorbed(ctx, in); taken {
			return absorbed, nil
		}
	}
	return driver.DriveResult{}, fmt.Errorf("%w after %d attempts: %s", ErrRouteContended, r.routeRetries, lastErr.Error())
}

// routeCommit is the commit half of route for callers that drive themselves:
// ok is false and ref names the Turn the input landed in; ok is true and
// ref names the Turn that absorbed it, when another driver took the input
// first.
func (r *SessionRuntime) routeCommit(ctx context.Context, in run.AgentInput) (ref turn.TurnRef, absorbed bool, err error) {
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

// commitRoute is the deliver-or-start half of routing: Deliver into the
// active Turn when there is one, Start a new one when there is none.
func (r *SessionRuntime) commitRoute(ctx context.Context, inputs []run.AgentInput) (turn.TurnRef, error) {
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
func (r *SessionRuntime) absorbed(ctx context.Context, in run.AgentInput) (driver.DriveResult, bool) {
	chat, err := chatlog.ReadSurface(ctx, r.proj, r.sid)
	if err != nil {
		return driver.DriveResult{}, false
	}
	v, ok := chat.Inputs.Get(chatlog.InputID(in.ID))
	if !ok || v.Status == chatlog.InputSubmitted {
		return driver.DriveResult{}, false
	}
	ref := r.ref(turn.TurnID(v.Input.TurnID))
	out := driver.DriveResult{TurnResponse: turn.TurnResponse{Ref: ref}, AlreadyDriving: true}
	if resp, err := r.turns.Status(ctx, ref); err == nil {
		out.TurnResponse = resp
		out.AlreadyDriving = true
	}
	return out, true
}

// drain starts the next Turn from the backlog of submitted, undelivered
// inputs and drives it; ok is false when there is none.
func (r *SessionRuntime) drain(ctx context.Context) (driver.DriveResult, bool, error) {
	chat, err := chatlog.ReadSurface(ctx, r.proj, r.sid)
	if err != nil {
		return driver.DriveResult{}, false, err
	}
	pending := chat.SubmittedInputs()
	if len(pending) == 0 {
		return driver.DriveResult{}, false, nil
	}
	inputs := make([]run.AgentInput, len(pending))
	for i, in := range pending {
		inputs[i] = run.AgentInput{ID: run.InputID(in.ID), Digest: in.Digest}
	}
	resp, err := r.Route(ctx, inputs)
	return resp, err == nil, err
}

// settle is what follows one drive: while the settlement leaves submitted,
// undelivered inputs, the next Turn starts from them; when the backlog is
// drained and no Turn is active, the quiescence hook runs.
func (r *SessionRuntime) settle(ctx context.Context, resp driver.DriveResult) ([]driver.DriveResult, error) {
	out := []driver.DriveResult{resp}
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
			if r.onQuiescent != nil {
				r.onQuiescent(ctx)
			}
			return out, nil
		}
		out = append(out, next)
		if next.AlreadyDriving {
			return out, nil
		}
	}
	return out, ErrDrainBudget
}

// driveAsync drives the Turn to settlement and drains the backlog under the
// runtime's background context; failures reach Config.OnBackgroundFailure.
func (r *SessionRuntime) driveAsync(ref turn.TurnRef) {
	r.track()
	go func() {
		defer r.untrack()
		resp, err := r.driver.Drive(r.bg, r.w, ref.TurnID)
		if err != nil {
			r.fail(ref, fmt.Errorf("driving turn %s: %w", ref.TurnID, err))
			return
		}
		if _, err := r.settle(r.bg, resp); err != nil {
			r.fail(ref, fmt.Errorf("settling turn %s: %w", ref.TurnID, err))
		}
	}()
}
