package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/felinics/twilight/agent/context/compaction"
	"github.com/felinics/twilight/agent/input"
	"github.com/felinics/twilight/agent/workspace"
	"github.com/felinics/twilight/agentcore/driver"
	"github.com/felinics/twilight/agentcore/owner"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	rt "github.com/felinics/twilight/agentcore/runtime"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/turn"
)

// SessionOptions tunes OpenSession.
type SessionOptions struct {
	// Preset is the decision identity new Turns run under (required).
	Preset preset.PresetRef
	// NewTurnID mints TurnIDs; nil selects the random default.
	NewTurnID func() turn.TurnID
	// ResumeActive resumes a still-active Turn synchronously inside
	// OpenSession. Interactive hosts leave it false and call Resume themselves.
	ResumeActive bool
	// CompactAfterEntries triggers automatic compaction when the context
	// grows past this many entries after a settlement; zero disables it.
	CompactAfterEntries int
	// CompactRetainEntries is the pair-closed suffix a compaction keeps
	// verbatim; zero selects the default.
	CompactRetainEntries int
	// CompactWarn receives automatic-compaction failures; they never change
	// the settled results. Nil discards them.
	CompactWarn func(error)
	// RouteRetries bounds how many times one input's route is re-committed
	// after a conflict with a concurrent route before the last conflict is
	// returned to the caller; the input stays submitted and the next Send,
	// Drain or Resume routes it. Zero selects DefaultRouteRetries.
	RouteRetries int
	// DrainBudget bounds how many Turns one settlement drains from the
	// backlog before returning ErrDrainBudget with the Results so far; the
	// remaining backlog stays submitted for the next call. Zero selects
	// DefaultDrainBudget.
	DrainBudget int
	// InboxPoll is how often the open Session reads its command inbox
	// besides being woken; zero selects DefaultInboxPoll.
	InboxPoll time.Duration
	// InheritedWorkspace is what this Session does, when opened, with a
	// workspace binding it inherited from its fork parent; the zero value
	// keeps the parent's Workspace.
	InheritedWorkspace workspace.InheritedPolicy
}

// DefaultRouteRetries and DefaultDrainBudget are the liveness bounds a
// SessionOptions with zero values takes.
const (
	DefaultRouteRetries = rt.DefaultRouteRetries
	DefaultDrainBudget  = rt.DefaultDrainBudget
)

// ErrDrainBudget reports a settlement that stopped draining the backlog at
// the DrainBudget with inputs still submitted.
var ErrDrainBudget = rt.ErrDrainBudget

// ErrRouteContended reports a route that lost to concurrent routes
// RouteRetries times. The input is submitted and stays in the backlog; the
// next Send, Drain or Resume routes it. It is a transient answer, unlike the
// turn.ErrConflict of a Turn that admits no route.
var ErrRouteContended = rt.ErrRouteContended

// Result is the conversation-level outcome of one settled (or steered) Turn.
type Result struct {
	TurnID      turn.TurnID
	Status      turn.TurnStatus
	Disposition turn.ResumeDisposition
	// AlreadyDriving reports that another driver in this process carries the
	// Turn: the input is committed, its settlement and reply are reported by
	// that driver. It is a fact about this process, not a Turn disposition.
	AlreadyDriving bool
	// Reply is the settled Turn's last assistant text; empty while the Turn
	// still runs or when the attempt produced no text.
	Reply string
}

// SessionStatus reports what a caller may need to act on after opening.
type SessionStatus struct {
	Active turn.TurnID
	Failed []turn.TurnID
}

// Session is the application's conversation over one owned Session: the
// host-facing façade whose routing, driving, draining and background
// execution the runtime owns, with this layer's replies, workspace
// binding and snapshot policy and automatic compaction on top.
// Concurrent calls are safe: writes serialize in the Session Writer, and a
// call whose input lands in a running Turn reports already_driving.
type Session struct {
	// Recovered is the takeover disposition count from opening.
	Recovered int

	app  *Application
	a    *owner.Owner
	h    *owner.Handle
	sid  session.SessionID
	opts SessionOptions
	rt   *rt.SessionRuntime

	// loops bounds the Session's service goroutines (the inbox applier and
	// the idle release); loopsCancel ends them and Close waits for them
	// before releasing the runtime's background tasks and the ownership.
	loops       sync.WaitGroup
	loopsCtx    context.Context
	loopsCancel context.CancelFunc
	// inboxWake wakes the applier; inboxMu serializes applier passes.
	inboxWake chan struct{}
	inboxMu   sync.Mutex
	// snapshotMu serializes the background snapshots of this Session.
	snapshotMu sync.Mutex
}

// OpenSession ensures the stream exists, takes ownership per the
// application's Ownership configuration, runs the takeover disposition and
// returns the conversation.
func (app *Application) OpenSession(ctx context.Context, sid session.SessionID, opts SessionOptions) (*Session, error) {
	if opts.Preset.ID == "" || opts.Preset.Digest == "" {
		return nil, errors.New("app: open session requires a preset ref")
	}
	a := app.Owner
	if _, err := a.Presets.Resolve(opts.Preset); err != nil {
		return nil, err
	}
	if err := a.EnsureSession(ctx, sid); err != nil {
		return nil, err
	}
	h, err := a.Open(ctx, sid)
	if err != nil {
		return nil, err
	}
	s := &Session{Recovered: h.Recovered, app: app, a: a, h: h, sid: sid, opts: opts}
	s.rt, err = rt.New(rt.Config{
		Writer: h.Writer(), Driver: a.Driver, Turns: a.Turns, Chatlog: a.Chatlog, Projections: a.Projections,
		Preset: opts.Preset, NewTurnID: opts.NewTurnID,
		RouteRetries: opts.RouteRetries, DrainBudget: opts.DrainBudget,
		OnQuiescent: s.quiescentPolicies, OnBackgroundFailure: s.backgroundFailed,
	})
	if err != nil {
		_ = h.Close(context.WithoutCancel(ctx))
		return nil, err
	}
	app.track(s)
	// A binding inherited from the fork parent is settled by this Session's
	// policy before anything runs in it.
	if err := s.applyInheritedWorkspace(ctx); err != nil {
		_ = s.Close(context.WithoutCancel(ctx))
		return nil, err
	}
	s.loopsCtx, s.loopsCancel = context.WithCancel(context.Background())
	// Commands left while no one owned the Session are applied before
	// anything else this owner does.
	s.startInbox(ctx)
	if opts.ResumeActive {
		if _, _, err := s.Resume(ctx); err != nil {
			_ = s.Close(context.WithoutCancel(ctx))
			return nil, err
		}
	}
	s.startIdleRelease()
	return s, nil
}

// ID is the Session's identity.
func (s *Session) ID() session.SessionID { return s.sid }

// Handle is the ownership capability the conversation runs under.
func (s *Session) Handle() *owner.Handle { return s.h }

func (s *Session) ref(turnID turn.TurnID) turn.TurnRef {
	return turn.TurnRef{SessionID: s.sid, TurnID: turnID}
}

// Wait blocks until every background drive Submit has started so far has
// finished, or ctx ends. It does not cancel anything; Close does.
func (s *Session) Wait(ctx context.Context) error { return s.rt.Wait(ctx) }

// Status reports the active Turn and the Turns awaiting Retry or Settle.
func (s *Session) Status(ctx context.Context) (SessionStatus, error) {
	surface, err := turn.ReadSurface(ctx, s.a.Projections, s.sid)
	if err != nil {
		return SessionStatus{}, err
	}
	var out SessionStatus
	if v, ok := surface.Active(); ok {
		out.Active = v.TurnID
	}
	return out, nil
}

// Events is this Session's event stream from now on.
func (s *Session) Events(ctx context.Context) <-chan Event { return s.app.Events(ctx, s.sid) }

// Send submits text and blocks until it is settled or absorbed: the first
// Result is the Turn the input landed in, further Results are backlog Turns
// this call drained after settlement. A settlement that stopped at the
// DrainBudget returns the Results so far with ErrDrainBudget.
func (s *Session) Send(ctx context.Context, text string) ([]Result, error) {
	settlements, err := s.rt.Send(ctx, chatlog.NewInputID(), input.Text(text))
	if settlements == nil {
		return nil, err
	}
	return s.results(ctx, settlements), err
}

// Submit submits text under a fresh InputID; see SubmitInput.
func (s *Session) Submit(ctx context.Context, text string) (turn.TurnRef, error) {
	return s.SubmitInput(ctx, chatlog.NewInputID(), text)
}

// SubmitInput submits text under the caller's InputID, commits its route
// and returns the Turn it landed in without waiting. The InputID is the
// idempotency key: a retried submission replays. The Turn is driven to
// settlement -- and the backlog drained -- in the background; progress and
// the reply arrive on Events, failures on Events and Config.Warn. Close
// cancels the background drive; a cancelled Turn stays active and resumes
// on the next open.
func (s *Session) SubmitInput(ctx context.Context, id run.InputID, text string) (turn.TurnRef, error) {
	return s.rt.SubmitAsync(ctx, id, input.Text(text))
}

// Stop stops the active Turn; ok is false when no Turn is active. The
// stopped Turn's drive observes the cancellation and returns.
func (s *Session) Stop(ctx context.Context, reason string) (turn.TurnResponse, bool, error) {
	return s.rt.Stop(ctx, reason)
}

// Route commits the inputs' route -- Deliver into the active Turn, or Start
// a new one -- then drives the Turn to its next quiescent point.
func (s *Session) Route(ctx context.Context, inputs []run.AgentInput) (driver.DriveResult, error) {
	return s.rt.Route(ctx, inputs)
}

// Drain starts the next Turn from the backlog of submitted, undelivered
// inputs and drives it; ok is false when there is none.
func (s *Session) Drain(ctx context.Context) (driver.DriveResult, bool, error) {
	return s.rt.Drain(ctx)
}

// Resume drives a still-active Turn (after a restart) to settlement and
// drains the backlog; ok is false when no Turn is active. A settlement that
// stopped at the DrainBudget returns the Results so far with ErrDrainBudget.
func (s *Session) Resume(ctx context.Context) ([]Result, bool, error) {
	settlements, ok, err := s.rt.Resume(ctx)
	if !ok {
		return nil, false, err
	}
	return s.results(ctx, settlements), true, err
}

// results wraps settled drive results with each Turn's reply;
// materialization failures are reported to Warn and leave Reply empty.
func (s *Session) results(ctx context.Context, settlements []driver.DriveResult) []Result {
	out := make([]Result, len(settlements))
	for i := range settlements {
		out[i] = s.result(ctx, &settlements[i])
	}
	return out
}

// result wraps one drive result with the settled Turn's reply;
// materialization failures are reported to Warn and leave Reply empty.
func (s *Session) result(ctx context.Context, resp *driver.DriveResult) Result {
	r := Result{TurnID: resp.Ref.TurnID, Status: resp.Status, Disposition: resp.Disposition, AlreadyDriving: resp.AlreadyDriving}
	if !resp.AlreadyDriving && resp.Disposition == turn.ResumeFinished {
		text, err := s.app.Reply(ctx, resp.Ref)
		if err != nil {
			s.app.warn(fmt.Errorf("app: materialize reply of turn %s: %w", resp.Ref.TurnID, err))
		}
		r.Reply = text
	}
	return r
}

// quiescentPolicies is the runtime's settlement hook: a settlement that
// drained the backlog snapshots the bound Workspace and runs the automatic
// compaction policy.
func (s *Session) quiescentPolicies(ctx context.Context) {
	s.maybeSnapshot(ctx)
	s.maybeCompact(ctx)
}

// backgroundFailed reports a failed background drive to Warn and, as an
// Event, to the Session's subscribers.
func (s *Session) backgroundFailed(ref turn.TurnRef, err error) {
	s.app.fail(ref.SessionID, fmt.Errorf("app: %w", err))
}

// BindWorkspace binds the Session to the Workspace: from the next tool call
// on, its workspace-placed tools run there. A Session that inherited the
// binding from its fork parent makes it its own.
func (s *Session) BindWorkspace(ctx context.Context, id workspace.ID) error {
	if s.app.workspaces == nil {
		return ErrNoWorkspaces
	}
	if _, err := s.app.workspaces.Store.Get(ctx, id); err != nil {
		return err
	}
	return s.app.bindings.Bind(ctx, s.h.Writer(), id)
}

// UnbindWorkspace records that the Session works in no Workspace, ending
// its own or an inherited binding.
func (s *Session) UnbindWorkspace(ctx context.Context, reason string) error {
	if s.app.workspaces == nil {
		return ErrNoWorkspaces
	}
	return s.app.bindings.Unbind(ctx, s.h.Writer(), reason)
}

// Workspace is the Session's current binding.
func (s *Session) Workspace(ctx context.Context) (workspace.Binding, error) {
	return s.app.Workspace(ctx, s.sid)
}

// ErrNoSnapshots reports a snapshot in a process with no Snapshotter: the
// workspace backend takes snapshots, in this process or through its client
// (Config.Workspaces.Snapshots).
var ErrNoSnapshots = errors.New("app: no workspace snapshotter is configured")

// ErrUnboundWorkspace reports a workspace operation on a Session bound to
// no Workspace.
var ErrUnboundWorkspace = errors.New("app: the session is bound to no workspace")

// SnapshotWorkspace takes a Snapshot of the Session's bound Workspace and
// records it on the Session. A Workspace never materialized yields
// sandbox.ErrNothingToSnapshot.
func (s *Session) SnapshotWorkspace(ctx context.Context) (workspace.Snapshot, error) {
	if s.app.workspaces == nil {
		return workspace.Snapshot{}, ErrNoWorkspaces
	}
	if s.app.snapshots == nil {
		return workspace.Snapshot{}, ErrNoSnapshots
	}
	b, err := s.Workspace(ctx)
	if err != nil {
		return workspace.Snapshot{}, err
	}
	if !b.Bound {
		return workspace.Snapshot{}, ErrUnboundWorkspace
	}
	snap, err := s.app.snapshots.Snapshot(ctx, b.Workspace)
	if err != nil {
		return workspace.Snapshot{}, err
	}
	if err := s.app.bindings.RecordSnapshot(ctx, s.h.Writer(), &snap); err != nil {
		return workspace.Snapshot{}, err
	}
	return snap, nil
}

// maybeSnapshot is the SnapshotAfterTurn policy: after a settlement that
// drained the backlog, the bound Workspace is snapshotted in the
// background, one snapshot at a time per Session, so the settlement does
// not wait on the copy; Wait covers it. A Session bound to none or a
// Workspace with no environment yet is nothing to do, other failures reach
// Config.Warn.
func (s *Session) maybeSnapshot(context.Context) {
	if s.app.workspaces == nil || !s.app.workspaces.SnapshotAfterTurn || s.app.snapshots == nil {
		return
	}
	s.rt.Go(func(ctx context.Context) {
		s.snapshotMu.Lock()
		defer s.snapshotMu.Unlock()
		_, err := s.SnapshotWorkspace(ctx)
		if err != nil && !errors.Is(err, ErrUnboundWorkspace) && !errors.Is(err, workspace.ErrNothingToSnapshot) && ctx.Err() == nil {
			s.app.warn(fmt.Errorf("app: snapshot of the workspace of %s: %w", s.sid, err))
		}
	})
}

// applyInheritedWorkspace runs SessionOptions.InheritedWorkspace on a
// binding this Session inherited; an own binding, or none, is left alone.
func (s *Session) applyInheritedWorkspace(ctx context.Context) error {
	if s.app.workspaces == nil || s.opts.InheritedWorkspace == workspace.InheritShare {
		return nil
	}
	b, err := s.Workspace(ctx)
	if err != nil {
		return err
	}
	if !b.InheritedBy(s.sid) {
		return nil
	}
	store := s.app.workspaces.Store
	parent, err := store.Get(ctx, b.Workspace)
	if err != nil {
		return fmt.Errorf("app: inherited workspace %s: %w", b.Workspace, err)
	}
	var own workspace.Workspace
	switch policy := s.opts.InheritedWorkspace; policy {
	case workspace.InheritNone:
		return s.UnbindWorkspace(ctx, "inherited workspace policy: none")
	case workspace.InheritAllocate:
		own = workspace.Workspace{ID: workspace.NewID(), Project: parent.Project, Base: parent.Base}
		if err := store.Create(ctx, own); err != nil {
			return err
		}
	case workspace.InheritClone:
		if parent.Snapshot == nil {
			return fmt.Errorf("app: inherited workspace policy clone: workspace %s has no snapshot", parent.ID)
		}
		if own, err = store.Fork(ctx, workspace.Fork{Source: parent.ID, Destination: workspace.NewID(), Snapshot: *parent.Snapshot}); err != nil {
			return err
		}
	case workspace.InheritRestore:
		if b.Snapshot == "" {
			return fmt.Errorf("app: inherited workspace policy restore: no snapshot of %s is recorded at the fork point", parent.ID)
		}
		if own, err = store.Fork(ctx, workspace.Fork{Source: parent.ID, Destination: workspace.NewID(), Snapshot: b.Snapshot}); err != nil {
			return err
		}
	default:
		return fmt.Errorf("app: unknown inherited workspace policy %d", policy)
	}
	return s.BindWorkspace(ctx, own.ID)
}

// Compact summarizes the context with the preset's model and commits a
// compaction retaining a pair-closed suffix; ok is false when the context
// is already within the retain window. It runs between Turns and, under
// the quiescent-Run guard, between the steps of a Turn.
func (s *Session) Compact(ctx context.Context) (chatlog.CompactionID, bool, error) {
	policy := compaction.Policy{AfterEntries: s.opts.CompactAfterEntries, RetainEntries: s.opts.CompactRetainEntries}
	cctx, err := chatlog.ReadContext(ctx, s.h.Writer().Projections(), s.sid)
	if err != nil {
		return "", false, err
	}
	retain, withinWindow := policy.Retain(cctx.Entries)
	if withinWindow {
		return "", false, nil
	}
	materialized, err := chatlog.NewMaterializer(s.a.Content).Entries(ctx, cctx.Entries)
	if err != nil {
		return "", false, err
	}
	summary, err := compaction.Summarizer{
		ResolvePreset: s.a.Presets.Resolve, Content: s.a.Frozen, Executor: s.a.Executor, Watcher: s.a.Driver.OutcomeWatcher(),
	}.Summarize(ctx, s.sid, s.opts.Preset, materialized)
	if err != nil {
		return "", false, err
	}
	id, err := s.a.Chatlog.Compact(ctx, s.h.Writer(), summary, retain, rt.RequireQuiescentRun)
	if err != nil {
		return "", false, err
	}
	return id, true, nil
}

// maybeCompact runs the automatic policy after a settlement and between the
// steps of a Turn; failures reach the caller through CompactWarn and never
// change the settled results or stop the drive.
func (s *Session) maybeCompact(ctx context.Context) {
	if s.opts.CompactAfterEntries <= 0 {
		return
	}
	cctx, err := chatlog.ReadContext(ctx, s.h.Writer().Projections(), s.sid)
	if err == nil && len(cctx.Entries) <= s.opts.CompactAfterEntries {
		return
	}
	if err == nil {
		_, _, err = s.Compact(ctx)
	}
	if err != nil && s.opts.CompactWarn != nil {
		s.opts.CompactWarn(err)
	}
}

// Close cancels the Session's service goroutines, waits for them, then
// cancels the runtime's background drives and waits for them, then releases
// this Session's ownership; other Sessions of the application stay open. A
// Turn a cancelled drive left active resumes on the next open.
func (s *Session) Close(ctx context.Context) error {
	s.app.untrack(s)
	if s.loopsCancel != nil {
		s.loopsCancel()
	}
	s.loops.Wait()
	s.rt.Close()
	return s.h.Close(ctx)
}
