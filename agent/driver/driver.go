// Package driver advances durable Turns: it drives an active attempt to its
// next quiescent point (Drive), runs the takeover disposition when a Session
// is opened (Open) and settles Outcomes that survived a previous owner
// (reattach). It composes the Loop of each AgentPreset over the shared
// Executor and caches it, so every drive of a Run meets the same
// already-driving guard. It decides nothing about where inputs go or what a
// reply is; those are the caller's.
package driver

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/felinics/twilight/agent/decision"
	"github.com/felinics/twilight/agent/run/effect"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/run/reconcile"
	"github.com/felinics/twilight/agent/session"
	runmod "github.com/felinics/twilight/agent/session/run"
	"github.com/felinics/twilight/agent/session/writer"
	"github.com/felinics/twilight/agent/turn"
)

// DriveResult is what one drive of a Turn reports: the Turn's committed
// response, and whether another local driver of the same Run was already
// carrying it, in which case this call drove nothing and Response is the
// status as read. AlreadyDriving is a fact about this process, not about the
// Turn, so it is not a TurnResponse disposition: the Turn's durable
// vocabulary stays the Coordinator's.
type DriveResult struct {
	turn.TurnResponse
	AlreadyDriving bool
}

// Presets resolves a PresetRef to its immutable AgentPreset (PST-2).
type Presets interface {
	Resolve(turn.PresetRef) (turn.AgentPreset, error)
}

// Driver is the execution orchestrator over the fact and effect layers
// (DRV).
type Driver struct {
	// Runs is the Run module's Session adapter; every drive binds it to the
	// caller's Writer (AUTH-OWN-2).
	Runs      *runmod.SessionRunStore
	Turns     turn.Reader
	Executor  effect.ExecutionPort
	Presets   Presets
	Decisions *decision.PromptBuilders
	Sources   decision.Sources
	Targets   loop.TargetResolver
	// Fail receives failures of work the Driver does outside any caller's
	// call, such as settling a reattached Outcome; nil discards them.
	Fail func(session.SessionID, error)

	mu       sync.Mutex
	loops    map[turn.PresetRef]*loop.Loop
	recovery map[session.SessionID]*recoveryLifetime
}

// New returns a Driver with no Loops built and no Sessions open.
func New() *Driver {
	return &Driver{loops: make(map[turn.PresetRef]*loop.Loop), recovery: make(map[session.SessionID]*recoveryLifetime)}
}

func (d *Driver) fail(sid session.SessionID, err error) {
	if d.Fail != nil {
		d.Fail(sid, err)
	}
}

// loopFor returns the Loop that drives Runs of one AgentPreset. A Loop binds
// the preset's prompt builder and settings to the shared Executor; it is
// built once per PresetRef (DRV-2, RUN-CMT-6).
func (d *Driver) loopFor(ref turn.PresetRef) (*loop.Loop, error) {
	preset, err := d.Presets.Resolve(ref)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if l, ok := d.loops[ref]; ok {
		return l, nil
	}
	builder, err := d.Decisions.Resolve(preset, d.Sources)
	if err != nil {
		return nil, err
	}
	l, err := loop.New(d.Executor, builder, loop.Settings{
		Scheduling:       preset.Scheduling,
		MalformedRetries: preset.MalformedRetries,
		TargetResolver:   d.Targets,
	})
	if err != nil {
		return nil, err
	}
	d.loops[ref] = l
	return l, nil
}

// Drive is DRV-1: while the Turn is active, resolve its recorded preset
// and drive the active attempt to the next quiescent point, then read the
// committed Status. w is the caller's ownership capability over the Session:
// the decision whether to drive reads w's own projections, and the Loop
// commits through w, so a superseded owner plans against its own epoch's
// view and is fenced at commit instead of adopting the new owner's state
// (AUTH-OWN-2, RUN-LOP-5). The caller's ctx bounds the drive, so
// cancellation is the caller's decision. A concurrent local driver of the
// same Run yields AlreadyDriving with the Turn's status as read.
func (d *Driver) Drive(ctx context.Context, w writer.Writer, turnID turn.TurnID) (DriveResult, error) {
	ref := turn.TurnRef{SessionID: w.SessionID(), TurnID: turnID}
	surface, err := turn.ReadSurface(ctx, w.Projections(), ref.SessionID)
	if err != nil {
		return DriveResult{}, err
	}
	view, ok := surface.Turns[ref.TurnID]
	if !ok {
		return DriveResult{}, fmt.Errorf("%w: unknown turn %s", turn.ErrConflict, ref.TurnID)
	}
	if view.Status == turn.TurnActive {
		l, err := d.loopFor(view.Preset)
		if err != nil {
			return DriveResult{}, err
		}
		res, err := l.Run(ctx, d.Runs.Bind(w), view.ActiveRun, nil)
		if err != nil {
			if errors.Is(err, loop.ErrRunAlreadyRunning) {
				resp, rerr := d.Turns.Status(ctx, ref)
				if rerr != nil {
					return DriveResult{}, rerr
				}
				return DriveResult{TurnResponse: resp, AlreadyDriving: true}, nil
			}
			return DriveResult{}, err
		}
		if res.ExecutionRecovery {
			// The drive quiesced with executions in flight and no local
			// waiter. Offer every Executing target reattachment and dispose
			// what no executor answers, instead of leaving the Turn to a
			// driver that already returned (RUN-CMT-7).
			if _, err := d.recoverInterrupted(context.WithoutCancel(ctx), w); err != nil {
				d.fail(ref.SessionID, fmt.Errorf("driver: recovering a quiesced drive: %w", err))
			}
		}
	}
	resp, err := d.Turns.Status(ctx, ref)
	return DriveResult{TurnResponse: resp}, err
}

// reattachDeliver is what the reconciler hands a kept attempt's Outcome to
// (RUN-CMT-7): an Outcome of an attempt that survived the previous owner is
// settled through the Loop of the Turn that owns its Run, and the Run is
// driven on from there.
func (d *Driver) reattachDeliver(ctx context.Context, w writer.Writer) func(effect.Outcome) {
	sid := w.SessionID()
	return func(out effect.Outcome) {
		if ctx.Err() != nil {
			return
		}
		surface, err := turn.ReadSurface(ctx, w.Projections(), sid)
		if err != nil {
			d.fail(sid, fmt.Errorf("driver: reattached outcome for run %s: %w", out.Key.RunID, err))
			return
		}
		turnID, ok := surface.OwnerOf(out.Key.RunID)
		if !ok {
			d.fail(sid, fmt.Errorf("driver: reattached outcome for run %s: no owning turn", out.Key.RunID))
			return
		}
		l, err := d.loopFor(surface.Turns[turnID].Preset)
		if err != nil {
			d.fail(sid, fmt.Errorf("driver: reattached outcome for run %s: %w", out.Key.RunID, err))
			return
		}
		res, err := l.Deliver(ctx, d.Runs.Bind(w), out, nil)
		if err != nil {
			d.fail(sid, fmt.Errorf("driver: settling reattached outcome for run %s: %w", out.Key.RunID, err))
			return
		}
		if res.Disposition != loop.LoopDelivered {
			return
		}
		if _, err := l.Run(ctx, d.Runs.Bind(w), out.Key.RunID, nil); err != nil && !errors.Is(err, loop.ErrRunAlreadyRunning) {
			d.fail(sid, fmt.Errorf("driver: driving run %s after a reattached outcome: %w", out.Key.RunID, err))
		}
	}
}

// --- takeover recovery -------------------------------------------------------------

// recoveryLifetime is the detached context the Session's recovery goroutines
// -- reattached outcome reads -- live under, and the cancel that stops them.
type recoveryLifetime struct {
	ctx    context.Context
	cancel context.CancelFunc
	// w is the Writer the Session was opened with; reattached Outcomes settle
	// through it (AUTH-OWN-2).
	w writer.Writer
}

// installRecoveryLifetimeLocked replaces the Session's recovery lifetime with
// one derived from parent, stopping the previous listeners. Callers hold d.mu.
func (d *Driver) installRecoveryLifetimeLocked(w writer.Writer, parent context.Context) *recoveryLifetime {
	sid := w.SessionID()
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	if previous := d.recovery[sid]; previous != nil {
		previous.cancel()
	}
	lt := &recoveryLifetime{ctx: ctx, cancel: cancel, w: w}
	d.recovery[sid] = lt
	return lt
}

// ensureRecoveryLifetime returns the Session's recovery lifetime, installing a
// detached one when absent. Open replaces it instead: a takeover supersedes
// the previous owner's listeners.
func (d *Driver) ensureRecoveryLifetime(w writer.Writer) *recoveryLifetime {
	d.mu.Lock()
	defer d.mu.Unlock()
	if lt, ok := d.recovery[w.SessionID()]; ok {
		return lt
	}
	return d.installRecoveryLifetimeLocked(w, context.Background())
}

// recoverInterrupted runs the takeover disposition (RUN-CMT-7) for the
// Session: the reconciler compares every Executing target with the execution
// store and disposes only what no record answers for. It is the explicit
// recovery behind an unknown dispatch boundary -- the drive kept the call
// Executing, so the durable record, not a duplicate dispatch, decides the
// settlement.
func (d *Driver) recoverInterrupted(ctx context.Context, w writer.Writer) (int, error) {
	lt := d.ensureRecoveryLifetime(w)
	sid := w.SessionID()
	rec := &reconcile.Reconciler{Executions: d.Executor, Lifetime: lt.ctx, Deliver: d.reattachDeliver(lt.ctx, lt.w),
		Fail: func(key effect.AssignmentKey, err error) {
			d.fail(sid, fmt.Errorf("driver: outcome of run %s effect %s cannot be read; the target stays executing until the next takeover: %w", key.RunID, key.Effect, err))
		}}
	return d.Runs.RecoverInterrupted(ctx, w, rec)
}

// Open installs the Session's recovery lifetime under w and runs the
// takeover disposition (RUN-CMT-7). It returns the number of recovery
// commands issued.
func (d *Driver) Open(ctx context.Context, w writer.Writer) (int, error) {
	d.mu.Lock()
	d.installRecoveryLifetimeLocked(w, ctx)
	d.mu.Unlock()
	n, err := d.recoverInterrupted(ctx, w)
	if err != nil {
		d.Stop(w.SessionID())
	}
	return n, err
}

// Stop cancels the Session's recovery listeners.
func (d *Driver) Stop(sid session.SessionID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if lt := d.recovery[sid]; lt != nil {
		lt.cancel()
		delete(d.recovery, sid)
	}
}

// Close cancels every recovery listener.
func (d *Driver) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for sid, lt := range d.recovery {
		lt.cancel()
		delete(d.recovery, sid)
	}
}
