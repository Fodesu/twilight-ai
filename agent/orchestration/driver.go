// Package orchestration drives durable Turns: it advances active attempts
// to their next quiescent point (Drive), runs the takeover disposition when
// a Session opens (Open), and settles Outcomes that survived a previous
// owner (reattach). It is the reusable execution orchestration between the
// application host and any child-session driver (subagents): it holds no
// model client, tool implementation or event stream — failures leave
// through Fail, observations through the caller's surfaces.
package orchestration

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/felinics/twilight/agent/decision"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/effect"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/turn"
)

// ResumeAlreadyDriving extends the turn disposition vocabulary for hosts:
// the inputs (if any) are committed and another local driver of the same
// Run carries them forward. The Coordinator itself never produces it.
const ResumeAlreadyDriving turn.ResumeDisposition = "already_driving"

// Presets resolves a PresetRef to its immutable AgentPreset (HST-PST-2).
type Presets interface {
	Resolve(turn.PresetRef) (turn.AgentPreset, error)
}

// Surfaces reads the turn surface projection of one Session.
type Surfaces func(ctx context.Context, sid session.SessionID) (turn.TurnSurface, error)

// Driver is the execution orchestrator over the fact and effect layers
// (HST-DRV). Construct it with the Executor effects actually dispatch to —
// for a Host with the subagent effect, that is the wrapped Executor, not
// the deployment's inner one.
type Driver struct {
	Runtime     run.Runtime
	Coordinator turn.Service
	Executor    effect.Port
	Presets     Presets
	Decisions   *decision.PromptBuilders
	Sources     decision.Sources
	Targets     loop.TargetResolver
	// Surfaces reads turn surfaces.
	Surfaces Surfaces
	// Fail receives failures of work the Driver does outside any caller's
	// call, such as settling a reattached Outcome; nil discards them.
	Fail func(session.SessionID, error)

	mu       sync.Mutex
	loops    map[turn.PresetRef]*loop.Loop
	recovery map[session.SessionID]*recoveryLifetime
}

// New returns an empty Driver.
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
// built once per PresetRef so every drive of a Run meets the same
// already-driving guard (HST-DRV-2).
func (d *Driver) loopFor(ref turn.PresetRef) (*loop.Loop, turn.AgentPreset, error) {
	preset, err := d.Presets.Resolve(ref)
	if err != nil {
		return nil, turn.AgentPreset{}, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if l, ok := d.loops[ref]; ok {
		return l, preset, nil
	}
	builder, err := d.Decisions.Resolve(preset, d.Sources)
	if err != nil {
		return nil, turn.AgentPreset{}, err
	}
	l, err := loop.New(d.Executor, builder, loop.Settings{
		Scheduling:       preset.Scheduling,
		MalformedRetries: preset.MalformedRetries,
		TargetResolver:   d.Targets,
	})
	if err != nil {
		return nil, turn.AgentPreset{}, err
	}
	d.loops[ref] = l
	return l, preset, nil
}

// Drive is HST-DRV-1: while the Turn is active, resolve its recorded preset
// and drive the active attempt to the next quiescent point, then read the
// committed Status. The caller's ctx bounds the drive, so cancellation is a
// host decision. A concurrent local driver of the same Run yields
// ResumeAlreadyDriving.
func (d *Driver) Drive(ctx context.Context, ref turn.TurnRef) (turn.TurnResponse, error) {
	surface, err := d.Surfaces(ctx, ref.SessionID)
	if err != nil {
		return turn.TurnResponse{}, err
	}
	view, ok := surface.Turns[ref.TurnID]
	if !ok {
		return turn.TurnResponse{}, fmt.Errorf("%w: unknown turn %s", turn.ErrConflict, ref.TurnID)
	}
	if view.Status == turn.TurnActive {
		l, _, err := d.loopFor(view.Preset)
		if err != nil {
			return turn.TurnResponse{}, err
		}
		res, err := l.Run(ctx, d.Runtime, ref.SessionID, view.ActiveRun, nil)
		if err != nil {
			if errors.Is(err, loop.ErrRunAlreadyRunning) {
				resp, rerr := d.Coordinator.Status(ctx, ref)
				if rerr != nil {
					return turn.TurnResponse{}, rerr
				}
				resp.Disposition = ResumeAlreadyDriving
				return resp, nil
			}
			return turn.TurnResponse{}, err
		}
		if res.ExecutionRecovery {
			// The drive quiesced with executions in flight and no local
			// waiter. Offer every Executing target reattachment and dispose
			// what no executor answers, instead of leaving the Turn to a
			// driver that already returned (RUN-CMT-7).
			if _, err := d.recoverInterrupted(context.WithoutCancel(ctx), ref.SessionID); err != nil {
				d.fail(ref.SessionID, fmt.Errorf("orchestration: recovering a quiesced drive: %w", err))
			}
		}
	}
	return d.Coordinator.Status(ctx, ref)
}

// reattachDeliver is the glue a takeover hands the Executor (RUN-CMT-7): an
// Outcome of an attempt that survived the previous owner is settled through
// the Loop of the Turn that owns its Run, and the Run is driven on from there.
func (d *Driver) reattachDeliver(ctx context.Context, sid session.SessionID) loop.Deliver {
	return func(out loop.Outcome) {
		if ctx.Err() != nil {
			return
		}
		surface, err := d.Surfaces(ctx, sid)
		if err != nil {
			d.fail(sid, fmt.Errorf("orchestration: reattached outcome for run %s: %w", out.Key.RunID, err))
			return
		}
		turnID, ok := surface.RunOwner[out.Key.RunID]
		if !ok {
			d.fail(sid, fmt.Errorf("orchestration: reattached outcome for run %s: no owning turn", out.Key.RunID))
			return
		}
		l, _, err := d.loopFor(surface.Turns[turnID].Preset)
		if err != nil {
			d.fail(sid, fmt.Errorf("orchestration: reattached outcome for run %s: %w", out.Key.RunID, err))
			return
		}
		res, err := l.Deliver(ctx, d.Runtime, sid, out, nil)
		if err != nil {
			d.fail(sid, fmt.Errorf("orchestration: settling reattached outcome for run %s: %w", out.Key.RunID, err))
			return
		}
		if res.Disposition != loop.LoopDelivered {
			return
		}
		if _, err := l.Run(ctx, d.Runtime, sid, out.Key.RunID, nil); err != nil && !errors.Is(err, loop.ErrRunAlreadyRunning) {
			d.fail(sid, fmt.Errorf("orchestration: driving run %s after a reattached outcome: %w", out.Key.RunID, err))
		}
	}
}

// --- takeover recovery -------------------------------------------------------------

// recoveryLifetime is the detached context the Session's recovery goroutines
// -- reattached outcome reads -- live under, and the cancel that stops them.
type recoveryLifetime struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// installRecoveryLifetimeLocked replaces the Session's recovery lifetime with
// one derived from parent, stopping the previous listeners. Callers hold d.mu.
func (d *Driver) installRecoveryLifetimeLocked(sid session.SessionID, parent context.Context) *recoveryLifetime {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	if previous := d.recovery[sid]; previous != nil {
		previous.cancel()
	}
	lt := &recoveryLifetime{ctx: ctx, cancel: cancel}
	d.recovery[sid] = lt
	return lt
}

// ensureRecoveryLifetime returns the Session's recovery lifetime, installing a
// detached one when absent. Open replaces it instead: a takeover supersedes
// the previous owner's listeners.
func (d *Driver) ensureRecoveryLifetime(sid session.SessionID) *recoveryLifetime {
	d.mu.Lock()
	defer d.mu.Unlock()
	if lt, ok := d.recovery[sid]; ok {
		return lt
	}
	return d.installRecoveryLifetimeLocked(sid, context.Background())
}

// recoverInterrupted runs the takeover disposition (RUN-CMT-7) for the
// Session: every Executing target is offered to the Executor for reattachment
// and disposed only when no running attempt answers. It is the explicit
// recovery behind an unknown dispatch boundary -- the drive kept the call
// Executing, so the durable record, not a duplicate dispatch, decides the
// settlement.
func (d *Driver) recoverInterrupted(ctx context.Context, sid session.SessionID) (int, error) {
	lt := d.ensureRecoveryLifetime(sid)
	n, err := d.Runtime.RecoverInterrupted(ctx, sid, loop.Reattach(lt.ctx, d.Executor, sid, d.reattachDeliver(lt.ctx, sid)))
	return n, err
}

// Open takes ownership of the Session and runs the takeover disposition
// (RUN-CMT-7). It returns the number of recovery commands issued.
func (d *Driver) Open(ctx context.Context, sid session.SessionID) (int, error) {
	d.mu.Lock()
	d.installRecoveryLifetimeLocked(sid, ctx)
	d.mu.Unlock()
	n, err := d.recoverInterrupted(ctx, sid)
	if err != nil {
		d.Stop(sid)
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
