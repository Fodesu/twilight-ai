// Package execution advances Runs from a Session's durable state: the drive
// chain over one effect layer, exposed to hosts as the Engine.
package execution

import (
	"context"
	"errors"
	"time"

	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/decision"
	"github.com/felinics/twilight/agentcore/observe"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/reconcile"
	"github.com/felinics/twilight/agentcore/run/redispatch"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
)

// Engine is what a host advances Sessions with. Every method runs on the
// caller's goroutine; work the Engine finishes on its own -- a reattached
// Outcome settled, a wait answered -- is reported through Config.Notify and
// never drives a Turn.
type Engine interface {
	// Drive drives the Turn to its next quiescent point through the caller's
	// Writer. alreadyDriving reports a concurrent local drive of the same
	// Run carried it, in which case this call drove nothing.
	Drive(ctx context.Context, w writer.Writer, turnID turn.TurnID) (alreadyDriving bool, err error)
	// Takeover runs the takeover disposition for a Session whose Writer was
	// just acquired and returns the recovery commands it issued.
	Takeover(ctx context.Context, w writer.Writer) (recovered int, err error)
	// ResumeWaiting answers the waits a previous owner left with a
	// Responder. Each answer is committed and reported through Notify;
	// nothing is driven here.
	ResumeWaiting(ctx context.Context, w writer.Writer)
	// Detach ends the Session's listeners; the Writer's release follows.
	Detach(sid session.SessionID)
	// Once performs one effect outside any Run: it dispatches a, waits for
	// its Outcome and acknowledges it once read. The key is a's as given, so
	// a retry under the same key replays. A cancelled ctx cancels the effect.
	Once(ctx context.Context, a effect.Assignment) (effect.Outcome, error)
	// Presets is the registry of decision identities Turns start under.
	Presets() preset.Registry
	// Progress is the transient stream of the running effects and the
	// failures reported here.
	Progress() *observe.Progresses
	// Close ends the listeners of every Session and the settlement
	// subscription; every Session was detached before this.
	Close()
}

// Sources are the Session-side services the drive chain reads; a
// composition root assembles them over the same stores the Sessions live
// in.
type Sources struct {
	// Runs is the Run module's Session adapter.
	Runs *sessionstore.SessionRunStore
	// Projections reads the Sessions' folded state.
	Projections session.ProjectionReader
	// Content materializes the frozen bodies projections name.
	Content chatlog.ContentResolver
}

// Config composes one Engine: the effect layer it drives, the decision
// identities it resolves and the policies of the drive chain.
type Config struct {
	// Executor is the effect layer (RUN-EXE-3): Execution is required, the
	// optional capabilities are used when set.
	Executor effect.Ports
	// Presets is the registry of decision identities; nil selects an
	// in-memory registry.
	Presets preset.Registry
	// Decisions resolve each preset's PromptBuilderRef (DEC-CAT): required.
	Decisions *decision.Catalog
	// Planner, when set, is consulted between the steps of every Run with
	// the Writer of the Session being driven.
	Planner Planner
	// Responders answer ExternalResponse waits by the ToolRef whose calls
	// they answer; nil answers none.
	Responders map[run.ToolRef]Responder
	// TargetResolver supplies the opaque resource target of each effect
	// (RUN-LOP-9); nil gives every effect no target.
	TargetResolver loop.TargetResolver
	// Dispatch bounds the re-offers of an Assignment the executor refused as
	// retryable; the zero value selects loop's defaults.
	Dispatch loop.DispatchPolicy
	// Notify is called after the Engine settles an Outcome or commits an
	// answer outside a caller's Drive: the host advances the Session from
	// there. nil discards.
	Notify func(session.SessionID)
	// Fail receives failures of work the Engine's components do outside any
	// caller's call. The callback decides whether a failure reaches the
	// Progress stream; when nil, the Engine publishes it to Progress itself.
	Fail func(session.SessionID, error)
	// MissingEffects is the takeover policy for an Executing effect the
	// Executor holds nothing for (RUN-CMT-7): the zero value disposes it,
	// reconcile.RedispatchMissing hands it to the Executor again within a
	// budget (RUN-EXE-15) and requires Redispatches.
	MissingEffects reconcile.MissingPolicy
	// Redispatches is the dispatch ledger RedispatchMissing writes; durable
	// like every store. Unused under DisposeMissing.
	Redispatches redispatch.Store
	// MaxRedispatches bounds the redispatches of one effect under
	// RedispatchMissing; zero selects reconcile.DefaultMaxRedispatches.
	MaxRedispatches int
	// OrphanProbe is how often the Watcher attaches a key still waiting and
	// hands an orphaned one to RecoverExecution; zero selects
	// effect.DefaultWatchProbe.
	OrphanProbe time.Duration
}

// engine is the assembled drive chain behind Engine.
type engine struct {
	ports    effect.Ports
	presets  preset.Registry
	watcher  *effect.Watcher
	driver   *driver
	recovery *recovery
	progress *observe.Progresses
}

// New assembles an Engine over the Session-side sources of the same
// process. The Session stores and the Engine assemble independently;
// pairing them is the composition root's act.
func New(cfg Config, src Sources) (Engine, error) { //nolint:gocritic // hugeParam: Config is a by-value options struct read once
	if cfg.Executor.Execution == nil {
		return nil, errors.New("execution: an Executor port is required")
	}
	if cfg.Decisions == nil {
		return nil, errors.New("execution: a prompt builder catalog is required (Config.Decisions)")
	}
	if cfg.MissingEffects == reconcile.RedispatchMissing && cfg.Redispatches == nil {
		return nil, errors.New("execution: MissingEffects=redispatch requires a dispatch ledger (Config.Redispatches, RUN-EXE-15)")
	}
	presets := cfg.Presets
	if presets == nil {
		presets = preset.NewMemory()
	}
	x := &engine{ports: cfg.Executor, presets: presets, progress: observe.NewProgresses()}
	x.watcher = &effect.Watcher{Port: cfg.Executor.Execution, Settlements: cfg.Executor.Settlements, Recover: cfg.Executor.Recover, Probe: cfg.OrphanProbe}
	// Failures the components report outside any caller's call reach the
	// caller's callback, which owns their delivery to the transient stream;
	// without one they reach the stream directly (OBS-1).
	report := cfg.Fail
	if report == nil {
		report = x.progress.Failed
	}
	notify := func(lt *lifetime) {
		if cfg.Notify != nil {
			cfg.Notify(lt.w.SessionID())
		}
	}
	sink := progressSink{x.progress}
	lps := &loops{ports: cfg.Executor, presets: presets, decisions: cfg.Decisions, targets: cfg.TargetResolver, dispatch: cfg.Dispatch,
		sources: decision.Sources{Projections: src.Projections, Content: src.Content}, watcher: x.watcher, planner: cfg.Planner}
	x.recovery = &recovery{runs: src.Runs, ports: cfg.Executor, loops: lps, watcher: x.watcher, fail: report, notify: notify,
		missingEffects: cfg.MissingEffects, redispatches: cfg.Redispatches, maxRedispatches: cfg.MaxRedispatches, sink: sink}
	var rs *responders
	if len(cfg.Responders) > 0 {
		rs = &responders{runs: src.Runs, tools: cfg.Responders, fail: report}
	}
	x.driver = &driver{runs: src.Runs, loops: lps, recovery: x.recovery, responders: rs, sink: sink}
	return x, nil
}

func (x *engine) Drive(ctx context.Context, w writer.Writer, turnID turn.TurnID) (bool, error) {
	return x.driver.Drive(ctx, w, turnID)
}

func (x *engine) Takeover(ctx context.Context, w writer.Writer) (int, error) {
	return x.recovery.Open(ctx, w)
}

func (x *engine) ResumeWaiting(ctx context.Context, w writer.Writer) {
	x.driver.ResumeWaiting(ctx, w, x.recovery.settled)
}

func (x *engine) Detach(sid session.SessionID) { x.recovery.Stop(sid) }

// Once dispatches a, waits on the shared settlement subscription for its
// Outcome and acknowledges the key once the Outcome is read, so the
// executor may collect the record. A ctx that ends first cancels the effect.
func (x *engine) Once(ctx context.Context, a effect.Assignment) (effect.Outcome, error) {
	if err := x.ports.Execution.Dispatch(ctx, a); err != nil {
		return effect.Outcome{}, err
	}
	out, err := x.watcher.Await(ctx, a.Key())
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			_ = x.ports.Execution.Cancel(context.WithoutCancel(ctx), a.Key())
		}
		return effect.Outcome{}, err
	}
	if x.ports.Ack != nil {
		_ = x.ports.Ack.Acknowledge(ctx, a.Key())
	}
	return out, nil
}

func (x *engine) Presets() preset.Registry      { return x.presets }
func (x *engine) Progress() *observe.Progresses { return x.progress }

// Close ends the recovery listeners, then the settlement subscription.
func (x *engine) Close() {
	x.recovery.Close()
	x.watcher.Close()
}

// progressSink is the drive's loop.EventSink: provisional observations
// become transient Progress events; committed observations are already on
// the committed stream from the Writer, so they are dropped here.
type progressSink struct{ progress *observe.Progresses }

func (s progressSink) Emit(_ context.Context, e loop.Event) error { //nolint:gocritic // hugeParam: EventSink contract takes the Event by value
	if e.Durability != loop.EventProvisional {
		return nil
	}
	s.progress.Publish(session.SessionID(e.Session), observe.Progress{RunID: e.RunID, Effect: e.Effect, Generation: e.Generation,
		Sequence: e.Sequence, Kind: string(e.Kind), Payload: e.Payload})
	return nil
}
