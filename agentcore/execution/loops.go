package execution

import (
	"context"
	"fmt"
	"sync"

	"github.com/felinics/twilight/agentcore/decision"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
)

// Planner is the between-steps hook: it runs while a Run is Open and about
// to plan a model request, with the Writer of the Session being driven, so
// what it commits (an in-turn checkpoint) is what the PromptBuilder reads
// next. Errors stop the drive.
type Planner interface {
	BeforePrepare(ctx context.Context, w writer.Writer, input run.PromptInput) error
}

// loops builds the Loop of each AgentPreset once and hands the same Loop to
// every drive, redispatch and reattachment of a Run under that preset, so
// all of them meet the same already-driving guard. The Loop settings are
// fixed when the first Loop is built; a field changed later reaches only
// presets not built yet.
type loops struct {
	ports     effect.Ports
	presets   preset.Registry
	decisions *decision.Catalog
	sources   decision.Sources
	// targets resolves the opaque target of each effect; every Loop shares
	// it.
	targets loop.TargetResolver
	// dispatch is the re-offer policy of every Loop for a retryable dispatch
	// refusal; the zero value selects loop's defaults.
	dispatch loop.DispatchPolicy
	// planner, when set, is consulted through every Loop's BeforePrepare:
	// the between-steps context policy, given the Writer of the Session
	// being driven. writerOf finds that Writer by the Run's Scope among the
	// Sessions this process owns; required with planner.
	planner  Planner
	writerOf func(session.SessionID) (writer.Writer, bool)

	mu    sync.Mutex
	loops map[preset.PresetRef]*loop.Loop
}

// For returns the Loop of the preset, building it on first use.
func (l *loops) For(ref preset.PresetRef) (*loop.Loop, error) {
	ap, err := l.presets.Resolve(ref)
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if lp, ok := l.loops[ref]; ok {
		return lp, nil
	}
	builder, err := l.decisions.Resolve(ap, l.sources)
	if err != nil {
		return nil, err
	}
	settings := loop.Settings{
		Scheduling:       ap.Scheduling,
		MalformedRetries: ap.MalformedRetries,
		TargetResolver:   l.targets,
		Dispatch:         l.dispatch,
	}
	if l.planner != nil {
		settings.BeforePrepare = l.beforePrepare
	}
	lp, err := loop.New(l.ports, builder, settings)
	if err != nil {
		return nil, err
	}
	if l.loops == nil {
		l.loops = make(map[preset.PresetRef]*loop.Loop)
	}
	l.loops[ref] = lp
	return lp, nil
}

// ForRun returns the Loop of the Turn that owns the Run, read from the
// Session's turn surface through w, and that Turn's ID.
func (l *loops) ForRun(ctx context.Context, w writer.Writer, runID run.RunID) (*loop.Loop, turn.TurnID, error) {
	surface, err := turn.ReadSurface(ctx, w.Projections(), w.SessionID())
	if err != nil {
		return nil, "", err
	}
	turnID, ok := surface.OwnerOf(runID)
	if !ok {
		return nil, "", fmt.Errorf("execution: run %s: no owning turn", runID)
	}
	lp, err := l.For(surface.Turns[turnID].Preset)
	if err != nil {
		return nil, "", err
	}
	return lp, turnID, nil
}

// beforePrepare hands the Loop's hook to the Planner with the Writer of
// the Session the Run belongs to.
func (l *loops) beforePrepare(ctx context.Context, scope run.Scope, input run.PromptInput) error {
	w, ok := l.writerOf(session.SessionID(scope))
	if !ok {
		return fmt.Errorf("execution: planner: session %s is not open in this process", scope)
	}
	return l.planner.BeforePrepare(ctx, w, input)
}
