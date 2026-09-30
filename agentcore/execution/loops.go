package execution

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/felinics/twilight/agentcore/decision"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/store"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
)

// Planner is the between-steps hook: it runs while a Run is Open and about
// to plan a model request, with the Writer of the Session being driven, so
// what it commits (an in-turn checkpoint) is what the PromptBuilder reads
// next. Errors stop the drive.
type Planner interface {
	BeforePrepare(ctx context.Context, w writer.Writer, input decision.Input) error
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
	// watcher is where every Loop waits for Outcomes: one settlement
	// subscription to the Executor shared with the recovery; required.
	watcher *effect.Watcher
	// planner, when set, is consulted through every Loop's BeforePrepare:
	// the between-steps context policy, given the Writer the drive commits
	// through.
	planner Planner

	mu    sync.Mutex
	loops map[preset.PresetRef]*loop.Loop
}

// For returns the Loop of the preset, building it on first use.
func (l *loops) For(ref preset.PresetRef) (*loop.Loop, error) {
	if l.watcher == nil {
		return nil, errors.New("execution: loops require a watcher")
	}
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
		Watcher:          l.watcher,
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

// beforePrepare hands the Loop's hook to the Planner with the Writer the
// bound store commits through.
func (l *loops) beforePrepare(ctx context.Context, st store.RunStore, input decision.Input) error {
	owned, ok := st.(interface{ Writer() writer.Writer })
	if !ok {
		return fmt.Errorf("execution: run store %T exposes no writer for the planner", st)
	}
	return l.planner.BeforePrepare(ctx, owned.Writer(), input)
}
