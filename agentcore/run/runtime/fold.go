package runtime

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/schema"
)

// FoldRun rebuilds a MachineState from the complete fact sequence of one Run
// in stream order (RUN-NEW-2): the first fact must be RunCreated, which binds
// the Schema for every later fact. No Decide, no effects, no replay.
func FoldRun(schemaVersion uint16, facts []run.Fact) (run.MachineState, error) {
	if len(facts) == 0 {
		return run.MachineState{}, errors.New("agent: fold: no facts")
	}
	if _, ok := facts[0].(run.RunCreated); !ok {
		return run.MachineState{}, fmt.Errorf("agent: fold: first fact is %T, want RunCreated", facts[0])
	}
	sch, err := schema.For(schemaVersion)
	if err != nil {
		return run.MachineState{}, err
	}
	var state run.MachineState
	for i, f := range facts {
		f, err = run.SnapshotFact(f)
		if err != nil {
			return run.MachineState{}, err
		}
		state, err = sch.Machine.Evolve(state, f)
		if err != nil {
			return run.MachineState{}, fmt.Errorf("agent: fold: fact %d (%s): %w", i, sch.Wire.FactType(f), err)
		}
	}
	return state, nil
}
