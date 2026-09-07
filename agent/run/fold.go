package run

import (
	"bytes"
	"errors"
	"fmt"
)

// FoldRun rebuilds a MachineState from the complete fact sequence of one Run
// in stream order (RUN-NEW-2): the first fact must be RunCreated, which binds
// the Protocol for every later fact. No Decide, no effects, no replay.
func FoldRun(facts []Fact) (MachineState, error) {
	if len(facts) == 0 {
		return MachineState{}, errors.New("agent: fold: no facts")
	}
	created, ok := facts[0].(RunCreated)
	if !ok {
		return MachineState{}, fmt.Errorf("agent: fold: first fact is %T, want RunCreated", facts[0])
	}
	proto, err := ProtocolFor(created.SchemaVersion)
	if err != nil {
		return MachineState{}, err
	}
	var state MachineState
	for i, f := range facts {
		f, err = snapshotFact(f)
		if err != nil {
			return MachineState{}, err
		}
		state, err = proto.Evolve(state, f)
		if err != nil {
			return MachineState{}, fmt.Errorf("agent: fold: fact %d (%s): %w", i, factType(f), err)
		}
	}
	return state, nil
}

// StatesEquivalent compares two states via their canonical snapshot encoding.
func StatesEquivalent(a, b *MachineState) bool { return statesEquivalent(a, b) }

func statesEquivalent(a, b *MachineState) bool {
	ab, errA := encodeMachineStateV1(a)
	bb, errB := encodeMachineStateV1(b)
	if errA != nil || errB != nil {
		return false
	}
	return bytes.Equal(ab, bb)
}
