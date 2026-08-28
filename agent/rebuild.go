package agent

import (
	"errors"
	"fmt"
)

// ErrLogTruncated reports that a Run's event log ends below its revision
// watermark: accepted facts are permanently gone. The Run halts — continuing
// on a truncated log would upgrade the loss into wrong repeated execution
// (spec §5.1). Recovery is a disaster-recovery matter, not a protocol one.
var ErrLogTruncated = errors.New("agent: event log ends below the revision watermark")

// FoldEvents rebuilds a MachineState by folding the event log from the
// initial (Revision 0) state with Evolve only: no Decide, no external
// effects, no command replay (spec §9.1). It verifies (Revision, Index)
// ordering and per-fact digests as it goes; any gap or mismatch means the log
// itself is damaged and the fold stops.
func FoldEvents(initial MachineState, events []AgentEvent) (MachineState, uint64, error) {
	state := initial
	var revision uint64
	var index uint16
	inTransition := false
	for _, e := range events {
		switch {
		case !inTransition || e.Revision != revision:
			if e.Revision != revision+1 || e.Index != 0 {
				return initial, 0, fmt.Errorf("agent: fold: gap at revision %d index %d (expected %d/0)", e.Revision, e.Index, revision+1)
			}
			revision = e.Revision
			index = 0
			inTransition = true
		default:
			if e.Index != index+1 {
				return initial, 0, fmt.Errorf("agent: fold: gap at revision %d index %d (expected %d)", e.Revision, e.Index, index+1)
			}
			index = e.Index
		}
		wantDigest, err := DigestFact(e.SchemaVersion, e.Type, e.Fact)
		if err != nil {
			return initial, 0, err
		}
		if e.Digest != wantDigest {
			return initial, 0, fmt.Errorf("agent: fold: fact digest mismatch at revision %d index %d", e.Revision, e.Index)
		}
		fact, err := snapshotFact(e.Fact)
		if err != nil {
			return initial, 0, err
		}
		state, err = Evolve(state, fact)
		if err != nil {
			return initial, 0, err
		}
	}
	return state, revision, nil
}

// Rebuild discards the in-memory snapshot and refolds it from the event log,
// arbitrating per spec §5.1: the log wins when it is complete
// (maxRevision >= watermark); a log tail below the watermark halts with
// ErrLogTruncated. It returns true when the refolded state differed from the
// stored snapshot — with a correct implementation this never happens, so a
// true return is an audit signal (Evolve bug, out-of-band write, or snapshot
// corruption occurred).
func (m *MemoryRuntime) Rebuild() (rebuilt bool, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	folded, maxRevision, err := FoldEvents(cloneMachineState(m.initial), m.log)
	if err != nil {
		return false, err
	}
	if maxRevision < m.watermark {
		return false, fmt.Errorf("%w: log ends at %d, watermark %d", ErrLogTruncated, maxRevision, m.watermark)
	}
	diverged := m.revision != maxRevision || !statesEquivalent(m.state, folded)
	m.state = folded
	m.revision = maxRevision
	return diverged, nil
}

// statesEquivalent compares two states via their canonical serialization —
// the same identity rule the protocol uses everywhere else.
func statesEquivalent(a, b MachineState) bool {
	ab, errA := marshalCanonical(stateComparable(a))
	bb, errB := marshalCanonical(stateComparable(b))
	if errA != nil || errB != nil {
		return false
	}
	return string(ab) == string(bb)
}

// stateComparable flattens MachineState including the interface-typed Current
// step, which encoding/json cannot round-trip on its own.
func stateComparable(s MachineState) map[string]any {
	m := map[string]any{
		"runId": s.RunID, "status": s.Status, "config": s.Config,
		"modelSteps": s.ModelSteps, "lastClosedStep": s.LastClosedStep,
		"usage": s.Usage, "pendingInputs": s.PendingInputs,
		"lastModelResult": s.LastModelResult, "result": s.Result,
	}
	switch cur := s.Current.(type) {
	case ModelStep:
		m["modelStep"] = cur
	case ToolStep:
		m["toolStep"] = cur
	}
	return m
}
