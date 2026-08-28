package agent

import (
	"bytes"
	"errors"
	"fmt"
)

// ErrLogTruncated reports that a Run's event log ends below its revision
// watermark: accepted facts are permanently gone. The Run halts — continuing
// on a truncated log would upgrade the loss into wrong repeated execution
// (spec §5.1). Recovery is a disaster-recovery matter, not a protocol one.
var ErrLogTruncated = errors.New("agent: event log ends below the revision watermark")

// FoldEvents rebuilds a MachineState by folding a complete flat event stream
// from the initial (Revision 0) state with EvolveVersion only: no Decide, no
// external effects, no command replay (spec §9.1). It verifies (Revision,
// Index) ordering and per-fact digests as it goes. Authority runtimes should
// prefer FoldTransitions because only TransitionRecord can prove the last
// transition's event group is complete.
//
//nolint:gocritic // hugeParam: public replay API folds from an initial value state without mutating caller-owned state.
func FoldEvents(initial MachineState, events []AgentEvent) (MachineState, uint64, error) {
	state := initial
	var revision uint64
	var index uint16
	var commandID CommandID
	var commandDigest Digest
	inTransition := false
	for _, e := range events {
		if e.RunID != initial.RunID {
			return initial, 0, fmt.Errorf("agent: fold: event run %q does not match initial run %q", e.RunID, initial.RunID)
		}
		if !isSupportedSchemaVersion(e.SchemaVersion) {
			return initial, 0, fmt.Errorf("agent: fold: unsupported schema version %d", e.SchemaVersion)
		}
		typ := factType(e.Fact)
		if typ == "" || e.Type != typ {
			return initial, 0, fmt.Errorf("agent: fold: event type %q does not match fact variant %T", e.Type, e.Fact)
		}
		switch {
		case !inTransition || e.Revision != revision:
			if e.Revision != revision+1 || e.Index != 0 {
				return initial, 0, fmt.Errorf("agent: fold: gap at revision %d index %d (expected %d/0)", e.Revision, e.Index, revision+1)
			}
			if e.CommandID == "" || e.CommandDigest == "" {
				return initial, 0, fmt.Errorf("agent: fold: revision %d missing command identity", e.Revision)
			}
			revision = e.Revision
			index = 0
			commandID = e.CommandID
			commandDigest = e.CommandDigest
			inTransition = true
		default:
			if e.Index != index+1 {
				return initial, 0, fmt.Errorf("agent: fold: gap at revision %d index %d (expected %d)", e.Revision, e.Index, index+1)
			}
			if e.CommandID != commandID || e.CommandDigest != commandDigest {
				return initial, 0, fmt.Errorf("agent: fold: revision %d command identity changed within transition", e.Revision)
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
		state, err = EvolveVersion(e.SchemaVersion, state, fact)
		if err != nil {
			return initial, 0, err
		}
	}
	return state, revision, nil
}

// FoldTransitions rebuilds a MachineState by folding authoritative transition
// records from the immutable initial state. The source of truth is the
// admission-created initial state plus the complete TransitionRecord log.
//
//nolint:gocritic // hugeParam: public replay API folds from an initial value state without mutating caller-owned state.
func FoldTransitions(initial MachineState, records []TransitionRecord) (MachineState, uint64, error) {
	state := initial
	var revision uint64
	for i := range records {
		record := records[i]
		if err := ValidateTransitionRecord(&record); err != nil {
			return initial, 0, err
		}
		if record.RunID != initial.RunID {
			return initial, 0, fmt.Errorf("agent: fold: transition run %q does not match initial run %q", record.RunID, initial.RunID)
		}
		if record.Revision != revision+1 {
			return initial, 0, fmt.Errorf("agent: fold: gap at transition revision %d (expected %d)", record.Revision, revision+1)
		}
		for j := range record.Events {
			e := record.Events[j]
			fact, err := snapshotFact(e.Fact)
			if err != nil {
				return initial, 0, err
			}
			state, err = EvolveVersion(e.SchemaVersion, state, fact)
			if err != nil {
				return initial, 0, err
			}
		}
		revision = record.Revision
	}
	return state, revision, nil
}

// Rebuild discards the in-memory snapshot and refolds it from the transition
// log, arbitrating per spec §5.1: the log wins when it is complete
// (maxRevision >= watermark); a log tail below the watermark halts with
// ErrLogTruncated. It returns true when the refolded state differed from the
// stored snapshot — with a correct implementation this never happens, so a
// true return is an audit signal (Evolve bug, out-of-band write, or snapshot
// corruption occurred).
func (m *MemoryRuntime) Rebuild() (rebuilt bool, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	folded, maxRevision, err := FoldTransitions(cloneMachineState(&m.initial), m.log)
	if err != nil {
		return false, err
	}
	if maxRevision < m.watermark {
		return false, fmt.Errorf("%w: log ends at %d, watermark %d", ErrLogTruncated, maxRevision, m.watermark)
	}
	diverged := m.revision != maxRevision || !statesEquivalent(&m.state, &folded)
	m.state = folded
	m.revision = maxRevision
	return diverged, nil
}

// statesEquivalent compares two states via their canonical serialization —
// the same identity rule the protocol uses everywhere else.
func statesEquivalent(a, b *MachineState) bool {
	ab, errA := marshalCanonical(stateComparable(a))
	bb, errB := marshalCanonical(stateComparable(b))
	if errA != nil || errB != nil {
		return false
	}
	return bytes.Equal(ab, bb)
}

// stateComparable flattens MachineState including the interface-typed Current
// step, which encoding/json cannot round-trip on its own.
func stateComparable(s *MachineState) map[string]any {
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
