package run

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/memohai/twilight/agent/es"
)

// ErrLogTruncated reports that the transition log ends below the revision the
// caller expected. It identifies an audit gap for the Runtime authority; the
// adapter decides how to surface or repair that gap.
var ErrLogTruncated = errors.New("agent: transition log ends below the expected revision")

// FoldEvents rebuilds a MachineState by folding a complete flat event stream
// from the initial (Revision 0) state with Protocol.Evolve only: no Decide, no
// external effects, no command replay (RUN-NEW-2). It verifies (Revision,
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
		proto, err := ProtocolFor(e.SchemaVersion)
		if err != nil {
			return initial, 0, err
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
		wantDigest, err := proto.DigestFact(e.Type, e.Fact)
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
		state, err = proto.Evolve(state, fact)
		if err != nil {
			return initial, 0, err
		}
	}
	return state, revision, nil
}

// FoldTransitions rebuilds a MachineState from the immutable initial state
// and complete TransitionRecord sequence. Together they are the canonical
// diagnostic commit record for replay and consistency checks.
//
//nolint:gocritic // hugeParam: public replay API folds from an initial value state without mutating caller-owned state.
func FoldTransitions(initial MachineState, records []TransitionRecord) (MachineState, uint64, error) {
	views := make([]es.RecordView[AgentEvent], len(records))
	for i := range records {
		record := records[i]
		// Run-specific metadata (command identity and aggregate digest) is
		// validated by the Run adapter; es validates the generic complete
		// record structure and folding order below.
		if err := ValidateTransitionRecord(&record); err != nil {
			return initial, 0, err
		}
		views[i] = transitionRecordView(&record)
	}
	state, revision, err := es.FoldRecords(
		initial,
		es.StreamID(initial.RunID),
		views,
		supportsRunSchema,
		inspectTransitionEvent,
		func(schemaVersion uint16, state MachineState, event AgentEvent) (MachineState, error) {
			proto, err := ProtocolFor(schemaVersion)
			if err != nil {
				return MachineState{}, err
			}
			fact, err := snapshotFact(event.Fact)
			if err != nil {
				return MachineState{}, err
			}
			return proto.Evolve(state, fact)
		},
	)
	if err != nil {
		return initial, 0, err
	}
	return state, uint64(revision), nil
}

// Rebuild is an optional diagnostic (RUN-CMT-2): it refolds the state from
// the transition log and replaces the in-memory state with the fold result.
// Runtime.Commit remains the normal state transition path. A log shorter than
// the last committed revision returns ErrLogTruncated (audit gap). It returns
// true when the refolded state differs from the stored state, which identifies
// an Evolve bug, out-of-band write, or storage corruption.
func (m *MemoryRuntime) Rebuild(runID RunID) (rebuilt bool, err error) {
	entry, err := m.entry(runID)
	if err != nil {
		return false, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()

	folded, maxRevision, err := FoldTransitions(cloneMachineState(&entry.initial), entry.log)
	if err != nil {
		return false, err
	}
	if maxRevision < entry.watermark {
		return false, fmt.Errorf("%w: log ends at %d, watermark %d", ErrLogTruncated, maxRevision, entry.watermark)
	}
	diverged := entry.revision != maxRevision || !statesEquivalent(&entry.state, &folded)
	entry.state = folded
	entry.revision = maxRevision
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
		"runId": s.RunID, "status": s.Status,
		"modelSteps": s.ModelSteps, "lastClosedStep": s.LastClosedStep,
		"usage": s.Usage, "pendingInputs": s.PendingInputs,
		"lastModelResult": s.LastModelResult, "result": s.Result,
	}
	if s.LastToolStep != nil {
		m["lastToolStep"] = s.LastToolStep
	}
	switch cur := s.Current.(type) {
	case ModelStep:
		m["modelStep"] = cur
	case ToolStep:
		m["toolStep"] = cur
	}
	return m
}
