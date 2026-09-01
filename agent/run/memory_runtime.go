package run

import (
	"errors"
	"fmt"
)

// MemoryRuntime is the in-process reference Runtime: a Store-backed runtime
// over MemoryStore. Tests that mutate storage for Rebuild diagnostics use
// this concrete type; Loop and Turn depend only on Runtime.
type MemoryRuntime struct {
	*runtime
	mem *MemoryStore
}

// NewMemoryRuntime creates an empty, RunID-addressed in-process Runtime.
// Leases do not expire; grantless recovery requires a Store with deadlines.
func NewMemoryRuntime() *MemoryRuntime {
	mem := NewMemoryStore()
	return &MemoryRuntime{runtime: newRuntime(mem, RuntimeOptions{}), mem: mem}
}

func (m *MemoryRuntime) entry(runID RunID) (*memoryRun, error) {
	if m == nil || m.mem == nil {
		return nil, errors.New("agent: memory runtime: nil runtime")
	}
	return m.mem.entry(runID)
}

// Rebuild is an optional diagnostic (RUN-CMT-2): it refolds the state from
// the transition log and replaces the stored state with the fold result.
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
