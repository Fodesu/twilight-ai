package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
)

// MemoryRuntime is the in-process reference Runtime: mutex + MachineState +
// AgentEvent map (spec §8.1). The event log is the source of truth; the state
// is the same-transaction projection. It is the conformance reference; it
// does not survive the process and does not store product history.
type MemoryRuntime struct {
	mu       sync.Mutex
	state    MachineState
	revision uint64
	// initial is the Revision-0 state; Rebuild folds the log from it.
	initial MachineState
	// watermark witnesses log-tail completeness: it advances with every
	// commit and is never cleared by a rebuild (spec §5.1).
	watermark uint64
	// events keyed by CommandID: the full event group of each transition.
	events map[CommandID][]AgentEvent
	// log holds every event in (Revision, Index) order for replay.
	log []AgentEvent
	// occupancy: live grants per target (one model step or one call).
	grants map[string]ExecutionGrant
}

// NewMemoryRuntime starts from an Initialize-produced state at Revision 0.
//
//nolint:gocritic // hugeParam: constructor takes a value snapshot and clones it into runtime authority storage.
func NewMemoryRuntime(initial MachineState) *MemoryRuntime {
	frozenInitial := cloneMachineState(&initial)
	return &MemoryRuntime{
		state:   cloneMachineState(&frozenInitial),
		initial: frozenInitial,
		events:  make(map[CommandID][]AgentEvent),
		grants:  make(map[string]ExecutionGrant),
	}
}

func (m *MemoryRuntime) Load(ctx context.Context) (RuntimeSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return RuntimeSnapshot{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// Deep copy: returned snapshots are read-only views; caller mutation must
	// never reach authoritative storage (spec appendix A).
	return RuntimeSnapshot{State: cloneMachineState(&m.state), Revision: m.revision}, nil
}

func grantKey(c AgentCommand) string {
	switch cmd := c.(type) {
	case StartModelExecution:
		return "model/" + string(cmd.StepID)
	case SubmitModelResult:
		return "model/" + string(cmd.StepID)
	case SubmitModelFailure:
		return "model/" + string(cmd.StepID)
	case RejectModelResult:
		return "model/" + string(cmd.StepID)
	case RecoverModelExecution:
		return "model/" + string(cmd.StepID)
	case StartToolCall:
		return "call/" + string(cmd.StepID) + "/" + string(cmd.CallID)
	case SubmitToolResult:
		return "call/" + string(cmd.StepID) + "/" + string(cmd.CallID)
	case SubmitToolFailure:
		return "call/" + string(cmd.StepID) + "/" + string(cmd.CallID)
	default:
		return ""
	}
}

func newGrant() ExecutionGrant {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("agent: memory runtime: %v", err))
	}
	return ExecutionGrant(hex.EncodeToString(b[:]))
}

func foldCommittedEventGroup(state *MachineState, events []AgentEvent) (MachineState, error) {
	current := *state
	for _, e := range events {
		want, err := DigestFact(e.SchemaVersion, e.Type, e.Fact)
		if err != nil {
			return current, err
		}
		if e.Digest != want {
			return current, fmt.Errorf("agent: memory runtime: stored fact digest mismatch at revision %d index %d", e.Revision, e.Index)
		}
		fact, err := snapshotFact(e.Fact)
		if err != nil {
			return current, err
		}
		current, err = Evolve(current, fact)
		if err != nil {
			return current, err
		}
	}
	return current, nil
}

//nolint:gocritic // hugeParam: CommitRequest is the value DTO of the Runtime authority boundary.
func (m *MemoryRuntime) Commit(ctx context.Context, req CommitRequest) (CommitResult, error) {
	if err := ctx.Err(); err != nil {
		return CommitResult{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	key := grantKey(req.Command.Command)
	grantValid := false
	if key != "" && req.Grant != "" {
		grantValid = m.grants[key] == req.Grant
	}
	// MemoryRuntime has no lease expiry: a grantless recovery command is only
	// valid when no live occupancy exists for the target (the worker died with
	// the process, which memory state does not survive; recoveryValid mainly
	// serves conformance tests).
	recoveryValid := false
	if key != "" && req.Grant == "" {
		_, occupied := m.grants[key]
		recoveryValid = !occupied
	}

	decision, err := EvaluateCommit(m.state, m.revision, m.events[req.Command.ID], req, grantValid, recoveryValid)
	if err != nil {
		return CommitResult{}, err
	}
	switch decision.Kind {
	case DecisionAlreadyApplied:
		// Replay never re-grants execution (spec §5.4).
		return CommitResult{
			Status:   CommitAlreadyApplied,
			Snapshot: RuntimeSnapshot{State: cloneMachineState(&m.state), Revision: m.revision},
			Events:   cloneEvents(decision.Events),
		}, nil
	case DecisionConflict:
		return CommitResult{}, ErrCommandConflict
	case DecisionStale:
		if decision.Reject != nil && !errors.Is(decision.Reject, ErrStaleRuntime) {
			return CommitResult{}, fmt.Errorf("%w: %w", ErrStaleRuntime, decision.Reject)
		}
		return CommitResult{}, ErrStaleRuntime
	case DecisionTerminal:
		return CommitResult{}, ErrRunTerminal
	}

	// DecisionApply: persist owned events and derive the authoritative snapshot
	// from those same stored facts. The event log is the source of truth; the
	// in-memory state is only its same-transaction projection.
	stored := cloneEvents(decision.Events)
	newState, err := foldCommittedEventGroup(&m.state, stored)
	if err != nil {
		return CommitResult{}, err
	}
	m.state = cloneMachineState(&newState)
	m.revision++
	m.watermark = m.revision
	m.events[req.Command.ID] = stored
	m.log = append(m.log, stored...)

	var minted ExecutionGrant
	switch req.Command.Command.(type) {
	case StartModelExecution, StartToolCall:
		minted = newGrant()
		m.grants[key] = minted
	case SubmitModelResult, SubmitModelFailure, RejectModelResult, RecoverModelExecution,
		SubmitToolResult, SubmitToolFailure:
		delete(m.grants, key)
	}
	if m.state.Status.Terminal() {
		// Terminal invalidates every outstanding grant (spec §3.7.3).
		m.grants = make(map[string]ExecutionGrant)
	}

	return CommitResult{
		Status:   CommitAccepted,
		Snapshot: RuntimeSnapshot{State: cloneMachineState(&m.state), Revision: m.revision},
		Events:   cloneEvents(stored),
		Grant:    minted,
	}, nil
}

// Events returns a deep copy of the full event log in (Revision, Index)
// order. Test and replay helper; not part of the Runtime contract.
func (m *MemoryRuntime) Events() []AgentEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return cloneEvents(m.log)
}
