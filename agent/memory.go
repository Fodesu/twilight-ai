package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
)

// MemoryRuntime is the in-process reference Runtime: mutex + MachineState +
// AgentEvent map (spec §8.1). It is the conformance reference; it does not
// survive the process and does not store product history.
type MemoryRuntime struct {
	mu       sync.Mutex
	state    MachineState
	revision uint64
	// events keyed by CommandID: the full event group of each transition.
	events map[CommandID][]AgentEvent
	// log holds every event in (Revision, Index) order for replay.
	log []AgentEvent
	// occupancy: live grants per target (one model step or one call).
	grants map[string]ExecutionGrant
}

// NewMemoryRuntime starts from an Initialize-produced state at Revision 0.
func NewMemoryRuntime(initial MachineState) *MemoryRuntime {
	return &MemoryRuntime{
		state:  initial,
		events: make(map[CommandID][]AgentEvent),
		grants: make(map[string]ExecutionGrant),
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
	return RuntimeSnapshot{State: cloneMachineState(m.state), Revision: m.revision}, nil
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
			Snapshot: RuntimeSnapshot{State: cloneMachineState(m.state), Revision: m.revision},
			Events:   cloneEvents(decision.Events),
		}, nil
	case DecisionConflict:
		return CommitResult{}, ErrCommandConflict
	case DecisionStale:
		if decision.Reject != nil && decision.Reject != ErrStaleRuntime {
			return CommitResult{}, fmt.Errorf("%w: %v", ErrStaleRuntime, decision.Reject)
		}
		return CommitResult{}, ErrStaleRuntime
	case DecisionTerminal:
		return CommitResult{}, ErrRunTerminal
	}

	// DecisionApply: persist state + events atomically under the lock, manage
	// occupancy, and mint the grant for an accepted start. Facts are cloned on
	// the way in so caller-held command buffers cannot mutate stored events.
	m.state = decision.NewState
	m.revision++
	stored := cloneEvents(decision.Events)
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
		Snapshot: RuntimeSnapshot{State: cloneMachineState(m.state), Revision: m.revision},
		Events:   cloneEvents(decision.Events),
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
