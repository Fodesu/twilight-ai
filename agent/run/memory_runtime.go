package run

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
)

// MemoryRuntime is the in-process reference Runtime. Its collection lock is
// used only to create or find stable per-Run entries; each entry has an
// independent lock for state, canonical commit records, revision, and grants.
// Runtime operations are context-aware before and after acquiring a lock so a
// request cancelled while waiting never reads or writes an entry.
type MemoryRuntime struct {
	mu   sync.RWMutex
	runs map[RunID]*memoryRun
}

type memoryRun struct {
	mu          sync.Mutex
	header      RunHeader
	state       MachineState
	revision    uint64
	initial     MachineState
	watermark   uint64
	transitions map[CommandID]TransitionRecord
	log         []TransitionRecord
	grants      map[string]ExecutionGrant
}

// NewMemoryRuntime creates an empty, RunID-addressed in-process Runtime.
func NewMemoryRuntime() *MemoryRuntime {
	return &MemoryRuntime{runs: make(map[RunID]*memoryRun)}
}

func (m *MemoryRuntime) entry(runID RunID) (*memoryRun, error) {
	if m == nil {
		return nil, errors.New("agent: memory runtime: nil runtime")
	}
	m.mu.RLock()
	entry := m.runs[runID]
	m.mu.RUnlock()
	if entry == nil {
		return nil, ErrRunNotFound
	}
	return entry, nil
}

func (m *MemoryRuntime) Create(ctx context.Context, run NewRun) (CreateResult, error) {
	if err := checkContext(ctx); err != nil {
		return CreateResult{}, err
	}
	header, err := BuildRunHeaderFromNewRun(run)
	if err != nil {
		return CreateResult{}, err
	}
	if m == nil {
		return CreateResult{}, errors.New("agent: memory runtime: nil runtime")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := checkContext(ctx); err != nil {
		return CreateResult{}, err
	}
	if existing := m.runs[run.RunID]; existing != nil {
		// Header is immutable after admission, so the collection lock protects
		// this lookup without taking the entry's execution lock.
		existingHeader := cloneRunHeader(existing.header)
		equal, err := canonicalHeadersEqual(existingHeader, header)
		if err != nil {
			return CreateResult{}, err
		}
		if !equal {
			return CreateResult{}, ErrCreateConflict
		}
		return CreateResult{Header: existingHeader, Created: false}, nil
	}

	stored := cloneRunHeader(header)
	initial := cloneMachineState(&stored.InitialState)
	m.runs[run.RunID] = &memoryRun{
		header:      stored,
		state:       cloneMachineState(&initial),
		initial:     initial,
		transitions: make(map[CommandID]TransitionRecord),
		grants:      make(map[string]ExecutionGrant),
	}
	return CreateResult{Header: cloneRunHeader(stored), Created: true}, nil
}

func (m *MemoryRuntime) Load(ctx context.Context, runID RunID) (RuntimeSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return RuntimeSnapshot{}, err
	}
	entry, err := m.entry(runID)
	if err != nil {
		return RuntimeSnapshot{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if err := checkContext(ctx); err != nil {
		return RuntimeSnapshot{}, err
	}
	return RuntimeSnapshot{State: cloneMachineState(&entry.state), Revision: entry.revision}, nil
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

// Commit atomically evaluates and writes the Run addressed by Command.RunID.
//
//nolint:gocritic // hugeParam: CommitRequest is the value DTO of the Runtime authority boundary.
func (m *MemoryRuntime) Commit(ctx context.Context, req CommitRequest) (CommitResult, error) {
	if err := checkContext(ctx); err != nil {
		return CommitResult{}, err
	}
	entry, err := m.entry(req.Command.RunID)
	if err != nil {
		return CommitResult{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if err := checkContext(ctx); err != nil {
		return CommitResult{}, err
	}

	key := grantKey(req.Command.Command)
	grantValid := key != "" && req.Grant != "" && entry.grants[key] == req.Grant
	recoveryValid := false
	if key != "" && req.Grant == "" {
		_, occupied := entry.grants[key]
		recoveryValid = !occupied
	}

	var prior *TransitionRecord
	if record, ok := entry.transitions[req.Command.ID]; ok {
		copy := cloneTransitionRecord(&record)
		prior = &copy
	}
	decision, err := EvaluateCommit(entry.state, entry.revision, prior, req, grantValid, recoveryValid)
	if err != nil {
		return CommitResult{}, err
	}
	switch decision.Kind {
	case DecisionAlreadyApplied:
		return CommitResult{Status: CommitAlreadyApplied,
			Snapshot: RuntimeSnapshot{State: cloneMachineState(&entry.state), Revision: entry.revision},
			Events:   cloneEvents(decision.Events)}, nil
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

	stored := cloneTransitionRecord(&decision.Transition)
	entry.state = cloneMachineState(&decision.NewState)
	entry.revision++
	entry.watermark = entry.revision
	entry.transitions[req.Command.ID] = stored
	entry.log = append(entry.log, stored)

	var minted ExecutionGrant
	switch req.Command.Command.(type) {
	case StartModelExecution, StartToolCall:
		minted = newGrant()
		entry.grants[key] = minted
	case SubmitModelResult, SubmitModelFailure, RejectModelResult, RecoverModelExecution,
		SubmitToolResult, SubmitToolFailure:
		delete(entry.grants, key)
	}
	if entry.state.Status.Terminal() {
		entry.grants = make(map[string]ExecutionGrant)
	}
	return CommitResult{Status: CommitAccepted,
		Snapshot: RuntimeSnapshot{State: cloneMachineState(&entry.state), Revision: entry.revision},
		Events:   cloneEvents(stored.Events), Grant: minted}, nil
}

// Record returns a detached consistent point-in-time snapshot and verifies it
// against the immutable header and complete transition records.
func (m *MemoryRuntime) Record(ctx context.Context, runID RunID) (RunRecord, error) {
	if err := checkContext(ctx); err != nil {
		return RunRecord{}, err
	}
	entry, err := m.entry(runID)
	if err != nil {
		return RunRecord{}, err
	}
	entry.mu.Lock()
	if err := checkContext(ctx); err != nil {
		entry.mu.Unlock()
		return RunRecord{}, err
	}
	header := cloneRunHeader(entry.header)
	snapshot := RuntimeSnapshot{State: cloneMachineState(&entry.state), Revision: entry.revision}
	transitions := cloneTransitionRecords(entry.log)
	entry.mu.Unlock()
	if err := checkContext(ctx); err != nil {
		return RunRecord{}, err
	}
	if err := ValidateRunHeader(&header); err != nil {
		return RunRecord{}, fmt.Errorf("agent: memory runtime: invalid header: %w", err)
	}
	for i := range transitions {
		if err := ValidateTransitionRecord(&transitions[i]); err != nil {
			return RunRecord{}, fmt.Errorf("agent: memory runtime: invalid transition %d: %w", i, err)
		}
	}
	folded, revision, err := FoldRun(&header, transitions)
	if err != nil {
		return RunRecord{}, fmt.Errorf("agent: memory runtime: fold: %w", err)
	}
	if revision != snapshot.Revision || !statesEquivalent(&folded, &snapshot.State) {
		return RunRecord{}, errors.New("agent: memory runtime: snapshot diverges from transition log")
	}
	return RunRecord{Header: cloneRunHeader(header), Snapshot: cloneRuntimeSnapshot(snapshot), Transitions: cloneTransitionRecords(transitions)}, nil
}
