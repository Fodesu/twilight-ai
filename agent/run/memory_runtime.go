package run

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

// MemoryRuntime is the in-process reference Runtime. Its collection lock is
// used only to create or find stable per-Run entries; each entry has an
// independent lock for state, canonical commit records, revision, and grants.
// Runtime operations check context before and after entry access and before
// mutating the protected state.
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
	claims      map[string]ExecutionClaim
	// startGrants lets an exact start-command replay recover the capability
	// that was issued before its response was lost.
	startGrants map[CommandID]ExecutionGrant
}

func lockMemory(ctx context.Context, mu *sync.Mutex) error {
	for {
		if mu.TryLock() {
			return nil
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
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
		claims:      make(map[string]ExecutionClaim),
		startGrants: make(map[CommandID]ExecutionGrant),
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
	if err := lockMemory(ctx, &entry.mu); err != nil {
		return RuntimeSnapshot{}, err
	}
	if err := checkContext(ctx); err != nil {
		entry.mu.Unlock()
		return RuntimeSnapshot{}, err
	}
	snapshot := RuntimeSnapshot{State: cloneMachineState(&entry.state), Revision: entry.revision, SchemaVersion: entry.header.SchemaVersion}
	header := cloneRunHeader(entry.header)
	entry.mu.Unlock()
	if err := ValidateRunHeader(&header); err != nil {
		return RuntimeSnapshot{}, fmt.Errorf("agent: memory runtime: invalid header: %w", err)
	}
	if err := ValidateMachineState(&snapshot.State); err != nil {
		return RuntimeSnapshot{}, fmt.Errorf("agent: memory runtime: invalid snapshot: %w", err)
	}
	return snapshot, nil
}

func verifyRuntimeSnapshot(header *RunHeader, initial *MachineState, snapshot RuntimeSnapshot, transitions []TransitionRecord) error {
	if err := ValidateRunHeader(header); err != nil {
		return err
	}
	folded, revision, err := FoldTransitions(*initial, transitions)
	if err != nil {
		return err
	}
	if revision != snapshot.Revision || !statesEquivalent(&folded, &snapshot.State) {
		return errors.New("snapshot diverges from transition log")
	}
	return nil
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

func (entry *memoryRun) forgetStartGrant(grant ExecutionGrant) {
	if grant == "" {
		return
	}
	for commandID, candidate := range entry.startGrants {
		if candidate == grant {
			delete(entry.startGrants, commandID)
		}
	}
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
	if err := lockMemory(ctx, &entry.mu); err != nil {
		return CommitResult{}, err
	}
	defer entry.mu.Unlock()
	if err := checkContext(ctx); err != nil {
		return CommitResult{}, err
	}

	key := grantKey(req.Command.Command)
	grantValid := key != "" && req.Grant != "" && entry.grants[key] == req.Grant
	if cmd, ok := req.Command.Command.(RecoverModelExecution); ok && req.Grant != "" {
		grantValid = grantValid && entry.claims[key] == cmd.Claim
	}
	recoveryValid := false

	var prior *TransitionRecord
	if record, ok := entry.transitions[req.Command.ID]; ok {
		copy := cloneTransitionRecord(&record)
		prior = &copy
	}
	decision, err := EvaluateCommit(entry.state, entry.revision, prior, req, grantValid, recoveryValid, entry.header.SchemaVersion)
	if err != nil {
		return CommitResult{}, err
	}
	switch decision.Kind {
	case DecisionAlreadyApplied:
		var grant ExecutionGrant
		if _, ok := req.Command.Command.(StartModelExecution); ok {
			candidate := entry.startGrants[req.Command.ID]
			if candidate != "" && entry.grants[key] == candidate {
				grant = candidate
			}
		} else if _, ok := req.Command.Command.(StartToolCall); ok {
			candidate := entry.startGrants[req.Command.ID]
			if candidate != "" && entry.grants[key] == candidate {
				grant = candidate
			}
		}
		return CommitResult{Status: CommitAlreadyApplied,
			Snapshot: RuntimeSnapshot{State: cloneMachineState(&entry.state), Revision: entry.revision, SchemaVersion: entry.header.SchemaVersion},
			Events:   cloneEvents(decision.Events), Grant: grant}, nil
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
	switch cmd := req.Command.Command.(type) {
	case StartModelExecution, StartToolCall:
		minted = newGrant()
		entry.grants[key] = minted
		switch c := any(cmd).(type) {
		case StartModelExecution:
			entry.claims[key] = c.Claim
		case StartToolCall:
			entry.claims[key] = c.Claim
		}
		entry.startGrants[req.Command.ID] = minted
	case SubmitModelResult, SubmitModelFailure, RejectModelResult, RecoverModelExecution,
		SubmitToolResult, SubmitToolFailure:
		activeGrant := entry.grants[key]
		delete(entry.grants, key)
		delete(entry.claims, key)
		entry.forgetStartGrant(activeGrant)
	}
	if entry.state.Status.Terminal() {
		entry.grants = make(map[string]ExecutionGrant)
		entry.claims = make(map[string]ExecutionClaim)
		entry.startGrants = make(map[CommandID]ExecutionGrant)
	}
	return CommitResult{Status: CommitAccepted,
		Snapshot: RuntimeSnapshot{State: cloneMachineState(&entry.state), Revision: entry.revision, SchemaVersion: entry.header.SchemaVersion},
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
	if err := lockMemory(ctx, &entry.mu); err != nil {
		return RunRecord{}, err
	}
	if err := checkContext(ctx); err != nil {
		entry.mu.Unlock()
		return RunRecord{}, err
	}
	header := cloneRunHeader(entry.header)
	snapshot := RuntimeSnapshot{State: cloneMachineState(&entry.state), Revision: entry.revision, SchemaVersion: header.SchemaVersion}
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
