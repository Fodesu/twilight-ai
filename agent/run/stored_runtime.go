package run

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// runtime is the single Runtime implementation over a Store. Loop and Turn
// depend only on the Runtime interface.
type runtime struct {
	store    Store
	leaseTTL time.Duration
	now      func() time.Time
}

// RuntimeOptions configures lease occupancy for a Store-backed Runtime.
// LeaseTTL 0 means leases do not expire (grant-holder recovery only).
type RuntimeOptions struct {
	LeaseTTL time.Duration
	Now      func() time.Time
}

// NewRuntime constructs a Runtime over store. SQLite and Postgres adapters
// pass their Store; tests use MemoryStore.
func NewRuntime(store Store) Runtime {
	return NewRuntimeWithOptions(store, RuntimeOptions{})
}

func NewRuntimeWithOptions(store Store, opts RuntimeOptions) Runtime {
	return newRuntime(store, opts)
}

func newRuntime(store Store, opts RuntimeOptions) *runtime {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &runtime{store: store, leaseTTL: opts.LeaseTTL, now: now}
}

func (r *runtime) Create(ctx context.Context, run NewRun) (CreateResult, error) {
	if err := checkContext(ctx); err != nil {
		return CreateResult{}, err
	}
	if r == nil || r.store == nil {
		return CreateResult{}, errors.New("agent: runtime: nil store")
	}
	header, err := BuildRunHeaderFromNewRun(run)
	if err != nil {
		return CreateResult{}, err
	}
	created, existing, err := r.store.Create(ctx, header)
	if err != nil {
		return CreateResult{}, err
	}
	if !created {
		equal, err := canonicalHeadersEqual(existing, header)
		if err != nil {
			return CreateResult{}, err
		}
		if !equal {
			return CreateResult{}, ErrCreateConflict
		}
		return CreateResult{Header: existing, Created: false}, nil
	}
	return CreateResult{Header: cloneRunHeader(header), Created: true}, nil
}

func (r *runtime) Load(ctx context.Context, runID RunID) (RuntimeSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return RuntimeSnapshot{}, err
	}
	stored, err := r.store.View(ctx, runID)
	if err != nil {
		return RuntimeSnapshot{}, err
	}
	snapshot := RuntimeSnapshot{State: stored.State, Revision: stored.Revision, SchemaVersion: stored.Header.SchemaVersion}
	if stored.Header.RunID != runID || stored.State.RunID != runID {
		return RuntimeSnapshot{}, fmt.Errorf("agent: runtime: stored RunID %q/%q does not match %q", stored.Header.RunID, stored.State.RunID, runID)
	}
	if err := ValidateRunHeader(&stored.Header); err != nil {
		return RuntimeSnapshot{}, fmt.Errorf("agent: runtime: invalid header: %w", err)
	}
	if err := ValidateMachineState(&snapshot.State); err != nil {
		return RuntimeSnapshot{}, fmt.Errorf("agent: runtime: invalid snapshot: %w", err)
	}
	return snapshot, nil
}

func (r *runtime) Commit(ctx context.Context, req CommitRequest) (CommitResult, error) {
	if err := checkContext(ctx); err != nil {
		return CommitResult{}, err
	}
	var result CommitResult
	err := r.store.Update(ctx, req.Command.RunID, func(stored *StoredRun) error {
		var err error
		result, err = r.evaluateAndApply(stored, req)
		return err
	})
	return result, err
}

func (r *runtime) evaluateAndApply(stored *StoredRun, req CommitRequest) (CommitResult, error) {
	key := grantKey(req.Command.Command)
	lease, hasLease := stored.Leases[key]
	grantValid := hasLease && req.Grant != "" && lease.Grant == req.Grant
	if cmd, ok := req.Command.Command.(RecoverModelExecution); ok && req.Grant != "" {
		grantValid = grantValid && lease.Claim == cmd.Claim
	}
	recoveryValid := hasLease && req.Grant == "" && r.leaseExpired(lease)
	if cmd, ok := req.Command.Command.(RecoverModelExecution); ok && req.Grant == "" {
		recoveryValid = recoveryValid && lease.Claim == cmd.Claim
	}

	var prior *TransitionRecord
	if record, ok := stored.Transitions[req.Command.ID]; ok {
		copy := cloneTransitionRecord(&record)
		prior = &copy
	}
	proto, err := ProtocolFor(stored.Header.SchemaVersion)
	if err != nil {
		return CommitResult{}, err
	}
	decision, err := EvaluateCommit(stored.State, stored.Revision, prior, req, grantValid, recoveryValid, proto)
	if err != nil {
		return CommitResult{}, err
	}
	switch decision.Kind {
	case DecisionAlreadyApplied:
		var grant ExecutionGrant
		if _, ok := req.Command.Command.(StartModelExecution); ok {
			candidate := stored.StartGrants[req.Command.ID]
			if candidate != "" && stored.Leases[key].Grant == candidate {
				grant = candidate
			}
		} else if _, ok := req.Command.Command.(StartToolCall); ok {
			candidate := stored.StartGrants[req.Command.ID]
			if candidate != "" && stored.Leases[key].Grant == candidate {
				grant = candidate
			}
		}
		return CommitResult{Status: CommitAlreadyApplied,
			Snapshot: RuntimeSnapshot{State: cloneMachineState(&stored.State), Revision: stored.Revision, SchemaVersion: stored.Header.SchemaVersion},
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

	storedRecord := cloneTransitionRecord(&decision.Transition)
	stored.State = cloneMachineState(&decision.NewState)
	stored.Revision++
	stored.Watermark = stored.Revision
	if stored.Transitions == nil {
		stored.Transitions = make(map[CommandID]TransitionRecord)
	}
	stored.Transitions[req.Command.ID] = storedRecord
	stored.Log = append(stored.Log, storedRecord)

	var minted ExecutionGrant
	switch cmd := req.Command.Command.(type) {
	case StartModelExecution, StartToolCall:
		minted = newGrant()
		if stored.Leases == nil {
			stored.Leases = make(map[string]ExecutionLease)
		}
		if stored.StartGrants == nil {
			stored.StartGrants = make(map[CommandID]ExecutionGrant)
		}
		lease := ExecutionLease{Grant: minted, StartCommandID: req.Command.ID, Deadline: r.leaseDeadline()}
		switch c := any(cmd).(type) {
		case StartModelExecution:
			lease.Claim = c.Claim
		case StartToolCall:
			lease.Claim = c.Claim
		}
		stored.Leases[key] = lease
		stored.StartGrants[req.Command.ID] = minted
	case SubmitModelResult, SubmitModelFailure, RejectModelResult, RecoverModelExecution,
		SubmitToolResult, SubmitToolFailure:
		active := stored.Leases[key]
		delete(stored.Leases, key)
		stored.forgetStartGrant(active.Grant)
	}
	if stored.State.Status.Terminal() {
		stored.clearLeases()
	}
	return CommitResult{Status: CommitAccepted,
		Snapshot: RuntimeSnapshot{State: cloneMachineState(&stored.State), Revision: stored.Revision, SchemaVersion: stored.Header.SchemaVersion},
		Events:   cloneEvents(storedRecord.Events), Grant: minted}, nil
}

func (r *runtime) Record(ctx context.Context, runID RunID) (RunRecord, error) {
	if err := checkContext(ctx); err != nil {
		return RunRecord{}, err
	}
	stored, err := r.store.View(ctx, runID)
	if err != nil {
		return RunRecord{}, err
	}
	header := stored.Header
	snapshot := RuntimeSnapshot{State: stored.State, Revision: stored.Revision, SchemaVersion: header.SchemaVersion}
	transitions := stored.Log
	if err := ValidateRunHeader(&header); err != nil {
		return RunRecord{}, fmt.Errorf("agent: runtime: invalid header: %w", err)
	}
	for i := range transitions {
		if err := ValidateTransitionRecord(&transitions[i]); err != nil {
			return RunRecord{}, fmt.Errorf("agent: runtime: invalid transition %d: %w", i, err)
		}
	}
	folded, revision, err := FoldRun(&header, transitions)
	if err != nil {
		return RunRecord{}, fmt.Errorf("agent: runtime: fold: %w", err)
	}
	if revision != snapshot.Revision || !statesEquivalent(&folded, &snapshot.State) {
		return RunRecord{}, errors.New("agent: runtime: snapshot diverges from transition log")
	}
	return RunRecord{Header: cloneRunHeader(header), Snapshot: cloneRuntimeSnapshot(snapshot), Transitions: cloneTransitionRecords(transitions)}, nil
}

// RecoverExpired commits grantless recovery for every expired lease that
// still occupies an Executing target. An expired tool call is settled as
// Unknown; an expired model step is recovered to Prepared. Hosts call this
// on a timer; Loop does not. The dying process writes nothing.
func (r *runtime) RecoverExpired(ctx context.Context) (int, error) {
	ids, err := r.store.ListIDs(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, id := range ids {
		stored, err := r.store.View(ctx, id)
		if err != nil {
			return n, err
		}
		proto, err := ProtocolFor(stored.Header.SchemaVersion)
		if err != nil {
			return n, err
		}
		for key, lease := range stored.Leases {
			if !r.leaseExpired(lease) {
				continue
			}
			cmd, cmdID, ok := recoveryCommand(stored.State, key, lease)
			if !ok {
				continue
			}
			env, err := proto.BuildEnvelope(id, cmdID, cmd)
			if err != nil {
				return n, err
			}
			_, err = r.Commit(ctx, CommitRequest{Command: env})
			if err != nil {
				if errors.Is(err, ErrStaleRuntime) || errors.Is(err, ErrRunTerminal) {
					continue
				}
				return n, err
			}
			n++
		}
	}
	return n, nil
}

func recoveryCommand(state MachineState, key string, lease ExecutionLease) (AgentCommand, CommandID, bool) {
	switch cur := state.Current.(type) {
	case ModelStep:
		if cur.Status != ModelExecuting || key != "model/"+string(cur.RefValue.ID) {
			return nil, "", false
		}
		return RecoverModelExecution{StepID: cur.RefValue.ID, Claim: lease.Claim},
			DeriveModelRecoveryCommandID(state.RunID, cur.RefValue.ID, lease.Claim), true
	case ToolStep:
		prefix := "call/" + string(cur.RefValue.ID) + "/"
		if !strings.HasPrefix(key, prefix) {
			return nil, "", false
		}
		callID := CallID(strings.TrimPrefix(key, prefix))
		return SubmitToolFailure{
			StepID:  cur.RefValue.ID,
			CallID:  callID,
			Failure: ToolFailure{Class: FailureEffectUnknown, Message: "lease expired"},
			Outcome: ToolOutcomeUnknown,
		}, DeriveSystemCommandID(state.RunID, cur.RefValue.ID, callID, string(lease.Grant)), true
	default:
		return nil, "", false
	}
}

func (r *runtime) leaseDeadline() time.Time {
	if r.leaseTTL <= 0 {
		return time.Time{}
	}
	return r.now().Add(r.leaseTTL)
}

func (r *runtime) leaseExpired(lease ExecutionLease) bool {
	if lease.Deadline.IsZero() {
		return false
	}
	return !r.now().Before(lease.Deadline)
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
		panic(fmt.Sprintf("agent: runtime: %v", err))
	}
	return ExecutionGrant(hex.EncodeToString(b[:]))
}
