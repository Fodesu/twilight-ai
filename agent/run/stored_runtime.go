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
// LeaseTTL 0 means leases do not expire (grant-holder recovery only). A
// positive LeaseTTL must exceed the longest gap between a worker's start and
// its next RenewLease or settlement; see RUN-CMT-8.
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

func (r *runtime) loadHead(ctx context.Context, runID RunID) (RunHead, error) {
	head, err := r.store.LoadHead(ctx, runID)
	if err != nil {
		return RunHead{}, err
	}
	if err := r.validateHead(runID, &head); err != nil {
		return RunHead{}, err
	}
	return head, nil
}

func (r *runtime) validateHead(runID RunID, head *RunHead) error {
	if head.Header.RunID != runID || head.State.RunID != runID {
		return fmt.Errorf("agent: runtime: stored RunID %q/%q does not match %q", head.Header.RunID, head.State.RunID, runID)
	}
	if err := ValidateRunHeader(&head.Header); err != nil {
		return fmt.Errorf("agent: runtime: invalid header: %w", err)
	}
	if err := ValidateMachineState(&head.State); err != nil {
		return fmt.Errorf("agent: runtime: invalid snapshot: %w", err)
	}
	return nil
}

func snapshotOf(head *RunHead) RuntimeSnapshot {
	return RuntimeSnapshot{State: cloneMachineState(&head.State), Revision: head.Revision, SchemaVersion: head.Header.SchemaVersion}
}

func (r *runtime) Load(ctx context.Context, runID RunID) (RuntimeSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return RuntimeSnapshot{}, err
	}
	head, err := r.loadHead(ctx, runID)
	if err != nil {
		return RuntimeSnapshot{}, err
	}
	return snapshotOf(&head), nil
}

//nolint:gocritic // hugeParam: Runtime.Commit is the public value-based contract.
func (r *runtime) Commit(ctx context.Context, req CommitRequest) (CommitResult, error) {
	if err := checkContext(ctx); err != nil {
		return CommitResult{}, err
	}
	runID := req.Command.RunID
	var result CommitResult
	// The whole evaluation runs inside the Store's per-Run critical section
	// (RUN-CMT-2): the head cannot move between read and write, so there is
	// no compare-and-swap retry and concurrent writers of one Run serialize.
	err := r.store.Commit(ctx, runID, func(tx RunTx) (*Append, error) {
		head := tx.Head()
		if err := r.validateHead(runID, &head); err != nil {
			return nil, err
		}
		var err error
		var appendReq *Append
		result, appendReq, err = r.evaluate(tx, &head, &req)
		return appendReq, err
	})
	if err != nil {
		return CommitResult{}, err
	}
	return result, nil
}

// evaluate runs EvaluateCommit against head and returns either a final
// result (append == nil) or the Append the caller must persist.
func (r *runtime) evaluate(tx RunTx, head *RunHead, req *CommitRequest) (CommitResult, *Append, error) {
	key := grantKey(req.Command.Command)
	lease, hasLease := head.Leases[key]
	grantValid := hasLease && req.Grant != "" && lease.Grant == req.Grant
	if cmd, ok := req.Command.Command.(RecoverModelExecution); ok && req.Grant != "" {
		grantValid = grantValid && lease.Claim == cmd.Claim
	}
	recoveryValid := hasLease && req.Grant == "" && r.leaseExpired(lease)
	if cmd, ok := req.Command.Command.(RecoverModelExecution); ok && req.Grant == "" {
		recoveryValid = recoveryValid && lease.Claim == cmd.Claim
	}

	var prior *TransitionRecord
	if record, found, err := tx.LookupTransition(req.Command.ID); err != nil {
		return CommitResult{}, nil, err
	} else if found {
		prior = &record
	}
	proto, err := ProtocolFor(head.Header.SchemaVersion)
	if err != nil {
		return CommitResult{}, nil, err
	}
	decision, err := EvaluateCommit(head.State, head.Revision, prior, *req, grantValid, recoveryValid, proto)
	if err != nil {
		return CommitResult{}, nil, err
	}
	switch decision.Kind {
	case DecisionAlreadyApplied:
		var grant ExecutionGrant
		switch req.Command.Command.(type) {
		case StartModelExecution, StartToolCall:
			// The start's grant is live only while the lease it minted is
			// still the lease on record for this target.
			if hasLease && lease.StartCommandID == req.Command.ID {
				grant = lease.Grant
			}
		}
		return CommitResult{Status: CommitAlreadyApplied, Snapshot: snapshotOf(head),
			Events: cloneEvents(decision.Events), Grant: grant}, nil, nil
	case DecisionConflict:
		return CommitResult{}, nil, ErrCommandConflict
	case DecisionStale:
		if decision.Reject != nil && !errors.Is(decision.Reject, ErrStaleRuntime) {
			return CommitResult{}, nil, fmt.Errorf("%w: %w", ErrStaleRuntime, decision.Reject)
		}
		return CommitResult{}, nil, ErrStaleRuntime
	case DecisionTerminal:
		return CommitResult{}, nil, ErrRunTerminal
	}

	appendReq := Append{
		ExpectedRevision: head.Revision,
		Transition:       cloneTransitionRecord(&decision.Transition),
		State:            cloneMachineState(&decision.NewState),
	}
	var minted ExecutionGrant
	switch cmd := req.Command.Command.(type) {
	case StartModelExecution, StartToolCall:
		minted = newGrant()
		newLease := ExecutionLease{Grant: minted, StartCommandID: req.Command.ID, Deadline: r.leaseDeadline()}
		switch c := any(cmd).(type) {
		case StartModelExecution:
			newLease.Claim = c.Claim
		case StartToolCall:
			newLease.Claim = c.Claim
		}
		appendReq.Leases.Put = map[string]ExecutionLease{key: newLease}
	case SubmitModelResult, SubmitModelFailure, RejectModelResult, RecoverModelExecution,
		SubmitToolResult, SubmitToolFailure:
		appendReq.Leases.Delete = []string{key}
	}
	if decision.NewState.Status.Terminal() {
		appendReq.Leases = LeaseOps{Clear: true}
	}
	after := RunHead{Header: head.Header, State: decision.NewState, Revision: head.Revision + 1}
	return CommitResult{Status: CommitAccepted, Snapshot: snapshotOf(&after),
		Events: cloneEvents(appendReq.Transition.Events), Grant: minted}, &appendReq, nil
}

func (r *runtime) Record(ctx context.Context, runID RunID) (RunRecord, error) {
	if err := checkContext(ctx); err != nil {
		return RunRecord{}, err
	}
	head, transitions, err := r.store.LoadRecord(ctx, runID)
	if err != nil {
		return RunRecord{}, err
	}
	if err := r.validateHead(runID, &head); err != nil {
		return RunRecord{}, err
	}
	if uint64(len(transitions)) != head.Revision {
		return RunRecord{}, fmt.Errorf("%w: log has %d transitions, head revision %d", ErrLogTruncated, len(transitions), head.Revision)
	}
	for i := range transitions {
		if err := ValidateTransitionRecord(&transitions[i]); err != nil {
			return RunRecord{}, fmt.Errorf("agent: runtime: invalid transition %d: %w", i, err)
		}
	}
	folded, revision, err := FoldRun(&head.Header, transitions)
	if err != nil {
		return RunRecord{}, fmt.Errorf("agent: runtime: fold: %w", err)
	}
	if revision != head.Revision || !statesEquivalent(&folded, &head.State) {
		return RunRecord{}, errors.New("agent: runtime: snapshot diverges from transition log")
	}
	return RunRecord{Header: cloneRunHeader(head.Header), Snapshot: snapshotOf(&head), Transitions: transitions}, nil
}

// RenewLease extends the live lease behind grant on the Executing target of
// stepID/callID by one LeaseTTL from now. Workers call it periodically while
// an effect runs so a long tool call is not recovered as Unknown under it.
// With LeaseTTL zero it is a no-op after validating the grant.
func (r *runtime) RenewLease(ctx context.Context, runID RunID, stepID StepID, callID CallID, grant ExecutionGrant) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if grant == "" {
		return ErrStaleRuntime
	}
	key := leaseKey(stepID, callID)
	if key == "" {
		return errors.New("agent: runtime: renew requires a step")
	}
	return r.store.RenewLease(ctx, runID, key, grant, r.leaseDeadline())
}

// RecoverExpired commits grantless recovery for every expired lease that
// still occupies an Executing target. An expired tool call is settled as
// Unknown; an expired model step is recovered to Prepared. Hosts call this
// on a timer; Loop does not. The dying process writes nothing.
func (r *runtime) RecoverExpired(ctx context.Context) (int, error) {
	if r.leaseTTL <= 0 {
		return 0, nil
	}
	expired, err := r.store.ExpiredLeases(ctx, r.now())
	if err != nil {
		return 0, err
	}
	n := 0
	for _, e := range expired {
		head, err := r.loadHead(ctx, e.RunID)
		if err != nil {
			if errors.Is(err, ErrRunNotFound) {
				continue
			}
			return n, err
		}
		if !r.leaseExpired(e.Lease) {
			continue
		}
		proto, err := ProtocolFor(head.Header.SchemaVersion)
		if err != nil {
			return n, err
		}
		cmd, cmdID, ok := recoveryCommand(&head.State, e.Key, e.Lease)
		if !ok {
			continue
		}
		env, err := proto.BuildEnvelope(e.RunID, cmdID, cmd)
		if err != nil {
			return n, err
		}
		res, err := r.Commit(ctx, CommitRequest{Command: env})
		if err != nil {
			if errors.Is(err, ErrStaleRuntime) || errors.Is(err, ErrRunTerminal) || errors.Is(err, ErrCommandConflict) {
				continue
			}
			return n, err
		}
		if res.Status == CommitAccepted {
			n++
		}
	}
	return n, nil
}

func recoveryCommand(state *MachineState, key string, lease ExecutionLease) (AgentCommand, CommandID, bool) {
	switch cur := state.Current.(type) {
	case ModelStep:
		if cur.Status != ModelExecuting || key != leaseKey(cur.RefValue.ID, "") {
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
		}, DeriveToolRecoveryCommandID(state.RunID, cur.RefValue.ID, callID, lease.Claim), true
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

// leaseKey addresses the execution target of a start: a ModelStep by step,
// a tool call by step and call.
func leaseKey(stepID StepID, callID CallID) string {
	switch {
	case stepID == "":
		return ""
	case callID == "":
		return "model/" + string(stepID)
	default:
		return "call/" + string(stepID) + "/" + string(callID)
	}
}

func grantKey(c AgentCommand) string {
	switch cmd := c.(type) {
	case StartModelExecution:
		return leaseKey(cmd.StepID, "")
	case SubmitModelResult:
		return leaseKey(cmd.StepID, "")
	case SubmitModelFailure:
		return leaseKey(cmd.StepID, "")
	case RejectModelResult:
		return leaseKey(cmd.StepID, "")
	case RecoverModelExecution:
		return leaseKey(cmd.StepID, "")
	case StartToolCall:
		return leaseKey(cmd.StepID, cmd.CallID)
	case SubmitToolResult:
		return leaseKey(cmd.StepID, cmd.CallID)
	case SubmitToolFailure:
		return leaseKey(cmd.StepID, cmd.CallID)
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
