package runmod

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/memohai/twilight/agent/es"
	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/session"
	"github.com/memohai/twilight/agent/session/extension"
)

// SourceDigestCarrier is implemented by companion event values whose content
// a Run fact names by digest (TRN-MAP-3). The Runtime verifies every carried
// digest was recorded by a fact of the same commit (RUN-CMT-3 step 9).
type SourceDigestCarrier interface {
	SourceDigest() es.Digest
}

// SnapshotPolicy decides whether the machine projection snapshot is written
// in a commit's transaction. It sees the state after Evolve.
type SnapshotPolicy func(before, after *run.MachineState) bool

// DefaultSnapshotPolicy writes when the Run returns to Open or terminates.
func DefaultSnapshotPolicy(_, after *run.MachineState) bool {
	if after.Status.Terminal() {
		return true
	}
	_, open := after.Current.(run.Open)
	return open
}

// Config assembles a Runtime (agent-reference-assembly.md 5).
type Config struct {
	Store       session.Store
	Registry    *extension.Registry
	Appender    extension.SemanticAppender
	Projections extension.ProjectionReader
	Frozen      run.FrozenValueStore
	Companion   run.Companion
	Snapshot    SnapshotPolicy
	// LeaseTTL zero means leases never expire (in-process occupancy only).
	LeaseTTL time.Duration
	Now      func() time.Time
}

// Runtime is the run.Runtime over a Session (RUN-CMT-1).
type Runtime struct {
	cfg    Config
	leases extension.Leases
}

func NewRuntime(cfg Config) (*Runtime, error) {
	switch {
	case cfg.Store == nil, cfg.Registry == nil, cfg.Appender == nil, cfg.Projections == nil:
		return nil, errors.New("runmod: runtime requires store, registry, appender and projections")
	case cfg.Companion == nil:
		return nil, errors.New("runmod: runtime requires a Companion")
	}
	if cfg.Frozen == nil {
		cfg.Frozen = run.NewMemoryFrozenValues()
	}
	if cfg.Snapshot == nil {
		cfg.Snapshot = DefaultSnapshotPolicy
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Runtime{cfg: cfg, leases: extension.Leases{Store: cfg.Store}}, nil
}

// NewMemoryFrozenValues is the in-process FrozenValueStore.
func NewMemoryFrozenValues() *run.MemoryFrozenValues { return run.NewMemoryFrozenValues() }

func (r *Runtime) nowMilli() int64 { return r.cfg.Now().UnixMilli() }

// --- Load / Record --------------------------------------------------------------

func (r *Runtime) Load(ctx context.Context, sid session.SessionID, runID run.RunID) (run.RuntimeSnapshot, error) {
	if err := run.CheckContext(ctx); err != nil {
		return run.RuntimeSnapshot{}, err
	}
	state, head, err := r.cfg.Projections.Load(ctx, sid, MachineProjectionID, MachineProjection.Version)
	if err != nil {
		return run.RuntimeSnapshot{}, err
	}
	m := state.(Machine)
	if ms, ok := m.Active[runID]; ok {
		return run.RuntimeSnapshot{State: ms, Position: m.Positions[runID], Head: head, SchemaVersion: m.Schemas[runID]}, nil
	}
	// Not active: terminal or unknown. Terminal Runs leave the projection, so
	// fold the Run's own events to answer.
	record, err := r.record(ctx, sid, runID, nil)
	if err != nil {
		return run.RuntimeSnapshot{}, err
	}
	return record.Snapshot, nil
}

func (r *Runtime) Record(ctx context.Context, sid session.SessionID, runID run.RunID) (run.RunRecord, error) {
	if err := run.CheckContext(ctx); err != nil {
		return run.RunRecord{}, err
	}
	state, _, err := r.cfg.Projections.Load(ctx, sid, MachineProjectionID, MachineProjection.Version)
	if err != nil {
		return run.RunRecord{}, err
	}
	m := state.(Machine)
	var expect *run.MachineState
	if ms, ok := m.Active[runID]; ok {
		expect = &ms
	}
	return r.record(ctx, sid, runID, expect)
}

// record replays the Run's events, folds them and (when expect is given)
// compares the fold with the projection state.
func (r *Runtime) record(ctx context.Context, sid session.SessionID, runID run.RunID, expect *run.MachineState) (run.RunRecord, error) {
	page, err := r.cfg.Store.Replay(ctx, session.ReplayRequest{SessionID: sid, Types: []session.EventType{Prefix}})
	if err != nil {
		return run.RunRecord{}, err
	}
	commits := page.Commits
	for page.Next != nil {
		if page, err = r.cfg.Store.Replay(ctx, session.ReplayRequest{SessionID: sid, Types: []session.EventType{Prefix}, Cursor: page.Next}); err != nil {
			return run.RunRecord{}, err
		}
		commits = append(commits, page.Commits...)
	}
	var record run.RunRecord
	var position run.RunPosition
	for ci := range commits {
		c := &commits[ci]
		for i := range c.Events {
			e := &c.Events[i]
			if !session.HasTypePrefix(e.Type, []session.EventType{Prefix}) {
				continue
			}
			decoded, err := r.cfg.Registry.Decode(*e)
			if err != nil {
				return run.RunRecord{}, err
			}
			if decoded.Unknown {
				return run.RunRecord{}, fmt.Errorf("runmod: record: unknown run event %s v%d", e.Type, decoded.Version)
			}
			ev := decoded.Value.(Event)
			if ev.RunID != runID {
				continue
			}
			if len(record.Events) == 0 {
				record.Created = session.EventPosition{Revision: c.Revision, Index: e.Index, EventDigest: e.EventDigest}
			}
			record.Events = append(record.Events, *e)
			record.Facts = append(record.Facts, ev.Fact)
			position = run.RunPosition{Revision: c.Revision, Index: e.Index}
		}
	}
	if len(record.Facts) == 0 {
		return run.RunRecord{}, run.ErrRunNotFound
	}
	state, err := run.FoldRun(record.Facts)
	if err != nil {
		return run.RunRecord{}, fmt.Errorf("runmod: record: %w", err)
	}
	if expect != nil && !run.StatesEquivalent(&state, expect) {
		return run.RunRecord{}, errors.New("runmod: record: projection diverges from the event fold")
	}
	created := record.Facts[0].(run.RunCreated)
	record.Snapshot = run.RuntimeSnapshot{State: state, Position: position, Head: page.Head, SchemaVersion: created.SchemaVersion}
	return record, nil
}

// --- Commit ------------------------------------------------------------------------

func (r *Runtime) Commit(ctx context.Context, sid session.SessionID, req run.CommitRequest) (run.CommitResult, error) {
	if err := run.CheckContext(ctx); err != nil {
		return run.CommitResult{}, err
	}
	env := &req.Command
	if env.SessionID != sid {
		return run.CommitResult{}, fmt.Errorf("runmod: commit: envelope session %q does not match %q", env.SessionID, sid)
	}
	for _, a := range req.Attach {
		if session.HasTypePrefix(a.Type, []session.EventType{Prefix}) {
			return run.CommitResult{}, errors.New("runmod: commit: Attach must not carry twilight/run/ events")
		}
	}
	// The frozen request body must be readable before the fact that names it
	// is visible; Put is idempotent and content-addressed (RUN-CMT-3).
	if prep, ok := env.Command.(run.PrepareModelRequest); ok {
		body, err := run.EncodeFrozenRequest(&prep.Request, prep.RequestDigest)
		if err != nil {
			return run.CommitResult{}, fmt.Errorf("%w: %w", run.ErrStaleRuntime, err)
		}
		if err := r.cfg.Frozen.Put(ctx, prep.RequestDigest, body); err != nil {
			return run.CommitResult{}, err
		}
	}

	var out run.CommitResult
	var rejection error
	res, err := r.cfg.Appender.AppendSemanticIn(ctx, sid, func(tx extension.SemanticTx) (*extension.SemanticGroup, error) {
		group, result, reject, err := r.evaluate(tx, sid, &req)
		if err != nil {
			return nil, err
		}
		if reject != nil {
			rejection = reject
			return nil, errDiscard
		}
		out = result
		return group, nil
	})
	if err != nil {
		if errors.Is(err, errDiscard) {
			return run.CommitResult{}, rejection
		}
		return run.CommitResult{}, err
	}
	switch res.Outcome {
	case extension.SemanticApplied:
		out.Status = run.CommitAccepted
		out.Commit = *res.Commit
		out.Snapshot.Head = session.Head{Revision: res.Commit.Revision, Digest: res.Commit.CommitDigest}
		out.Snapshot.Position = run.RunPosition{Revision: res.Commit.Revision, Index: uint16(out.Snapshot.Position.Index)}
		return out, nil
	case extension.SemanticNoop:
		// evaluate found an exact replay and filled out.
		return out, nil
	case extension.SemanticAlreadyApplied:
		out.Status = run.CommitAlreadyApplied
		out.Commit = *res.Commit
		return out, nil
	case extension.SemanticCommitConflict:
		return run.CommitResult{}, run.ErrCommandConflict
	default:
		return run.CommitResult{}, fmt.Errorf("runmod: commit: %s: %s", res.Outcome, res.Detail)
	}
}

var errDiscard = errors.New("runmod: discard")

// evaluate is RUN-CMT-3 inside the transaction. It returns either a group to
// append with the prospective result, a filled result for an exact replay
// (group nil), or a rejection error.
func (r *Runtime) evaluate(tx extension.SemanticTx, sid session.SessionID, req *run.CommitRequest) (*extension.SemanticGroup, run.CommitResult, error, error) {
	env := &req.Command
	commitID := session.CommitID(env.ID)
	runID := env.RunID

	// Steps 2-3: exact replay vs same-ID conflict, via the control-plane
	// command index and the stored commit.
	if existing, found, err := tx.LookupCommit(commitID); err != nil {
		return nil, run.CommitResult{}, nil, err
	} else if found {
		entry, ok, err := tx.ControlGet(CommandNamespace, string(commitID))
		if err != nil {
			return nil, run.CommitResult{}, nil, err
		}
		if !ok || string(entry.Value) != string(env.Digest) {
			return nil, run.CommitResult{}, run.ErrCommandConflict, nil
		}
		snapshot, err := r.snapshotIn(tx, sid, runID)
		if err != nil {
			return nil, run.CommitResult{}, nil, err
		}
		result := run.CommitResult{Status: run.CommitAlreadyApplied, Snapshot: snapshot, Commit: existing}
		if run.IsStart(env.Command) {
			// The start's grant is live only while the lease it minted is still
			// the lease on record for this target.
			key := run.GrantTarget(runID, env.Command)
			if lease, has, err := extension.LookupLease(tx, LeaseNamespace, key); err != nil {
				return nil, run.CommitResult{}, nil, err
			} else if has && lease.Token == extension.DeriveLeaseToken(sid, LeaseNamespace, key, string(run.CommandClaim(env.Command)), commitID) {
				result.Grant = run.ExecutionGrant(lease.Token)
			}
		}
		return nil, result, nil, nil
	}

	// Step 5: current state from snapshot plus filtered tail.
	proj, before, err := r.loadMachine(tx)
	if err != nil {
		return nil, run.CommitResult{}, nil, err
	}
	state, active := proj.Active[runID]
	if !active {
		// Terminal Runs leave the projection; tell terminal from unknown.
		if _, err := r.terminalState(tx, runID); err != nil {
			return nil, run.CommitResult{}, err, nil
		}
		return nil, run.CommitResult{}, run.ErrRunTerminal, nil
	}
	schema := proj.Schemas[runID]
	if env.SchemaVersion != schema {
		return nil, run.CommitResult{}, nil, fmt.Errorf("runmod: commit: command schema %d does not match run schema %d", env.SchemaVersion, schema)
	}
	proto, err := run.ProtocolFor(schema)
	if err != nil {
		return nil, run.CommitResult{}, nil, err
	}

	// Step 6: grant and recovery authority from the lease table.
	key := run.GrantTarget(runID, env.Command)
	var lease extension.Lease
	hasLease := false
	if key != "" {
		lease, hasLease, err = extension.LookupLease(tx, LeaseNamespace, key)
		if err != nil {
			return nil, run.CommitResult{}, nil, err
		}
	}
	claim := run.CommandClaim(env.Command)
	grantValid := hasLease && req.Grant != "" && lease.Token == extension.LeaseToken(req.Grant)
	if _, recovering := env.Command.(run.RecoverModelExecution); recovering && req.Grant != "" {
		grantValid = grantValid && lease.Holder == string(claim)
	}
	// Recovery authority (RUN-CMT-6): the lease is expired and the command is
	// bound to its holder, either by the carried Claim (model recovery) or by
	// the derived tool-recovery CommandID (Unknown settlement).
	recoveryValid := hasLease && req.Grant == "" && r.expired(lease)
	if recoveryValid {
		switch cmd := env.Command.(type) {
		case run.RecoverModelExecution:
			recoveryValid = lease.Holder == string(cmd.Claim)
		case run.SubmitToolFailure:
			recoveryValid = cmd.Outcome == run.ToolOutcomeUnknown &&
				env.ID == run.DeriveToolRecoveryCommandID(runID, cmd.StepID, cmd.CallID, run.ExecutionClaim(lease.Holder))
		default:
			recoveryValid = false
		}
	}

	decision, err := run.EvaluateCommit(state, proj.Positions[runID], *req, grantValid, recoveryValid, proto)
	if err != nil {
		return nil, run.CommitResult{}, nil, err
	}
	switch decision.Kind {
	case run.DecisionConflict:
		return nil, run.CommitResult{}, run.ErrCommandConflict, nil
	case run.DecisionStale:
		if decision.Reject != nil && !errors.Is(decision.Reject, run.ErrStaleRuntime) {
			return nil, run.CommitResult{}, fmt.Errorf("%w: %w", run.ErrStaleRuntime, decision.Reject), nil
		}
		return nil, run.CommitResult{}, run.ErrStaleRuntime, nil
	case run.DecisionTerminal:
		return nil, run.CommitResult{}, run.ErrRunTerminal, nil
	}

	// Step 8: facts -> events.
	now := r.nowMilli()
	group := &extension.SemanticGroup{CommitID: commitID}
	recorded := map[es.Digest]struct{}{}
	for _, f := range decision.Facts {
		group.Events = append(group.Events, extension.TypedEvent{Type: EventType(f), RecordedAtUnixMilli: now, Value: Event{RunID: runID, Fact: f}})
		switch fact := f.(type) {
		case run.ModelStepCompleted:
			recorded[fact.ResultDigest] = struct{}{}
		case run.ToolCallCompleted:
			recorded[fact.OutputDigest] = struct{}{}
		case run.ToolCallAnswered:
			recorded[fact.ResponseDigest] = struct{}{}
		}
	}
	// Step 9: companion, then Attach.
	companion, err := r.cfg.Companion.Map(run.CompanionRequest{Session: sid, Owner: state.Owner, RunID: runID,
		Command: env.Command, Facts: decision.Facts, State: decision.NewState, RecordedAtUnixMilli: now})
	if err != nil {
		return nil, run.CommitResult{}, nil, fmt.Errorf("runmod: companion: %w", err)
	}
	for _, me := range companion {
		if session.HasTypePrefix(me.Type, []session.EventType{Prefix}) {
			return nil, run.CommitResult{}, nil, errors.New("runmod: companion must not produce twilight/run/ events")
		}
		// A carried digest must be one a fact of this commit recorded; content
		// without a Run-recorded digest (a failed call's tool_result) carries none.
		if carrier, ok := me.Value.(SourceDigestCarrier); ok {
			if d := carrier.SourceDigest(); d != "" {
				if _, recordedHere := recorded[d]; !recordedHere {
					return nil, run.CommitResult{}, nil, fmt.Errorf("runmod: companion %s SourceDigest is not recorded by a fact of this commit", me.Type)
				}
			}
		}
		group.Events = append(group.Events, extension.TypedEvent{Type: me.Type, RecordedAtUnixMilli: now, Value: me.Value})
	}
	for _, me := range req.Attach {
		group.Events = append(group.Events, extension.TypedEvent{Type: me.Type, RecordedAtUnixMilli: now, Value: me.Value})
	}

	// Step 10: lease changes, command index, snapshot.
	var grant run.ExecutionGrant
	switch {
	case run.IsStart(env.Command):
		acquired, err := extension.AcquireLease(tx, sid, commitID, now, extension.AcquireLeaseRequest{
			Namespace: LeaseNamespace, Key: key, Holder: string(claim), TTL: r.cfg.LeaseTTL})
		if err != nil {
			var xerr *extension.Error
			if errors.As(err, &xerr) && xerr.Code == extension.ErrConflict {
				return nil, run.CommitResult{}, run.ErrStaleRuntime, nil
			}
			return nil, run.CommitResult{}, nil, err
		}
		grant = run.ExecutionGrant(acquired.Token)
	case run.IsSettlement(env.Command) && hasLease:
		if err := extension.ReleaseLease(tx, LeaseNamespace, key, lease.Token); err != nil {
			return nil, run.CommitResult{}, nil, err
		}
	}
	if decision.NewState.Status.Terminal() {
		// Terminal commit revokes every grant of the Run (RUN-CMT-6): the
		// Executing targets of the pre-state name the live leases.
		for _, target := range executingTargets(&state) {
			if l, has, err := extension.LookupLease(tx, LeaseNamespace, target); err != nil {
				return nil, run.CommitResult{}, nil, err
			} else if has {
				if err := extension.ReleaseLease(tx, LeaseNamespace, target, l.Token); err != nil {
					return nil, run.CommitResult{}, nil, err
				}
			}
		}
	}
	if err := tx.ControlPut(CommandNamespace, string(commitID), []byte(env.Digest), 0); err != nil {
		return nil, run.CommitResult{}, nil, err
	}
	// A withdrawn request body ends its useful life; a Recovered step keeps it.
	if step, ok := env.Command.(run.WithdrawPreparedStep); ok {
		if ms, isModel := state.Current.(run.ModelStep); isModel && ms.RefValue.ID == step.StepID {
			if dropper, can := r.cfg.Frozen.(interface{ Delete(run.Digest) }); can {
				dropper.Delete(ms.RequestDigest)
			}
		}
	}
	if r.cfg.Snapshot(&state, &decision.NewState) {
		if err := extension.SaveSnapshotIn(tx, &MachineProjection, proj, before); err != nil {
			return nil, run.CommitResult{}, nil, err
		}
	}
	result := run.CommitResult{
		Snapshot: run.RuntimeSnapshot{State: decision.NewState, SchemaVersion: schema,
			Position: run.RunPosition{Index: uint16(len(decision.Facts) - 1)}}, // Revision filled after append
		Grant: grant,
	}
	return group, result, nil, nil
}

func (r *Runtime) loadMachine(tx extension.SemanticTx) (Machine, session.Head, error) {
	state, head, err := extension.LoadIn(tx, &MachineProjection)
	if err != nil {
		return Machine{}, session.Head{}, err
	}
	return state.(Machine), head, nil
}

// snapshotIn is Load inside the transaction.
func (r *Runtime) snapshotIn(tx extension.SemanticTx, sid session.SessionID, runID run.RunID) (run.RuntimeSnapshot, error) {
	proj, head, err := r.loadMachine(tx)
	if err != nil {
		return run.RuntimeSnapshot{}, err
	}
	if ms, ok := proj.Active[runID]; ok {
		return run.RuntimeSnapshot{State: ms, Position: proj.Positions[runID], Head: head, SchemaVersion: proj.Schemas[runID]}, nil
	}
	state, err := r.terminalState(tx, runID)
	if err != nil {
		return run.RuntimeSnapshot{}, err
	}
	return run.RuntimeSnapshot{State: state.state, Position: state.position, Head: head, SchemaVersion: state.schema}, nil
}

type foldedRun struct {
	state    run.MachineState
	position run.RunPosition
	schema   uint16
}

// terminalState folds a Run that is no longer in the projection; it returns
// ErrRunNotFound when the Run never existed in this Session.
func (r *Runtime) terminalState(tx extension.SemanticTx, runID run.RunID) (foldedRun, error) {
	commits, err := tx.Tail(session.Head{}, []session.EventType{Prefix})
	if err != nil {
		return foldedRun{}, err
	}
	var facts []run.Fact
	var position run.RunPosition
	for ci := range commits {
		for _, e := range commits[ci].Events {
			if !session.HasTypePrefix(e.Type, []session.EventType{Prefix}) {
				continue
			}
			decoded, err := tx.Decode(e)
			if err != nil {
				return foldedRun{}, err
			}
			if decoded.Unknown {
				continue
			}
			if ev := decoded.Value.(Event); ev.RunID == runID {
				facts = append(facts, ev.Fact)
				position = run.RunPosition{Revision: commits[ci].Revision, Index: e.Index}
			}
		}
	}
	if len(facts) == 0 {
		return foldedRun{}, run.ErrRunNotFound
	}
	state, err := run.FoldRun(facts)
	if err != nil {
		return foldedRun{}, err
	}
	return foldedRun{state: state, position: position, schema: facts[0].(run.RunCreated).SchemaVersion}, nil
}

func (r *Runtime) expired(l extension.Lease) bool {
	return l.DeadlineUnixMilli != 0 && l.DeadlineUnixMilli <= r.nowMilli()
}

// executingTargets lists the lease keys of every Executing target in state.
func executingTargets(s *run.MachineState) []string {
	switch cur := s.Current.(type) {
	case run.ModelStep:
		if cur.Status == run.ModelExecuting {
			return []string{run.LeaseKey(s.RunID, cur.RefValue.ID, "")}
		}
	case run.ToolStep:
		var out []string
		for _, c := range cur.Calls {
			if c.Status == run.ToolExecuting {
				out = append(out, run.LeaseKey(s.RunID, cur.RefValue.ID, c.CallID))
			}
		}
		return out
	}
	return nil
}

// --- frozen request, lease renewal, recovery -------------------------------------------

func (r *Runtime) FrozenRequest(ctx context.Context, digest run.Digest) (run.ModelRequest, error) {
	if err := run.CheckContext(ctx); err != nil {
		return run.ModelRequest{}, err
	}
	if digest == "" {
		return run.ModelRequest{}, errors.New("runmod: empty request digest")
	}
	raw, ok, err := r.cfg.Frozen.Get(ctx, digest)
	if err != nil {
		return run.ModelRequest{}, err
	}
	if !ok {
		return run.ModelRequest{}, fmt.Errorf("%w: request %s", run.ErrFrozenValueMissing, digest)
	}
	return run.DecodeFrozenRequest(raw, digest)
}

func (r *Runtime) RenewLease(ctx context.Context, sid session.SessionID, runID run.RunID, stepID run.StepID, callID run.CallID, grant run.ExecutionGrant) error {
	if err := run.CheckContext(ctx); err != nil {
		return err
	}
	if grant == "" {
		return run.ErrStaleRuntime
	}
	key := run.LeaseKey(runID, stepID, callID)
	if key == "" {
		return errors.New("runmod: renew requires a step")
	}
	err := r.leases.Renew(ctx, sid, LeaseNamespace, key, extension.LeaseToken(grant), r.cfg.LeaseTTL, r.nowMilli())
	if err != nil {
		var xerr *extension.Error
		if errors.As(err, &xerr) && xerr.Code == extension.ErrStale {
			return run.ErrStaleRuntime
		}
		return err
	}
	return nil
}

// RecoverExpired commits grantless recovery for every expired lease that still
// occupies an Executing target (RUN 5.1).
func (r *Runtime) RecoverExpired(ctx context.Context) (int, error) {
	if r.cfg.LeaseTTL <= 0 {
		return 0, nil
	}
	type expiredLease struct {
		sid   session.SessionID
		lease extension.Lease
	}
	var expired []expiredLease
	err := r.leases.Expired(ctx, LeaseNamespace, r.nowMilli(), func(sid session.SessionID, l extension.Lease) (bool, error) {
		expired = append(expired, expiredLease{sid, l})
		return true, nil
	})
	if err != nil {
		return 0, err
	}
	n := 0
	for _, e := range expired {
		runID := runIDOfLeaseKey(e.lease.Key)
		if runID == "" {
			continue
		}
		snapshot, err := r.Load(ctx, e.sid, runID)
		if err != nil {
			if errors.Is(err, run.ErrRunNotFound) {
				continue
			}
			return n, err
		}
		cmd, cmdID, ok := run.RecoveryCommand(&snapshot.State, e.lease.Key, run.ExecutionClaim(e.lease.Holder))
		if !ok {
			continue
		}
		proto, err := snapshot.Protocol()
		if err != nil {
			return n, err
		}
		env, err := proto.BuildEnvelope(e.sid, runID, cmdID, cmd)
		if err != nil {
			return n, err
		}
		res, err := r.Commit(ctx, e.sid, run.CommitRequest{Base: snapshot.Position, Command: env})
		if err != nil {
			if errors.Is(err, run.ErrStaleRuntime) || errors.Is(err, run.ErrRunTerminal) || errors.Is(err, run.ErrCommandConflict) {
				continue
			}
			return n, err
		}
		if res.Status == run.CommitAccepted {
			n++
		}
	}
	return n, nil
}

func runIDOfLeaseKey(key string) run.RunID {
	for _, sep := range []string{"/model/", "/call/"} {
		if i := strings.Index(key, sep); i > 0 {
			return run.RunID(key[:i])
		}
	}
	return ""
}

var _ run.Runtime = (*Runtime)(nil)
