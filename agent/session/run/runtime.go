package runmod

import (
	"context"
	"errors"
	"fmt"
	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
	"github.com/felinics/twilight/agent/session/extension/writer"
	"time"
)

// SourceDigestCarrier is implemented by companion event values whose content
// a Run fact names by digest (TRN-MAP-3). The Runtime verifies every carried
// digest was recorded by a fact of the same group (RUN-CMT-3 step 9).
type SourceDigestCarrier interface {
	SourceDigest() es.Digest
}

// SnapshotPolicy decides whether the machine projection is written to the
// projection cache after a commit. It sees the state before and after Evolve.
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
	Writers   writer.Writers
	Registry  *extension.Registry
	Store     session.Store // read side for Record and the terminal-Run fallback
	Frozen    run.FrozenValueStore
	Companion run.Companion
	Snapshot  SnapshotPolicy
	// Cache receives the machine projection per SnapshotPolicy; nil disables.
	Cache extension.ProjectionCache
	Now   func() time.Time
}

// Runtime is the run.Runtime over a Session Writer (RUN-CMT-1).
type Runtime struct {
	cfg Config
}

func NewRuntime(cfg Config) (*Runtime, error) {
	switch {
	case cfg.Writers == nil, cfg.Registry == nil, cfg.Store == nil:
		return nil, errors.New("runmod: runtime requires writers, registry and store")
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
	return &Runtime{cfg: cfg}, nil
}

// NewMemoryFrozenValues is the in-process FrozenValueStore.
func NewMemoryFrozenValues() *run.MemoryFrozenValues { return run.NewMemoryFrozenValues() }

func (r *Runtime) nowMilli() int64 { return r.cfg.Now().UnixMilli() }

func (r *Runtime) writer(ctx context.Context, sid session.SessionID) (writer.Writer, error) {
	w, err := r.cfg.Writers.Writer(ctx, sid)
	if err != nil {
		return nil, ownershipError(err)
	}
	return w, nil
}

// ownershipError maps the Writer's ownership loss onto the Run sentinel.
func ownershipError(err error) error {
	if errors.Is(err, &extension.Error{Code: extension.ErrOwnershipLost}) || session.IsCode(err, session.ErrOwnershipLost) {
		return fmt.Errorf("%w: %v", run.ErrOwnershipLost, err)
	}
	return err
}

// --- Load / Record --------------------------------------------------------------

func (r *Runtime) Load(ctx context.Context, sid session.SessionID, runID run.RunID) (run.RuntimeSnapshot, error) {
	if err := run.CheckContext(ctx); err != nil {
		return run.RuntimeSnapshot{}, err
	}
	w, err := r.writer(ctx, sid)
	if err != nil {
		return run.RuntimeSnapshot{}, err
	}
	state, head, err := w.Projections().Load(ctx, sid, MachineProjectionID, MachineProjection.Version)
	if err != nil {
		return run.RuntimeSnapshot{}, err
	}
	m := state.(Machine)
	if ms, ok := m.Active[runID]; ok {
		return run.RuntimeSnapshot{State: ms, Position: m.Positions[runID], Head: head, SchemaVersion: m.Schemas[runID]}, nil
	}
	// Not active: terminal or unknown. Terminal Runs leave the projection, so
	// fold the Run's own events to answer (RUN-CMT-1).
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
	w, err := r.writer(ctx, sid)
	if err != nil {
		return run.RunRecord{}, err
	}
	state, _, err := w.Projections().Load(ctx, sid, MachineProjectionID, MachineProjection.Version)
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

// record reads the Run's events from the Store, folds them and (when expect
// is given) compares the fold with the projection state.
func (r *Runtime) record(ctx context.Context, sid session.SessionID, runID run.RunID, expect *run.MachineState) (run.RunRecord, error) {
	page, err := r.cfg.Store.Read(ctx, session.ReadRequest{SessionID: sid, Types: []session.EventType{Prefix}})
	if err != nil {
		return run.RunRecord{}, err
	}
	var record run.RunRecord
	var position run.RunPosition
	for i := range page.Events {
		e := &page.Events[i]
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
			record.Created = e.Seq
		}
		record.Events = append(record.Events, *e)
		record.Facts = append(record.Facts, ev.Fact)
		position = e.Seq
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
	w, err := r.writer(ctx, sid)
	if err != nil {
		return run.CommitResult{}, err
	}

	var out evaluated
	var rejection error
	var before, after run.MachineState
	res, err := w.Commit(ctx, func(view writer.View) (*writer.SemanticGroup, error) {
		group, result, reject, err := r.evaluate(ctx, view, sid, &req)
		if err != nil {
			return nil, err
		}
		if reject != nil {
			rejection = reject
			return nil, nil
		}
		out = result
		if group != nil {
			before, after = result.before, result.Snapshot.State
		}
		return group, nil
	})
	if err != nil {
		return run.CommitResult{}, ownershipError(err)
	}
	if rejection != nil {
		return run.CommitResult{}, rejection
	}
	switch res.Outcome {
	case writer.CommitApplied:
		out.Status = run.CommitAccepted
		out.Events = res.Events
		last := res.Events[len(res.Events)-1]
		out.Snapshot.Head = session.Head{Next: last.Seq + 1, Digest: last.Digest}
		out.Snapshot.Position = res.Events[out.lastFact].Seq
		r.afterCommit(ctx, w, sid, &before, &after)
		return out.CommitResult, nil
	case writer.CommitNoop:
		// evaluate found an exact replay and filled out.
		return out.CommitResult, nil
	case writer.CommitConflict:
		return run.CommitResult{}, run.ErrCommandConflict
	default:
		return run.CommitResult{}, fmt.Errorf("runmod: commit: %s: %s", res.Outcome, res.Detail)
	}
}

// afterCommit writes the machine projection to the cache when the policy asks
// for it (RUN-CMT-2). Cache failures never affect the commit.
func (r *Runtime) afterCommit(ctx context.Context, w writer.Writer, sid session.SessionID, before, after *run.MachineState) {
	if r.cfg.Cache == nil || !r.cfg.Snapshot(before, after) {
		return
	}
	state, head, err := w.Projections().Load(ctx, sid, MachineProjectionID, MachineProjection.Version)
	if err != nil {
		return
	}
	_ = extension.SaveProjection(ctx, r.cfg.Cache, r.cfg.Registry, sid, MachineProjectionID, MachineProjection.Version, state, head)
}

type evaluated struct {
	run.CommitResult
	before   run.MachineState
	lastFact int
}

// evaluate is RUN-CMT-3 inside the Writer. It returns either a group to
// append with the prospective result, a filled result for an exact replay
// (group nil), or a rejection error.
func (r *Runtime) evaluate(ctx context.Context, view writer.View, sid session.SessionID, req *run.CommitRequest) (*writer.SemanticGroup, evaluated, error, error) {
	env := &req.Command
	commitID := session.CommitID(env.ID)
	runID := env.RunID

	// Steps 2-3: replay. Idempotency is the Writer's (SessionID, CommitID)
	// index alone (RUN-CMT-5): every Run CommandID is content-derived, so a hit
	// is the same command. No Decide runs on replay.
	if existing, found := view.LookupCommit(commitID); found {
		snapshot, err := r.snapshotIn(ctx, view, sid, runID)
		if err != nil {
			return nil, evaluated{}, nil, err
		}
		return nil, evaluated{CommitResult: run.CommitResult{Status: run.CommitAlreadyApplied, Snapshot: snapshot, Events: existing}}, nil, nil
	}

	// Step 5: current state from the Writer's projection.
	proj, err := loadMachine(view)
	if err != nil {
		return nil, evaluated{}, nil, err
	}
	state, active := proj.Active[runID]
	if !active {
		// Terminal Runs leave the projection; tell terminal from unknown.
		if _, ended := proj.Ended[runID]; ended {
			return nil, evaluated{}, run.ErrRunTerminal, nil
		}
		if _, err := r.record(ctx, sid, runID, nil); err != nil {
			return nil, evaluated{}, err, nil
		}
		return nil, evaluated{}, run.ErrRunTerminal, nil
	}
	schema := proj.Schemas[runID]
	if env.SchemaVersion != schema {
		return nil, evaluated{}, nil, fmt.Errorf("runmod: commit: command schema %d does not match run schema %d", env.SchemaVersion, schema)
	}
	proto, err := run.ProtocolFor(schema)
	if err != nil {
		return nil, evaluated{}, nil, err
	}

	decision, err := run.EvaluateCommit(state, proj.Positions[runID], *req, proto)
	if err != nil {
		return nil, evaluated{}, nil, err
	}
	switch decision.Kind {
	case run.DecisionConflict:
		return nil, evaluated{}, run.ErrCommandConflict, nil
	case run.DecisionStale:
		if decision.Reject != nil && !errors.Is(decision.Reject, run.ErrStaleRuntime) {
			return nil, evaluated{}, fmt.Errorf("%w: %w", run.ErrStaleRuntime, decision.Reject), nil
		}
		return nil, evaluated{}, run.ErrStaleRuntime, nil
	case run.DecisionTerminal:
		return nil, evaluated{}, run.ErrRunTerminal, nil
	}

	// Step 8: facts -> events.
	now := r.nowMilli()
	group := &writer.SemanticGroup{CommitID: commitID}
	recorded := map[es.Digest]struct{}{}
	for _, f := range decision.Facts {
		group.Events = append(group.Events, writer.TypedEvent{Type: EventType(f), RecordedAtUnixMilli: now, Value: Event{RunID: runID, Fact: f}})
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
		return nil, evaluated{}, nil, fmt.Errorf("runmod: companion: %w", err)
	}
	for _, me := range companion {
		if session.HasTypePrefix(me.Type, []session.EventType{Prefix}) {
			return nil, evaluated{}, nil, errors.New("runmod: companion must not produce twilight/run/ events")
		}
		// A carried digest must be one a fact of this group recorded; content
		// without a Run-recorded digest (a failed call's tool_result) carries none.
		if carrier, ok := me.Value.(SourceDigestCarrier); ok {
			if d := carrier.SourceDigest(); d != "" {
				if _, recordedHere := recorded[d]; !recordedHere {
					return nil, evaluated{}, nil, fmt.Errorf("runmod: companion %s SourceDigest is not recorded by a fact of this group", me.Type)
				}
			}
		}
		group.Events = append(group.Events, writer.TypedEvent{Type: me.Type, RecordedAtUnixMilli: now, Value: me.Value})
	}
	for _, me := range req.Attach {
		group.Events = append(group.Events, writer.TypedEvent{Type: me.Type, RecordedAtUnixMilli: now, Value: me.Value})
	}
	// A withdrawn request body ends its useful life; a Recovered step keeps it.
	if step, ok := env.Command.(run.WithdrawPreparedStep); ok {
		if ms, isModel := state.Current.(run.ModelStep); isModel && ms.RefValue.ID == step.StepID {
			if dropper, can := r.cfg.Frozen.(interface{ Delete(run.Digest) }); can {
				dropper.Delete(ms.RequestDigest)
			}
		}
	}
	result := evaluated{
		CommitResult: run.CommitResult{Snapshot: run.RuntimeSnapshot{State: decision.NewState, SchemaVersion: schema}},
		before:       state,
		lastFact:     len(decision.Facts) - 1,
	}
	return group, result, nil, nil
}

func loadMachine(view writer.View) (Machine, error) {
	state, err := view.Projection(MachineProjectionID, MachineProjection.Version)
	if err != nil {
		return Machine{}, err
	}
	return state.(Machine), nil
}

// snapshotIn is Load inside the Writer.
func (r *Runtime) snapshotIn(ctx context.Context, view writer.View, sid session.SessionID, runID run.RunID) (run.RuntimeSnapshot, error) {
	proj, err := loadMachine(view)
	if err != nil {
		return run.RuntimeSnapshot{}, err
	}
	if ms, ok := proj.Active[runID]; ok {
		return run.RuntimeSnapshot{State: ms, Position: proj.Positions[runID], Head: view.Head(), SchemaVersion: proj.Schemas[runID]}, nil
	}
	record, err := r.record(ctx, sid, runID, nil)
	if err != nil {
		return run.RuntimeSnapshot{}, err
	}
	return record.Snapshot, nil
}

// --- frozen request, takeover -------------------------------------------------------

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

// RecoverInterrupted is RUN-CMT-7: every Executing target of the Session gets
// one recovery command under the takeover claim of the current Epoch.
func (r *Runtime) RecoverInterrupted(ctx context.Context, sid session.SessionID) (int, error) {
	if err := run.CheckContext(ctx); err != nil {
		return 0, err
	}
	w, err := r.writer(ctx, sid)
	if err != nil {
		return 0, err
	}
	state, _, err := w.Projections().Load(ctx, sid, MachineProjectionID, MachineProjection.Version)
	if err != nil {
		return 0, err
	}
	claim := run.DeriveTakeoverClaim(sid, w.Epoch())
	n := 0
	for runID, ms := range state.(Machine).Active {
		proto, err := run.ProtocolFor(state.(Machine).Schemas[runID])
		if err != nil {
			return n, err
		}
		for _, rec := range run.RecoveryCommands(&ms, claim) {
			env, err := proto.BuildEnvelope(sid, runID, rec.ID, rec.Command)
			if err != nil {
				return n, err
			}
			res, err := r.Commit(ctx, sid, run.CommitRequest{Command: env})
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
	}
	return n, nil
}

var _ run.Runtime = (*Runtime)(nil)
