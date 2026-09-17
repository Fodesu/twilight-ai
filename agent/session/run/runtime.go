package runmod

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/felinics/twilight/agent/artifact"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
	"github.com/felinics/twilight/agent/session/writer"
)

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

// Config assembles a Runtime (agent-runtime.md 10).
type Config struct {
	Registry *extension.Registry
	// Store is the read side: Load and Record fold from it (through Cache)
	// without taking ownership; commands write through the Writer they are
	// handed (AUTH-OWN-2).
	Store  session.Store
	Frozen run.FrozenValueStore
	// Bindings registers the Binding of every frozen body before the fact
	// naming it is committed, so the Writer's admission can resolve it and
	// claim the body for the commit (RUN-WIR-4, EXT-WRT-3). It must be the
	// same store the Writers admit against; nil skips registration, which
	// is only correct for Writers whose Admission has no resolver.
	Bindings artifact.BindingStore
	Snapshot SnapshotPolicy
	// Cache receives the machine projection per SnapshotPolicy; nil disables.
	Cache extension.ProjectionCache
	Now   func() time.Time
}

// Runtime is the run.Runtime (RUN-CMT-1): commands commit through the
// caller's Writer, reads fold from the Store.
type Runtime struct {
	cfg    Config
	reader extension.ProjectionReader
}

func NewRuntime(cfg Config) (*Runtime, error) {
	if cfg.Registry == nil || cfg.Store == nil {
		return nil, errors.New("runmod: runtime requires registry and store")
	}
	if cfg.Frozen == nil {
		cfg.Frozen = FrozenValuesInMemory()
	}
	if cfg.Snapshot == nil {
		cfg.Snapshot = DefaultSnapshotPolicy
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Runtime{cfg: cfg, reader: extension.NewProjectionReader(cfg.Store, cfg.Registry, cfg.Cache)}, nil
}

func (r *Runtime) nowMilli() int64 { return r.cfg.Now().UnixMilli() }

// ownershipError maps the Writer's ownership loss onto the Run sentinel.
func ownershipError(err error) error {
	if errors.Is(err, &extension.Error{Code: extension.ErrOwnershipLost}) || session.IsCode(err, session.ErrOwnershipLost) {
		return fmt.Errorf("%w: %v", run.ErrOwnershipLost, err)
	}
	return err
}

// --- Load / Record --------------------------------------------------------------

func (r *Runtime) Load(ctx context.Context, w writer.Writer, runID run.RunID) (run.RuntimeSnapshot, error) {
	if err := run.CheckContext(ctx); err != nil {
		return run.RuntimeSnapshot{}, err
	}
	if w == nil {
		return run.RuntimeSnapshot{}, errors.New("runmod: load requires the session's writer")
	}
	sid := w.SessionID()
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
	state, _, err := r.reader.Load(ctx, sid, MachineProjectionID, MachineProjection.Version)
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
	page, err := r.cfg.Store.ReadStream(ctx, session.StreamReadRequest{SessionID: sid, Stream: runStream(runID)})
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
			record.Created = session.StreamSeq(i)
		}
		record.Events = append(record.Events, *e)
		record.Facts = append(record.Facts, ev.Fact)
		position = session.StreamSeq(i)
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

// runStream is the fixed logical stream of one Run's facts.
func runStream(runID run.RunID) session.StreamRef {
	return session.StreamRef{Kind: session.StreamKindRun, ID: string(runID)}
}

// flattenCommitEvents returns the commit's events in batch order.
func flattenCommitEvents(c session.Commit) []session.Event {
	var out []session.Event
	for _, b := range c.Batches {
		out = append(out, b.Events...)
	}
	return out
}

// --- Commit ------------------------------------------------------------------------

func (r *Runtime) Commit(ctx context.Context, w writer.Writer, req run.CommitRequest) (run.CommitResult, error) {
	if err := run.CheckContext(ctx); err != nil {
		return run.CommitResult{}, err
	}
	if w == nil {
		return run.CommitResult{}, errors.New("runmod: commit requires the session's writer")
	}
	sid := w.SessionID()
	env := &req.Command
	if env.SessionID != sid {
		return run.CommitResult{}, fmt.Errorf("runmod: commit: envelope session %q does not match %q", env.SessionID, sid)
	}
	for _, a := range req.Attach {
		if session.HasTypePrefix(a.Type, []session.EventType{Prefix}) {
			return run.CommitResult{}, errors.New("runmod: commit: Attach must not carry twilight/run/ events")
		}
	}
	// A frozen body must be readable before the fact that names it is
	// visible; Put is idempotent and content-addressed, so a rejected or
	// replayed command leaves nothing inconsistent behind (RUN-CMT-3).
	if err := r.freezeBodies(ctx, env.Command); err != nil {
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
		out.Events = flattenCommitEvents(res.Commit)
		out.Snapshot.Head = session.Head{Next: res.Commit.Seq + 1, Digest: res.Commit.Digest}
		out.Snapshot.Position = out.position
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

// freezeBodies stores the bodies a command's facts will name by digest
// (RUN-WIR-4): the request of a Prepare, the result of a model settlement,
// the output of a tool settlement and the payload of an external response.
// Each body is encoded as the envelope its digest was computed from and its
// Binding is registered for the Writer's admission.
func (r *Runtime) freezeBodies(ctx context.Context, cmd run.AgentCommand) error {
	var digest run.Digest
	var body []byte
	var err error
	switch c := cmd.(type) {
	case run.PrepareModelRequest:
		digest = c.RequestDigest
		body, err = run.EncodeFrozenRequest(&c.Request, digest)
	case run.SubmitModelResult:
		if digest, err = run.ProtocolV1().DigestModelResult(c.Result); err == nil {
			body, err = run.EncodeFrozenModelResult(&c.Result, digest)
		}
	case run.SubmitToolResult:
		if digest, err = run.ProtocolV1().DigestToolOutput(c.Result.Output); err == nil {
			body, err = run.EncodeFrozenToolOutput(c.Result.Output, digest)
		}
	case run.SubmitToolResponse:
		digest = c.ResponseDigest
		body, err = run.EncodeFrozenToolResponse(c.Payload, digest)
	default:
		return nil
	}
	if err != nil {
		return fmt.Errorf("%w: %w", run.ErrStaleRuntime, err)
	}
	if err := r.cfg.Frozen.Put(ctx, digest, body); err != nil {
		return err
	}
	if r.cfg.Bindings == nil {
		return nil
	}
	binding, err := FrozenBinding(digest)
	if err != nil {
		return err
	}
	// An identical Binding is already registered on a replay; only a
	// differing Ref under the same BindingID conflicts (ART-BND-1).
	_, err = r.cfg.Bindings.CreateBinding(ctx, binding)
	return err
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
	position session.StreamSeq
}

// evaluate is RUN-CMT-3 inside the Writer. It returns either a group to
// append with the prospective result, a filled result for an exact replay
// (group nil), or a rejection error.
func (r *Runtime) evaluate(ctx context.Context, view writer.View, sid session.SessionID, req *run.CommitRequest) (*writer.SemanticGroup, evaluated, error, error) {
	env := &req.Command
	commitID := session.CommitID(env.ID)
	runID := env.RunID

	// Steps 2-3: replay. Idempotency is the Session's (SessionID, CommitID)
	// index alone (RUN-CMT-5): every Run CommandID is content-derived, so a hit
	// is the same command. No Decide runs on replay.
	if existing, found, err := view.LookupCommit(commitID); err != nil {
		return nil, evaluated{}, nil, err
	} else if found {
		snapshot, err := r.snapshotIn(ctx, view, sid, runID)
		if err != nil {
			return nil, evaluated{}, nil, err
		}
		return nil, evaluated{CommitResult: run.CommitResult{Status: run.CommitAlreadyApplied, Snapshot: snapshot, Events: flattenCommitEvents(existing)}}, nil, nil
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

	// Step 8: facts -> the Run's own stream.
	now := r.nowMilli()
	group := &writer.SemanticGroup{CommitID: commitID}
	runEvents := make([]writer.TypedEvent, 0, len(decision.Facts))
	for _, f := range decision.Facts {
		runEvents = append(runEvents, writer.TypedEvent{Type: EventType(f), RecordedAtUnixMilli: now, Value: Event{RunID: runID, Fact: f}})
	}
	// Step 9: the caller's attached events -> the session stream.
	sessionEvents := []writer.TypedEvent{}
	for _, me := range req.Attach {
		sessionEvents = append(sessionEvents, writer.TypedEvent{Type: me.Type, RecordedAtUnixMilli: now, Value: me.Value})
	}
	group.Batches = []writer.TypedBatch{{Stream: runStream(runID), Events: runEvents}}
	if len(sessionEvents) > 0 {
		group.Batches = append(group.Batches, writer.TypedBatch{Stream: session.StreamRef{Kind: session.StreamKindSession}, Events: sessionEvents})
	}
	result := evaluated{
		CommitResult: run.CommitResult{Snapshot: run.RuntimeSnapshot{State: decision.NewState, SchemaVersion: schema}},
		before:       state,
		position:     proj.Positions[runID] + session.StreamSeq(len(decision.Facts)),
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

// RecoverInterrupted is RUN-CMT-7: every Executing target of the Session is
// either reattached (its attempt still runs under an executor the new owner
// can reach, so the Outcome will arrive under the original Claim) or disposed
// with one recovery command under the takeover claim of the current Epoch.
func (r *Runtime) RecoverInterrupted(ctx context.Context, w writer.Writer, reattach run.Reattacher) (int, error) {
	if err := run.CheckContext(ctx); err != nil {
		return 0, err
	}
	if w == nil {
		return 0, errors.New("runmod: recovery requires the session's writer")
	}
	sid := w.SessionID()
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
		for _, target := range run.RecoveryTargets(&ms) {
			target.Schema = state.(Machine).Schemas[runID]
			if reattach != nil && target.Claim != "" {
				attached, err := reattach.Attach(ctx, target)
				if err != nil {
					return n, err
				}
				if !attached.Valid() {
					return n, fmt.Errorf("runmod: unknown recovery disposition %q", attached)
				}
				if attached.PreservesExecution() {
					// Active/terminal delivery is already arranged; deferred means
					// a durable record exists but control-plane takeover is still
					// required. Neither case proves the effect was absent.
					continue
				}
				// Only RecoveryMissing permits automatic disposition below.
			}
			rec := run.RecoveryCommand(target, claim)
			env, err := proto.BuildEnvelope(sid, runID, rec.ID, rec.Command)
			if err != nil {
				return n, err
			}
			res, err := r.Commit(ctx, w, run.CommitRequest{Command: env})
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
