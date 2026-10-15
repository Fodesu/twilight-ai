package driver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/compaction"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/prompt"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/frozen"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/unit"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/sessionkernel"
)

// DefaultCompactionAttempts bounds maintenance for one preparation even if
// concurrent context changes repeatedly invalidate generated summaries.
const DefaultCompactionAttempts = 4

var (
	ErrCompactionNoProgress = errors.New("driver: compaction cannot bring this candidate within policy")
	ErrSummaryFailed        = errors.New("driver: summary model failed")
)

// ContextDecision is policy, not structure: the core selects a tool-pair-closed
// suffix for RetainEntries. A negative retention is invalid.
type ContextDecision struct {
	Compact       bool
	RetainEntries int
}

// CompactionContext is a snapshot. Policy may render its materialized entries
// into a summary request without having to discover legal tool-call boundaries.
type CompactionContext struct {
	SessionID session.SessionID
	Plan      chatlog.CompactionPlan
	Entries   []chatlog.Materialized
}

type SummaryRequest struct {
	Model   run.ModelRef
	Request model.ModelRequest
	Target  *run.TargetRef
}

// ContextPolicy inspects the complete candidate request. It supplies token
// estimation, model limits, thresholds and summary content/model, not an effect
// implementation: BuildSummary only builds a request; Compactions executes it.
type ContextPolicy interface {
	Decide(context.Context, prompt.Input, *prompt.Prompt) (ContextDecision, error)
	BuildSummary(context.Context, *CompactionContext) (SummaryRequest, error)
}

type summaryRequests interface {
	FrozenRequest(context.Context, run.Digest) (model.ModelRequest, error)
}

// Compactions drives Session maintenance through the same Executor and Watcher
// as Runs. Its locks prevent two local Await registrations replacing each
// other; persistent intent, executor identities and Writer fences provide the
// cross-process guarantees. No model client or Run state is held here.
type Compactions struct {
	Executor    effect.ExecutionPort
	Watcher     *effect.Watcher
	Requests    summaryRequests
	Frozen      frozen.Store
	Content     chatlog.ContentResolver
	Policy      ContextPolicy
	MaxAttempts int
	Now         func() time.Time
	// Fail receives cleanup errors asynchronously, outside operation locks
	// and joined recovery work, so it may close the Session safely.
	Fail func(session.SessionID, error)

	mu    sync.Mutex
	locks map[session.SessionID]*compactionLock
}

type compactionLock struct {
	token chan struct{}
	refs  int
}

func (c *Compactions) acquire(ctx context.Context, sid session.SessionID) (func(), error) {
	c.mu.Lock()
	if c.locks == nil {
		c.locks = map[session.SessionID]*compactionLock{}
	}
	lock := c.locks[sid]
	if lock == nil {
		lock = &compactionLock{token: make(chan struct{}, 1)}
		lock.token <- struct{}{}
		c.locks[sid] = lock
	}
	lock.refs++
	c.mu.Unlock()
	drop := func() {
		c.mu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(c.locks, sid)
		}
		c.mu.Unlock()
	}
	select {
	case <-ctx.Done():
		drop()
		return nil, ctx.Err()
	case <-lock.token:
		return func() { lock.token <- struct{}{}; drop() }, nil
	}
}

func (c *Compactions) now() int64 {
	if c.Now != nil {
		return c.Now().UnixMilli()
	}
	return time.Now().UnixMilli()
}

func preparation(input *prompt.Input) (run.Digest, error) {
	body, err := jsonstable.EncodeTypedPayload(1, "twilight/compaction/preparation", input)
	if err != nil {
		return "", err
	}
	return jsonstable.DigestBytes(body), nil
}

// CheckCandidate records work but never calls a model. The Loop returns to the
// Session driver before any summary wait, then builds a fresh candidate after
// maintenance. A successful automatic compaction is allowed once per request
// boundary, durably, so an oversized retained suffix cannot cause an endless
// summarize/rebuild loop (including after an owner restart).
func (c *Compactions) CheckCandidate(ctx context.Context, w writer.Writer, input prompt.Input, candidate *prompt.Prompt) (loop.CandidateDecision, error) {
	release, err := c.acquire(ctx, w.SessionID())
	if err != nil {
		return loop.CandidateReady, err
	}
	defer release()
	state, err := compaction.Read(ctx, w.Projections(), w.SessionID())
	if err != nil {
		return loop.CandidateReady, err
	}
	if state.Pending != "" {
		return loop.CandidateMaintenance, nil
	}
	c.acknowledgeFinished(ctx, w, &state)
	if c.Policy == nil {
		return loop.CandidateReady, nil
	}
	decision, err := c.Policy.Decide(ctx, input, candidate)
	if err != nil || !decision.Compact {
		return loop.CandidateReady, err
	}
	boundary, err := preparation(&input)
	if err != nil {
		return loop.CandidateReady, err
	}
	attempts := 0
	for id := range state.Operations {
		op := state.Operations[id]
		if op.Request.Preparation == boundary {
			attempts++
			if op.Status == compaction.Applied {
				return loop.CandidateReady, ErrCompactionNoProgress
			}
		}
	}
	limit := c.MaxAttempts
	if limit <= 0 {
		limit = DefaultCompactionAttempts
	}
	if attempts >= limit {
		return loop.CandidateReady, fmt.Errorf("%w: summary attempt limit %d", ErrCompactionNoProgress, limit)
	}
	contextState, err := chatlog.ReadContext(ctx, w.Projections(), w.SessionID())
	if err != nil {
		return loop.CandidateReady, err
	}
	plan, possible, err := chatlog.PlanCompaction(contextState.Entries, decision.RetainEntries)
	if err != nil {
		return loop.CandidateReady, err
	}
	if !possible {
		return loop.CandidateReady, ErrCompactionNoProgress
	}
	entries, err := chatlog.NewMaterializer(c.Content).Entries(ctx, contextState.Entries)
	if err != nil {
		return loop.CandidateReady, err
	}
	request, err := c.Policy.BuildSummary(ctx, &CompactionContext{SessionID: w.SessionID(), Plan: plan, Entries: entries})
	if err != nil {
		return loop.CandidateReady, err
	}
	id, err := newCompactionID()
	if err != nil {
		return loop.CandidateReady, err
	}
	intent, err := c.freeze(ctx, id, &plan, boundary, &request)
	if err != nil {
		return loop.CandidateReady, err
	}
	if err := c.start(ctx, w, &intent); err != nil {
		if errors.Is(err, chatlog.ErrContextChanged) {
			return loop.CandidateRebuild, nil
		}
		if errors.Is(err, compaction.ErrPending) {
			return loop.CandidateMaintenance, nil
		}
		return loop.CandidateReady, err
	}
	return loop.CandidateMaintenance, nil
}

func newCompactionID() (compaction.ID, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return compaction.ID(hex.EncodeToString(bytes[:])), nil
}

func (c *Compactions) freeze(ctx context.Context, id compaction.ID, plan *chatlog.CompactionPlan, boundary run.Digest, request *SummaryRequest) (compaction.Request, error) {
	if c.Frozen == nil {
		return compaction.Request{}, errors.New("driver: compaction requires a frozen store")
	}
	if request.Model == "" {
		request.Model = run.ModelRef(request.Request.Model)
	}
	if request.Model == "" || string(request.Model) != request.Request.Model || len(request.Request.Tools) != 0 {
		return compaction.Request{}, errors.New("driver: summary request must name its model and have no tools")
	}
	digest, err := schema.Canonical().DigestRequest(request.Request)
	if err != nil {
		return compaction.Request{}, err
	}
	body, err := schema.Bodies().EncodeRequest(&request.Request, digest)
	if err != nil {
		return compaction.Request{}, err
	}
	if err := c.Frozen.Put(ctx, digest, body); err != nil {
		return compaction.Request{}, err
	}
	return compaction.Request{ID: id, Plan: *plan, Model: request.Model, RequestDigest: digest, Preparation: boundary, Target: request.Target}, nil
}

// Start explicitly accepts a preplanned operation. Its ID belongs to the
// caller; an identical retry is a no-op, while a changed request conflicts.
// This entry point and automatic checks share the same durable protocol.
func (c *Compactions) Start(ctx context.Context, w writer.Writer, id compaction.ID, plan chatlog.CompactionPlan, request *SummaryRequest) error {
	release, err := c.acquire(ctx, w.SessionID())
	if err != nil {
		return err
	}
	defer release()
	intent, err := c.freeze(ctx, id, &plan, "", request)
	if err != nil {
		return err
	}
	return c.start(ctx, w, &intent)
}

func (c *Compactions) start(ctx context.Context, w writer.Writer, request *compaction.Request) error {
	part := compaction.Start(*request)
	res, err := w.Commit(ctx, func(v writer.View) (*writer.SemanticGroup, error) {
		batches, err := part.Prepare(ctx, v, c.now())
		if err != nil {
			return nil, err
		}
		if len(batches) == 0 {
			return nil, nil
		}
		if err := sessionkernel.RequireQuiescentRun(v); err != nil {
			return nil, err
		}
		return &writer.SemanticGroup{CommitID: ledger.CommitID("compaction-start/" + request.ID), Batches: batches}, nil
	})
	return commitOK("start", &res, err)
}

// NeedsRecovery is the activation/idle-release predicate for hosts. Pending
// operations and unacknowledged settlements remain work even without a Run.
func (c *Compactions) NeedsRecovery(ctx context.Context, w writer.Writer) (bool, error) {
	state, err := compaction.Read(ctx, w.Projections(), w.SessionID())
	if err != nil {
		return false, err
	}
	return state.NeedsRecovery(), nil
}

// Recover finishes an operation left anywhere between acceptance, dispatch,
// outcome production, Session commit and executor acknowledgement. Cancellation
// leaves the durable intent intact; it does not cancel a replacement owner's
// shared execution. The host must arrange later activation after closing a
// Session explicitly; this protocol is not a global scheduling index.
func (c *Compactions) Recover(ctx context.Context, w writer.Writer) error {
	release, err := c.acquire(ctx, w.SessionID())
	if err != nil {
		return err
	}
	defer release()
	state, err := compaction.Read(ctx, w.Projections(), w.SessionID())
	if err != nil {
		return err
	}
	c.acknowledgeFinished(ctx, w, &state)
	if state.Pending == "" {
		return nil
	}
	op := state.Operations[state.Pending]
	request, err := c.Requests.FrozenRequest(ctx, op.Request.RequestDigest)
	if err != nil {
		return err
	}
	assignment := effect.Assignment{Session: run.Scope(w.SessionID()), RunID: op.Request.Key(w.SessionID()).RunID,
		StepID: "summary", Effect: op.Request.Key(w.SessionID()).Effect, Target: op.Request.Target,
		Body: effect.ModelAssignment{Model: op.Request.Model, Request: &request, RequestDigest: op.Request.RequestDigest}}
	attachment, err := c.Executor.Attach(ctx, assignment.Key())
	if err != nil {
		return err
	}
	switch attachment.State {
	case effect.AttachmentMissing:
		// An obsolete request need not reach the model. Abort closes the key
		// against any delayed dispatch before disposing its Session intent.
		contextState, err := chatlog.ReadContext(ctx, w.Projections(), w.SessionID())
		if err != nil {
			return err
		}
		if err := op.Request.Plan.Validate(contextState.Entries); err != nil {
			if !errors.Is(err, chatlog.ErrContextChanged) {
				return err
			}
			closed, err := c.Executor.Abort(ctx, assignment.Key())
			if err != nil {
				return err
			}
			if closed.State == effect.AttachmentAborted {
				return c.finish(ctx, w, &op, compaction.Stale, "context_changed", "", "")
			}
		} else {
			unavailable, err := c.Executor.Validate(ctx, assignment)
			if err != nil {
				return err
			}
			if unavailable != nil {
				closed, err := c.Executor.Abort(ctx, assignment.Key())
				if err != nil {
					return err
				}
				if closed.State == effect.AttachmentAborted {
					return c.finish(ctx, w, &op, compaction.Failed, unavailable.Class, unavailable.Message, "")
				}
			} else if err := c.Executor.Dispatch(ctx, assignment); err != nil {
				// Unknown is not a safe refusal. Keep the intent and recover
				// by key instead of manufacturing a failure or a fresh effect.
				return err
			}
		}
	case effect.AttachmentOrphaned:
		if recoverer, ok := c.Executor.(effect.Recoverer); ok {
			if err := recoverer.RecoverExecution(ctx, assignment.Key()); err != nil {
				return err
			}
		}
	case effect.AttachmentAborted:
		return c.finish(ctx, w, &op, compaction.Failed, "aborted", "summary execution was aborted", "")
	case effect.AttachmentActive, effect.AttachmentTerminal:
	default:
		return fmt.Errorf("driver: invalid summary attachment %q", attachment.State)
	}
	outcome, err := c.Watcher.Await(ctx, assignment.Key())
	if err != nil {
		return err
	}
	if outcome.Key != assignment.Key() {
		return errors.New("driver: summary outcome identity mismatch")
	}
	switch result := outcome.Result.(type) {
	case effect.ModelSucceeded:
		if strings.TrimSpace(result.Result.Text) == "" || len(result.Result.ToolCalls) != 0 {
			return c.finish(ctx, w, &op, compaction.Failed, "invalid_summary", "summary must be nonempty text without tool calls", "")
		}
		return c.finish(ctx, w, &op, compaction.Applied, "", "", result.Result.Text)
	case effect.ModelFailed:
		return c.finish(ctx, w, &op, compaction.Failed, string(result.Code), result.Message, "")
	case effect.Cancelled:
		return c.finish(ctx, w, &op, compaction.Failed, "cancelled", "summary execution was cancelled", "")
	case effect.Unknown:
		return c.finish(ctx, w, &op, compaction.Failed, "unknown", result.Message, "")
	default:
		return fmt.Errorf("driver: unexpected summary outcome %T", outcome.Result)
	}
}

func (c *Compactions) finish(ctx context.Context, w writer.Writer, op *compaction.Operation, status compaction.Status, code, message, text string) error {
	result := compaction.FinishedPayload{ID: op.Request.ID, Status: status, Code: code, Message: message}
	parts := make([]unit.Part, 0, 3)
	if status == compaction.Applied {
		result.CompactionID = chatlog.CompactionID("compact-" + op.Request.ID)
		summary := chatlog.Summary{ID: chatlog.SummaryID("summary-" + op.Request.ID), Parts: chatlog.Parts{chatlog.TextPart{Text: text}}}
		var err error
		if summary.Digest, err = chatlog.DigestSummary(&summary); err != nil {
			return err
		}
		parts = append(parts, unit.PartFunc(func(_ context.Context, v writer.View, _ int64) ([]writer.TypedBatch, error) {
			return nil, sessionkernel.RequireQuiescentRun(v)
		}), chatlog.CompactContext(result.CompactionID, summary, op.Request.Plan))
	}
	parts = append(parts, compaction.Finish(result))
	res, err := unit.Commit(ctx, w, c.now(), unit.Work{CommitID: ledger.CommitID("compaction-finish/" + op.Request.ID), Parts: parts})
	if errors.Is(err, chatlog.ErrContextChanged) && status == compaction.Applied {
		return c.finish(ctx, w, op, compaction.Stale, "context_changed", "", "")
	}
	if err := commitOK("finish", &res, err); err != nil {
		return err
	}
	state, err := compaction.Read(ctx, w.Projections(), w.SessionID())
	if err != nil {
		return err
	}
	settled, found := state.Operations[op.Request.ID]
	if !found || settled.Status == compaction.Pending {
		return errors.New("driver: compaction finish has no durable disposition")
	}
	c.acknowledge(ctx, w, &settled)
	if settled.Status == compaction.Failed {
		return fmt.Errorf("%w: %s: %s", ErrSummaryFailed, settled.Code, settled.Message)
	}
	return nil
}

func commitOK(operation string, result *writer.CommitResult, err error) error {
	if err != nil {
		return err
	}
	switch result.Outcome {
	case writer.CommitApplied, writer.CommitAlreadyApplied, writer.CommitNoop:
		return nil
	default:
		return fmt.Errorf("driver: compaction %s: %s: %s", operation, result.Outcome, result.Detail)
	}
}

func (c *Compactions) acknowledgeFinished(ctx context.Context, w writer.Writer, state *compaction.State) {
	ids := make([]compaction.ID, 0, len(state.Operations))
	for id := range state.Operations {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		op := state.Operations[id]
		if op.Status != compaction.Pending && !op.Acknowledged {
			c.acknowledge(ctx, w, &op)
		}
	}
}

func (c *Compactions) acknowledge(ctx context.Context, w writer.Writer, op *compaction.Operation) {
	if op.Acknowledged || op.Status == compaction.Pending {
		return
	}
	var err error
	if port, ok := c.Executor.(effect.Acknowledger); ok {
		err = port.Acknowledge(ctx, op.Request.Key(w.SessionID()))
	}
	if err == nil {
		var res writer.CommitResult
		res, err = unit.Commit(ctx, w, c.now(), unit.Work{CommitID: ledger.CommitID("compaction-ack/" + op.Request.ID), Parts: []unit.Part{compaction.Acknowledge(op.Request.ID)}})
		err = commitOK("acknowledge", &res, err)
	}
	if err != nil && c.Fail != nil {
		go c.Fail(w.SessionID(), fmt.Errorf("driver: acknowledge summary %s: %w", op.Request.ID, err))
	}
}
