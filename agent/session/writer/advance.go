package writer

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/artifact"
	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
)

// AdvanceRequest is what an AdvanceFn decides (EXT-WRT-10): the Schema the
// new segment is written under, its creation metadata and causation, and
// the bootstrap commits that seed it. Target is declared into Metadata
// (extension.DeclareSchema); a Metadata already declaring another Schema is
// invalid. Every bootstrap group is encoded under Target, not under the
// tip's Schema: the groups are the first commits of the new segment.
type AdvanceRequest struct {
	Target      extension.SchemaVersion
	CausationID es.CausationID
	Metadata    jsonstable.Value
	Bootstrap   []SemanticGroup
}

// AdvanceFn decides the segment to publish against the tip's View, the same
// View a CommitFn sees; nil publishes nothing.
type AdvanceFn func(View) (*AdvanceRequest, error)

// AdvanceOutcome is the semantic answer of an Advance.
type AdvanceOutcome string

const (
	AdvanceApplied AdvanceOutcome = "applied"
	AdvanceInvalid AdvanceOutcome = "invalid"
	AdvanceNoop    AdvanceOutcome = "noop"
)

// AdvanceResult is the outcome of an Advance. As with CommitResult, Outcome
// carries the semantic answer and error is reserved for failures: a request
// the Writer refuses is AdvanceInvalid with Detail and a nil error. Header
// and Commits are the published segment and its sealed bootstrap for
// AdvanceApplied.
type AdvanceResult struct {
	Outcome AdvanceOutcome
	Header  session.SegmentHeader
	Commits []session.Commit
	Detail  string
}

func advanceInvalid(format string, args ...any) (AdvanceResult, error) {
	return AdvanceResult{Outcome: AdvanceInvalid, Detail: fmt.Sprintf(format, args...)}, nil
}

// Advance is the segment pipeline (EXT-WRT-10): read (View) -> decide (fn) ->
// declare the Schema -> encode the bootstrap under it -> admit -> rebase
// every projection onto the new boundary -> claim -> kernel Advance ->
// install -> refresh cache / notify observers. It is the one way a Session's
// Schema changes (EXT-SCH-3): the new segment is the child of the tip at its
// head, published as the Session's tip in one durable step (SES-ADV-2), and
// this Writer continues on it. What the caller decides -- that the Session
// is at a quiescent point, what the new segment stands for, what its
// bootstrap holds -- is decided inside fn against the View; the Writer
// guarantees that the transition is atomic, that the tip is unchanged when
// it is refused, and that nothing is written under a Schema the registry
// cannot encode.
func (w *sessionWriter) Advance(ctx context.Context, fn AdvanceFn) (AdvanceResult, error) {
	if fn == nil {
		return AdvanceResult{}, errors.New("writer: nil fn")
	}
	w.mu.Lock()
	var writes []cacheWrite
	var applied []session.Commit
	defer func() {
		if len(applied) > 0 && w.observers.any() {
			w.observers.hold()
			w.mu.Unlock()
			w.projections.saveRefresh(ctx, writes)
			for i := range applied {
				w.observers.notify(ctx, w.sid, applied[i])
			}
			w.observers.release()
			return
		}
		w.mu.Unlock()
		w.projections.saveRefresh(ctx, writes)
	}()
	if w.lost != nil {
		return AdvanceResult{}, w.lost
	}
	if w.head.Next == 0 {
		return advanceInvalid("the tip holds no commit to anchor the new segment to")
	}
	req, err := fn(view{w})
	if err != nil {
		return AdvanceResult{}, err
	}
	if req == nil {
		return AdvanceResult{Outcome: AdvanceNoop}, nil
	}
	if req.Target == 0 {
		return advanceInvalid("schema 0 is not a schema")
	}
	if !w.registry.SupportsSchema(req.Target) {
		return AdvanceResult{}, &session.Error{Code: session.ErrUnsupported, Operation: "advance", SessionID: w.sid,
			Detail: fmt.Sprintf("no registered module has a codec under schema %d", req.Target)}
	}
	meta, err := extension.DeclareSchema(req.Metadata, req.Target)
	if err != nil {
		return advanceInvalid("%s", err.Error())
	}
	proposals := make([]session.Proposal, 0, len(req.Bootstrap))
	provisional := make([]session.Commit, 0, len(req.Bootstrap))
	refsOf := make([][]bindingRef, 0, len(req.Bootstrap))
	seen := make(map[session.CommitID]struct{}, len(req.Bootstrap))
	for i := range req.Bootstrap {
		g := &req.Bootstrap[i]
		if g.CommitID == "" {
			return advanceInvalid("bootstrap %d: empty CommitID", i)
		}
		if _, dup := seen[g.CommitID]; dup || w.kernel.Committed(g.CommitID) {
			return advanceInvalid("bootstrap %d: CommitID %s already in the ledger", i, g.CommitID)
		}
		seen[g.CommitID] = struct{}{}
		batches, refs, invalid := encode(w.registry, req.Target, g)
		if invalid != "" {
			return advanceInvalid("bootstrap %d: %s", i, invalid)
		}
		if err := session.ValidateBatches(batches); err != nil {
			return advanceInvalid("bootstrap %d: %s", i, err.Error())
		}
		for _, ref := range refs {
			if invalid, err := w.admission.admit(ctx, ref.id, ref.decl); err != nil {
				return AdvanceResult{}, err
			} else if invalid != "" {
				return advanceInvalid("bootstrap %d: %s: %s", i, ref.where, invalid)
			}
		}
		proposals = append(proposals, session.Proposal{CommitID: g.CommitID, Intent: g.Intent, Batches: batches})
		// The provisional commits are what a reader of the published segment
		// folds too: the kernel seals PrevDigest and Digest, and nothing a
		// projection reads differs between the two.
		provisional = append(provisional, session.Commit{Seq: w.head.Next + session.CommitSeq(i), CommitID: g.CommitID, Epoch: w.kernel.Epoch(), Intent: g.Intent, Batches: batches})
		refsOf = append(refsOf, refs)
	}
	// Every projection is re-derived for the new tip before anything is
	// persisted: the whole log becomes inherited history (EXT-PRJ-8), and
	// the bootstrap is the new tip's own. The log is read from the Store
	// because the kernel handle holds the index, not the commits.
	page, err := w.store.ReadCommits(ctx, session.CommitReadRequest{SessionID: w.sid})
	if err != nil {
		return AdvanceResult{}, err
	}
	if page.Head != w.head {
		// The log the Store holds is not the one this Writer has been
		// appending to: another owner wrote under this SessionID.
		w.lost = &extension.Error{Code: extension.ErrOwnershipLost, Detail: fmt.Sprintf("log head %+v, writer head %+v", page.Head, w.head)}
		return AdvanceResult{}, w.lost
	}
	boundary := session.SegmentHeader{Parent: &session.LedgerRef{Seq: w.head.Next - 1, Digest: w.head.Digest}}
	next, err := w.projections.rebase(page.Commits, boundary, provisional)
	if err != nil {
		return advanceInvalid("%s", err.Error())
	}
	claims := make([]*artifact.RetentionClaim, 0, len(proposals))
	release := func() {
		for _, c := range claims {
			w.admission.release(ctx, c) // best effort; openWriter reconciles any orphan
		}
	}
	for i := range proposals {
		claim, invalid, err := w.admission.claim(ctx, proposals[i].CommitID, bindingIDs(refsOf[i]))
		if err != nil {
			release()
			return AdvanceResult{}, err
		}
		if invalid != "" {
			release()
			return advanceInvalid("bootstrap %d: %s", i, invalid)
		}
		if claim != nil {
			claims = append(claims, claim)
		}
	}
	header, sealed, err := w.kernel.Advance(ctx, session.AdvanceRequest{CausationID: req.CausationID, Metadata: meta, Bootstrap: proposals})
	if err != nil {
		if session.IsCode(err, session.ErrHandleFailed) && header.HeaderDigest != "" {
			// Published, but the handle cannot answer for the new tip. The
			// Writer fails closed on a known outcome: a reopened Writer
			// continues on the new segment, and a retry of the same
			// transition finds it already applied.
			w.lost = err
			return AdvanceResult{}, w.lost
		}
		if session.IsCode(err, session.ErrConflict) || appendOutcomeKnown(err) {
			release()
		}
		if session.IsCode(err, session.ErrOwnershipLost) {
			w.lost = &extension.Error{Code: extension.ErrOwnershipLost, Detail: err.Error()}
			return AdvanceResult{}, w.lost
		}
		if session.IsCode(err, session.ErrConflict) {
			return advanceInvalid("%s", err.Error())
		}
		if appendOutcomeKnown(err) {
			return AdvanceResult{}, err
		}
		// Whether the root moved is unknown to this Writer: fail closed, as
		// Commit does; a reopened Writer reads the root (EXT-WRT-4).
		w.lost = &extension.Error{Code: extension.ErrUnknownOutcome, Detail: err.Error()}
		return AdvanceResult{}, w.lost
	}
	w.projections.install(next)
	w.header, w.schema = header, req.Target
	w.head = w.kernel.Head()
	if len(sealed) > 0 {
		writes = w.projections.planRefresh(w.head, true)
	}
	applied = sealed
	return AdvanceResult{Outcome: AdvanceApplied, Header: header, Commits: sealed}, nil
}
