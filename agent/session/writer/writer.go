// Package writer is the single in-process write entry of one Session
// (EXT-SCP-1). The Writer itself stays small: it reads the head, evaluates
// the caller's decision against one transactional View, and appends the
// commit atomically under the kernel's ownership fence and CommitID index.
// Everything else that happens around a commit is a stage composed onto that
// pipeline, each in its own file: encoding and stream affinity (encode.go),
// artifact admission and retention claims (admission.go), transactional
// projection folding and the projection cache (projector.go), observer
// fan-out (observers.go) and the replay fingerprint (fingerprint.go). A new
// capability joins the pipeline as a stage, not as a field of the Writer.
package writer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/felinics/twilight/agent/artifact"
	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
)

// TypedEvent is a module value plus its event metadata. The payload is
// encoded and validated against the Registry at commit time.
type TypedEvent struct {
	Type                session.EventType
	RecordedAtUnixMilli int64
	Value               any
}

// TypedBatch is the caller's view of one StreamBatch: the events of one
// logical stream inside one commit. A commit carries at most one batch per
// stream and may span several streams.
type TypedBatch struct {
	Stream session.StreamRef
	Events []TypedEvent
}

// SemanticGroup is what a CommitFn decides: the commit identity and the
// per-stream batches it carries.
type SemanticGroup struct {
	CommitID session.CommitID
	// Intent is the digest of the operation the group realizes, sealed into
	// the commit (session.Commit.Intent). When set, a replay of the CommitID
	// is judged by intent: the same intent is already applied, a different
	// one is a conflict, whether or not the events could be rebuilt. Empty
	// falls back to the event fingerprint (EXT-WRT-2).
	Intent  es.Digest
	Batches []TypedBatch
}

// View is what a CommitFn may read: head, idempotency index and projections
// folded to the current head (EXT-WRT-1).
type View interface {
	Head() session.Head
	Epoch() session.Epoch
	// Committed reports whether a commit is already in the stream. It is
	// answered from an index the kernel already keeps, without touching storage.
	Committed(session.CommitID) bool
	// LookupCommit returns the sealed commit. It comes from storage when the
	// kernel handle does not hold it, so a caller that only needs the answer
	// uses Committed.
	LookupCommit(session.CommitID) (session.Commit, bool, error)
	// StreamHead reports whether this Session has written to a logical
	// stream and the StreamSeq its next event takes (SES-REP-3).
	StreamHead(session.StreamRef) (session.StreamSeq, bool)
	// Projection returns a detached state that the caller owns.
	Projection(extension.ProjectionID, extension.ProjectionVersion) (any, error)
}

// CommitFn decides the commit to write; nil means write nothing.
type CommitFn func(View) (*SemanticGroup, error)

type CommitOutcome string

const (
	CommitApplied        CommitOutcome = "applied"
	CommitAlreadyApplied CommitOutcome = "already_applied" // same CommitID, same fingerprint
	CommitConflict       CommitOutcome = "conflict"        // same CommitID, different fingerprint
	CommitInvalid        CommitOutcome = "invalid"
	CommitNoop           CommitOutcome = "noop"
)

// CommitResult is the outcome of a Commit. Outcome carries the semantic
// result and error is reserved for an infrastructure failure, so a caller must
// branch on Outcome: CommitInvalid and CommitConflict are reported with a nil
// error because they are answers, not failures. A configuration that cannot
// serve the registry is not an answer -- OpenWriter rejects it up front.
// Commit is the sealed commit for applied and already_applied, zero otherwise.
type CommitResult struct {
	Outcome CommitOutcome
	Commit  session.Commit
	Claim   *artifact.RetentionClaim
	Detail  string
}

// Writer is the single in-process write entry of one Session (EXT-SCP-1).
type Writer interface {
	SessionID() session.SessionID
	Epoch() session.Epoch
	Commit(context.Context, CommitFn) (CommitResult, error)
	Projections() extension.ProjectionReader
	// OwnerExists reports whether a CommitID is in this stream; artifact's
	// reconciliation uses it through artifact.OwnerVerifier.
	OwnerExists(context.Context, artifact.ClaimOwner) (bool, error)
	Close(context.Context) error
}

// Writers is the host-maintained SessionID to Writer map (EXT-WRT-6).
type Writers interface {
	Writer(context.Context, session.SessionID) (Writer, error)
}

// WritersConfig carries the deployment's projection cache and observers. Every
// field is optional: with no cache the Writer folds every registered
// projection from the beginning of the log and stores nothing; with no
// observers nothing is notified.
type WritersConfig struct {
	// Cache holds folded projection states, so a reopening Writer starts from
	// one instead of refolding the whole log (EXT-PRJ-3).
	Cache extension.ProjectionCache
	// CachePolicy decides which projections the Writer refreshes and when; nil
	// means extension.CacheEvery(extension.DefaultCacheEvery). It never affects reading: an entry
	// the cache already holds is used whoever wrote it.
	CachePolicy extension.CachePolicy
	// Observers are notified of every applied commit (EXT-WRT-7).
	Observers []CommitObserver
}

// sessionWriter is the Writer: the kernel handle (head, fence, CommitID and
// stream index) plus the stages composed onto its commit pipeline.
type sessionWriter struct {
	mu       sync.Mutex
	kernel   session.Handle
	registry *extension.Registry
	sid      session.SessionID
	head     session.Head
	lost     error

	projections *projector
	admission   *admitter
	observers   *observers
}

// OpenWriter takes ownership of sid and rebuilds the idempotency index and
// every registered projection from the whole log (EXT-WRT-1). When a ledger
// is configured it reconciles this Session's claims before returning
// (ART-RET-3): no Commit can be in flight yet.
func OpenWriter(ctx context.Context, store session.Store, registry *extension.Registry, admission Admission, sid session.SessionID, opts session.OpenOptions) (Writer, error) {
	return openWriter(ctx, store, registry, admission, sid, opts, WritersConfig{})
}

func openWriter(ctx context.Context, store session.Store, registry *extension.Registry, admission Admission, sid session.SessionID, opts session.OpenOptions, cfg WritersConfig) (Writer, error) {
	if store == nil || registry == nil {
		return nil, errors.New("writer: nil store or registry")
	}
	header, err := store.Header(ctx, sid)
	if err != nil {
		return nil, err
	}
	// The registry encodes payloads for one protocol version; a Session
	// created under another one must not be written through it (EXT-WRT-1).
	if header.ProtocolVersion != registry.ProtocolVersion {
		return nil, &session.Error{Code: session.ErrUnsupportedProfile, Operation: "open", SessionID: sid,
			Detail: fmt.Sprintf("session protocol v%d, registry protocol v%d", header.ProtocolVersion, registry.ProtocolVersion)}
	}
	kernel, err := store.Open(ctx, sid, opts)
	if err != nil {
		return nil, err
	}
	w := &sessionWriter{kernel: kernel, registry: registry, sid: sid,
		projections: newProjector(registry, sid, cfg.Cache, cfg.CachePolicy),
		admission:   &admitter{Admission: admission, protocol: registry.ProtocolVersion, sid: sid},
		observers:   &observers{list: cfg.Observers}}
	page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid})
	if err != nil {
		_ = kernel.Close(ctx)
		return nil, err
	}
	if err := w.projections.rebuild(ctx, &page); err != nil {
		_ = kernel.Close(ctx)
		return nil, err
	}
	w.head = page.Head
	if err := w.admission.reconcile(ctx, w); err != nil {
		_ = kernel.Close(ctx)
		return nil, err
	}
	return w, nil
}

func (w *sessionWriter) SessionID() session.SessionID { return w.sid }
func (w *sessionWriter) Epoch() session.Epoch         { return w.kernel.Epoch() }

func (w *sessionWriter) OwnerExists(_ context.Context, owner artifact.ClaimOwner) (bool, error) {
	if owner.Kind != ClaimOwnerKind || owner.Authority != string(w.sid) {
		return false, &artifact.Error{Code: artifact.ErrInvalid, Operation: "owner_exists", Detail: "owner is not a commit of this session"}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.lost != nil {
		return false, w.lost
	}
	return w.kernel.Committed(session.CommitID(owner.Identity)), nil
}

// errWriterClosed is the failure a closed Writer keeps returning; Writers
// recognizes it so a forgotten Writer is not closed a second time.
var errWriterClosed = &extension.Error{Code: extension.ErrInvalid, Detail: "writer closed"}

func (w *sessionWriter) Close(ctx context.Context) error {
	w.mu.Lock()
	var writes []cacheWrite
	if w.head.Next > 0 {
		writes = w.projections.planRefresh(w.head, true)
	}
	w.lost = errWriterClosed
	err := w.kernel.Close(ctx)
	w.mu.Unlock()
	w.projections.saveRefresh(ctx, writes)
	return err
}

// --- view --------------------------------------------------------------------------

type view struct{ w *sessionWriter }

func (v view) Head() session.Head   { return v.w.head }
func (v view) Epoch() session.Epoch { return v.w.kernel.Epoch() }

// Committed and LookupCommit are answered by the kernel, which already holds
// the CommitID index Append needs (SES-REP-3/4): the Writer keeps no copy of
// the log.
func (v view) Committed(id session.CommitID) bool { return v.w.kernel.Committed(id) }

func (v view) LookupCommit(id session.CommitID) (session.Commit, bool, error) {
	return v.w.kernel.LookupCommit(id)
}

func (v view) StreamHead(stream session.StreamRef) (session.StreamSeq, bool) {
	return v.w.kernel.StreamHead(stream)
}

func (v view) Projection(id extension.ProjectionID, ver extension.ProjectionVersion) (any, error) {
	return v.w.projections.detached(id, ver)
}

func (w *sessionWriter) Projections() extension.ProjectionReader { return memoryReader{w} }

// --- commit --------------------------------------------------------------------------

// Commit is the pipeline: read (View) -> decide (fn) -> encode -> fingerprint
// replay check -> projection pre-fold -> retention claim -> Append -> advance
// projections -> refresh cache / notify observers (EXT-WRT-1). The critical
// section ends after Append and the state advance; cache writes and observer
// notifications are derived work and run after the unlock (EXT-PRJ-7,
// EXT-WRT-7).
func (w *sessionWriter) Commit(ctx context.Context, fn CommitFn) (CommitResult, error) {
	if fn == nil {
		return CommitResult{}, errors.New("writer: nil fn")
	}
	w.mu.Lock()
	var writes []cacheWrite
	var applied *session.Commit
	defer func() {
		// notifyMu is taken before mu is released, so notifications keep the
		// commit order while a later Commit already runs.
		if applied != nil && w.observers.any() {
			w.observers.hold()
			w.mu.Unlock()
			w.projections.saveRefresh(ctx, writes)
			w.observers.notify(ctx, w.sid, *applied)
			w.observers.release()
			return
		}
		w.mu.Unlock()
		w.projections.saveRefresh(ctx, writes)
	}()
	if w.lost != nil {
		return CommitResult{}, w.lost
	}
	group, err := fn(view{w})
	if err != nil {
		return CommitResult{}, err
	}
	if group == nil {
		return CommitResult{Outcome: CommitNoop}, nil
	}
	if group.CommitID == "" {
		return CommitResult{Outcome: CommitInvalid, Detail: "empty CommitID"}, nil
	}
	batches, refs, invalid := encode(w.registry, group)
	if invalid != "" {
		return CommitResult{Outcome: CommitInvalid, Detail: invalid}, nil
	}
	if err := session.ValidateBatches(batches); err != nil {
		return CommitResult{Outcome: CommitInvalid, Detail: err.Error()}, nil
	}
	for _, ref := range refs {
		if invalid, err := w.admission.admit(ctx, ref.id, ref.decl); err != nil {
			return CommitResult{}, err
		} else if invalid != "" {
			return CommitResult{Outcome: CommitInvalid, Detail: fmt.Sprintf("%s: %s", ref.where, invalid)}, nil
		}
	}
	if existing, committed, err := w.kernel.LookupCommit(group.CommitID); err != nil {
		return CommitResult{}, err
	} else if committed {
		return replayVerdict(existing, group.Intent, batches)
	}
	// Projections must accept the commit before anything is persisted. The
	// provisional commit is what a reader folds too: the kernel assigns
	// PrevDigest and Digest inside Append, and nothing a projection may read
	// differs between the two paths.
	provisional := session.Commit{Seq: w.head.Next, CommitID: group.CommitID, Epoch: w.kernel.Epoch(), Batches: batches}
	// Only an authoritative projection's fold refuses the commit; a derived
	// one that cannot fold is marked unhealthy once the commit lands.
	next, err := w.projections.fold(provisional)
	if err != nil {
		return CommitResult{Outcome: CommitInvalid, Detail: err.Error()}, nil
	}
	claim, invalid, err := w.admission.claim(ctx, group.CommitID, bindingIDs(refs))
	if err != nil {
		return CommitResult{}, err
	}
	if invalid != "" {
		return CommitResult{Outcome: CommitInvalid, Detail: invalid}, nil
	}
	sealed, err := w.kernel.Append(ctx, session.Proposal{CommitID: group.CommitID, Intent: group.Intent, Batches: batches})
	if err != nil {
		if claim != nil && appendOutcomeKnown(err) {
			w.admission.release(ctx, claim) // best effort; OpenWriter reconciles any orphan
		}
		if session.IsCode(err, session.ErrOwnershipLost) {
			w.lost = &extension.Error{Code: extension.ErrOwnershipLost, Detail: err.Error()}
			return CommitResult{}, w.lost
		}
		if session.IsCode(err, session.ErrConflict) {
			return CommitResult{Outcome: CommitConflict, Detail: err.Error()}, nil
		}
		if appendOutcomeKnown(err) {
			return CommitResult{}, err // rejected before any write; the Writer's state still matches the log
		}
		// The claim stays active until reopening can verify the owner commit.
		// Anything else leaves the log's content unknown to this Writer: its
		// head and folded states may be one commit behind what is on disk, and
		// continuing would assign Seqs the kernel has already used. Fail
		// closed; a reopened Writer rebuilds from the log and a replay of the
		// same commit is answered by the kernel's index (EXT-WRT-4).
		w.lost = &extension.Error{Code: extension.ErrUnknownOutcome, Detail: err.Error()}
		return CommitResult{}, w.lost
	}
	w.projections.advance(next)
	w.head = w.kernel.Head()
	writes = w.projections.planRefresh(w.head, false)
	applied = &sealed
	return CommitResult{Outcome: CommitApplied, Commit: sealed, Claim: claim}, nil
}

// replayVerdict tells a replay of a committed CommitID from a conflict
// (EXT-WRT-2). When both the commit and the retry declare an intent, the
// intents decide; otherwise the kernel holds the commit, not a fingerprint,
// so the old commit's event fingerprint is recomputed and compared. Only a
// CommitID hit pays for either.
func replayVerdict(existing session.Commit, intent es.Digest, batches []session.StreamBatch) (CommitResult, error) {
	if existing.Intent != "" && intent != "" {
		if existing.Intent == intent {
			return CommitResult{Outcome: CommitAlreadyApplied, Commit: existing}, nil
		}
		return CommitResult{Outcome: CommitConflict, Detail: "same CommitID, different intent"}, nil
	}
	fp, err := fingerprintCommit(existing.CommitID, batches)
	if err != nil {
		return CommitResult{}, err
	}
	old, err := fingerprintCommit(existing.CommitID, existing.Batches)
	if err != nil {
		return CommitResult{}, err
	}
	if old == fp {
		return CommitResult{Outcome: CommitAlreadyApplied, Commit: existing}, nil
	}
	return CommitResult{Outcome: CommitConflict}, nil
}

// appendOutcomeKnown reports the Append errors that guarantee nothing was
// written: the kernel's validation rejections, and a context error, which an
// adapter may only return before it starts writing (SES-APP-1).
func appendOutcomeKnown(err error) bool {
	if session.IsCode(err, session.ErrInvalid) || session.IsCode(err, session.ErrNotFound) {
		return true
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
