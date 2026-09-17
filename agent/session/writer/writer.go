package writer

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/felinics/twilight/agent/artifact"
	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/jsonstable"
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
	Batches  []TypedBatch
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

// Admission supplies Binding admission and the claim ledger. Both may be nil
// while no committed group actually references an artifact: an event type
// declaring Bindings only means its payloads *may* carry references, so a
// deployment that never does needs neither.
type Admission struct {
	Bindings artifact.BindingResolver
	Ledger   artifact.RetentionLedger
}

// ClaimOwnerKind is the ClaimOwner.Kind of Session commits (EXT-WRT-5).
const ClaimOwnerKind = "twilight/session/commit"

// CommitObserver sees every commit a Writer applies, in commit order, after
// it is durable (EXT-WRT-7). It is the one source every observation of a
// Session derives from: Loop events, turn lifecycle and chatlog entries are
// all events of applied commits. Observers run outside the Writer's critical
// section and are best effort: a panic is contained and never reaches the
// committer.
type CommitObserver interface {
	Committed(ctx context.Context, sid session.SessionID, commit session.Commit)
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

// DeriveClaimID is EXT-WRT-5.
func DeriveClaimID(protocolVersion uint16, sid session.SessionID, commitID session.CommitID, refSet artifact.RefSetDigest) artifact.ClaimID {
	raw, _ := es.EncodeTypedPayload(session.ProtocolVersion1, "twilight/session-extension/claim", []string{"1", fmt.Sprintf("%d", protocolVersion), string(sid), string(commitID), string(refSet)})
	return artifact.ClaimID(es.DigestBytes(raw))
}

func nextClaimID(released artifact.ClaimID) artifact.ClaimID {
	raw, _ := es.EncodeTypedPayload(session.ProtocolVersion1, "twilight/session-extension/claim-successor", []string{"1", string(released)})
	return artifact.ClaimID(es.DigestBytes(raw))
}

// CommitOwner is the ClaimOwner of a Session commit.
func CommitOwner(sid session.SessionID, id session.CommitID) artifact.ClaimOwner {
	return artifact.ClaimOwner{Kind: ClaimOwnerKind, Authority: string(sid), Identity: string(id)}
}

// projectionKey names one projection version in the Writer's own maps.
type projectionKey struct {
	id      extension.ProjectionID
	version extension.ProjectionVersion
}

type sessionWriter struct {
	mu        sync.Mutex
	kernel    session.Handle
	registry  *extension.Registry
	admission Admission
	sid       session.SessionID
	// fork is the Session's anchor when it is a fork; its parent's cache
	// entries may seed a projection (EXT-PRJ-3).
	fork   *session.ForkPoint
	head   session.Head
	states map[projectionKey]any
	scopes map[projectionKey]*extension.ProjectionScope
	// cache, cachePolicy and cached carry EXT-PRJ-3: cached records the head each
	// projection's cache entry already reflects, which is what a policy measures
	// the next refresh against.
	cache       extension.ProjectionCache
	cachePolicy extension.CachePolicy
	cached      map[projectionKey]session.Head
	lost        error
	// observers are notified after each applied commit; notifyMu keeps the
	// notifications in commit order without holding mu (EXT-WRT-7).
	observers []CommitObserver
	notifyMu  sync.Mutex
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
	policy := cfg.CachePolicy
	if policy == nil {
		policy = extension.CacheEvery(extension.DefaultCacheEvery)
	}
	w := &sessionWriter{kernel: kernel, registry: registry, admission: admission, sid: sid, fork: header.ParentFork,
		states: make(map[projectionKey]any), scopes: make(map[projectionKey]*extension.ProjectionScope),
		cache: cfg.Cache, cachePolicy: policy, cached: make(map[projectionKey]session.Head), observers: cfg.Observers}
	if err := w.rebuild(ctx, store); err != nil {
		_ = kernel.Close(ctx)
		return nil, err
	}
	if admission.Ledger != nil {
		if _, err := artifact.Reconcile(ctx, admission.Ledger, artifact.ClaimOwnerScope{Kind: ClaimOwnerKind, Authority: string(sid)}, w); err != nil {
			_ = kernel.Close(ctx)
			return nil, err
		}
	}
	return w, nil
}

// rebuild restores the idempotency index and every registered projection
// (EXT-WRT-1). The kernel keeps the CommitID index Append needs; projections
// fold from the whole log, or from a cache entry that ends on a commit
// boundary of this log plus the commits after it, which is what keeps a long
// session from refolding quadratically (EXT-PRJ-3).
func (w *sessionWriter) rebuild(ctx context.Context, store session.Store) error {
	page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: w.sid})
	if err != nil {
		return err
	}
	for _, def := range w.registry.Projections() {
		k := projectionKey{def.ID, def.Version}
		scope, err := w.registry.ScopeFor(def.ID, def.Version)
		if err != nil {
			return err
		}
		w.scopes[k] = scope
		state, through, ok := w.startState(ctx, scope, page.Commits)
		if !ok {
			if state, err = scope.Def.Initial(); err != nil {
				return err
			}
		}
		from := session.CommitSeq(0)
		if ok {
			w.cached[k] = through
			from = through.Next
		}
		if from < session.CommitSeq(len(page.Commits)) {
			if state, err = w.registry.Fold(scope, state, page.Commits[from:]); err != nil {
				return err
			}
		}
		w.states[k] = state
	}
	w.head = page.Head
	return nil
}

// startState returns the state this projection should begin folding from: the
// cached one when its entry covers a commit boundary of this log and still
// decodes, otherwise nothing. Anything unusable -- absent, corrupt, ahead of
// the log, or recorded at a digest the log does not have -- falls back to a
// full fold, so a stale or damaged cache only costs time (EXT-PRJ-3). It is
// the Writer's counterpart of the store reader's startState. A fork with no
// entry of its own may start from its parent's entry when that entry ends
// inside the inherited prefix: the prefix is the same commits under the same
// digests, so the alignment predicate judges it exactly as it would the
// fork's own entry.
func (w *sessionWriter) startState(ctx context.Context, scope *extension.ProjectionScope, commits []session.Commit) (any, session.Head, bool) {
	if w.cache == nil {
		return nil, session.Head{}, false
	}
	if state, through, ok := w.cachedState(ctx, w.sid, scope, commits); ok {
		return state, through, true
	}
	if w.fork != nil {
		if state, through, ok := w.cachedState(ctx, w.fork.ParentSessionID, scope, commits); ok && through.Next <= w.fork.Seq+1 {
			return state, through, true
		}
	}
	return nil, session.Head{}, false
}

func (w *sessionWriter) cachedState(ctx context.Context, sid session.SessionID, scope *extension.ProjectionScope, commits []session.Commit) (any, session.Head, bool) {
	encoded, through, ok, err := w.cache.Load(ctx, sid, scope.Def.ID, scope.Def.Version)
	if err != nil || !ok || !coversCommit(commits, through) {
		return nil, session.Head{}, false
	}
	state, err := scope.Def.StateCodec.Decode(encoded)
	if err != nil {
		return nil, session.Head{}, false
	}
	return state, through, true
}

// coversCommit reports whether through names the commit before its Next -- a
// commit boundary of this log -- with the digest the entry recorded.
func coversCommit(commits []session.Commit, through session.Head) bool {
	if through.Next == 0 || through.Next > session.CommitSeq(len(commits)) {
		return false
	}
	return extension.SealedAt(commits[through.Next-1], through)
}

// cacheWrite is one projection entry the policy asked to refresh: the state
// and the head it covers, captured under the lock and written outside it.
type cacheWrite struct {
	key   projectionKey
	state any
	head  session.Head
}

// planRefresh selects the entries the deployment's policy wants refreshed and
// records them as covering the current head. The caller holds the lock. The
// writes themselves happen outside it (saveRefresh): a cache entry is derived
// data with no ordering constraint against later commits (EXT-PRJ-7), and the
// captured states are immutable (EXT-PRJ-1), so nothing in the critical
// section depends on the IO.
func (w *sessionWriter) planRefresh(closing bool) []cacheWrite {
	if w.cache == nil {
		return nil
	}
	var writes []cacheWrite
	for k := range w.scopes {
		if !w.cachePolicy(k.id, k.version, w.head, w.cached[k], closing) {
			continue
		}
		writes = append(writes, cacheWrite{key: k, state: w.states[k], head: w.head})
		w.cached[k] = w.head
	}
	return writes
}

// saveRefresh performs planned writes. Best effort and never fatal: the cache
// is derived data, so a failed Save only means a later Writer folds more
// (EXT-PRJ-3); the policy then asks again at its next threshold.
func (w *sessionWriter) saveRefresh(ctx context.Context, writes []cacheWrite) {
	for _, cw := range writes {
		_ = extension.SaveProjection(ctx, w.cache, w.registry, w.sid, cw.key.id, cw.key.version, cw.state, cw.head)
	}
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
		writes = w.planRefresh(true)
	}
	w.lost = errWriterClosed
	err := w.kernel.Close(ctx)
	w.mu.Unlock()
	w.saveRefresh(ctx, writes)
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
func (v view) Projection(id extension.ProjectionID, ver extension.ProjectionVersion) (any, error) {
	k := projectionKey{id, ver}
	state, ok := v.w.states[k]
	if !ok {
		return nil, &extension.Error{Code: extension.ErrInvalid, Detail: fmt.Sprintf("unknown projection %q v%d", id, ver)}
	}
	codec := v.w.scopes[k].Def.StateCodec
	encoded, err := codec.Encode(state)
	if err != nil {
		return nil, err
	}
	return codec.Decode(encoded)
}

type memoryReader struct{ w *sessionWriter }

func (w *sessionWriter) Projections() extension.ProjectionReader { return memoryReader{w} }

func (r memoryReader) Load(_ context.Context, sid session.SessionID, id extension.ProjectionID, v extension.ProjectionVersion) (any, session.Head, error) {
	if sid != r.w.sid {
		return nil, session.Head{}, &extension.Error{Code: extension.ErrInvalid, Detail: "writer projections are session-local"}
	}
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	state, err := view{r.w}.Projection(id, v)
	return state, r.w.head, err
}

// --- commit --------------------------------------------------------------------------

func (w *sessionWriter) Commit(ctx context.Context, fn CommitFn) (CommitResult, error) {
	if fn == nil {
		return CommitResult{}, errors.New("writer: nil fn")
	}
	w.mu.Lock()
	// The critical section is read → decide → validate → append (EXT-WRT-1).
	// Cache writes and observer notifications of an applied commit run after
	// the unlock: they are derived work and must not lengthen the section
	// (EXT-PRJ-7, EXT-WRT-7). notifyMu is taken before mu is released, so
	// notifications keep the commit order while a later Commit already runs.
	var writes []cacheWrite
	var applied *session.Commit
	defer func() {
		if applied != nil && len(w.observers) > 0 {
			w.notifyMu.Lock()
			w.mu.Unlock()
			w.saveRefresh(ctx, writes)
			w.notify(ctx, *applied)
			w.notifyMu.Unlock()
			return
		}
		w.mu.Unlock()
		w.saveRefresh(ctx, writes)
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
	batches, refs, invalid, err := w.encode(ctx, group)
	if err != nil {
		return CommitResult{}, err
	}
	if invalid != "" {
		return CommitResult{Outcome: CommitInvalid, Detail: invalid}, nil
	}
	if err := session.ValidateBatches(batches); err != nil {
		return CommitResult{Outcome: CommitInvalid, Detail: err.Error()}, nil
	}
	fp, err := fingerprintCommit(group.CommitID, batches)
	if err != nil {
		return CommitResult{}, err
	}
	if existing, committed, err := w.kernel.LookupCommit(group.CommitID); err != nil {
		return CommitResult{}, err
	} else if committed {
		// The kernel holds the commit, not a fingerprint, so a replay recomputes
		// the old commit's fingerprint to tell a replay from a conflict
		// (EXT-WRT-2). Only a hit pays for this.
		old, err := fingerprintCommit(existing.CommitID, existing.Batches)
		if err != nil {
			return CommitResult{}, err
		}
		if old == fp {
			return CommitResult{Outcome: CommitAlreadyApplied, Commit: existing}, nil
		}
		return CommitResult{Outcome: CommitConflict}, nil
	}
	// Projections must accept the commit before anything is persisted. The
	// provisional commit is what a reader folds too: the kernel assigns
	// PrevDigest and Digest inside Append, and nothing a projection may read
	// differs between the two paths.
	provisional := session.Commit{Seq: w.head.Next, CommitID: group.CommitID, Epoch: w.kernel.Epoch(), Batches: batches}
	next := make(map[projectionKey]any, len(w.states))
	for k, scope := range w.scopes {
		state, err := w.registry.Fold(scope, w.states[k], []session.Commit{provisional})
		if err != nil {
			return CommitResult{Outcome: CommitInvalid, Detail: err.Error()}, nil
		}
		next[k] = state
	}
	var claim *artifact.RetentionClaim
	if len(refs) > 0 {
		claim, invalid, err = w.claim(ctx, group.CommitID, refs)
		if err != nil {
			return CommitResult{}, err
		}
		if invalid != "" {
			return CommitResult{Outcome: CommitInvalid, Detail: invalid}, nil
		}
	}
	sealed, err := w.kernel.Append(ctx, session.Proposal{CommitID: group.CommitID, Batches: batches})
	if err != nil {
		if claim != nil && appendOutcomeKnown(err) {
			_ = w.admission.Ledger.ReleaseActive(ctx, claim.ID) // best effort; OpenWriter reconciles any orphan
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
	for k, s := range next {
		w.states[k] = s
	}
	w.head = w.kernel.Head()
	writes = w.planRefresh(false)
	applied = &sealed
	return CommitResult{Outcome: CommitApplied, Commit: sealed, Claim: claim}, nil
}

// notify hands an applied commit to every observer. A panicking observer is
// contained: observation is derived work and never fails a Commit.
func (w *sessionWriter) notify(ctx context.Context, commit session.Commit) {
	for _, o := range w.observers {
		func() {
			defer func() { _ = recover() }()
			o.Committed(ctx, w.sid, commit)
		}()
	}
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

// encode validates and encodes every event, extracts and admits bindings, and
// returns the proposal batches.
func (w *sessionWriter) encode(ctx context.Context, group *SemanticGroup) ([]session.StreamBatch, []artifact.BindingID, string, error) {
	batches := make([]session.StreamBatch, len(group.Batches))
	var refs []artifact.BindingID
	for bi, tb := range group.Batches {
		events := make([]session.Event, len(tb.Events))
		for i, te := range tb.Events {
			where := fmt.Sprintf("batch %d event %d", bi, i)
			_, def, ok := w.registry.LookupEvent(te.Type)
			if !ok {
				return nil, nil, fmt.Sprintf("%s: unknown type %s", where, te.Type), nil
			}
			payload, _, err := w.registry.Encode(te.Type, te.Value)
			if err != nil {
				return nil, nil, fmt.Sprintf("%s: %v", where, err), nil
			}
			if verdict := checkStreamAffinity(tb.Stream, def.Stream, payload); verdict != "" {
				return nil, nil, fmt.Sprintf("%s: %s", where, verdict), nil
			}
			for _, decl := range def.Bindings {
				ids, err := decl.Extractor.BindingIDs(te.Value)
				if err != nil {
					return nil, nil, fmt.Sprintf("%s: binding extraction: %v", where, err), nil
				}
				if uint32(len(ids)) < decl.Cardinality.Min || (decl.Cardinality.Max != nil && uint32(len(ids)) > *decl.Cardinality.Max) {
					return nil, nil, fmt.Sprintf("%s: binding cardinality violated", where), nil
				}
				for _, id := range ids {
					if invalid, err := w.admit(ctx, id, &decl); err != nil {
						return nil, nil, "", err
					} else if invalid != "" {
						return nil, nil, fmt.Sprintf("%s: %s", where, invalid), nil
					}
				}
				refs = append(refs, ids...)
			}
			events[i] = session.Event{Type: te.Type, RecordedAtUnixMilli: te.RecordedAtUnixMilli, Payload: payload}
		}
		batches[bi] = session.StreamBatch{Stream: tb.Stream, Events: events}
	}
	return batches, refs, "", nil
}

// checkStreamAffinity verifies a batch's stream attribution against the
// event type's declared StreamPolicy. It returns a human verdict for the
// commit's detail string; policies themselves are validated at BuildRegistry.
func checkStreamAffinity(stream session.StreamRef, pol extension.StreamPolicy, payload jsonstable.Value) string {
	switch pol.Kind {
	case session.StreamKindSession:
		if stream.Kind != session.StreamKindSession {
			return fmt.Sprintf("event is session-scoped but the batch is %s", stream.Kind)
		}
	case session.StreamKindRun:
		if stream.Kind != session.StreamKindRun {
			return fmt.Sprintf("event is run-scoped but the batch is %s", stream.Kind)
		}
		decoded, err := payload.Any()
		if err != nil {
			return fmt.Sprintf("payload is not decodable for the stream binding: %v", err)
		}
		fields, ok := decoded.(map[string]any)
		if !ok {
			return "payload is not an object"
		}
		id, ok := fields[pol.IDField].(string)
		if !ok || id == "" {
			return fmt.Sprintf("payload lacks the stream binding field %q", pol.IDField)
		}
		if id != stream.ID {
			return fmt.Sprintf("payload %s %q does not match the batch stream %q", pol.IDField, id, stream.ID)
		}
	default:
		return "event type declares no stream policy"
	}
	return ""
}

func (w *sessionWriter) admit(ctx context.Context, id artifact.BindingID, decl *extension.BindingReferenceDefinition) (string, error) {
	if w.admission.Bindings == nil {
		// A configuration error, not a verdict on the commit: returning it as an
		// error keeps it from reading like a data rejection.
		return "", errors.New("writer: the event references artifacts but no binding resolver is configured")
	}
	binding, err := w.admission.Bindings.ResolveBinding(ctx, id)
	if err != nil {
		var aerr *artifact.Error
		if errors.As(err, &aerr) {
			return fmt.Sprintf("binding %s: %v", id, aerr), nil
		}
		return "", err
	}
	if len(decl.AllowedSchemes) > 0 {
		allowed := false
		for _, s := range decl.AllowedSchemes {
			if s == binding.Ref.Scheme {
				allowed = true
			}
		}
		if !allowed {
			return fmt.Sprintf("binding %s: scheme %s not allowed", id, binding.Ref.Scheme), nil
		}
	}
	if binding.Ref.Durability.Rank() < decl.RequiredDurability.Rank() {
		return fmt.Sprintf("binding %s: durability %s below required %s", id, binding.Ref.Durability, decl.RequiredDurability), nil
	}
	return "", nil
}

// claim activates the retention claim before Append (EXT-WRT-3).
func (w *sessionWriter) claim(ctx context.Context, commitID session.CommitID, refs []artifact.BindingID) (*artifact.RetentionClaim, string, error) {
	if w.admission.Ledger == nil {
		// See admit: a missing ledger is a configuration error.
		return nil, "", errors.New("writer: the commit references artifacts but no retention ledger is configured")
	}
	set, err := artifact.SetBuilder{Resolver: w.admission.Bindings}.Build(ctx, refs)
	if err != nil {
		var aerr *artifact.Error
		if errors.As(err, &aerr) {
			return nil, "binding set: " + aerr.Error(), nil
		}
		return nil, "", err
	}
	id := DeriveClaimID(w.registry.ProtocolVersion, w.sid, commitID, set.RefSetDigest)
	owner := CommitOwner(w.sid, commitID)
	for {
		existing, ok, err := w.admission.Ledger.LookupClaim(ctx, id)
		if err != nil {
			return nil, "", err
		}
		if !ok {
			break
		}
		if existing.ID != id || existing.Owner != owner || existing.BindingSet.RefSetDigest != set.RefSetDigest || !slices.Equal(existing.BindingSet.BindingIDs, set.BindingIDs) {
			return nil, fmt.Sprintf("claim %s: owner or binding set conflicts", id), nil
		}
		if existing.State != artifact.ClaimReleased {
			break
		}
		// Released claims remain terminal; a replay acquires a new retention root.
		id = nextClaimID(id)
	}
	claim, err := w.admission.Ledger.Activate(ctx, id, owner, set)
	if err != nil {
		var aerr *artifact.Error
		if errors.As(err, &aerr) {
			return nil, "claim: " + aerr.Error(), nil
		}
		return nil, "", err
	}
	return &claim, "", nil
}

type fingerprintEvent struct {
	Type    session.EventType `json:"type"`
	Payload string            `json:"payload"`
}

type fingerprintBatch struct {
	Stream session.StreamRef  `json:"stream"`
	Events []fingerprintEvent `json:"events"`
}

// fingerprintCommit covers what makes a retry "the same commit": CommitID,
// stream attribution, Types and payloads, never timestamps or Seq (a retry
// after reopen lands at the head the log actually has) (EXT-WRT-2). The
// SessionID is not covered: the CommitID index is already per Session, and a
// fork's inherited commits were sealed under an ancestor's SessionID yet must
// answer a replay through the fork as already applied (SES-FRK-3).
func fingerprintCommit(commitID session.CommitID, batches []session.StreamBatch) (es.Digest, error) {
	body := struct {
		CommitID session.CommitID   `json:"commitId"`
		Batches  []fingerprintBatch `json:"batches"`
	}{CommitID: commitID, Batches: make([]fingerprintBatch, len(batches))}
	for i, b := range batches {
		fb := fingerprintBatch{Stream: b.Stream, Events: make([]fingerprintEvent, len(b.Events))}
		for j, e := range b.Events {
			fb.Events[j] = fingerprintEvent{e.Type, e.Payload.String()}
		}
		body.Batches[i] = fb
	}
	raw, err := es.EncodeTypedPayload(session.ProtocolVersion1, "twilight/session-extension/fingerprint", body)
	if err != nil {
		return "", err
	}
	return es.DigestBytes(raw), nil
}

// --- writers -------------------------------------------------------------------------

type writerSet struct {
	store     session.Store
	registry  *extension.Registry
	admission Admission
	opts      session.OpenOptions
	cfg       WritersConfig
	mu        sync.Mutex
	open      map[session.SessionID]Writer
}

// NewWriters returns a Writers that opens each Session once and hands out the
// same Writer afterwards (EXT-WRT-6).
func NewWriters(store session.Store, registry *extension.Registry, admission Admission, opts session.OpenOptions, cfg WritersConfig) Writers {
	return &writerSet{store: store, registry: registry, admission: admission, opts: opts, cfg: cfg, open: make(map[session.SessionID]Writer)}
}

func (ws *writerSet) Writer(ctx context.Context, sid session.SessionID) (Writer, error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if w, ok := ws.open[sid]; ok {
		lost := failure(w)
		if lost == nil {
			return w, nil
		}
		// Ownership loss is not confined to this instance: reopening would take
		// the Session back from the process that owns it now, and that is the
		// host's decision (CloseWriter forgets the failed Writer first). Every
		// other failure is: a fresh Writer rebuilds from the log and a replay
		// of the same CommitID is answered by the kernel's index (EXT-WRT-4).
		if errors.Is(lost, &extension.Error{Code: extension.ErrOwnershipLost}) {
			return nil, lost
		}
		if lost != errWriterClosed {
			_ = w.Close(ctx)
		}
		delete(ws.open, sid)
	}
	w, err := openWriter(ctx, ws.store, ws.registry, ws.admission, sid, ws.opts, ws.cfg)
	if err != nil {
		return nil, err
	}
	ws.open[sid] = w
	return w, nil
}

// failure returns the error a failed Writer keeps returning, or nil.
func failure(w Writer) error {
	lw, ok := w.(*sessionWriter)
	if !ok {
		return nil
	}
	lw.mu.Lock()
	defer lw.mu.Unlock()
	return lw.lost
}

// CloseWriter closes and forgets the Writer of one Session, so the next
// Writer(sid) reopens it from the log; a Session without an open Writer is a
// no-op.
func (ws *writerSet) CloseWriter(ctx context.Context, sid session.SessionID) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	w, ok := ws.open[sid]
	if !ok {
		return nil
	}
	delete(ws.open, sid)
	if failure(w) == errWriterClosed {
		return nil
	}
	return w.Close(ctx)
}

// Close closes every open Writer and forgets it; a later Writer(sid) reopens.
func (ws *writerSet) Close(ctx context.Context) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	var first error
	for sid, w := range ws.open {
		delete(ws.open, sid)
		if failure(w) == errWriterClosed {
			continue
		}
		if err := w.Close(ctx); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// CloseWriters closes and forgets every Writer of a NewWriters value.
func CloseWriters(ctx context.Context, ws Writers) error {
	if c, ok := ws.(interface{ Close(context.Context) error }); ok {
		return c.Close(ctx)
	}
	return nil
}

// CloseWriter closes and forgets one Session's Writer of a NewWriters value
// (EXT-WRT-6). Another Writers implementation has its Writer closed in place.
func CloseWriter(ctx context.Context, ws Writers, sid session.SessionID) error {
	if c, ok := ws.(interface {
		CloseWriter(context.Context, session.SessionID) error
	}); ok {
		return c.CloseWriter(ctx, sid)
	}
	w, err := ws.Writer(ctx, sid)
	if err != nil {
		return err
	}
	return w.Close(ctx)
}
