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

// TypedEvent is a module value plus row metadata. Ignorable comes from the
// EventDefinition, not from the caller.
type TypedEvent struct {
	Type                session.EventType
	RecordedAtUnixMilli int64
	SourceSeqs          []session.Seq
	Value               any
}

type SemanticGroup struct {
	CommitID session.CommitID
	Events   []TypedEvent
}

// View is what a CommitFn may read: head, idempotency index and projections
// folded to the current head (EXT-WRT-1).
type View interface {
	Head() session.Head
	Epoch() session.Epoch
	// Committed reports whether a commit is already in the stream. It is
	// answered from an index the kernel already keeps, without touching storage.
	Committed(session.CommitID) bool
	// LookupCommit returns the rows of a committed group. The rows come from
	// storage when the kernel handle does not hold them, so a caller that only
	// needs the answer uses Committed.
	LookupCommit(session.CommitID) ([]session.SessionEvent, bool, error)
	Projection(extension.ProjectionID, extension.ProjectionVersion) (any, error)
}

// CommitFn decides the group to write; nil means write nothing.
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
type CommitResult struct {
	Outcome CommitOutcome
	Events  []session.SessionEvent
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

// WritersConfig carries the deployment's projection cache. Both fields are
// optional: with no cache the Writer folds every registered projection from the
// beginning of the log and stores nothing, exactly as before.
type WritersConfig struct {
	// Cache holds folded projection states, so a reopening Writer starts from
	// one instead of refolding the whole log (EXT-PRJ-3).
	Cache extension.ProjectionCache
	// CachePolicy decides which projections the Writer refreshes and when; nil
	// means extension.CacheEvery(extension.DefaultCacheEvery). It never affects reading: an entry
	// the cache already holds is used whoever wrote it.
	CachePolicy extension.CachePolicy
}

// DeriveClaimID is EXT-WRT-5.
func DeriveClaimID(protocolVersion uint16, sid session.SessionID, commitID session.CommitID, refSet artifact.RefSetDigest) artifact.ClaimID {
	raw, _ := es.EncodeTypedPayload(session.ProtocolVersion1, "twilight/session-extension/claim", []string{"1", fmt.Sprintf("%d", protocolVersion), string(sid), string(commitID), string(refSet)})
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
	head      session.Head
	states    map[projectionKey]any
	scopes    map[projectionKey]*extension.ProjectionScope
	// cache, cachePolicy and cached carry EXT-PRJ-3: cached records the head each
	// projection's cache entry already reflects, which is what a policy measures
	// the next refresh against.
	cache       extension.ProjectionCache
	cachePolicy extension.CachePolicy
	cached      map[projectionKey]session.Head
	lost        error
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
	kernel, err := store.Open(ctx, sid, opts)
	if err != nil {
		return nil, err
	}
	policy := cfg.CachePolicy
	if policy == nil {
		policy = extension.CacheEvery(extension.DefaultCacheEvery)
	}
	w := &sessionWriter{kernel: kernel, registry: registry, admission: admission, sid: sid,
		states: make(map[projectionKey]any), scopes: make(map[projectionKey]*extension.ProjectionScope),
		cache: cfg.Cache, cachePolicy: policy, cached: make(map[projectionKey]session.Head)}
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
// (EXT-WRT-1). The index needs every group, so the log is always read in full;
// a projection whose cache entry ends on a group boundary of this log
// starts from that state and skips the groups it already covers, which is what
// keeps a long session from refolding quadratically (EXT-PRJ-3).
func (w *sessionWriter) rebuild(ctx context.Context, store session.Store) error {
	page, err := store.Read(ctx, session.ReadRequest{SessionID: w.sid})
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
		if state, through, ok := w.startState(ctx, scope, page.Events); ok {
			w.states[k] = state
			w.cached[k] = through
			continue
		}
		state, err := scope.Def.Initial()
		if err != nil {
			return err
		}
		w.states[k] = state
	}
	rows := page.Events
	for i := 0; i < len(rows); {
		end := i
		for end < len(rows) && !rows[end].Last {
			end++
		}
		if end >= len(rows) {
			return &extension.Error{Code: extension.ErrInvalid, Detail: "log ends in an incomplete group"}
		}
		group := rows[i : end+1]
		if err := w.foldGroup(group); err != nil {
			return err
		}
		i = end + 1
	}
	w.head = page.Head
	return nil
}

// startState returns the state this projection should begin folding from: the
// cached one when its entry covers a group boundary of this log and still
// decodes, otherwise Initial. Anything unusable -- absent, corrupt, ahead of the
// log, or recorded mid-group -- falls back to a full fold, so a stale or
// damaged cache only costs time (EXT-PRJ-3). It is the Writer's counterpart of
// storeReader.startState.
func (w *sessionWriter) startState(ctx context.Context, scope *extension.ProjectionScope, rows []session.SessionEvent) (any, session.Head, bool) {
	if w.cache == nil {
		return nil, session.Head{}, false
	}
	encoded, through, ok, err := w.cache.Load(ctx, w.sid, scope.Def.ID, scope.Def.Version)
	if err != nil || !ok || !coversGroupBoundary(rows, through) {
		return nil, session.Head{}, false
	}
	state, err := scope.Def.StateCodec.Decode(encoded)
	if err != nil {
		return nil, session.Head{}, false
	}
	return state, through, true
}

// coversGroupBoundary reports whether through names the row before a group
// boundary -- the last row of a complete group -- with the digest the entry
// recorded. An entry that stops inside a group must not be started from,
// because a group is applied atomically (EXT-PRJ-1).
func coversGroupBoundary(rows []session.SessionEvent, through session.Head) bool {
	if through.Next == 0 || through.Next > session.Seq(len(rows)) {
		return false
	}
	return extension.EndsGroupAt(&rows[through.Next-1], through)
}

// foldGroup folds one complete group into every projection that does not
// already cover it; no state is published if any projection rejects the group.
func (w *sessionWriter) foldGroup(group []session.SessionEvent) error {
	next := make(map[projectionKey]any, len(w.states))
	for k, scope := range w.scopes {
		if through, fromCache := w.cached[k]; fromCache && group[0].Seq < through.Next {
			continue // already covered by the entry the fold started from
		}
		state, err := w.registry.Fold(scope, w.states[k], group)
		if err != nil {
			return err
		}
		next[k] = state
	}
	for k, s := range next {
		w.states[k] = s
	}
	return nil
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
	return w.kernel.Committed(session.CommitID(owner.Identity)), nil
}

func (w *sessionWriter) Close(ctx context.Context) error {
	w.mu.Lock()
	var writes []cacheWrite
	if w.head.Next > 0 {
		writes = w.planRefresh(true)
	}
	w.lost = &extension.Error{Code: extension.ErrInvalid, Detail: "writer closed"}
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

func (v view) LookupCommit(id session.CommitID) ([]session.SessionEvent, bool, error) {
	return v.w.kernel.LookupCommit(id)
}
func (v view) Projection(id extension.ProjectionID, ver extension.ProjectionVersion) (any, error) {
	state, ok := v.w.states[projectionKey{id, ver}]
	if !ok {
		return nil, &extension.Error{Code: extension.ErrInvalid, Detail: fmt.Sprintf("unknown projection %q v%d", id, ver)}
	}
	return state, nil
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
	// Cache writes planned by an applied commit run after the unlock: they are
	// derived data and must not lengthen the section (EXT-PRJ-7).
	var writes []cacheWrite
	defer func() {
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
	if group.CommitID == "" || len(group.Events) == 0 {
		return CommitResult{Outcome: CommitInvalid, Detail: "empty CommitID or event group"}, nil
	}
	rows, uncommitted, refs, invalid, err := w.encode(ctx, group)
	if err != nil {
		return CommitResult{}, err
	}
	if invalid != "" {
		return CommitResult{Outcome: CommitInvalid, Detail: invalid}, nil
	}
	fp, err := fingerprintRows(w.sid, rows)
	if err != nil {
		return CommitResult{}, err
	}
	if existing, committed, err := w.kernel.LookupCommit(group.CommitID); err != nil {
		return CommitResult{}, err
	} else if committed {
		// The kernel holds the rows, not a fingerprint, so a replay recomputes
		// the old group's fingerprint to tell a replay from a conflict
		// (EXT-WRT-2). Only a hit pays for this.
		old, err := fingerprintRows(w.sid, existing)
		if err != nil {
			return CommitResult{}, err
		}
		if old == fp {
			return CommitResult{Outcome: CommitAlreadyApplied, Events: append([]session.SessionEvent(nil), existing...)}, nil
		}
		return CommitResult{Outcome: CommitConflict}, nil
	}
	// Projections must accept the group before anything is persisted; the
	// provisional rows carry every field Apply may read except Digest.
	next := make(map[projectionKey]any, len(w.states))
	for k, scope := range w.scopes {
		state, err := w.registry.Fold(scope, w.states[k], rows)
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
	sealed, err := w.kernel.Append(ctx, session.Group{CommitID: group.CommitID, Events: uncommitted})
	if err != nil {
		if claim != nil {
			_ = w.admission.Ledger.ReleaseActive(ctx, claim.ID) // best effort; an orphan is reconciled later
		}
		if session.IsCode(err, session.ErrOwnershipLost) {
			w.lost = &extension.Error{Code: extension.ErrOwnershipLost, Detail: err.Error()}
			return CommitResult{}, w.lost
		}
		if session.IsCode(err, session.ErrConflict) {
			return CommitResult{Outcome: CommitConflict, Detail: err.Error()}, nil
		}
		return CommitResult{}, err
	}
	for k, s := range next {
		w.states[k] = s
	}
	w.head = w.kernel.Head()
	writes = w.planRefresh(false)
	return CommitResult{Outcome: CommitApplied, Events: append([]session.SessionEvent(nil), sealed...), Claim: claim}, nil
}

// encode validates and encodes the group, extracts and admits bindings, and
// returns provisional rows (Seq assigned, Digest empty) plus the kernel input.
func (w *sessionWriter) encode(ctx context.Context, group *SemanticGroup) ([]session.SessionEvent, []session.UncommittedEvent, []artifact.BindingID, string, error) {
	rows := make([]session.SessionEvent, len(group.Events))
	uncommitted := make([]session.UncommittedEvent, len(group.Events))
	var refs []artifact.BindingID
	for i, te := range group.Events {
		_, def, ok := w.registry.LookupEvent(te.Type)
		if !ok {
			return nil, nil, nil, fmt.Sprintf("event %d: unknown type %s", i, te.Type), nil
		}
		payload, _, err := w.registry.Encode(te.Type, te.Value)
		if err != nil {
			return nil, nil, nil, fmt.Sprintf("event %d: %v", i, err), nil
		}
		for _, decl := range def.Bindings {
			ids, err := decl.Extractor.BindingIDs(te.Value)
			if err != nil {
				return nil, nil, nil, fmt.Sprintf("event %d: binding extraction: %v", i, err), nil
			}
			if uint32(len(ids)) < decl.Cardinality.Min || (decl.Cardinality.Max != nil && uint32(len(ids)) > *decl.Cardinality.Max) {
				return nil, nil, nil, fmt.Sprintf("event %d: binding cardinality violated", i), nil
			}
			for _, id := range ids {
				if invalid, err := w.admit(ctx, id, &decl); err != nil {
					return nil, nil, nil, "", err
				} else if invalid != "" {
					return nil, nil, nil, fmt.Sprintf("event %d: %s", i, invalid), nil
				}
			}
			refs = append(refs, ids...)
		}
		u := session.UncommittedEvent{Type: te.Type, RecordedAtUnixMilli: te.RecordedAtUnixMilli, SourceSeqs: append([]session.Seq(nil), te.SourceSeqs...), Ignorable: def.Ignorable, Payload: payload}
		if err := session.ValidateUncommitted(&u); err != nil {
			return nil, nil, nil, fmt.Sprintf("event %d: %v", i, err), nil
		}
		uncommitted[i] = u
		rows[i] = session.SessionEvent{Seq: w.head.Next + session.Seq(i), CommitID: group.CommitID, Index: uint16(i), Last: i == len(group.Events)-1,
			Type: u.Type, RecordedAtUnixMilli: u.RecordedAtUnixMilli, SourceSeqs: u.SourceSeqs, Ignorable: u.Ignorable, Payload: u.Payload}
	}
	return rows, uncommitted, refs, "", nil
}

func (w *sessionWriter) admit(ctx context.Context, id artifact.BindingID, decl *extension.BindingReferenceDefinition) (string, error) {
	if w.admission.Bindings == nil {
		// A configuration error, not a verdict on the group: returning it as an
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
		return nil, "", errors.New("writer: the group references artifacts but no retention ledger is configured")
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
	claim, err := w.admission.Ledger.Activate(ctx, id, CommitOwner(w.sid, commitID), set)
	if err != nil {
		var aerr *artifact.Error
		if errors.As(err, &aerr) {
			return nil, "claim: " + aerr.Error(), nil
		}
		return nil, "", err
	}
	return &claim, "", nil
}

type fingerprintRow struct {
	Type       session.EventType `json:"type"`
	SourceSeqs []session.Seq     `json:"sourceSeqs,omitempty"`
	Payload    string            `json:"payload"`
}

// fingerprintRows covers what makes a retry "the same group": CommitID,
// Types, SourceSeqs and payloads, never timestamps (EXT-WRT-2).
func fingerprintRows(sid session.SessionID, rows []session.SessionEvent) (es.Digest, error) {
	body := struct {
		SessionID session.SessionID `json:"sessionId"`
		CommitID  session.CommitID  `json:"commitId"`
		Rows      []fingerprintRow  `json:"rows"`
	}{SessionID: sid, CommitID: rows[0].CommitID, Rows: make([]fingerprintRow, len(rows))}
	for i, r := range rows {
		body.Rows[i] = fingerprintRow{r.Type, r.SourceSeqs, r.Payload.String()}
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
		if lw, ok := w.(*sessionWriter); ok && lw.lost != nil {
			return nil, lw.lost
		}
		return w, nil
	}
	w, err := openWriter(ctx, ws.store, ws.registry, ws.admission, sid, ws.opts, ws.cfg)
	if err != nil {
		return nil, err
	}
	ws.open[sid] = w
	return w, nil
}

// Close closes every open Writer and forgets it; a later Writer(sid) reopens.
func (ws *writerSet) Close(ctx context.Context) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	var first error
	for sid, w := range ws.open {
		if err := w.Close(ctx); err != nil && first == nil {
			first = err
		}
		delete(ws.open, sid)
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
