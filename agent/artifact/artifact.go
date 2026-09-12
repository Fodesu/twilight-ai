// Package artifact is the Artifact Core (docs/design/agent-artifact.md):
// Ref, Binding and the two-state RetentionLedger. The ledger
// persists itself; claims are activated before the owner fact is appended and
// orphans are released by the pre-collection reconciliation (ART-RET-3).
package artifact

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/felinics/twilight/agent/es"
)

type (
	WireVersion   uint16
	Scheme        string
	Authority     string
	Key           string
	BindingID     string
	BindingDigest string
	ClaimID       string
	RefSetDigest  string
)

const WireVersion1 WireVersion = 1

type Durability string

const (
	Ephemeral  Durability = "ephemeral"
	EventBound Durability = "event_bound"
	Pinned     Durability = "pinned"
)

// Rank orders durabilities: Ephemeral < EventBound < Pinned.
func (d Durability) Rank() int {
	switch d {
	case Ephemeral:
		return 0
	case EventBound:
		return 1
	case Pinned:
		return 2
	default:
		return -1
	}
}

type Integrity struct {
	Algorithm string `json:"algorithm"`
	Value     string `json:"value"`
}

// Ref locates and verifies immutable content (ART-REF-1).
type Ref struct {
	Scheme             Scheme     `json:"scheme"`
	Authority          Authority  `json:"authority"`
	Key                Key        `json:"key"`
	MediaType          string     `json:"mediaType,omitempty"`
	SizeBytes          *uint64    `json:"sizeBytes,omitempty"`
	Integrity          *Integrity `json:"integrity,omitempty"`
	Durability         Durability `json:"durability"`
	ExpiresAtUnixMilli *int64     `json:"expiresAtUnixMilli,omitempty"`
}

// Validate applies ART-ID-1 and ART-REF-2.
func (r Ref) Validate() error {
	if r.Scheme == "" || r.Authority == "" || r.Key == "" {
		return &Error{Code: ErrInvalid, Operation: "ref", Detail: "empty locator component"}
	}
	if r.Durability.Rank() < 0 {
		return &Error{Code: ErrInvalid, Operation: "ref", Detail: "unknown durability"}
	}
	if r.Scheme == "cas" && r.Integrity == nil {
		return &Error{Code: ErrInvalid, Operation: "ref", Detail: "cas ref requires integrity"}
	}
	if r.ExpiresAtUnixMilli != nil && r.Durability != Ephemeral {
		return &Error{Code: ErrInvalid, Operation: "ref", Detail: "only ephemeral refs may expire"}
	}
	return nil
}

// Identity is the versioned canonical wire identity of the complete Ref.
func (r Ref) Identity() (string, error) {
	if err := r.Validate(); err != nil {
		return "", err
	}
	raw, err := es.EncodeTypedPayload(uint16(WireVersion1), "twilight/artifact/ref", r)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// Binding maps a stable BindingID to an immutable Ref (ART-BND-1).
type Binding struct {
	ID     BindingID     `json:"id"`
	Ref    Ref           `json:"ref"`
	Digest BindingDigest `json:"digest"`
}

// DigestBinding covers the domain, BindingID and full RefWireIdentity.
func DigestBinding(id BindingID, ref Ref) (BindingDigest, error) {
	identity, err := ref.Identity()
	if err != nil {
		return "", err
	}
	raw, err := es.EncodeTypedPayload(uint16(WireVersion1), "twilight/artifact/binding", struct {
		ID       BindingID `json:"id"`
		Identity string    `json:"identity"`
	}{id, identity})
	if err != nil {
		return "", err
	}
	return BindingDigest(es.DigestBytes(raw)), nil
}

// NewBinding builds a Binding with its digest.
func NewBinding(id BindingID, ref Ref) (Binding, error) {
	if id == "" {
		return Binding{}, &Error{Code: ErrInvalid, Operation: "binding", Detail: "empty BindingID"}
	}
	d, err := DigestBinding(id, ref)
	if err != nil {
		return Binding{}, err
	}
	return Binding{ID: id, Ref: ref, Digest: d}, nil
}

type BindingResolver interface {
	ResolveBinding(context.Context, BindingID) (Binding, error)
}

type BindingStore interface {
	BindingResolver
	CreateBinding(context.Context, Binding) (Binding, error)
	LookupBinding(context.Context, BindingID) (Binding, bool, error)
}

// MemoryBindingStore is the in-process BindingStore.
type MemoryBindingStore struct {
	mu       sync.RWMutex
	bindings map[BindingID]Binding
}

func NewMemoryBindingStore() *MemoryBindingStore {
	return &MemoryBindingStore{bindings: make(map[BindingID]Binding)}
}

func (m *MemoryBindingStore) CreateBinding(ctx context.Context, b Binding) (Binding, error) {
	if err := ctx.Err(); err != nil {
		return Binding{}, err
	}
	want, err := DigestBinding(b.ID, b.Ref)
	if err != nil {
		return Binding{}, err
	}
	if b.Digest != want {
		return Binding{}, &Error{Code: ErrInvalid, Operation: "create_binding", Identity: string(b.ID), Detail: "binding digest mismatch"}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.bindings[b.ID]; ok {
		if existing.Digest != b.Digest {
			return Binding{}, &Error{Code: ErrConflict, Operation: "create_binding", Identity: string(b.ID)}
		}
		return existing, nil
	}
	m.bindings[b.ID] = b
	return b, nil
}

func (m *MemoryBindingStore) LookupBinding(ctx context.Context, id BindingID) (Binding, bool, error) {
	if err := ctx.Err(); err != nil {
		return Binding{}, false, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.bindings[id]
	return b, ok, nil
}

func (m *MemoryBindingStore) ResolveBinding(ctx context.Context, id BindingID) (Binding, error) {
	b, ok, err := m.LookupBinding(ctx, id)
	if err != nil {
		return Binding{}, err
	}
	if !ok {
		return Binding{}, &Error{Code: ErrNotFound, Operation: "resolve_binding", Identity: string(id)}
	}
	return b, nil
}

// --- retention ledger --------------------------------------------------------

type ClaimOwner struct {
	Kind      string `json:"kind"`
	Authority string `json:"authority"`
	Identity  string `json:"identity"`
}

// ClaimOwnerScope selects every owner of one Kind under one Authority.
type ClaimOwnerScope struct {
	Kind      string
	Authority string
}

type ClaimState string

const (
	ClaimActive   ClaimState = "active"
	ClaimReleased ClaimState = "released"
)

// BindingSet is a canonical, resolved retention set (ART-RET-1).
type BindingSet struct {
	BindingIDs   []BindingID  `json:"bindingIds"`
	RefSetDigest RefSetDigest `json:"refSetDigest"`
}

type RetentionClaim struct {
	ID         ClaimID    `json:"id"`
	Owner      ClaimOwner `json:"owner"`
	BindingSet BindingSet `json:"bindingSet"`
	State      ClaimState `json:"state"`
}

// BindingSetBuilder is the only way to construct a BindingSet.
type BindingSetBuilder interface {
	Build(context.Context, []BindingID) (BindingSet, error)
}

// SetBuilder resolves every binding and digests the sorted (ID, Digest) pairs.
type SetBuilder struct{ Resolver BindingResolver }

func (b SetBuilder) Build(ctx context.Context, ids []BindingID) (BindingSet, error) {
	if b.Resolver == nil {
		return BindingSet{}, errors.New("artifact: set builder: nil resolver")
	}
	sorted := SortedUniqueBindingIDs(ids)
	type pair struct {
		ID     BindingID     `json:"id"`
		Digest BindingDigest `json:"digest"`
	}
	pairs := make([]pair, 0, len(sorted))
	for _, id := range sorted {
		binding, err := b.Resolver.ResolveBinding(ctx, id)
		if err != nil {
			return BindingSet{}, err
		}
		if binding.Ref.Durability.Rank() < EventBound.Rank() {
			return BindingSet{}, &Error{Code: ErrInvalid, Operation: "build_set", Identity: string(id), Detail: "ephemeral binding cannot be claimed"}
		}
		pairs = append(pairs, pair{id, binding.Digest})
	}
	raw, err := es.EncodeTypedPayload(uint16(WireVersion1), "twilight/artifact/ref-set", pairs)
	if err != nil {
		return BindingSet{}, err
	}
	return BindingSet{BindingIDs: sorted, RefSetDigest: RefSetDigest(es.DigestBytes(raw))}, nil
}

func SortedUniqueBindingIDs(ids []BindingID) []BindingID {
	out := append([]BindingID(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	n := 0
	for i := range out {
		if n == 0 || out[i] != out[n-1] {
			out[n] = out[i]
			n++
		}
	}
	if n == 0 {
		return nil
	}
	return out[:n]
}

// ClaimOwnerQuery selects claims by owner (ART-RET-3). Identities nil selects
// every owner of the Kind under the Authority; an explicit list selects those
// owners only, and an empty identity in it matches nothing. Limit is the page
// size, 0 meaning DefaultClaimPageSize.
type ClaimOwnerQuery struct {
	Kind       string
	Authority  string
	Identities []string
	Limit      int
}

// ClaimCursor pages ClaimsByOwner. Watermark is the highest ClaimID the
// enumeration covers, fixed on the first page so claims activated while paging
// are excluded; After is the last ClaimID returned. The zero cursor starts an
// enumeration.
type ClaimCursor struct {
	Watermark ClaimID
	After     ClaimID
}

// ClaimPage is one page of ClaimsByOwner; Next is nil once exhausted.
type ClaimPage struct {
	Items []RetentionClaim
	Next  *ClaimCursor
}

// DefaultClaimPageSize is the ClaimsByOwner page size when the query gives none.
const DefaultClaimPageSize = 256

// RetentionLedger keeps Active/Released claims (ART-RET-2). It persists
// itself; Activate returns only once the claim is durable.
type RetentionLedger interface {
	Activate(context.Context, ClaimID, ClaimOwner, BindingSet) (RetentionClaim, error)
	LookupClaim(context.Context, ClaimID) (RetentionClaim, bool, error)
	ReleaseActive(context.Context, ClaimID) error
	// ClaimsByOwner enumerates the matching claims of every state in stable
	// ClaimID order under a watermark cursor (ART-RET-3).
	ClaimsByOwner(context.Context, ClaimOwnerQuery, ClaimCursor) (ClaimPage, error)
}

// ActiveClaims drains ClaimsByOwner for the scope and keeps the Active claims,
// in ClaimID order.
func ActiveClaims(ctx context.Context, ledger RetentionLedger, scope ClaimOwnerScope) ([]RetentionClaim, error) {
	var out []RetentionClaim
	cursor := ClaimCursor{}
	for {
		page, err := ledger.ClaimsByOwner(ctx, ClaimOwnerQuery{Kind: scope.Kind, Authority: scope.Authority}, cursor)
		if err != nil {
			return nil, err
		}
		for _, c := range page.Items {
			if c.State == ClaimActive {
				out = append(out, c)
			}
		}
		if page.Next == nil {
			return out, nil
		}
		cursor = *page.Next
	}
}

// OwnerVerifier is supplied by the owner's host: does the owner fact exist?
type OwnerVerifier interface {
	OwnerExists(context.Context, ClaimOwner) (bool, error)
}

// Reconcile releases Active claims in scope whose owner no longer exists
// (ART-RET-3). The caller guarantees no owner write in scope is in flight.
func Reconcile(ctx context.Context, ledger RetentionLedger, scope ClaimOwnerScope, verifier OwnerVerifier) (int, error) {
	claims, err := ActiveClaims(ctx, ledger, scope)
	if err != nil {
		return 0, err
	}
	released := 0
	for _, c := range claims {
		exists, err := verifier.OwnerExists(ctx, c.Owner)
		if err != nil {
			return released, err
		}
		if exists {
			continue
		}
		if err := ledger.ReleaseActive(ctx, c.ID); err != nil {
			return released, err
		}
		released++
	}
	return released, nil
}

// MemoryLedger is the in-process RetentionLedger. Builder, when set, rebuilds
// and verifies every incoming set (ART-RET-1).
type MemoryLedger struct {
	Builder BindingSetBuilder
	mu      sync.Mutex
	claims  map[ClaimID]RetentionClaim
}

func NewMemoryLedger(builder BindingSetBuilder) *MemoryLedger {
	return &MemoryLedger{Builder: builder, claims: make(map[ClaimID]RetentionClaim)}
}

func (l *MemoryLedger) Activate(ctx context.Context, id ClaimID, owner ClaimOwner, set BindingSet) (RetentionClaim, error) {
	if err := ctx.Err(); err != nil {
		return RetentionClaim{}, err
	}
	if id == "" || owner.Kind == "" || owner.Identity == "" {
		return RetentionClaim{}, &Error{Code: ErrInvalid, Operation: "activate", Identity: string(id), Detail: "empty claim identity or owner"}
	}
	if len(set.BindingIDs) == 0 || set.RefSetDigest == "" {
		return RetentionClaim{}, &Error{Code: ErrInvalid, Operation: "activate", Identity: string(id), Detail: "empty binding set"}
	}
	if l.Builder != nil {
		rebuilt, err := l.Builder.Build(ctx, set.BindingIDs)
		if err != nil {
			return RetentionClaim{}, err
		}
		if !sameSet(rebuilt, set) {
			return RetentionClaim{}, &Error{Code: ErrInvalid, Operation: "activate", Identity: string(id), Detail: "binding set does not verify"}
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if existing, ok := l.claims[id]; ok {
		if existing.State != ClaimActive || existing.Owner != owner || !sameSet(existing.BindingSet, set) {
			return RetentionClaim{}, &Error{Code: ErrConflict, Operation: "activate", Identity: string(id)}
		}
		return existing, nil
	}
	claim := RetentionClaim{ID: id, Owner: owner, BindingSet: set, State: ClaimActive}
	l.claims[id] = claim
	return claim, nil
}

func (l *MemoryLedger) LookupClaim(ctx context.Context, id ClaimID) (RetentionClaim, bool, error) {
	if err := ctx.Err(); err != nil {
		return RetentionClaim{}, false, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	c, ok := l.claims[id]
	return c, ok, nil
}

func (l *MemoryLedger) ReleaseActive(ctx context.Context, id ClaimID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	c, ok := l.claims[id]
	if !ok {
		return &Error{Code: ErrNotFound, Operation: "release", Identity: string(id)}
	}
	c.State = ClaimReleased
	l.claims[id] = c
	return nil
}

func (l *MemoryLedger) ClaimsByOwner(ctx context.Context, q ClaimOwnerQuery, cursor ClaimCursor) (ClaimPage, error) {
	if err := ctx.Err(); err != nil {
		return ClaimPage{}, err
	}
	if q.Kind == "" || q.Authority == "" {
		return ClaimPage{}, &Error{Code: ErrInvalid, Operation: "claims_by_owner", Detail: "empty owner kind or authority"}
	}
	limit := q.Limit
	if limit <= 0 {
		limit = DefaultClaimPageSize
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	var matched []RetentionClaim
	for _, c := range l.claims {
		if c.Owner.Kind == q.Kind && c.Owner.Authority == q.Authority && q.matchesIdentity(c.Owner.Identity) {
			matched = append(matched, c)
		}
	}
	sort.Slice(matched, func(i, j int) bool { return matched[i].ID < matched[j].ID })
	return pageClaims(matched, cursor, limit), nil
}

// matchesIdentity is ART-RET-3: nil selects every identity, an explicit list
// selects its members, and an empty identity never matches.
func (q ClaimOwnerQuery) matchesIdentity(identity string) bool {
	if identity == "" {
		return false
	}
	if q.Identities == nil {
		return true
	}
	for _, want := range q.Identities {
		if want != "" && want == identity {
			return true
		}
	}
	return false
}

// pageClaims applies the watermark cursor to claims sorted by ClaimID: the
// first page fixes the watermark at the last ID present, later pages return
// IDs after the cursor and never past the watermark.
func pageClaims(sorted []RetentionClaim, cursor ClaimCursor, limit int) ClaimPage {
	if cursor.Watermark == "" {
		if len(sorted) == 0 {
			return ClaimPage{}
		}
		cursor.Watermark = sorted[len(sorted)-1].ID
	}
	var items []RetentionClaim
	for _, c := range sorted {
		if c.ID <= cursor.After || c.ID > cursor.Watermark {
			continue
		}
		if len(items) == limit {
			return ClaimPage{Items: items, Next: &ClaimCursor{Watermark: cursor.Watermark, After: items[len(items)-1].ID}}
		}
		items = append(items, c)
	}
	return ClaimPage{Items: items}
}

func sameSet(a, b BindingSet) bool {
	if a.RefSetDigest != b.RefSetDigest || len(a.BindingIDs) != len(b.BindingIDs) {
		return false
	}
	for i := range a.BindingIDs {
		if a.BindingIDs[i] != b.BindingIDs[i] {
			return false
		}
	}
	return true
}

// --- errors --------------------------------------------------------------------

type ErrorCode string

const (
	ErrInvalid      ErrorCode = "invalid"
	ErrNotFound     ErrorCode = "not_found"
	ErrConflict     ErrorCode = "conflict"
	ErrUnauthorized ErrorCode = "unauthorized"
	ErrExpired      ErrorCode = "expired"
	ErrCorrupt      ErrorCode = "corrupt"
	ErrUnsupported  ErrorCode = "unsupported"
	ErrUnavailable  ErrorCode = "unavailable"
)

type Error struct {
	Code      ErrorCode
	Operation string
	Identity  string
	Detail    string
}

func (e *Error) Error() string {
	s := fmt.Sprintf("artifact: %s: %s", e.Operation, e.Code)
	if e.Identity != "" {
		s += " " + e.Identity
	}
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	return s
}

func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code
}
