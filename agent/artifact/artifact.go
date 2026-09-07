// Package artifact is the Artifact Core (docs/design/agent-artifact.md): Ref,
// Binding and the two-state RetentionLedger. v1 keeps the Memory reference
// implementation only; claims are activated inside the host's transaction
// through ClaimKV.
package artifact

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/memohai/twilight/agent/es"
	"github.com/memohai/twilight/agent/jsonstable"
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

// ClaimKV is the host's same-transaction KV view. In Session deployments it
// adapts session.SessionTx's control-plane KV (namespace twilight/artifact/claim).
type ClaimKV interface {
	Get(key string) ([]byte, bool, error)
	Put(key string, value []byte) error
	Delete(key string) error
}

// RetentionLedger keeps Active/Released claims (ART-RET-2).
type RetentionLedger interface {
	ActivateIn(kv ClaimKV, id ClaimID, owner ClaimOwner, set BindingSet) (RetentionClaim, error)
	LookupClaim(context.Context, ClaimID) (RetentionClaim, bool, error)
	ReleaseActive(context.Context, ClaimID) error
}

// KVLedger stores claims as canonical JSON under the ClaimID key. Reads and
// releases outside a transaction go through the ClaimKVProvider the host
// supplies (for Session: a store-backed adapter).
type KVLedger struct {
	Builder BindingSetBuilder
	// Outside is the non-transactional KV view for LookupClaim/ReleaseActive.
	Outside func(context.Context) ClaimKV
}

func (l KVLedger) ActivateIn(kv ClaimKV, id ClaimID, owner ClaimOwner, set BindingSet) (RetentionClaim, error) {
	if id == "" || owner.Kind == "" || owner.Identity == "" {
		return RetentionClaim{}, &Error{Code: ErrInvalid, Operation: "activate", Identity: string(id), Detail: "empty claim identity or owner"}
	}
	if len(set.BindingIDs) == 0 || set.RefSetDigest == "" {
		return RetentionClaim{}, &Error{Code: ErrInvalid, Operation: "activate", Identity: string(id), Detail: "empty binding set"}
	}
	claim := RetentionClaim{ID: id, Owner: owner, BindingSet: set, State: ClaimActive}
	raw, ok, err := kv.Get(string(id))
	if err != nil {
		return RetentionClaim{}, err
	}
	if ok {
		var existing RetentionClaim
		if err := json.Unmarshal(raw, &existing); err != nil {
			return RetentionClaim{}, &Error{Code: ErrCorrupt, Operation: "activate", Identity: string(id), Detail: err.Error()}
		}
		if existing.State != ClaimActive || existing.Owner != owner || !sameSet(existing.BindingSet, set) {
			return RetentionClaim{}, &Error{Code: ErrConflict, Operation: "activate", Identity: string(id)}
		}
		return existing, nil
	}
	encoded, err := jsonstable.MarshalCanonical(claim)
	if err != nil {
		return RetentionClaim{}, err
	}
	if err := kv.Put(string(id), encoded); err != nil {
		return RetentionClaim{}, err
	}
	return claim, nil
}

func (l KVLedger) LookupClaim(ctx context.Context, id ClaimID) (RetentionClaim, bool, error) {
	if l.Outside == nil {
		return RetentionClaim{}, false, errors.New("artifact: ledger: no outside KV")
	}
	raw, ok, err := l.Outside(ctx).Get(string(id))
	if err != nil || !ok {
		return RetentionClaim{}, false, err
	}
	var claim RetentionClaim
	if err := json.Unmarshal(raw, &claim); err != nil {
		return RetentionClaim{}, false, &Error{Code: ErrCorrupt, Operation: "lookup_claim", Identity: string(id), Detail: err.Error()}
	}
	return claim, true, nil
}

func (l KVLedger) ReleaseActive(ctx context.Context, id ClaimID) error {
	claim, ok, err := l.LookupClaim(ctx, id)
	if err != nil {
		return err
	}
	if !ok {
		return &Error{Code: ErrNotFound, Operation: "release", Identity: string(id)}
	}
	if claim.State == ClaimReleased {
		return nil
	}
	claim.State = ClaimReleased
	encoded, err := jsonstable.MarshalCanonical(claim)
	if err != nil {
		return err
	}
	return l.Outside(ctx).Put(string(id), encoded)
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
