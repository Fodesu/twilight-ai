package artifact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// SchemeCAS is the standard content-addressed scheme (ART-CAP-2): Key is the
// content digest in the form <algorithm>:<hex> and equals Integrity.
const SchemeCAS Scheme = "cas"

// IntegritySHA256 is the one integrity algorithm the reference store speaks.
const IntegritySHA256 = "sha256"

// Info is what a Resolver knows about a Ref's content (ART-BND-2).
type Info struct {
	MediaType  string
	SizeBytes  *uint64
	Integrity  *Integrity
	Durability Durability
}

// PutRequest stores new content. Reader is consumed to EOF.
type PutRequest struct {
	MediaType  string
	Reader     io.Reader
	Durability Durability
}

// PromoteRequest asks for a Ref of the same content at TargetScheme and
// TargetAuthority with at least the durability of the source (ART-REF-2).
type PromoteRequest struct {
	TargetScheme    Scheme
	TargetAuthority Authority
	Durability      Durability
}

// Resolver reads content behind a Ref and verifies it against the Ref's
// declared size and integrity (ART-BND-2). Failures are classified by
// ErrorCode (ART-CAP-1): ErrNotFound, ErrExpired, ErrUnauthorized (a key of
// another Authority), ErrCorrupt (stored bytes disagree with the Ref) and
// ErrUnsupported (a scheme this resolver does not speak).
type Resolver interface {
	Stat(context.Context, Ref) (Info, error)
	Open(context.Context, Ref) (io.ReadCloser, Info, error)
}

// Store writes content and returns its Ref only once the write is durable.
// A repeated Put of identical bytes is idempotent (ART-BND-2).
type Store interface {
	Put(context.Context, PutRequest) (Ref, error)
}

// Promoter produces a new Ref for existing content at the requested target
// and durability; it never lowers durability and never rewrites a Binding
// (ART-BND-2, ART-REF-2).
type Promoter interface {
	Promote(context.Context, Ref, PromoteRequest) (Ref, error)
}

// ContentStore is one Authority's full capability set.
type ContentStore interface {
	Resolver
	Store
	Promoter
}

// MemoryContentStoreOptions tunes the in-process cas store.
type MemoryContentStoreOptions struct {
	// Now is the clock ephemeral expiry is judged by; nil selects time.Now.
	Now func() time.Time
	// EphemeralTTL is how long an Ephemeral Put stays resolvable; zero means
	// no expiry is recorded on the Ref.
	EphemeralTTL time.Duration
	// MaxBytes caps one Put; zero means DefaultMaxContentBytes (ART-CAP-1,
	// size amplification).
	MaxBytes int64
}

// DefaultMaxContentBytes bounds a Put when the options give no cap.
const DefaultMaxContentBytes int64 = 64 << 20

// MemoryContentStore is the in-process ContentStore of one Authority for the
// cas scheme. Content is immutable and addressed by its SHA-256; the entry
// remembers the highest durability any Ref of it has reached, so a promoted
// Ref stops expiring while the original Ref keeps its own terms.
type MemoryContentStore struct {
	authority Authority
	opts      MemoryContentStoreOptions
	mu        sync.Mutex
	entries   map[Key]*contentEntry
}

type contentEntry struct {
	bytes      []byte
	mediaType  string
	durability Durability
}

// NewMemoryContentStore builds the store for authority.
func NewMemoryContentStore(authority Authority, opts MemoryContentStoreOptions) (*MemoryContentStore, error) {
	if authority == "" {
		return nil, &Error{Code: ErrInvalid, Operation: "content_store", Detail: "empty authority"}
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.MaxBytes <= 0 {
		opts.MaxBytes = DefaultMaxContentBytes
	}
	return &MemoryContentStore{authority: authority, opts: opts, entries: make(map[Key]*contentEntry)}, nil
}

// Authority is the logical store instance this store serves.
func (s *MemoryContentStore) Authority() Authority { return s.authority }

// CASKey is the cas Key and Integrity value of data.
func CASKey(data []byte) (Key, Integrity) {
	sum := sha256.Sum256(data)
	hexSum := hex.EncodeToString(sum[:])
	return Key(IntegritySHA256 + ":" + hexSum), Integrity{Algorithm: IntegritySHA256, Value: hexSum}
}

func (s *MemoryContentStore) Put(ctx context.Context, req PutRequest) (Ref, error) {
	if err := ctx.Err(); err != nil {
		return Ref{}, err
	}
	if req.Reader == nil {
		return Ref{}, &Error{Code: ErrInvalid, Operation: "put", Detail: "nil reader"}
	}
	if req.Durability.Rank() < 0 {
		return Ref{}, &Error{Code: ErrInvalid, Operation: "put", Detail: "unknown durability"}
	}
	data, err := io.ReadAll(io.LimitReader(req.Reader, s.opts.MaxBytes+1))
	if err != nil {
		return Ref{}, &Error{Code: ErrUnavailable, Operation: "put", Detail: err.Error()}
	}
	if int64(len(data)) > s.opts.MaxBytes {
		return Ref{}, &Error{Code: ErrInvalid, Operation: "put", Detail: fmt.Sprintf("content exceeds %d bytes", s.opts.MaxBytes)}
	}
	key, _ := CASKey(data)
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[key]
	if ok {
		if !bytes.Equal(e.bytes, data) {
			return Ref{}, &Error{Code: ErrCorrupt, Operation: "put", Identity: string(key), Detail: "stored content differs from the new bytes under the same digest"}
		}
		if e.mediaType != req.MediaType {
			return Ref{}, &Error{Code: ErrConflict, Operation: "put", Identity: string(key), Detail: "same content declared with another media type"}
		}
		if req.Durability.Rank() > e.durability.Rank() {
			e.durability = req.Durability
		}
	} else {
		e = &contentEntry{bytes: data, mediaType: req.MediaType, durability: req.Durability}
		s.entries[key] = e
	}
	return s.refFor(key, e, req.Durability), nil
}

// refFor builds the Ref of an entry at durability; an Ephemeral Ref carries
// the expiry the options prescribe.
func (s *MemoryContentStore) refFor(key Key, e *contentEntry, durability Durability) Ref {
	size := uint64(len(e.bytes))
	_, integrity := CASKey(e.bytes)
	ref := Ref{Scheme: SchemeCAS, Authority: s.authority, Key: key, MediaType: e.mediaType, SizeBytes: &size, Integrity: &integrity, Durability: durability}
	if durability == Ephemeral && s.opts.EphemeralTTL > 0 {
		at := s.opts.Now().Add(s.opts.EphemeralTTL).UnixMilli()
		ref.ExpiresAtUnixMilli = &at
	}
	return ref
}

// locate is the shared Resolver check: scheme, authority, presence, expiry
// and agreement between the Ref's declarations and the stored bytes.
func (s *MemoryContentStore) locate(op string, ref Ref) (*contentEntry, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	if ref.Scheme != SchemeCAS {
		return nil, &Error{Code: ErrUnsupported, Operation: op, Identity: string(ref.Key), Detail: fmt.Sprintf("scheme %s", ref.Scheme)}
	}
	if ref.Authority != s.authority {
		return nil, &Error{Code: ErrUnauthorized, Operation: op, Identity: string(ref.Key), Detail: fmt.Sprintf("authority %s is not %s", ref.Authority, s.authority)}
	}
	if ref.ExpiresAtUnixMilli != nil && s.opts.Now().UnixMilli() >= *ref.ExpiresAtUnixMilli {
		return nil, &Error{Code: ErrExpired, Operation: op, Identity: string(ref.Key)}
	}
	s.mu.Lock()
	e, ok := s.entries[ref.Key]
	s.mu.Unlock()
	if !ok {
		return nil, &Error{Code: ErrNotFound, Operation: op, Identity: string(ref.Key)}
	}
	key, integrity := CASKey(e.bytes)
	if key != ref.Key || *ref.Integrity != integrity {
		return nil, &Error{Code: ErrCorrupt, Operation: op, Identity: string(ref.Key), Detail: "stored bytes do not match the ref's integrity"}
	}
	if ref.SizeBytes != nil && *ref.SizeBytes != uint64(len(e.bytes)) {
		return nil, &Error{Code: ErrCorrupt, Operation: op, Identity: string(ref.Key), Detail: "stored size does not match the ref"}
	}
	if ref.MediaType != e.mediaType {
		return nil, &Error{Code: ErrCorrupt, Operation: op, Identity: string(ref.Key), Detail: "ref media type does not match the stored declaration"}
	}
	return e, nil
}

func (s *MemoryContentStore) Stat(ctx context.Context, ref Ref) (Info, error) {
	if err := ctx.Err(); err != nil {
		return Info{}, err
	}
	e, err := s.locate("stat", ref)
	if err != nil {
		return Info{}, err
	}
	return infoOf(e, ref.Durability), nil
}

func (s *MemoryContentStore) Open(ctx context.Context, ref Ref) (io.ReadCloser, Info, error) {
	if err := ctx.Err(); err != nil {
		return nil, Info{}, err
	}
	e, err := s.locate("open", ref)
	if err != nil {
		return nil, Info{}, err
	}
	return io.NopCloser(bytes.NewReader(e.bytes)), infoOf(e, ref.Durability), nil
}

func infoOf(e *contentEntry, durability Durability) Info {
	size := uint64(len(e.bytes))
	_, integrity := CASKey(e.bytes)
	return Info{MediaType: e.mediaType, SizeBytes: &size, Integrity: &integrity, Durability: durability}
}

// Promote returns a Ref of the same content at the requested durability. The
// target must be this store (cas under its Authority); a cross-store
// promotion is CopyPromoter. The source Ref must still resolve, the new
// durability must not be lower than the source's, and a Ref above Ephemeral
// never expires (ART-REF-2).
func (s *MemoryContentStore) Promote(ctx context.Context, ref Ref, req PromoteRequest) (Ref, error) {
	if err := ctx.Err(); err != nil {
		return Ref{}, err
	}
	if req.TargetScheme != SchemeCAS {
		return Ref{}, &Error{Code: ErrUnsupported, Operation: "promote", Identity: string(ref.Key), Detail: fmt.Sprintf("target scheme %s", req.TargetScheme)}
	}
	if req.TargetAuthority != s.authority {
		return Ref{}, &Error{Code: ErrUnauthorized, Operation: "promote", Identity: string(ref.Key), Detail: fmt.Sprintf("target authority %s is not %s", req.TargetAuthority, s.authority)}
	}
	if req.Durability.Rank() < 0 {
		return Ref{}, &Error{Code: ErrInvalid, Operation: "promote", Identity: string(ref.Key), Detail: "unknown durability"}
	}
	if req.Durability.Rank() < ref.Durability.Rank() {
		return Ref{}, &Error{Code: ErrInvalid, Operation: "promote", Identity: string(ref.Key), Detail: fmt.Sprintf("cannot lower durability from %s to %s", ref.Durability, req.Durability)}
	}
	e, err := s.locate("promote", ref)
	if err != nil {
		return Ref{}, err
	}
	s.mu.Lock()
	if req.Durability.Rank() > e.durability.Rank() {
		e.durability = req.Durability
	}
	s.mu.Unlock()
	return s.refFor(ref.Key, e, req.Durability), nil
}

// CopyPromoter promotes across stores: it resolves the source Ref, writes the
// bytes into Target at the requested durability and returns the new Ref,
// verifying that both sides name the same content.
type CopyPromoter struct {
	Source Resolver
	Target Store
}

func (p CopyPromoter) Promote(ctx context.Context, ref Ref, req PromoteRequest) (Ref, error) {
	if p.Source == nil || p.Target == nil {
		return Ref{}, errors.New("artifact: copy promoter: nil source or target")
	}
	if req.Durability.Rank() < ref.Durability.Rank() {
		return Ref{}, &Error{Code: ErrInvalid, Operation: "promote", Identity: string(ref.Key), Detail: fmt.Sprintf("cannot lower durability from %s to %s", ref.Durability, req.Durability)}
	}
	rc, info, err := p.Source.Open(ctx, ref)
	if err != nil {
		return Ref{}, err
	}
	defer rc.Close()
	out, err := p.Target.Put(ctx, PutRequest{MediaType: info.MediaType, Reader: rc, Durability: req.Durability})
	if err != nil {
		return Ref{}, err
	}
	if out.Scheme != req.TargetScheme || out.Authority != req.TargetAuthority {
		return Ref{}, &Error{Code: ErrUnsupported, Operation: "promote", Identity: string(ref.Key), Detail: "target store does not serve the requested scheme and authority"}
	}
	if ref.Integrity != nil && out.Integrity != nil && *ref.Integrity != *out.Integrity {
		return Ref{}, &Error{Code: ErrCorrupt, Operation: "promote", Identity: string(ref.Key), Detail: "target content integrity differs from the source"}
	}
	return out, nil
}
