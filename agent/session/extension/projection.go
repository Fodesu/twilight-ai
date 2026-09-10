package extension

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/session"
)

// ProjectionDefinition is a pure fold over decoded events (EXT-PRJ-1).
type ProjectionDefinition struct {
	ID         ProjectionID
	Version    ProjectionVersion
	Consumes   []session.EventType
	Initial    func() (any, error)
	Apply      func(any, DecodedEvent) (any, error)
	StateCodec PayloadCodec
}

// projectionScope is a definition bound to its module scope: the type prefixes
// a reader filters on and the modules whose unknown events must not be skipped.
type projectionScope struct {
	def      ProjectionDefinition
	consumes map[session.EventType]struct{}
	modules  map[ModuleKey]struct{}
	types    []session.EventType
}

func (r *Registry) scopeFor(id ProjectionID, v ProjectionVersion) (*projectionScope, error) {
	def, module, ok := r.LookupProjection(id, v)
	if !ok {
		return nil, &Error{Code: ErrInvalid, Detail: fmt.Sprintf("unknown projection %q v%d", id, v)}
	}
	s := &projectionScope{def: def, consumes: make(map[session.EventType]struct{}, len(def.Consumes)), modules: r.scopeOf(module)}
	for _, t := range def.Consumes {
		s.consumes[t] = struct{}{}
	}
	for m := range s.modules {
		s.types = append(s.types, ModulePrefix(m.Source, m.ID))
	}
	return s, nil
}

// fold applies rows to state group by group (EXT-PRJ-1/2). rows must be
// whole groups in Seq order.
func (r *Registry) fold(s *projectionScope, state any, rows []session.SessionEvent) (any, error) {
	for i := 0; i < len(rows); {
		end := i
		for end < len(rows) && !rows[end].Last {
			end++
		}
		if end >= len(rows) {
			return nil, &Error{Code: ErrInvalid, Detail: "fold received an incomplete group"}
		}
		next := state
		for j := i; j <= end; j++ {
			var err error
			next, err = r.applyRow(s, next, &rows[j])
			if err != nil {
				return nil, err
			}
		}
		state = next
		i = end + 1
	}
	return state, nil
}

func (r *Registry) applyRow(s *projectionScope, state any, row *session.SessionEvent) (any, error) {
	if _, want := s.consumes[row.Type]; !want {
		if _, registered := r.events[row.Type]; registered {
			return state, nil // known type of some module, not consumed here
		}
		module, known := r.ModuleOf(row.Type)
		if _, inScope := s.modules[module]; known && inScope && !row.Ignorable {
			return nil, &Error{Code: ErrUnknownEvent, Type: row.Type, Detail: fmt.Sprintf("projection %q: unregistered non-ignorable event of module %s/%s at seq %d", s.def.ID, module.Source, module.ID, row.Seq)}
		}
		return state, nil
	}
	decoded, err := r.Decode(*row)
	if err != nil {
		return nil, err
	}
	if decoded.Unknown {
		if row.Ignorable {
			return state, nil
		}
		return nil, &Error{Code: ErrUnknownEvent, Type: row.Type, Detail: fmt.Sprintf("projection %q cannot decode v%d at seq %d", s.def.ID, decoded.Version, row.Seq)}
	}
	next, err := s.def.Apply(state, decoded)
	if err != nil {
		return nil, fmt.Errorf("projection %s: seq %d: %w", s.def.ID, row.Seq, err)
	}
	return next, nil
}

// ProjectionReader loads a projection state together with the stream head it
// covers.
type ProjectionReader interface {
	Load(ctx context.Context, sid session.SessionID, id ProjectionID, v ProjectionVersion) (state any, through session.Head, err error)
}

// ProjectionCache is the optional derived cache of EXT-PRJ-3. Entries may be
// lost or stale at any time; readers verify Through against the stream.
type ProjectionCache interface {
	Load(ctx context.Context, sid session.SessionID, id ProjectionID, v ProjectionVersion) (state jsonstable.Value, through session.Head, ok bool, err error)
	Save(ctx context.Context, sid session.SessionID, id ProjectionID, v ProjectionVersion, state jsonstable.Value, through session.Head) error
}

// ProjectionCacheProvider is implemented by a Store adapter that can back its
// projection cache durably, so assembly code can pick it without
// knowing the adapter. A Store that does not implement it gets an in-memory
// cache or none.
type ProjectionCacheProvider interface {
	ProjectionCache() ProjectionCache
}

// DefaultCacheEvery is the row gap a projection's cached state may fall behind
// the head when the deployment chooses no other policy. It bounds the work a
// reopening Writer repeats: after an abrupt end it refolds at most this many
// rows, and after a clean Close none.
const DefaultCacheEvery session.Seq = 64

// CachePolicy decides whether the Writer refreshes one projection's entry in
// the ProjectionCache. The Writer asks it after every applied commit, and once
// more with closing set when it is closed, so a policy can treat the last
// question differently from a routine one.
//
// cached is the head the projection's cache entry already reflects, or the zero
// Head when the cache holds no entry for it; head is where the fold itself now
// stands, so the pair is "how far the stream has come" against "how far the
// cached copy reaches". A policy only governs *writing*: a Writer always uses
// whatever entry it finds, whoever wrote it, because a stale or hostile entry is
// rejected when it is validated against the stream.
type CachePolicy func(id ProjectionID, v ProjectionVersion, head, cached session.Head, closing bool) bool

// CacheEvery refreshes a projection once the head has moved n rows past the
// entry the cache already covers, and always at Close. n <= 0 means
// DefaultCacheEvery.
func CacheEvery(n session.Seq) CachePolicy {
	return func(_ ProjectionID, _ ProjectionVersion, head, cached session.Head, closing bool) bool {
		if closing {
			return true
		}
		if n <= 0 {
			n = DefaultCacheEvery
		}
		return head.Next >= cached.Next+n
	}
}

// Exclude declines the named projections and defers to p for the rest. An
// assembly uses it for a projection whose owning component refreshes the cache
// itself at checkpoint points the Writer must not preempt.
func (p CachePolicy) Exclude(ids ...ProjectionID) CachePolicy {
	return func(id ProjectionID, v ProjectionVersion, head, cached session.Head, closing bool) bool {
		for _, excluded := range ids {
			if id == excluded {
				return false
			}
		}
		return p(id, v, head, cached, closing)
	}
}

// MemoryProjectionCache is the in-process ProjectionCache.
type MemoryProjectionCache struct {
	mu      sync.Mutex
	entries map[cacheKey]cacheEntry
}

type cacheKey struct {
	sid session.SessionID
	id  ProjectionID
	v   ProjectionVersion
}
type cacheEntry struct {
	state   jsonstable.Value
	through session.Head
}

func NewMemoryProjectionCache() *MemoryProjectionCache {
	return &MemoryProjectionCache{entries: make(map[cacheKey]cacheEntry)}
}

func (c *MemoryProjectionCache) Load(_ context.Context, sid session.SessionID, id ProjectionID, v ProjectionVersion) (jsonstable.Value, session.Head, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[cacheKey{sid, id, v}]
	return e.state, e.through, ok, nil
}

func (c *MemoryProjectionCache) Save(_ context.Context, sid session.SessionID, id ProjectionID, v ProjectionVersion, state jsonstable.Value, through session.Head) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[cacheKey{sid, id, v}] = cacheEntry{state, through}
	return nil
}

// Delete drops one entry; tests use it to prove the cache is discardable.
func (c *MemoryProjectionCache) Delete(sid session.SessionID, id ProjectionID, v ProjectionVersion) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, cacheKey{sid, id, v})
}

// SaveProjection encodes state with the projection's StateCodec and stores it
// in cache covering through.
func SaveProjection(ctx context.Context, cache ProjectionCache, registry *Registry, sid session.SessionID, id ProjectionID, v ProjectionVersion, state any, through session.Head) error {
	if cache == nil {
		return nil
	}
	def, _, ok := registry.LookupProjection(id, v)
	if !ok {
		return &Error{Code: ErrInvalid, Detail: fmt.Sprintf("unknown projection %q v%d", id, v)}
	}
	encoded, err := def.StateCodec.Encode(state)
	if err != nil {
		return err
	}
	return cache.Save(ctx, sid, id, v, encoded, through)
}

type storeReader struct {
	store    session.Store
	registry *Registry
	cache    ProjectionCache
}

// NewProjectionReader reads projections from the Store: cache entry (when it
// is a prefix of the stream) plus the filtered tail, or a full fold. It is
// the observer's path; the owner process reads through Writer.Projections().
func NewProjectionReader(store session.Store, registry *Registry, cache ProjectionCache) ProjectionReader {
	return &storeReader{store: store, registry: registry, cache: cache}
}

func (r *storeReader) Load(ctx context.Context, sid session.SessionID, id ProjectionID, v ProjectionVersion) (any, session.Head, error) {
	scope, err := r.registry.scopeFor(id, v)
	if err != nil {
		return nil, session.Head{}, err
	}
	state, from, err := r.startState(ctx, sid, scope)
	if err != nil {
		return nil, session.Head{}, err
	}
	page, err := r.store.Read(ctx, session.ReadRequest{SessionID: sid, From: from.Next, Types: scope.types})
	if err != nil {
		return nil, session.Head{}, err
	}
	state, err = r.registry.fold(scope, state, page.Events)
	if err != nil {
		return nil, session.Head{}, err
	}
	return state, page.Head, nil
}

// startState returns the cached state when its Through is a prefix of the
// stream; otherwise the projection's initial state and the empty head.
func (r *storeReader) startState(ctx context.Context, sid session.SessionID, scope *projectionScope) (any, session.Head, error) {
	if r.cache != nil {
		encoded, through, ok, err := r.cache.Load(ctx, sid, scope.def.ID, scope.def.Version)
		if err != nil {
			return nil, session.Head{}, err
		}
		if ok && through.Next > 0 && r.isPrefix(ctx, sid, through) {
			if state, err := scope.def.StateCodec.Decode(encoded); err == nil {
				return state, through, nil
			}
		}
	}
	state, err := scope.def.Initial()
	return state, session.Head{}, err
}

// isPrefix checks that the row before through.Next carries through.Digest.
func (r *storeReader) isPrefix(ctx context.Context, sid session.SessionID, through session.Head) bool {
	page, err := r.store.Read(ctx, session.ReadRequest{SessionID: sid, From: through.Next - 1, Limit: 1})
	if err != nil || len(page.Events) == 0 {
		return false
	}
	return page.Events[0].Seq == through.Next-1 && page.Events[0].Digest == through.Digest
}

// JSONStateCodec is a StateCodec for projection states that marshal to JSON.
type JSONStateCodec[T any] struct{}

func (JSONStateCodec[T]) Validate(value any) error {
	if _, ok := value.(T); !ok {
		var zero T
		return fmt.Errorf("state is %T, want %T", value, zero)
	}
	return nil
}
func (c JSONStateCodec[T]) Encode(value any) (jsonstable.Value, error) {
	if err := c.Validate(value); err != nil {
		return jsonstable.Value{}, err
	}
	return jsonstable.FromValue(value)
}
func (JSONStateCodec[T]) Decode(wire jsonstable.Value) (any, error) {
	var v T
	if wire.IsZero() {
		return nil, errors.New("empty projection state")
	}
	if err := StrictDecode(wire, &v); err != nil {
		return nil, err
	}
	return v, nil
}

var _ = es.Digest("")
