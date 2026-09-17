package writer

import (
	"context"
	"fmt"

	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
)

// projectionKey names one projection version in the projector's maps.
type projectionKey struct {
	id      extension.ProjectionID
	version extension.ProjectionVersion
}

// projector is the projection stage of the commit pipeline: it holds the
// transactional state of every registered projection folded to the Writer's
// head (EXT-WRT-1), pre-folds each provisional commit so an invalid group is
// refused before anything is persisted, and refreshes the projection cache
// per the deployment's policy (EXT-PRJ-3, EXT-PRJ-7). It knows nothing of
// ownership, admission or observers.
type projector struct {
	registry *extension.Registry
	sid      session.SessionID
	states   map[projectionKey]any
	scopes   map[projectionKey]*extension.ProjectionScope
	// cache, policy and cached carry EXT-PRJ-3: cached records the head each
	// projection's cache entry already reflects, which is what a policy
	// measures the next refresh against.
	cache  extension.ProjectionCache
	policy extension.CachePolicy
	cached map[projectionKey]session.Head
}

func newProjector(registry *extension.Registry, sid session.SessionID, cache extension.ProjectionCache, policy extension.CachePolicy) *projector {
	if policy == nil {
		policy = extension.CacheEvery(extension.DefaultCacheEvery)
	}
	return &projector{registry: registry, sid: sid, states: make(map[projectionKey]any),
		scopes: make(map[projectionKey]*extension.ProjectionScope), cache: cache, policy: policy, cached: make(map[projectionKey]session.Head)}
}

// rebuild restores every registered projection from the whole log, or from a
// cache entry that ends on a commit boundary of this log plus the commits
// after it, which is what keeps a long session from refolding quadratically
// (EXT-PRJ-3).
func (p *projector) rebuild(ctx context.Context, page session.CommitPage) error {
	for _, def := range p.registry.Projections() {
		k := projectionKey{def.ID, def.Version}
		scope, err := p.registry.ScopeFor(def.ID, def.Version)
		if err != nil {
			return err
		}
		p.scopes[k] = scope
		state, through, ok := p.startState(ctx, scope, page.Commits)
		if !ok {
			if state, err = scope.Def.Initial(); err != nil {
				return err
			}
		}
		from := session.CommitSeq(0)
		if ok {
			p.cached[k] = through
			from = through.Next
		}
		if from < session.CommitSeq(len(page.Commits)) {
			if state, err = p.registry.FoldFrom(scope, state, page.Commits[from:], page.Header); err != nil {
				return err
			}
		}
		p.states[k] = state
	}
	return nil
}

// startState returns the state this projection should begin folding from: the
// cached one when its entry covers a commit boundary of this log and still
// decodes, otherwise nothing. Anything unusable -- absent, corrupt, ahead of
// the log, or recorded at a digest the log does not have -- falls back to a
// full fold, so a stale or damaged cache only costs time (EXT-PRJ-3). It is
// the Writer's counterpart of the store reader's startState. A fork folds its
// inherited prefix on first open and caches the result under its own
// SessionID like any Session.
func (p *projector) startState(ctx context.Context, scope *extension.ProjectionScope, commits []session.Commit) (any, session.Head, bool) {
	if p.cache == nil {
		return nil, session.Head{}, false
	}
	encoded, through, ok, err := p.cache.Load(ctx, p.sid, scope.Def.ID, scope.Def.Version)
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

// fold pre-folds one provisional commit into every projection and returns the
// next states without installing them: the caller installs them with advance
// once the commit is durable.
func (p *projector) fold(provisional session.Commit) (map[projectionKey]any, error) {
	next := make(map[projectionKey]any, len(p.states))
	for k, scope := range p.scopes {
		state, err := p.registry.Fold(scope, p.states[k], []session.Commit{provisional})
		if err != nil {
			return nil, err
		}
		next[k] = state
	}
	return next, nil
}

// advance installs the states fold produced.
func (p *projector) advance(next map[projectionKey]any) {
	for k, s := range next {
		p.states[k] = s
	}
}

// detached returns a copy of one projection state that the caller owns.
func (p *projector) detached(id extension.ProjectionID, ver extension.ProjectionVersion) (any, error) {
	k := projectionKey{id, ver}
	state, ok := p.states[k]
	if !ok {
		return nil, &extension.Error{Code: extension.ErrInvalid, Detail: fmt.Sprintf("unknown projection %q v%d", id, ver)}
	}
	codec := p.scopes[k].Def.StateCodec
	encoded, err := codec.Encode(state)
	if err != nil {
		return nil, err
	}
	return codec.Decode(encoded)
}

// cacheWrite is one projection entry the policy asked to refresh: the state
// and the head it covers, captured under the lock and written outside it.
type cacheWrite struct {
	key   projectionKey
	state any
	head  session.Head
}

// planRefresh selects the entries the deployment's policy wants refreshed and
// records them as covering head. The caller holds the Writer's lock. The
// writes themselves happen outside it (saveRefresh): a cache entry is derived
// data with no ordering constraint against later commits (EXT-PRJ-7), and the
// captured states are immutable (EXT-PRJ-1), so nothing in the critical
// section depends on the IO.
func (p *projector) planRefresh(head session.Head, closing bool) []cacheWrite {
	if p.cache == nil {
		return nil
	}
	var writes []cacheWrite
	for k := range p.scopes {
		if !p.policy(k.id, k.version, head, p.cached[k], closing) {
			continue
		}
		writes = append(writes, cacheWrite{key: k, state: p.states[k], head: head})
		p.cached[k] = head
	}
	return writes
}

// saveRefresh performs planned writes. Best effort and never fatal: the cache
// is derived data, so a failed Save only means a later Writer folds more
// (EXT-PRJ-3); the policy then asks again at its next threshold.
func (p *projector) saveRefresh(ctx context.Context, writes []cacheWrite) {
	for _, cw := range writes {
		_ = extension.SaveProjection(ctx, p.cache, p.registry, p.sid, cw.key.id, cw.key.version, cw.state, cw.head)
	}
}

// memoryReader reads the Writer's transactional projections (EXT-PRJ-4).
type memoryReader struct{ w *sessionWriter }

func (r memoryReader) Load(_ context.Context, sid session.SessionID, id extension.ProjectionID, v extension.ProjectionVersion) (any, session.Head, error) {
	if sid != r.w.sid {
		return nil, session.Head{}, &extension.Error{Code: extension.ErrInvalid, Detail: "writer projections are session-local"}
	}
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	state, err := r.w.projections.detached(id, v)
	return state, r.w.head, err
}
