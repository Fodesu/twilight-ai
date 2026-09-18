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
	// unhealthy records derived projections that failed to fold an applied
	// commit (EXT-PRJ-9): their state stays at the last good commit, reads
	// report the failure and the cache is not refreshed for them.
	unhealthy map[projectionKey]error
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
	return &projector{registry: registry, sid: sid, states: make(map[projectionKey]any), unhealthy: make(map[projectionKey]error),
		scopes: make(map[projectionKey]*extension.ProjectionScope), cache: cache, policy: policy, cached: make(map[projectionKey]session.Head)}
}

// rebuild restores every registered projection from the whole log, or from a
// cache entry that ends on a commit boundary of this log plus the commits
// after it, which is what keeps a long session from refolding quadratically
// (EXT-PRJ-3).
func (p *projector) rebuild(ctx context.Context, page *session.CommitPage) error {
	for _, def := range p.registry.Projections() {
		k := projectionKey{def.ID, def.Version}
		scope, err := p.registry.ScopeFor(def.ID, def.Version)
		if err != nil {
			return err
		}
		p.scopes[k] = scope
		state, through, ok := p.startState(ctx, scope, page.Commits, page.Header)
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
			folded, err := p.registry.FoldFrom(scope, state, page.Commits[from:], page.Header)
			if err != nil {
				if scope.Def.Authoritative {
					return err
				}
				// A derived projection that cannot fold the log does not keep
				// the Session from opening (EXT-PRJ-9): it stops at its last
				// good commit and stays unhealthy until a registry that folds
				// it reopens the Session.
				folded, err = p.lastGood(scope, state, page.Commits[from:], page.Header)
				p.unhealthy[k] = err
			}
			state = folded
		}
		p.states[k] = state
	}
	return nil
}

// lastGood folds commits one at a time and returns the state before the
// first commit the projection cannot fold, with that failure.
func (p *projector) lastGood(scope *extension.ProjectionScope, state any, commits []session.Commit, header session.SegmentHeader) (any, error) {
	for i := range commits {
		next, err := p.registry.FoldFrom(scope, state, commits[i:i+1], header)
		if err != nil {
			return state, err
		}
		state = next
	}
	return state, nil
}

// startState returns the state this projection should begin folding from: the
// cached one when its entry covers a commit boundary the tip segment wrote
// itself and still decodes, otherwise nothing. Anything unusable -- absent,
// corrupt, ahead of the log, recorded at a digest the log does not have, or
// ending on an inherited commit -- falls back to a full fold, so a stale or
// damaged cache only costs time (EXT-PRJ-3). It is the Writer's counterpart
// of the store reader's startState. A fork, and a Session whose tip just
// advanced, fold their inherited prefix on first open and cache the result
// once they hold a commit of their own.
func (p *projector) startState(ctx context.Context, scope *extension.ProjectionScope, commits []session.Commit, header session.SegmentHeader) (any, session.Head, bool) {
	if p.cache == nil {
		return nil, session.Head{}, false
	}
	encoded, through, ok, err := p.cache.Load(ctx, p.sid, scope.Def.ID, scope.Def.Version)
	if err != nil || !ok || !coversCommit(commits, header, through) {
		return nil, session.Head{}, false
	}
	state, err := scope.Def.StateCodec.Decode(encoded)
	if err != nil {
		return nil, session.Head{}, false
	}
	return state, through, true
}

// coversCommit reports whether through names the commit before its Next -- a
// commit boundary of this log that the tip header describes wrote itself --
// with the digest the entry recorded.
func coversCommit(commits []session.Commit, header session.SegmentHeader, through session.Head) bool {
	if !extension.OwnBoundary(header, through) || through.Next > session.CommitSeq(len(commits)) {
		return false
	}
	return extension.SealedAt(commits[through.Next-1], through)
}

// folded is what fold produced: the next states and, for derived
// projections that could not fold the commit, the failure to record once
// the commit is durable.
type folded struct {
	next   map[projectionKey]any
	failed map[projectionKey]error
}

// fold pre-folds one provisional commit into every projection without
// installing anything. An authoritative projection that refuses the commit
// refuses it for the Writer; a derived one keeps its last good state and is
// marked unhealthy once the commit lands (EXT-PRJ-9). A projection already
// unhealthy is not folded further.
func (p *projector) fold(provisional session.Commit) (folded, error) {
	out := folded{next: make(map[projectionKey]any, len(p.states))}
	for k, scope := range p.scopes {
		if _, down := p.unhealthy[k]; down {
			continue
		}
		state, err := p.registry.Fold(scope, p.states[k], []session.Commit{provisional})
		if err != nil {
			if scope.Def.Authoritative {
				return folded{}, err
			}
			if out.failed == nil {
				out.failed = make(map[projectionKey]error)
			}
			out.failed[k] = err
			continue
		}
		out.next[k] = state
	}
	return out, nil
}

// advance installs what fold produced for a commit that is now durable.
func (p *projector) advance(f folded) {
	for k, s := range f.next {
		p.states[k] = s
	}
	for k, err := range f.failed {
		p.unhealthy[k] = err
	}
}

// rebase re-derives every projection for a tip about to move (EXT-WRT-10):
// the whole log folded from the projection's initial state under the new
// segment's inheritance boundary -- every commit so far is inherited by the
// new tip (EXT-PRJ-8) -- followed by the bootstrap commits as the new tip's
// own. It installs nothing. An authoritative projection that refuses the
// result refuses the Advance; a derived one stops at its last good commit
// and is marked unhealthy once the segment is published (EXT-PRJ-9). A
// projection unhealthy on the old tip is folded afresh: the event it could
// not fold may be one the new tip does not inherit.
func (p *projector) rebase(commits []session.Commit, boundary session.SegmentHeader, bootstrap []session.Commit) (folded, error) {
	all := make([]session.Commit, 0, len(commits)+len(bootstrap))
	all = append(append(all, commits...), bootstrap...)
	out := folded{next: make(map[projectionKey]any, len(p.scopes))}
	for k, scope := range p.scopes {
		initial, err := scope.Def.Initial()
		if err != nil {
			return folded{}, err
		}
		state, err := p.registry.FoldFrom(scope, initial, all, boundary)
		if err != nil {
			if scope.Def.Authoritative {
				return folded{}, err
			}
			state, err = p.lastGood(scope, initial, all, boundary)
			if out.failed == nil {
				out.failed = make(map[projectionKey]error)
			}
			out.failed[k] = err
		}
		out.next[k] = state
	}
	return out, nil
}

// install replaces every projection with what rebase produced for a segment
// that is now published: states, health and the cache bookkeeping all start
// over on the new tip, whose entries are written from its own commits on.
func (p *projector) install(f folded) {
	p.states = f.next
	p.unhealthy = make(map[projectionKey]error, len(f.failed))
	for k, err := range f.failed {
		p.unhealthy[k] = err
	}
	p.cached = make(map[projectionKey]session.Head)
}

// detached returns a copy of one projection state that the caller owns.
func (p *projector) detached(id extension.ProjectionID, ver extension.ProjectionVersion) (any, error) {
	k := projectionKey{id, ver}
	state, ok := p.states[k]
	if !ok {
		return nil, &extension.Error{Code: extension.ErrInvalid, Detail: fmt.Sprintf("unknown projection %q v%d", id, ver)}
	}
	if err, down := p.unhealthy[k]; down {
		return nil, &extension.Error{Code: extension.ErrProjectionUnhealthy, Detail: fmt.Sprintf("projection %q v%d: %v", id, ver, err)}
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
		if _, down := p.unhealthy[k]; down {
			continue // its state is behind head; a cache entry would lie
		}
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
