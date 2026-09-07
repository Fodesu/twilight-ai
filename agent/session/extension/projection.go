package extension

import (
	"context"
	"errors"
	"fmt"

	"github.com/memohai/twilight/agent/jsonstable"
	"github.com/memohai/twilight/agent/session"
)

// ProjectionDefinition is a pure fold over decoded events (EXT-PRJ-1).
type ProjectionDefinition struct {
	ID              ProjectionID
	Version         ProjectionVersion
	Consumes        []session.EventType
	Ignores         []session.EventType
	RequireComplete []ModuleID
	Initial         func() (any, error)
	Apply           func(any, DecodedEvent) (any, error)
	StateCodec      PayloadCodec
}

// TypeFilter is the Types filter a reader uses for this projection: the
// consumed types plus the full prefixes of RequireComplete modules.
func (d *ProjectionDefinition) TypeFilter() []session.EventType {
	var out []session.EventType
	for _, m := range d.RequireComplete {
		out = append(out, ModulePrefix(m))
	}
	out = append(out, d.Consumes...)
	return out
}

// Fold applies the commits to state. It fails on an Unknown event of a
// RequireComplete module and never publishes a partial commit.
func Fold(registry *Registry, def *ProjectionDefinition, state any, commits []session.SessionCommit) (any, error) {
	consumes := make(map[session.EventType]struct{}, len(def.Consumes))
	for _, t := range def.Consumes {
		consumes[t] = struct{}{}
	}
	ignores := make(map[session.EventType]struct{}, len(def.Ignores))
	for _, t := range def.Ignores {
		ignores[t] = struct{}{}
	}
	required := make(map[ModuleID]struct{}, len(def.RequireComplete))
	for _, m := range def.RequireComplete {
		required[m] = struct{}{}
	}
	for ci := range commits {
		next := state
		for _, e := range commits[ci].Events {
			if _, skip := ignores[e.Type]; skip {
				continue
			}
			if _, want := consumes[e.Type]; !want {
				// Unknown event of a required module: refuse rather than skip.
				module, registered := registry.ModuleForEvent(e.Type)
				if _, mustBeComplete := required[module]; mustBeComplete && !registered {
					return nil, &Error{Code: ErrUnknownEvent, Type: e.Type, Detail: fmt.Sprintf("projection %q requires complete module %q", def.ID, module)}
				}
				continue
			}
			decoded, err := registry.Decode(e)
			if err != nil {
				return nil, err
			}
			decoded.Revision = commits[ci].Revision
			if decoded.Unknown {
				if _, mustBeComplete := required[decoded.ModuleID]; mustBeComplete {
					return nil, &Error{Code: ErrUnknownEvent, Type: e.Type, Detail: fmt.Sprintf("projection %q cannot decode v%d", def.ID, decoded.Version)}
				}
				continue
			}
			next, err = def.Apply(next, decoded)
			if err != nil {
				return nil, fmt.Errorf("projection %s: revision %d: %w", def.ID, commits[ci].Revision, err)
			}
		}
		state = next
	}
	return state, nil
}

// ProjectionReader loads a projection outside the critical section
// (EXT-PRJ-4): snapshot (if it is a prefix of the stream) plus filtered tail.
type ProjectionReader interface {
	Load(ctx context.Context, sid session.SessionID, id ProjectionID, v ProjectionVersion) (state any, through session.Head, err error)
}

type reader struct {
	store    session.Store
	registry *Registry
}

func NewProjectionReader(store session.Store, registry *Registry) ProjectionReader {
	return &reader{store: store, registry: registry}
}

func (r *reader) Load(ctx context.Context, sid session.SessionID, id ProjectionID, v ProjectionVersion) (any, session.Head, error) {
	def, ok := r.registry.LookupProjection(id, v)
	if !ok {
		return nil, session.Head{}, &Error{Code: ErrInvalid, Detail: fmt.Sprintf("unknown projection %q v%d", id, v)}
	}
	state, after, err := r.startState(ctx, sid, &def)
	if err != nil {
		return nil, session.Head{}, err
	}
	page, err := r.store.Replay(ctx, session.ReplayRequest{SessionID: sid, Types: def.TypeFilter()})
	if err != nil {
		return nil, session.Head{}, err
	}
	// Filtered replay pages by commit; a Types filter cannot use the digest
	// chain, so we page until Next is nil.
	commits := page.Commits
	for page.Next != nil {
		page, err = r.store.Replay(ctx, session.ReplayRequest{SessionID: sid, Types: def.TypeFilter(), Cursor: page.Next})
		if err != nil {
			return nil, session.Head{}, err
		}
		commits = append(commits, page.Commits...)
	}
	// Commits at or before the snapshot head are already folded.
	var tail []session.SessionCommit
	for i := range commits {
		if commits[i].Revision > after.Revision {
			tail = append(tail, commits[i])
		}
	}
	state, err = Fold(r.registry, &def, state, tail)
	if err != nil {
		return nil, session.Head{}, err
	}
	return state, page.Head, nil
}

// startState returns the snapshot state when the snapshot is a valid prefix
// of the stream; otherwise the projection's initial state and the empty head.
func (r *reader) startState(ctx context.Context, sid session.SessionID, def *ProjectionDefinition) (any, session.Head, error) {
	res, err := r.store.LoadSnapshot(ctx, session.SnapshotRequest{SessionID: sid, ProjectionKey: session.ProjectionKey(def.ID), ProjectionVersion: uint16(def.Version)})
	if err != nil {
		return nil, session.Head{}, err
	}
	if res.Found {
		if state, ok := r.snapshotState(ctx, sid, def, res.Snapshot); ok {
			return state, res.Snapshot.Through, nil
		}
	}
	state, err := def.Initial()
	return state, session.Head{}, err
}

func (r *reader) snapshotState(ctx context.Context, sid session.SessionID, def *ProjectionDefinition, snap *session.Snapshot) (any, bool) {
	if err := r.registry.Profile.ValidateSnapshot(*snap); err != nil {
		return nil, false
	}
	if !r.isPrefix(ctx, sid, snap.Through) {
		return nil, false
	}
	state, err := def.StateCodec.Decode(snap.State)
	if err != nil {
		return nil, false
	}
	return state, true
}

func (r *reader) isPrefix(ctx context.Context, sid session.SessionID, through session.Head) bool {
	if through.Revision == 0 {
		return false
	}
	// Replay one unfiltered page up to Through; the kernel verifies the chain.
	page, err := r.store.Replay(ctx, session.ReplayRequest{SessionID: sid, Limit: uint32(through.Revision)})
	if err != nil || len(page.Commits) < int(through.Revision) {
		return false
	}
	return page.Commits[through.Revision-1].CommitDigest == through.Digest
}

// LoadIn folds a projection inside a transaction from snapshot plus tail. It
// returns the state and the head it covers.
func LoadIn(tx SemanticTx, def *ProjectionDefinition) (any, session.Head, error) {
	registry := tx.Registry()
	res, err := tx.LoadSnapshot(session.ProjectionKey(def.ID), uint16(def.Version))
	if err != nil {
		return nil, session.Head{}, err
	}
	var state any
	var after session.Head
	if res.Found && registry.Profile.ValidateSnapshot(*res.Snapshot) == nil {
		if s, err := def.StateCodec.Decode(res.Snapshot.State); err == nil {
			// Tail validates Through against the stream; a stale snapshot makes
			// Tail fail and we fall back to a full fold.
			if commits, terr := tx.Tail(res.Snapshot.Through, def.TypeFilter()); terr == nil {
				state = s
				after = res.Snapshot.Through
				folded, err := Fold(registry, def, state, commits)
				if err != nil {
					return nil, session.Head{}, err
				}
				return folded, tx.Head(), nil
			}
		}
	}
	state, err = def.Initial()
	if err != nil {
		return nil, session.Head{}, err
	}
	commits, err := tx.Tail(after, def.TypeFilter())
	if err != nil {
		return nil, session.Head{}, err
	}
	folded, err := Fold(registry, def, state, commits)
	if err != nil {
		return nil, session.Head{}, err
	}
	return folded, tx.Head(), nil
}

// SaveSnapshotIn encodes state and stores it as the projection's snapshot
// covering through, in the same transaction.
func SaveSnapshotIn(tx SemanticTx, def *ProjectionDefinition, state any, through session.Head) error {
	if through.Revision == 0 {
		return nil
	}
	encoded, err := def.StateCodec.Encode(state)
	if err != nil {
		return err
	}
	snap := session.Snapshot{ProtocolVersion: tx.Registry().ProtocolVersion, SessionID: tx.SessionID(),
		ProjectionKey: session.ProjectionKey(def.ID), ProjectionVersion: uint16(def.Version), Through: through, State: encoded}
	d, err := tx.Registry().Profile.SnapshotDigest(snap)
	if err != nil {
		return err
	}
	snap.SnapshotDigest = d
	return tx.SaveSnapshot(snap)
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
		return nil, errors.New("empty snapshot state")
	}
	if err := StrictDecode(wire, &v); err != nil {
		return nil, err
	}
	return v, nil
}
