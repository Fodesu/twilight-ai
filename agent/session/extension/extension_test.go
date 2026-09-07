package extension

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/memohai/twilight/agent/jsonstable"
	"github.com/memohai/twilight/agent/session"
)

type notePayload struct {
	Text string `json:"text"`
}

type noteState struct {
	Notes []string `json:"notes"`
}

func noteModule(id ModuleID, requires ...ModuleRequirement) ModuleDescriptor {
	typ := ModulePrefix(id) + "note"
	return ModuleDescriptor{ID: id, Requires: requires,
		Events: []EventDefinition{{Type: typ, Current: 1, Codecs: map[PayloadVersion]PayloadCodec{1: JSONCodec[notePayload]{}}}},
		Projections: []ProjectionDefinition{{
			ID: ProjectionID(string(typ) + "s"), Version: 1, Consumes: []session.EventType{typ}, RequireComplete: []ModuleID{id},
			Initial: func() (any, error) { return noteState{}, nil },
			Apply: func(state any, e DecodedEvent) (any, error) {
				s := state.(noteState)
				s.Notes = append(append([]string(nil), s.Notes...), e.Value.(notePayload).Text)
				return s, nil
			},
			StateCodec: JSONStateCodec[noteState]{},
		}},
	}
}

func TestBuildRegistryValidatesRequires(t *testing.T) {
	cases := map[string][]ModuleDescriptor{
		"unregistered dependency": {noteModule("a", ModuleRequirement{Module: "zzz"})},
		"cycle":                   {noteModule("a", ModuleRequirement{Module: "b"}), noteModule("b", ModuleRequirement{Module: "a"})},
		"unhandled version": {noteModule("a"), noteModule("b", ModuleRequirement{Module: "a",
			Events: map[session.EventType][]PayloadVersion{ModulePrefix("a") + "note": {2}}})},
		"event outside module": {{ID: "a", Events: []EventDefinition{{Type: "twilight/b/x", Current: 1, Codecs: map[PayloadVersion]PayloadCodec{1: JSONCodec[notePayload]{}}}}}},
	}
	for name, modules := range cases {
		if _, err := BuildRegistry(session.ProfileV1(), modules...); err == nil {
			t.Errorf("%s: registry built", name)
		}
	}
	if _, err := BuildRegistry(session.ProfileV1(), noteModule("a"), noteModule("b", ModuleRequirement{Module: "a",
		Events: map[session.EventType][]PayloadVersion{ModulePrefix("a") + "note": {1}}})); err != nil {
		t.Fatalf("valid registry: %v", err)
	}
}

// Encode adds v; Decode selects the codec by v and keeps unknown versions raw.
func TestRegistryPayloadVersion(t *testing.T) {
	r, err := BuildRegistry(session.ProfileV1(), noteModule("a"))
	if err != nil {
		t.Fatal(err)
	}
	typ := ModulePrefix("a") + "note"
	wire, v, err := r.Encode(typ, notePayload{Text: "hi"})
	if err != nil || v != 1 || wire.String() != `{"text":"hi","v":1}` {
		t.Fatalf("encode = %s v%d %v", wire, v, err)
	}
	decoded, err := r.Decode(session.SessionEvent{Type: typ, Payload: wire})
	if err != nil || decoded.Unknown || decoded.Value.(notePayload).Text != "hi" {
		t.Fatalf("decode = %+v %v", decoded, err)
	}
	future, err := r.Decode(session.SessionEvent{Type: typ, Payload: jsonstable.MustParse(`{"text":"hi","v":2}`)})
	if err != nil || !future.Unknown || future.Version != 2 {
		t.Fatalf("future version = %+v %v", future, err)
	}
	if _, _, err := r.Encode(typ, notePayload{}); err != nil {
		t.Fatalf("encode zero value: %v", err)
	}
	if _, _, err := r.Encode("twilight/a/other", notePayload{}); err == nil {
		t.Fatal("unknown type encoded")
	}
}

func newAppender(t *testing.T) (session.Store, *Registry, SemanticAppender, session.SessionID) {
	t.Helper()
	store := session.NewMemoryStore()
	r, err := BuildRegistry(session.ProfileV1(), noteModule("a"))
	if err != nil {
		t.Fatal(err)
	}
	a, err := NewSemanticAppender(store, r, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(context.Background(), session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: "s"}); err != nil {
		t.Fatal(err)
	}
	return store, r, a, "s"
}

// Both append entries derive EventIDs from (type, CommitID, index), reject a
// same-ID different group, and the reader sees the projection with snapshot
// and tail equivalent.
func TestSemanticAppenderAndProjectionReader(t *testing.T) {
	store, r, a, sid := newAppender(t)
	ctx := context.Background()
	typ := ModulePrefix("a") + "note"
	group := SemanticGroup{CommitID: "c1", Events: []TypedEvent{{Type: typ, Value: notePayload{Text: "one"}}}}
	res, err := a.AppendSemanticIn(ctx, sid, func(tx SemanticTx) (*SemanticGroup, error) { return &group, nil })
	if err != nil || res.Outcome != SemanticApplied {
		t.Fatalf("append in = %+v %v", res, err)
	}
	if res.Commit.Events[0].EventID != DeriveEventID(typ, "c1", 0) {
		t.Fatal("EventID not derived from (type, CommitID, index)")
	}
	head, _ := store.Head(ctx, sid)
	res2, err := a.AppendSemantic(ctx, SemanticAppendRequest{SessionID: sid, ExpectedHead: head,
		Group: SemanticGroup{CommitID: "c2", Events: []TypedEvent{{Type: typ, Value: notePayload{Text: "two"}}}}})
	if err != nil || res2.Outcome != SemanticApplied {
		t.Fatalf("append = %+v %v", res2, err)
	}
	replay, _ := a.AppendSemantic(ctx, SemanticAppendRequest{SessionID: sid, ExpectedHead: head, Group: group})
	if replay.Outcome != SemanticAlreadyApplied {
		t.Fatalf("replay = %+v", replay)
	}
	conflict, _ := a.AppendSemantic(ctx, SemanticAppendRequest{SessionID: sid, ExpectedHead: head,
		Group: SemanticGroup{CommitID: "c1", Events: []TypedEvent{{Type: typ, Value: notePayload{Text: "changed"}}}}})
	if conflict.Outcome != SemanticCommitConflict {
		t.Fatalf("conflict = %+v", conflict)
	}
	stale, _ := a.AppendSemantic(ctx, SemanticAppendRequest{SessionID: sid, ExpectedHead: head,
		Group: SemanticGroup{CommitID: "c3", Events: []TypedEvent{{Type: typ, Value: notePayload{Text: "three"}}}}})
	if stale.Outcome != SemanticHeadConflict {
		t.Fatalf("stale = %+v", stale)
	}
	invalid, _ := a.AppendSemantic(ctx, SemanticAppendRequest{SessionID: sid, ExpectedHead: session.Head{Revision: res2.Commit.Revision, Digest: res2.Commit.CommitDigest},
		Group: SemanticGroup{CommitID: "c4", Events: []TypedEvent{{Type: "twilight/a/unknown", Value: notePayload{}}}}})
	if invalid.Outcome != SemanticInvalid {
		t.Fatalf("invalid = %+v", invalid)
	}

	reader := NewProjectionReader(store, r)
	def, _ := r.LookupProjection(ProjectionID(string(typ)+"s"), 1)
	state, through, err := reader.Load(ctx, sid, def.ID, def.Version)
	if err != nil || through.Revision != 2 {
		t.Fatalf("load = %+v %+v %v", state, through, err)
	}
	full := state.(noteState).Notes
	// Snapshot after the first commit, then the tail must give the same state.
	_, err = a.AppendSemanticIn(ctx, sid, func(tx SemanticTx) (*SemanticGroup, error) {
		s, _, err := LoadIn(tx, &def)
		if err != nil {
			return nil, err
		}
		if len(s.(noteState).Notes) != 2 {
			t.Fatalf("LoadIn = %+v", s)
		}
		return nil, SaveSnapshotIn(tx, &def, noteState{Notes: []string{"one"}}, session.Head{Revision: 1, Digest: res.Commit.CommitDigest})
	})
	if err != nil {
		t.Fatal(err)
	}
	state, _, err = reader.Load(ctx, sid, def.ID, def.Version)
	if err != nil || len(state.(noteState).Notes) != len(full) || state.(noteState).Notes[1] != "two" {
		t.Fatalf("snapshot+tail = %+v, full = %v, %v", state, full, err)
	}
}

func TestLeaseLifecycle(t *testing.T) {
	store, _, a, sid := newAppender(t)
	ctx := context.Background()
	typ := ModulePrefix("a") + "note"
	var lease Lease
	_, err := a.AppendSemanticIn(ctx, sid, func(tx SemanticTx) (*SemanticGroup, error) {
		l, err := AcquireLease(tx, sid, "c1", 1000, AcquireLeaseRequest{Namespace: "twilight/a/lease", Key: "k", Holder: "h1", TTL: time.Second})
		if err != nil {
			return nil, err
		}
		lease = l
		if again, err := AcquireLease(tx, sid, "c1", 1000, AcquireLeaseRequest{Namespace: "twilight/a/lease", Key: "k", Holder: "h1", TTL: time.Second}); err != nil || again.Token != l.Token {
			t.Fatalf("same holder re-acquire = %+v %v", again, err)
		}
		if _, err := AcquireLease(tx, sid, "c1", 1000, AcquireLeaseRequest{Namespace: "twilight/a/lease", Key: "k", Holder: "h2"}); !errors.Is(err, &Error{Code: ErrConflict}) {
			t.Fatalf("other holder = %v, want conflict", err)
		}
		return &SemanticGroup{CommitID: "c1", Events: []TypedEvent{{Type: typ, Value: notePayload{Text: "start"}}}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if lease.Token != DeriveLeaseToken(sid, "twilight/a/lease", "k", "h1", "c1") || lease.DeadlineUnixMilli != 2000 {
		t.Fatalf("lease = %+v", lease)
	}
	leases := Leases{Store: store}
	if err := leases.Renew(ctx, sid, "twilight/a/lease", "k", lease.Token, time.Second, 1500); err != nil {
		t.Fatalf("renew: %v", err)
	}
	if err := leases.Renew(ctx, sid, "twilight/a/lease", "k", "bad", time.Second, 1500); !errors.Is(err, &Error{Code: ErrStale}) {
		t.Fatalf("renew with bad token = %v", err)
	}
	var expired []string
	_ = leases.Expired(ctx, "twilight/a/lease", 2400, func(_ session.SessionID, l Lease) (bool, error) { expired = append(expired, l.Key); return true, nil })
	if len(expired) != 0 {
		t.Fatalf("renewed lease expired early: %v", expired)
	}
	_ = leases.Expired(ctx, "twilight/a/lease", 2600, func(_ session.SessionID, l Lease) (bool, error) { expired = append(expired, l.Key); return true, nil })
	if len(expired) != 1 {
		t.Fatalf("expired = %v", expired)
	}
	_, err = a.AppendSemanticIn(ctx, sid, func(tx SemanticTx) (*SemanticGroup, error) {
		if err := ReleaseLease(tx, "twilight/a/lease", "k", "bad"); !errors.Is(err, &Error{Code: ErrStale}) {
			t.Fatalf("release with bad token = %v", err)
		}
		if err := ReleaseLease(tx, "twilight/a/lease", "k", lease.Token); err != nil {
			return nil, err
		}
		return &SemanticGroup{CommitID: "c2", Events: []TypedEvent{{Type: typ, Value: notePayload{Text: "settle"}}}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := leases.Renew(ctx, sid, "twilight/a/lease", "k", lease.Token, time.Second, 1500); !errors.Is(err, &Error{Code: ErrStale}) {
		t.Fatalf("renew after release = %v", err)
	}
}
