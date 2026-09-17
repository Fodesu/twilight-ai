package writer

import (
	"context"
	"testing"

	"github.com/felinics/twilight/agent/artifact"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
)

// SES-FRK-2/3, EXT-WRT-8: a Writer over a fork folds its projections from
// the inherited prefix, answers a replay of an inherited CommitID as already
// applied, continues the ledger from the anchor, and Fork claims every
// artifact the prefix references under the fork's own owner.
func TestForkWriterInheritsPrefix(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	ref := artifact.Ref{Scheme: "cas", Authority: "local", Key: "k1", Durability: artifact.EventBound, Integrity: &artifact.Integrity{Algorithm: "sha256", Value: "x"}}
	binding, err := artifact.NewBinding("b1", ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.bindings.CreateBinding(ctx, binding); err != nil {
		t.Fatal(err)
	}
	parent := f.open(t, false)
	if _, err := parent.Commit(ctx, noteGroup("c1", "one")); err != nil {
		t.Fatal(err)
	}
	withRef := func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "c2", Batches: sessionBatch(TypedEvent{Type: tpfx("a") + "note", Value: notePayload{Text: "two", Refs: []string{"b1"}}})}, nil
	}
	if res, err := parent.Commit(ctx, withRef); err != nil || res.Outcome != CommitApplied {
		t.Fatalf("c2 = %+v %v", res, err)
	}
	if _, err := parent.Commit(ctx, noteGroup("c3", "three")); err != nil {
		t.Fatal(err)
	}

	header, err := Fork(ctx, f.store, f.registry, f.admission(), ForkRequest{Parent: "s", At: 1, Child: "child", CreatedAtUnixMilli: 5})
	if err != nil {
		t.Fatal(err)
	}
	if header.ParentFork == nil || header.ParentFork.Seq != 1 {
		t.Fatalf("header = %+v", header)
	}
	// The fork claim covers b1 under the fork owner and is idempotent.
	claims, err := artifact.ActiveClaims(ctx, f.ledger, artifact.ClaimOwnerScope{Kind: ForkOwnerKind, Authority: "child"})
	if err != nil || len(claims) != 1 || len(claims[0].BindingSet.BindingIDs) != 1 || claims[0].BindingSet.BindingIDs[0] != "b1" {
		t.Fatalf("fork claims = %+v %v", claims, err)
	}
	if _, err := Fork(ctx, f.store, f.registry, f.admission(), ForkRequest{Parent: "s", At: 1, Child: "child", CreatedAtUnixMilli: 5}); err != nil {
		t.Fatalf("repeat fork: %v", err)
	}
	if claims, _ = artifact.ActiveClaims(ctx, f.ledger, artifact.ClaimOwnerScope{Kind: ForkOwnerKind, Authority: "child"}); len(claims) != 1 {
		t.Fatalf("repeat fork changed claims: %+v", claims)
	}

	child, err := OpenWriter(ctx, f.store, f.registry, f.admission(), "child", session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	load := func(w Writer, sid session.SessionID) []string {
		state, _, err := w.Projections().Load(ctx, sid, extension.ProjectionID(string(tpfx("a"))+"notes"), 1)
		if err != nil {
			t.Fatal(err)
		}
		return state.(noteState).Notes
	}
	if got := load(child, "child"); len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("child projection = %v, want the prefix [one two]", got)
	}
	// Reconciliation of the child's commit claims left the fork claim alone.
	if claims, _ = artifact.ActiveClaims(ctx, f.ledger, artifact.ClaimOwnerScope{Kind: ForkOwnerKind, Authority: "child"}); len(claims) != 1 {
		t.Fatal("opening the fork released its prefix claim")
	}
	// An inherited CommitID replays as already applied; a differing group
	// under it conflicts.
	replay, err := child.Commit(ctx, noteGroup("c1", "one"))
	if err != nil || replay.Outcome != CommitAlreadyApplied || replay.Commit.Seq != 0 {
		t.Fatalf("replay of inherited c1 = %+v %v", replay, err)
	}
	if conflict, _ := child.Commit(ctx, noteGroup("c1", "other")); conflict.Outcome != CommitConflict {
		t.Fatalf("conflicting replay = %+v", conflict)
	}
	// Own commits continue from the anchor and only the child sees them.
	res, err := child.Commit(ctx, noteGroup("c4", "four"))
	if err != nil || res.Outcome != CommitApplied || res.Commit.Seq != 2 {
		t.Fatalf("own commit = %+v %v", res, err)
	}
	if got := load(child, "child"); len(got) != 3 || got[2] != "four" {
		t.Fatalf("child projection after own commit = %v", got)
	}
	if got := load(parent, "s"); len(got) != 3 || got[2] != "three" {
		t.Fatalf("parent projection = %v, want [one two three]", got)
	}
	// An observer reading the child from the Store folds the same state.
	state, head, err := extension.NewProjectionReader(f.store, f.registry, nil).Load(ctx, "child", extension.ProjectionID(string(tpfx("a"))+"notes"), 1)
	if err != nil || len(state.(noteState).Notes) != 3 || head.Next != 3 {
		t.Fatalf("observer = %+v %+v %v", state, head, err)
	}
}

// EXT-PRJ-3 on a fork: a child with no cache entry of its own starts from
// the parent's entry when it ends inside the inherited prefix, and never from
// one that reaches past the anchor.
func TestForkWriterSeedsFromParentCache(t *testing.T) {
	f := newCacheFixture(t)
	ctx := context.Background()
	cache := extension.NewMemoryProjectionCache()
	parent := f.open(t, WritersConfig{Cache: cache, CachePolicy: extension.CacheEvery(1)})
	f.commit(t, parent, "c1", "one")
	f.commit(t, parent, "c2", "two")
	if err := parent.Close(ctx); err != nil { // refreshes the parent's entries at Next=2
		t.Fatal(err)
	}
	if _, err := Fork(ctx, f.store, f.registry, Admission{}, ForkRequest{Parent: "s", At: 0, Child: "early"}); err != nil {
		t.Fatal(err)
	}
	if _, err := Fork(ctx, f.store, f.registry, Admission{}, ForkRequest{Parent: "s", At: 1, Child: "late"}); err != nil {
		t.Fatal(err)
	}
	notesOf := func(w Writer, sid session.SessionID) []string {
		state, _, err := w.Projections().Load(ctx, sid, alphaID, 1)
		if err != nil {
			t.Fatalf("load %s: %v", sid, err)
		}
		return state.(noteState).Notes
	}
	f.counter.reset()
	early, err := openWriter(ctx, f.store, f.registry, Admission{}, "early", session.OpenOptions{}, WritersConfig{Cache: cache})
	if err != nil {
		t.Fatal(err)
	}
	// The parent's entry ends at Next=2, past the anchor of "early" (Seq 0):
	// not usable, so the one inherited commit is folded.
	if got := notesOf(early, "early"); !sameNotes(got, []string{"one"}) || f.counter.get(alphaID) != 1 {
		t.Fatalf("early = %v folds=%d", got, f.counter.get(alphaID))
	}
	f.counter.reset()
	late, err := openWriter(ctx, f.store, f.registry, Admission{}, "late", session.OpenOptions{}, WritersConfig{Cache: cache})
	if err != nil {
		t.Fatal(err)
	}
	// "late" inherits exactly the two commits the parent's entry covers: the
	// entry seeds the projection and nothing is folded.
	if got := notesOf(late, "late"); !sameNotes(got, []string{"one", "two"}) || f.counter.get(alphaID) != 0 {
		t.Fatalf("late = %v folds=%d, want the cached state with no fold", got, f.counter.get(alphaID))
	}
}

// SES-GC-1/2, EXT-WRT-9: deleting a Session drops its root and releases the
// claims it owns; a fork that inherits its commits keeps reading them through
// its own prefix claim, and Collect reclaims only what no root reaches.
func TestDeleteReleasesClaimsAndKeepsInheritedPrefix(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	ref := artifact.Ref{Scheme: "cas", Authority: "local", Key: "k1", Durability: artifact.EventBound, Integrity: &artifact.Integrity{Algorithm: "sha256", Value: "x"}}
	binding, _ := artifact.NewBinding("b1", ref)
	if _, err := f.bindings.CreateBinding(ctx, binding); err != nil {
		t.Fatal(err)
	}
	parent := f.open(t, false)
	if _, err := parent.Commit(ctx, func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "c1", Batches: sessionBatch(TypedEvent{Type: tpfx("a") + "note", Value: notePayload{Text: "one", Refs: []string{"b1"}}})}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := parent.Commit(ctx, noteGroup("c2", "two")); err != nil {
		t.Fatal(err)
	}
	if _, err := Fork(ctx, f.store, f.registry, f.admission(), ForkRequest{Parent: "s", At: 0, Child: "child"}); err != nil {
		t.Fatal(err)
	}
	if err := parent.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := Delete(ctx, f.store, f.admission(), "s"); err != nil {
		t.Fatal(err)
	}
	if claims, _ := artifact.ActiveClaims(ctx, f.ledger, artifact.ClaimOwnerScope{Kind: ClaimOwnerKind, Authority: "s"}); len(claims) != 0 {
		t.Fatalf("parent commit claims after delete = %+v", claims)
	}
	if claims, _ := artifact.ActiveClaims(ctx, f.ledger, artifact.ClaimOwnerScope{Kind: ForkOwnerKind, Authority: "child"}); len(claims) != 1 {
		t.Fatalf("child fork claim after deleting the parent = %+v", claims)
	}
	if _, err := OpenWriter(ctx, f.store, f.registry, f.admission(), "s", session.OpenOptions{}); !session.IsCode(err, session.ErrNotFound) {
		t.Fatalf("open deleted = %v", err)
	}
	child, err := OpenWriter(ctx, f.store, f.registry, f.admission(), "child", session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	state, _, err := child.Projections().Load(ctx, "child", extension.ProjectionID(string(tpfx("a"))+"notes"), 1)
	if err != nil || len(state.(noteState).Notes) != 1 || state.(noteState).Notes[0] != "one" {
		t.Fatalf("child after deleting the parent = %+v %v", state, err)
	}
	report, err := Collect(ctx, f.store)
	if err != nil || len(report.Removed) != 0 || report.Truncated["s"] != 1 {
		t.Fatalf("collect = %+v %v, want the parent kept through commit 0", report, err)
	}
	if _, err := child.Commit(ctx, noteGroup("c3", "three")); err != nil {
		t.Fatal(err)
	}
	if err := child.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := Delete(ctx, f.store, f.admission(), "child"); err != nil {
		t.Fatal(err)
	}
	if claims, _ := artifact.ActiveClaims(ctx, f.ledger, artifact.ClaimOwnerScope{Kind: ForkOwnerKind, Authority: "child"}); len(claims) != 0 {
		t.Fatalf("fork claim after deleting the child = %+v", claims)
	}
	if report, err := Collect(ctx, f.store); err != nil || len(report.Removed) != 2 {
		t.Fatalf("final collect = %+v %v", report, err)
	}
}
