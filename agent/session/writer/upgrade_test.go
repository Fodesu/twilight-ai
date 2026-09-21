package writer

import (
	"context"
	"testing"

	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
)

// EXT-WRT-1, SES-ADV-1: a registry that writes a later kernel version
// advances a Session's tip to that version when it opens it; an older
// registry refuses a tip past its version; history is never rewritten.
func TestOpenWriterAdvancesTipToRegistryVersion(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	f.store = session.NewMemoryStore(session.WithProfile(session.ProfileVariant(2)))
	if _, err := f.store.Create(ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: "s"}); err != nil {
		t.Fatal(err)
	}
	v1 := f.open(t, false)
	if _, err := v1.Commit(ctx, noteGroup("c1", "one")); err != nil {
		t.Fatal(err)
	}
	if err := v1.Close(ctx); err != nil {
		t.Fatal(err)
	}
	r2, err := extension.BuildRegistry(2, noteModule("a"))
	if err != nil {
		t.Fatal(err)
	}
	w, err := OpenWriter(ctx, f.store, r2, f.admission(), "s", session.OpenOptions{})
	if err != nil {
		t.Fatalf("open with a later registry: %v", err)
	}
	header := w.Header()
	if header.ProtocolVersion != 2 || header.Parent == nil || header.Parent.Seq != 0 {
		t.Fatalf("tip after open = %+v, want v2 anchored at commit 0", header)
	}
	if got := notes(t, w); len(got) != 1 || got[0] != "one" {
		t.Fatalf("projection after advance = %v, want the inherited note", got)
	}
	res, err := w.Commit(ctx, noteGroup("c2", "two"))
	if err != nil || res.Outcome != CommitApplied || res.Commit.Seq != 1 {
		t.Fatalf("commit on the advanced tip = %+v %v", res, err)
	}
	if res, err := w.Commit(ctx, noteGroup("c1", "one")); err != nil || res.Outcome != CommitAlreadyApplied {
		t.Fatalf("replay of an inherited commit = %+v %v, want already applied", res, err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	page, err := f.store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "s"})
	if err != nil || len(page.Commits) != 2 || page.Commits[0].Digest != page.Commits[1].PrevDigest {
		t.Fatalf("history after upgrade = %+v %v", page.Commits, err)
	}
	// The v1 registry now meets a v2 tip: refused before ownership is taken.
	if _, err := OpenWriter(ctx, f.store, f.registry, f.admission(), "s", session.OpenOptions{}); !session.IsCode(err, session.ErrUnsupportedProfile) {
		t.Fatalf("v1 registry over a v2 tip = %v, want unsupported profile", err)
	}
	if again, err := OpenWriter(ctx, f.store, r2, f.admission(), "s", session.OpenOptions{}); err != nil || again.Header().HeaderDigest != header.HeaderDigest {
		t.Fatalf("reopen at the same version must not advance again: %v", err)
	}
}
