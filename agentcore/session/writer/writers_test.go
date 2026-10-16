package writer

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/session"
)

type bindReadFailure struct{ session.Store }

func (s bindReadFailure) ReadCommits(context.Context, session.CommitReadRequest) (session.CommitPage, error) {
	return session.CommitPage{}, errInjected
}

func TestBindAndCloseDoNotReleaseCallerHandle(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	h, err := f.store.Open(ctx, "s", session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(ctx)
	factory := &Factory{Store: f.store, Registry: f.registry, Admission: f.admission()}
	w, err := factory.Bind(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Commit(ctx, noteGroup("closed", "closed")); err == nil {
		t.Fatal("closed Writer accepted a commit")
	}
	if _, err := f.store.Open(ctx, "s", session.OpenOptions{}); !session.IsCode(err, session.ErrOwned) {
		t.Fatalf("Close released caller handle: %v", err)
	}
	if err := h.Renew(ctx); err != nil {
		t.Fatal(err)
	}
	// A failure while rebuilding Writer state also leaves the caller's
	// capability intact, so the same handle can be bound again.
	factory.Store = bindReadFailure{Store: f.store}
	if _, err := factory.Bind(ctx, h); !errors.Is(err, errInjected) {
		t.Fatalf("failed Bind = %v", err)
	}
	if err := h.Renew(ctx); err != nil {
		t.Fatalf("failed Bind released handle: %v", err)
	}
	factory.Store = f.store
	again, err := factory.Bind(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close(ctx)
	if res, err := again.Commit(ctx, noteGroup("c1", "one")); err != nil || res.Outcome != CommitApplied {
		t.Fatalf("rebind same handle: %+v %v", res, err)
	}
}
