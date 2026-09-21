package owner_test

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agent/executor"
	executorlocal "github.com/felinics/twilight/agent/executor/local"
	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/owner"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/filestore/filestoretest"
	runmod "github.com/felinics/twilight/agent/session/run"
	"github.com/felinics/twilight/agent/store/sqlite/sqlitetest"
)

func newAuthority(t *testing.T) *owner.Owner {
	t.Helper()
	p := basePorts(t)
	return newAuthorityFrom(t, &p)
}

// basePorts is the deployment every owner test starts from: a local
// executor and fresh durable stores under t.TempDir().
func basePorts(t *testing.T) owner.Ports {
	t.Helper()
	catalog, err := executorlocal.NewCatalog(nil)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := executorlocal.NewLocalExecutor(catalog, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	exec, err := executor.NewWorker(context.Background(), sqlitetest.Open(t).Executions(), []executor.Route{executorlocal.Route(backend)})
	if err != nil {
		t.Fatal(err)
	}
	bindings, ledger := sqlitetest.Artifacts(t)
	return owner.Ports{Store: filestoretest.Store(t), Content: filestoretest.Content(t, runmod.FrozenAuthority),
		Artifacts: owner.Artifacts{Bindings: bindings, Ledger: ledger}, Executor: exec}
}

func newAuthorityFrom(t *testing.T, p *owner.Ports) *owner.Owner {
	t.Helper()
	a, err := owner.New(*p)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close(context.Background()) })
	return a
}

// OWN-HDL-1: one generation of ownership at a time. A second Open of an
// open Session is refused; a Handle whose generation was released does not
// close the generation that replaced it; reads need no Handle.
func TestHandleGenerations(t *testing.T) {
	ctx := context.Background()
	a := newAuthority(t)
	const sid session.SessionID = "s-gen"
	if err := a.CreateSession(ctx, sid, jsonstable.Value{}); err != nil {
		t.Fatal(err)
	}
	first, err := a.Open(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Open(ctx, sid); !errors.Is(err, owner.ErrSessionOpen) {
		t.Fatalf("second open = %v, want ErrSessionOpen", err)
	}
	if err := first.Close(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := a.Open(ctx, sid)
	if err != nil {
		t.Fatalf("open after close: %v", err)
	}
	// The stale Handle's Close releases nothing: the second generation keeps
	// its Writer and can still commit.
	if err := first.Close(ctx); err != nil {
		t.Fatalf("stale close = %v, want nil", err)
	}
	if _, err := a.Chatlog.Submit(ctx, second.Writer(), "in-1", "hello"); err != nil {
		t.Fatalf("commit through the live generation after a stale close: %v", err)
	}
	// Reading takes no ownership: it works by SessionID while the Handle is
	// open and after it is closed.
	if chat, err := chatlog.ReadSurface(ctx, a.Projections, sid); err != nil || chat.Inputs.Len() != 1 {
		t.Fatalf("read while open = %d %v", chat.Inputs.Len(), err)
	}
	if err := second.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if chat, err := chatlog.ReadSurface(ctx, a.Projections, sid); err != nil || chat.Inputs.Len() != 1 {
		t.Fatalf("read after close = %d %v", chat.Inputs.Len(), err)
	}
	// Reading did not reopen the Session: a third Open succeeds.
	third, err := a.Open(ctx, sid)
	if err != nil {
		t.Fatalf("open after reads: %v", err)
	}
	if err := third.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
