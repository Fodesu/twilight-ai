// Package cloudtest is the process-level fixture of test-cloud-agent.md: the
// four components composed exactly as their binaries compose them, each
// behind its own HTTP server, sharing only the stores a deployment shares.
// It drives a conversation through the owner's command face and replaces
// the worker and the owner mid-execution.
package cloudtest_test

import (
	"context"
	"fmt"
	stdhttp "net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/app"
	ownerhttp "github.com/felinics/twilight/agent/app/http"
	"github.com/felinics/twilight/agent/component/modelbackend"
	"github.com/felinics/twilight/agent/component/ownerservice"
	"github.com/felinics/twilight/agent/component/toolbackend"
	"github.com/felinics/twilight/agent/component/worker"
	"github.com/felinics/twilight/agent/environment/local"
	executorlocal "github.com/felinics/twilight/agent/executor/local"
	"github.com/felinics/twilight/agent/tools"
	wshttp "github.com/felinics/twilight/agent/workspace/http"
	"github.com/felinics/twilight/agent/workspace/workspacetest"
	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	"github.com/felinics/twilight/agentcore/executor/store/storetest"
	"github.com/felinics/twilight/agentcore/inbox"
	"github.com/felinics/twilight/agentcore/inbox/inboxtest"
	"github.com/felinics/twilight/agentcore/owner"
	"github.com/felinics/twilight/agentcore/process/processtest"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore"
	runmod "github.com/felinics/twilight/agentcore/session/run"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/sdk"
)

// clock is the lease clock the worker and the execution store share.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// scripted answers requests from a queue, then "done".
type scripted struct {
	mu      sync.Mutex
	answers []sdk.ModelResult
	seen    int
}

func (m *scripted) Generate(_ context.Context, _ sdk.Request) (sdk.ModelResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seen++
	if len(m.answers) == 0 {
		return sdk.ModelResult{Text: "done", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}, nil
	}
	next := m.answers[0]
	m.answers = m.answers[1:]
	return next, nil
}

// gate blocks its first request until released: the model call in flight
// while the worker and the owner are replaced.
type gate struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *gate) Generate(ctx context.Context, _ sdk.Request) (sdk.ModelResult, error) {
	first := false
	g.once.Do(func() { first = true })
	if first {
		close(g.started)
		select {
		case <-g.release:
		case <-ctx.Done():
			return sdk.ModelResult{}, ctx.Err()
		}
		return sdk.ModelResult{Text: "late", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}, nil
	}
	return sdk.ModelResult{Text: "done", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}, nil
}

func shellCall(command string) sdk.ModelResult {
	return sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls, Usage: sdk.Usage{TotalTokens: 1},
		ToolCalls: []sdk.ToolCall{{ToolCallID: "c1", ToolName: string(tools.ShellRef), Input: sdk.ParseToolArguments(fmt.Sprintf(`{"command":%q}`, command))}}}
}

// proxy is the stable address of the worker Service: its target moves when
// a worker incarnation is replaced.
type proxy struct {
	mu     sync.Mutex
	target *url.URL
	rp     *httputil.ReverseProxy
}

func newProxy(target string) *proxy {
	p := &proxy{}
	p.rp = &httputil.ReverseProxy{FlushInterval: -1, Director: func(r *stdhttp.Request) {
		p.mu.Lock()
		t := p.target
		p.mu.Unlock()
		r.URL.Scheme, r.URL.Host, r.Host = t.Scheme, t.Host, t.Host
	}}
	p.point(target)
	return p
}

func (p *proxy) point(target string) {
	u, err := url.Parse(target)
	if err != nil {
		panic(err)
	}
	p.mu.Lock()
	p.target = u
	p.mu.Unlock()
}

func (p *proxy) ServeHTTP(w stdhttp.ResponseWriter, r *stdhttp.Request) { p.rp.ServeHTTP(w, r) }

// cluster is the deployment: shared stores, one model backend, one tool
// backend, the worker behind its Service address, and the owner(s).
type cluster struct {
	t        *testing.T
	ctx      context.Context
	clock    *clock
	records  *storetest.Map
	wsStore  *workspacetest.Map
	inbox    *inboxtest.Map
	sessions string
	content  string
	envRoot  string
	backends worker.Backends
	proxy    *proxy
	proxyURL string
	workers  int

	currentWorker *workerHandle
	// shutdown order: owners first (their streams end), then the Service
	// address, the workers, the backends.
	owners    []func()
	workerFn  []func()
	backends2 []func()
	proxyFn   func()
}

// shutdown closes the deployment in dependency order: a server is closed
// only after the clients holding streams to it are gone, so no Close waits
// on a connection that never ends.
func (c *cluster) shutdown() {
	for i := len(c.owners) - 1; i >= 0; i-- {
		c.owners[i]()
	}
	if c.proxyFn != nil {
		c.proxyFn()
	}
	for i := len(c.workerFn) - 1; i >= 0; i-- {
		c.workerFn[i]()
	}
	for i := len(c.backends2) - 1; i >= 0; i-- {
		c.backends2[i]()
	}
}

func newCluster(t *testing.T, model loop.ModelInvoker) *cluster {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	c := &cluster{t: t, ctx: ctx, clock: &clock{now: time.Unix(1_700_000_000, 0)}, wsStore: &workspacetest.Map{}, inbox: &inboxtest.Map{},
		sessions: filepath.Join(t.TempDir(), "sessions"), content: filepath.Join(t.TempDir(), "content"), envRoot: filepath.Join(t.TempDir(), "envs")}
	c.records = storetest.NewMap(c.clock.Now)
	// model backend
	catalog, err := executorlocal.NewCatalog(map[run.ModelRef]loop.ModelInvoker{"m-1": model})
	if err != nil {
		t.Fatal(err)
	}
	mb, err := modelbackend.New(catalog, false)
	if err != nil {
		t.Fatal(err)
	}
	mbServer := httptest.NewServer(mb.Handler())
	c.backends2 = append(c.backends2, func() { mbServer.CloseClientConnections(); mbServer.Close() })
	// tool backend
	provider, err := local.New(c.envRoot)
	if err != nil {
		t.Fatal(err)
	}
	tb, err := toolbackend.New(toolbackend.Options{Workspaces: c.wsStore, Provider: provider, Backend: local.Backend})
	if err != nil {
		t.Fatal(err)
	}
	tbServer := httptest.NewServer(tb.Handler())
	c.backends2 = append(c.backends2, func() { tbServer.CloseClientConnections(); tbServer.Close(); _ = tb.Close(context.Background()) })
	c.backends = worker.Backends{Model: mbServer.URL, Tool: tbServer.URL}
	// the first worker behind the Service address
	first := c.startWorker()
	c.proxy = newProxy(first)
	proxyServer := httptest.NewServer(c.proxy)
	c.proxyFn = func() { proxyServer.CloseClientConnections(); proxyServer.Close() }
	c.proxyURL = proxyServer.URL
	t.Cleanup(c.shutdown)
	return c
}

// startWorker composes a worker incarnation over the shared records and
// returns its address; Close of the previous incarnation is the caller's.
func (c *cluster) startWorker() string {
	c.t.Helper()
	c.workers++
	w, err := worker.New(c.ctx, worker.Options{ID: fmt.Sprintf("worker-%d", c.workers), Executions: c.records,
		Routes: worker.Routes(c.backends), Lease: 30 * time.Second, Clock: c.clock.Now})
	if err != nil {
		c.t.Fatal(err)
	}
	server := httptest.NewServer(w.Handler())
	handle := &workerHandle{component: w, server: server}
	c.workerFn = append(c.workerFn, handle.stop)
	c.currentWorker = handle
	return server.URL
}

type workerHandle struct {
	component *worker.Component
	server    *httptest.Server
	once      sync.Once
}

// stop kills the incarnation: its server drops its connections and stops,
// its Worker stops renewing; the records keep their leases until expiry.
func (h *workerHandle) stop() {
	h.once.Do(func() {
		h.server.CloseClientConnections()
		h.server.Close()
		_ = h.component.Close(context.Background())
	})
}

// stopWorker kills the current incarnation.
func (c *cluster) stopWorker() { c.currentWorker.stop() }

// startOwner composes an owner incarnation over the shared stores.
func (c *cluster) startOwner(id string, takeover bool) (*ownerservice.Component, *ownerhttp.Client) {
	c.t.Helper()
	store, err := filestore.New(c.sessions)
	if err != nil {
		c.t.Fatal(err)
	}
	content, err := filestore.NewContentStore(c.content, runmod.FrozenAuthority, filestore.ContentStoreOptions{})
	if err != nil {
		c.t.Fatal(err)
	}
	bindings, ledger := artifacttest.Stores(c.t)
	defs, err := app.WorkspaceTools(nil)
	if err != nil {
		c.t.Fatal(err)
	}
	preset, err := app.NewPreset("m-1", nil, app.WithSystemPrompt("be brief"), app.WithPublicTools(defs...))
	if err != nil {
		c.t.Fatal(err)
	}
	a, err := app.Build(app.Config{
		Store: store, Content: content, Artifacts: owner.Artifacts{Bindings: bindings, Ledger: ledger},
		Processes: &processtest.Map{}, Inbox: c.inbox,
		Executor:   app.ExecutorConfig{Mode: app.ExecutorRemote, Endpoint: c.proxyURL},
		Workspaces: &app.WorkspaceConfig{Store: c.wsStore, Snapshots: &wshttp.Client{BaseURL: c.backends.Tool}, SnapshotAfterTurn: true},
		Ownership:  session.OpenOptions{Owner: id, LeaseDuration: time.Minute, Takeover: takeover, Clock: c.clock.Now},
		Presets:    []app.Preset{{ID: "ws", Value: preset}},
		Warn:       func(err error) { c.t.Logf("%s: warn: %v", id, err) },
	})
	if err != nil {
		c.t.Fatal(err)
	}
	comp := ownerservice.New(a, app.SessionOptions{InboxPoll: 100 * time.Millisecond})
	server := httptest.NewServer(comp.Handler())
	c.owners = append(c.owners, func() {
		_ = comp.Close(context.Background())
		server.CloseClientConnections()
		server.Close()
	})
	return comp, &ownerhttp.Client{BaseURL: server.URL}
}

func (c *cluster) enqueue(client *ownerhttp.Client, sid session.SessionID, id string, kind inbox.Kind, payload any) inbox.Result {
	c.t.Helper()
	cmd, err := app.NewCommand(inbox.CommandID(id), kind, payload)
	if err != nil {
		c.t.Fatal(err)
	}
	if _, err := client.Enqueue(c.ctx, sid, cmd); err != nil {
		c.t.Fatal(err)
	}
	r, err := client.Await(c.ctx, sid, inbox.CommandID(id))
	if err != nil {
		c.t.Fatal(err)
	}
	return r
}

// awaitTurn polls the face until the Session's first Turn is completed and
// returns it with its reply.
func awaitTurn(t *testing.T, ctx context.Context, client *ownerhttp.Client, sid session.SessionID) ownerhttp.TurnResponse {
	t.Helper()
	for {
		surface, err := client.Turns(ctx, sid)
		if err != nil {
			t.Fatal(err)
		}
		if len(surface.Order) > 0 {
			view, err := client.Turn(ctx, sid, surface.Order[0])
			if err != nil {
				t.Fatal(err)
			}
			if view.Turn.Status == turn.TurnCompleted {
				return view
			}
			if view.Turn.Status != turn.TurnActive {
				t.Fatalf("turn ended %s", view.Turn.Status)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("turn did not complete")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// A conversation crosses all four components: the owner's inbox takes the
// commands, the worker dispatches the model call to the model backend and
// the shell call to the tool backend, the file lands in the workspace's
// environment, the workspace is snapshotted after the Turn, and the reply
// reads back through the face (CLD-CMP-2, Dispatch and GetOutcome).
func TestFourComponentsServeAConversation(t *testing.T) {
	model := &scripted{answers: []sdk.ModelResult{shellCall("printf hi > f.txt && cat f.txt")}}
	c := newCluster(t, model)
	_, client := c.startOwner("owner-a", false)
	const sid session.SessionID = "s-1"
	if err := client.Ensure(c.ctx, sid); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Open(c.ctx, sid, ownerhttp.OpenRequest{Preset: "ws"}); err != nil {
		t.Fatal(err)
	}
	ws, err := client.AllocateWorkspace(c.ctx, ownerhttp.AllocateWorkspaceRequest{Project: "repo"})
	if err != nil {
		t.Fatal(err)
	}
	if r := c.enqueue(client, sid, "bind", app.CommandBindWorkspace, app.BindWorkspaceCommand{WorkspaceID: ws.ID}); r.Status != inbox.StatusApplied {
		t.Fatalf("bind = %+v", r)
	}
	if r := c.enqueue(client, sid, "submit", app.CommandSubmit, app.SubmitCommand{InputID: "in-1", Text: "write hi"}); r.Status != inbox.StatusApplied {
		t.Fatalf("submit = %+v", r)
	}
	view := awaitTurn(t, c.ctx, client, sid)
	if view.Reply != "done" {
		t.Fatalf("reply = %q", view.Reply)
	}
	stored, err := c.wsStore.Get(c.ctx, ws.ID)
	if err != nil || stored.Runtime == nil || stored.Snapshot == nil {
		t.Fatalf("workspace after the turn = %+v %v, want an environment and a snapshot", stored, err)
	}
	if data, err := os.ReadFile(filepath.Join(c.envRoot, string(stored.Runtime.EnvironmentRef), "f.txt")); err != nil || string(data) != "hi" {
		t.Fatalf("file in the tool backend's environment = %q %v", data, err)
	}
	if owned, err := c.records.ListOwned(c.ctx, "worker-1"); err != nil || len(owned) != 3 {
		t.Fatalf("execution records leased by the worker = %d %v, want two model calls and the tool call", len(owned), err)
	}
	if b, err := client.Workspace(c.ctx, sid); err != nil || b.Snapshot == "" || b.Snapshot != *stored.Snapshot {
		t.Fatalf("binding snapshot = %+v %v, want %s", b, err, *stored.Snapshot)
	}
}

// The worker and the owner are replaced while a model call is in flight
// (CLD-DEV-2): the first worker dies holding the record's lease, a second
// incarnation takes the Service address, a second owner takes the Session
// over; its takeover disposition finds the record orphaned, hands it to the
// new worker, which attaches the execution still running in the model
// backend; the release of the model call settles through the new worker
// and the new owner completes the Turn. The old owner's later writes are
// fenced.
func TestWorkerAndOwnerReplacementMidExecution(t *testing.T) {
	g := &gate{started: make(chan struct{}), release: make(chan struct{})}
	c := newCluster(t, g)
	ownerA, clientA := c.startOwner("owner-a", false)
	const sid session.SessionID = "s-2"
	if err := clientA.Ensure(c.ctx, sid); err != nil {
		t.Fatal(err)
	}
	if _, err := clientA.Open(c.ctx, sid, ownerhttp.OpenRequest{Preset: "ws"}); err != nil {
		t.Fatal(err)
	}
	if r := c.enqueue(clientA, sid, "submit", app.CommandSubmit, app.SubmitCommand{InputID: "in-1", Text: "hello"}); r.Status != inbox.StatusApplied {
		t.Fatalf("submit = %+v", r)
	}
	select {
	case <-g.started:
	case <-c.ctx.Done():
		t.Fatal("the model call never reached the model backend")
	}
	// The worker dies; its lease outlives it until the clock passes it.
	c.stopWorker()
	c.clock.Advance(2 * time.Minute)
	c.proxy.point(c.startWorker())
	// A second owner takes the Session over and runs the takeover
	// disposition against the new worker.
	_, clientB := c.startOwner("owner-b", true)
	opened, err := clientB.Open(c.ctx, sid, ownerhttp.OpenRequest{Preset: "ws"})
	if err != nil {
		t.Fatal(err)
	}
	if opened.Active == "" {
		t.Fatalf("takeover open = %+v, want the active turn", opened)
	}
	lease, err := clientB.Lease(c.ctx, sid)
	if err != nil || !lease.Held || lease.Lease.Owner != "owner-b" {
		t.Fatalf("lease after takeover = %+v %v", lease, err)
	}
	close(g.release)
	view := awaitTurn(t, c.ctx, clientB, sid)
	if view.Reply != "late" {
		t.Fatalf("reply after the replacement = %q, want the in-flight call's result", view.Reply)
	}
	if owned, err := c.records.ListOwned(c.ctx, "worker-2"); err != nil || len(owned) != 1 {
		t.Fatalf("records leased by the new worker = %d %v, want the recovered model call", len(owned), err)
	}
	// The superseded owner cannot write: its command application is fenced.
	if opened, ok := ownerA.App.Opened(sid); ok {
		if _, err := opened.SubmitInput(c.ctx, "in-2", "again"); err == nil || !strings.Contains(strings.ToLower(err.Error()), "ownership") {
			t.Fatalf("write by the superseded owner = %v, want ownership lost", err)
		}
	}
}
