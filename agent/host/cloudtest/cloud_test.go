//go:build !windows

package cloudtest

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/decision"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/extension"
	"github.com/felinics/twilight/agent/session/filestore"
	runmod "github.com/felinics/twilight/agent/session/run"
	"github.com/felinics/twilight/agent/turn"
)

const (
	envRole     = "TWILIGHT_ROLE"
	envRoot     = "TWILIGHT_ROOT"
	envSession  = "TWILIGHT_SESSION"
	envListen   = "TWILIGHT_LISTEN"
	envExecutor = "TWILIGHT_EXECUTOR"
	envTakeover = "TWILIGHT_TAKEOVER"
	readyLine   = "READY"
	sid         = session.SessionID("cloud")
)

// TestMain turns the test binary into a role when asked, otherwise runs the
// tests, which spawn the roles.
func TestMain(m *testing.M) {
	switch os.Getenv(envRole) {
	case "authority":
		os.Exit(runAuthority())
	case "executor":
		os.Exit(runExecutor())
	}
	os.Exit(m.Run())
}

// --- process control --------------------------------------------------------------

type proc struct {
	t    *testing.T
	role string
	addr string
	cmd  *exec.Cmd
	done chan struct{}
	logs bytes.Buffer
	mu   sync.Mutex
}

// freeAddr reserves a loopback address by binding and releasing it; a role
// (or its replacement) then listens on it.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// start re-executes the test binary as role and waits for its READY line.
func start(t *testing.T, role, addr string, env map[string]string) *proc {
	t.Helper()
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), envRole+"="+role, envListen+"="+addr)
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	p := &proc{t: t, role: role, addr: addr, cmd: cmd, done: make(chan struct{})}
	cmd.Stderr = &lockedWriter{p: p}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if line == readyLine {
				close(ready)
				continue
			}
			p.log(line)
		}
	}()
	go func() {
		_ = cmd.Wait()
		close(p.done)
	}()
	t.Cleanup(func() {
		p.kill()
		if t.Failed() {
			t.Logf("%s@%s log:\n%s", role, addr, p.dump())
		}
	})
	select {
	case <-ready:
	case <-p.done:
		t.Fatalf("%s exited before READY:\n%s", role, p.dump())
	case <-time.After(15 * time.Second):
		t.Fatalf("%s did not become ready:\n%s", role, p.dump())
	}
	return p
}

type lockedWriter struct{ p *proc }

func (w *lockedWriter) Write(b []byte) (int, error) {
	w.p.log(strings.TrimRight(string(b), "\n"))
	return len(b), nil
}

func (p *proc) log(line string) {
	p.mu.Lock()
	p.logs.WriteString(line + "\n")
	p.mu.Unlock()
}

func (p *proc) dump() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.logs.String()
}

func (p *proc) url() string { return "http://" + p.addr }

// kill is SIGKILL: the process dies mid-effect with nothing written on the
// way down.
func (p *proc) kill() {
	select {
	case <-p.done:
		return
	default:
	}
	_ = p.cmd.Process.Kill()
	<-p.done
}

// freeze and thaw are SIGSTOP/SIGCONT: the process keeps every goroutine and
// socket but makes no progress until thawed.
func (p *proc) freeze() {
	if err := p.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		p.t.Fatal(err)
	}
}

func (p *proc) thaw() {
	if err := p.cmd.Process.Signal(syscall.SIGCONT); err != nil {
		p.t.Fatal(err)
	}
}

var httpc = &http.Client{Timeout: 10 * time.Second}

func startExecutor(t *testing.T, root, addr string) *proc {
	return start(t, "executor", addr, map[string]string{envRoot: root})
}

func startAuthority(t *testing.T, root, addr, executorURL string, takeover bool) *proc {
	env := map[string]string{envRoot: root, envSession: string(sid), envExecutor: executorURL}
	if takeover {
		env[envTakeover] = "1"
	}
	return start(t, "authority", addr, env)
}

func submit(t *testing.T, a *proc, text string) turn.TurnID {
	t.Helper()
	var resp struct {
		TurnID turn.TurnID `json:"turnId"`
	}
	if err := postJSON(httpc, a.url()+"/submit", map[string]string{"text": text}, &resp); err != nil {
		t.Fatalf("submit: %v", err)
	}
	return resp.TurnID
}

func status(t *testing.T, a *proc) statusReply {
	t.Helper()
	var out statusReply
	if err := getJSON(httpc, a.url()+"/status", &out); err != nil {
		t.Fatalf("status: %v", err)
	}
	return out
}

func waitToolStarted(t *testing.T, e *proc) {
	t.Helper()
	if err := getJSON(httpc, e.url()+"/started", nil); err != nil {
		t.Fatalf("gate tool never started: %v", err)
	}
}

func release(t *testing.T, e *proc) {
	t.Helper()
	if err := postJSON(httpc, e.url()+"/release", struct{}{}, nil); err != nil {
		t.Fatalf("release: %v", err)
	}
}

// gateStats reads how many times the role's gate tool has begun executing.
// The crash scenario uses it to prove the replacement settled by adoption
// rather than by re-running the tool.
func gateStats(t *testing.T, e *proc) int {
	t.Helper()
	var out struct {
		Starts int `json:"starts"`
	}
	if err := getJSON(httpc, e.url()+"/gate-stats", &out); err != nil {
		t.Fatalf("gate-stats: %v", err)
	}
	return out.Starts
}

// --- observer -----------------------------------------------------------------------

// observer reads the shared store directly, the way any process without
// ownership may (SES-OWN-4), and folds projections with the same engine the
// owner uses (EXT-PRJ-4).
type observer struct {
	t        *testing.T
	store    *filestore.Store
	registry *extension.Registry
	reader   extension.ProjectionReader
}

func newObserver(t *testing.T, root string) *observer {
	t.Helper()
	store, err := filestore.New(root)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := extension.BuildRegistry(session.ProtocolVersion1, chatlog.Module, runmod.Module, turn.Module)
	if err != nil {
		t.Fatal(err)
	}
	return &observer{t: t, store: store, registry: registry, reader: extension.NewProjectionReader(store, registry, nil)}
}

func (o *observer) rows() []session.SessionEvent {
	page, err := o.store.Read(context.Background(), session.ReadRequest{SessionID: sid})
	if err != nil {
		if session.IsCode(err, session.ErrNotFound) {
			return nil
		}
		o.t.Fatalf("read: %v", err)
	}
	return page.Events
}

func (o *observer) has(typ session.EventType) bool {
	for _, r := range o.rows() {
		if r.Type == typ {
			return true
		}
	}
	return false
}

// toolSettlements classifies every tool settlement fact in the stream.
func (o *observer) toolSettlements() (completed, knownFailed, unknown int) {
	for _, r := range o.rows() {
		switch r.Type {
		case runmod.Prefix + "tool_call_completed":
			completed++
		case runmod.Prefix + "tool_call_failed":
			d, err := o.registry.Decode(r)
			if err != nil {
				o.t.Fatal(err)
			}
			if f, ok := d.Value.(runmod.Event).Fact.(run.ToolCallFailed); ok && f.Outcome == run.ToolOutcomeUnknown {
				unknown++
			} else {
				knownFailed++
			}
		}
	}
	return
}

// reply is the last assistant text of the folded Context.
func (o *observer) reply() string {
	state, _, err := o.reader.Load(context.Background(), sid, chatlog.ContextProjectionID, chatlog.ContextProjection.Version)
	if err != nil {
		o.t.Fatalf("context: %v", err)
	}
	entries := state.(chatlog.Context).Entries
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].Kind == chatlog.EntryAssistant {
			return decision.PartsText(entries[i].Assistant.Parts)
		}
	}
	return ""
}

func (o *observer) projection(id string) []byte {
	var (
		state any
		err   error
		raw   []byte
	)
	switch id {
	case "context":
		state, _, err = o.reader.Load(context.Background(), sid, chatlog.ContextProjectionID, chatlog.ContextProjection.Version)
		if err == nil {
			v, e := chatlog.ContextProjection.StateCodec.Encode(state)
			raw, err = v.Bytes(), e
		}
	case "surface":
		state, _, err = o.reader.Load(context.Background(), sid, turn.SurfaceProjectionID, turn.SurfaceProjection.Version)
		if err == nil {
			v, e := turn.SurfaceProjection.StateCodec.Encode(state)
			raw, err = v.Bytes(), e
		}
	}
	if err != nil {
		o.t.Fatalf("observer projection %s: %v", id, err)
	}
	return raw
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// --- scenarios ----------------------------------------------------------------------

// A crashed executor settles the effect it was running as Unknown, and the
// Run goes on once an executor is back. The replacement adopts the durable
// execution record and settles Unknown without re-running the tool
// (TRN-DUR-4: Unknown is a fact, not a retry; the model sees it and answers).
func TestExecutorCrashSettlesUnknown(t *testing.T) {
	root := t.TempDir()
	execAddr := freeAddr(t)
	e1 := startExecutor(t, root, execAddr)
	a := startAuthority(t, root, freeAddr(t), e1.url(), false)
	obs := newObserver(t, root)

	turnID := submit(t, a, "go")
	waitToolStarted(t, e1)
	e1.kill()

	// The durable Execution Record outlives the process: the replacement
	// adopts the orphaned lease and settles Unknown instead of re-dispatching
	// an unbound tool (TRN-DUR-4). The lost dispatch ack left the call
	// Executing at an unknown boundary (RUN-EXE-3); /resume re-drives, the
	// drive quiesces with recovery pending, and the host's recovery
	// disposition re-attaches and reads the settled record (RUN-CMT-7). The
	// outcome reaches the model as an unknown tool result.
	e2 := startExecutor(t, root, execAddr)
	if err := postJSON(httpc, a.url()+"/resume", struct{}{}, nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the Unknown settlement", 10*time.Second, func() bool {
		_, _, unknown := obs.toolSettlements()
		return unknown == 1
	})
	if n := gateStats(t, e2); n != 0 {
		t.Fatalf("replacement executor started the gate %d times; adoption must not re-dispatch an unbound tool", n)
	}
	waitFor(t, "turn completion", 10*time.Second, func() bool { return obs.has(turn.TypeCompleted) })
	if got := obs.reply(); got != "answer: tool unknown" {
		t.Fatalf("reply = %q, want the model to see an unknown tool result", got)
	}
	completed, known, unknown := obs.toolSettlements()
	if completed != 0 || known != 0 || unknown != 1 {
		t.Fatalf("settlements completed=%d known=%d unknown=%d", completed, known, unknown)
	}
	if st := status(t, a); st.Active != "" {
		t.Fatalf("turn %s still active after completion: %+v", turnID, st)
	}
}

// takeover runs the shared part of the takeover scenarios: A starts a Turn
// whose tool is executing on E, A is stopped (killed or frozen), B takes the
// Session over, reattaches to the running tool, and finishes the Turn with the
// real result once the tool is released.
func takeover(t *testing.T, freeze bool) (root string, a, b, e *proc, obs *observer) {
	t.Helper()
	root = t.TempDir()
	e = startExecutor(t, root, freeAddr(t))
	a = startAuthority(t, root, freeAddr(t), e.url(), false)
	obs = newObserver(t, root)

	submit(t, a, "go")
	waitToolStarted(t, e)
	if freeze {
		a.freeze()
	} else {
		a.kill()
	}

	b = startAuthority(t, root, freeAddr(t), e.url(), true)
	st := status(t, b)
	if st.Recovered != 0 {
		t.Fatalf("takeover disposed %d targets; the running tool should have been reattached", st.Recovered)
	}
	if st.Active == "" {
		t.Fatal("takeover found no active turn")
	}
	if _, _, unknown := obs.toolSettlements(); unknown != 0 {
		t.Fatal("reattached call was settled Unknown")
	}

	release(t, e)
	waitFor(t, "turn completion after takeover", 10*time.Second, func() bool { return obs.has(turn.TypeCompleted) })
	if got := obs.reply(); got != "answer: tool ok" {
		t.Fatalf("reply = %q, want the real tool result", got)
	}
	completed, known, unknown := obs.toolSettlements()
	if completed != 1 || known != 0 || unknown != 0 {
		t.Fatalf("settlements completed=%d known=%d unknown=%d", completed, known, unknown)
	}
	return root, a, b, e, obs
}

// A killed authority's running tool survives in the executor; the new owner
// reattaches and receives the real result (TRN-DUR-1, RUN-CMT-7).
func TestAuthorityCrashReattachesRunningTool(t *testing.T) {
	takeover(t, false)
}

// A frozen authority thaws after the takeover and receives the duplicate
// Outcome the executor still owed its old callback; its settlement is fenced
// by the Epoch and nothing of it reaches the stream (SES-OWN-2).
func TestFencedOwnerLateSettlementIsRejected(t *testing.T) {
	_, a, _, _, obs := takeover(t, true)
	a.thaw()
	// The thawed owner learns of the fence from the first write it attempts:
	// the duplicate Outcome's settlement. From then on its Writer reports
	// ownership_lost on every access, and the failed drive is warned about.
	waitFor(t, "the old owner to report ownership loss", 10*time.Second, func() bool {
		st := status(t, a)
		if strings.Contains(st.Error, "ownership_lost") {
			return true
		}
		for _, w := range st.Warnings {
			if strings.Contains(w, "ownership") {
				return true
			}
		}
		return false
	})
	st := status(t, a)
	t.Logf("fenced owner: error=%q warnings=%v", st.Error, st.Warnings)
	completed, known, unknown := obs.toolSettlements()
	if completed != 1 || known != 0 || unknown != 0 {
		t.Fatalf("late settlement changed the stream: completed=%d known=%d unknown=%d", completed, known, unknown)
	}
	rows := obs.rows()
	waitFor(t, "stream to stay unchanged", 500*time.Millisecond, func() bool { return len(obs.rows()) == len(rows) })
}

// An observer with no ownership folds the same projections the owner
// serves, byte for byte (SES-OWN-4, EXT-PRJ-4).
func TestObserverProjectionsMatchTheOwner(t *testing.T) {
	_, _, b, _, obs := takeover(t, false)
	for _, id := range []string{"context", "surface"} {
		resp, err := httpc.Get(b.url() + "/projection?id=" + id)
		if err != nil {
			t.Fatal(err)
		}
		var owner bytes.Buffer
		_, _ = owner.ReadFrom(resp.Body)
		_ = resp.Body.Close()
		mine := obs.projection(id)
		if !bytes.Equal(bytes.TrimSpace(owner.Bytes()), bytes.TrimSpace(mine)) {
			t.Fatalf("%s projection differs between owner and observer:\nowner:    %s\nobserver: %s", id, owner.String(), mine)
		}
	}
}

// The authority's source never names a model or tool implementation: the
// role only knows refs and frozen definitions.
func TestAuthoritySourceHoldsNoImplementations(t *testing.T) {
	src, err := os.ReadFile(filepath.Join(".", "authority_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"scriptedModel{", "newGateTool", "NewCatalog", "NewLocalExecutor", "loop.ModelInvoker", "loop.ExecutableTool"} {
		if bytes.Contains(src, []byte(forbidden)) {
			t.Fatalf("authority_test.go references %q", forbidden)
		}
	}
	fmt.Fprintln(os.Stderr) // keep fmt imported for role mains sharing this package
}
