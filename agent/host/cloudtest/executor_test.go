package cloudtest

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/felinics/twilight/agent/host"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session/filestore"
	runmod "github.com/felinics/twilight/agent/session/run"
	"github.com/felinics/twilight/sdk"
)

// The executor process: the only place in this harness where a model or a
// tool is implemented. It serves a LocalExecutor over HTTP and posts Outcomes
// to every callback registered for an assignment -- the one Dispatch named
// and any a later Attach added -- so a superseded authority may receive a
// duplicate exactly as a real at-least-once transport would deliver one.

const (
	scriptedModelRef run.ModelRef = "scripted"
	gateToolRef      run.ToolRef  = "gate"
)

// gateDefinition is the tool's frozen public shape. The authority builds its
// AgentPreset from this alone; the implementation below never leaves the executor.
func gateDefinition() sdk.ToolDefinition {
	return sdk.ToolDefinition{Name: string(gateToolRef), Description: "blocks until released",
		Parameters: []byte(`{"type":"object","properties":{}}`)}
}

// scriptedModel asks for the gate tool once, then answers with the tool's
// status so a reader can tell a real result from an unknown one.
type scriptedModel struct{}

func (scriptedModel) Generate(_ context.Context, req sdk.Request) (sdk.ModelResult, error) {
	for i := len(req.Messages) - 1; i >= 0; i-- {
		msg := req.Messages[i]
		if msg.Role != sdk.MessageRoleTool {
			continue
		}
		status := "ok"
		for _, part := range msg.Content {
			if tr, ok := part.(sdk.ToolResultPart); ok && tr.IsError {
				status = "unknown"
			}
		}
		return sdk.ModelResult{Text: "answer: tool " + status, FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}, nil
	}
	return sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls, Usage: sdk.Usage{TotalTokens: 1},
		ToolCalls: []sdk.ToolCall{{ToolCallID: "c1", ToolName: string(gateToolRef), Input: `{}`}}}, nil
}

// gateTool blocks in Execute until the test releases it (or the executor
// cancels it), and reports when execution has begun.
type gateTool struct {
	startOnce   sync.Once
	releaseOnce sync.Once
	started     chan struct{}
	release     chan struct{}
}

func newGateTool() *gateTool {
	return &gateTool{started: make(chan struct{}), release: make(chan struct{})}
}

func (t *gateTool) Ref() run.ToolRef                          { return gateToolRef }
func (t *gateTool) Definition() sdk.ToolDefinition            { return gateDefinition() }
func (t *gateTool) ResponsePolicy() run.ResponsePolicy        { return run.DirectExecution }
func (t *gateTool) ValidateArguments(run.CanonicalJSON) error { return nil }
func (t *gateTool) Execute(ctx context.Context, _ loop.ToolExecutionRequest) loop.ToolExecutionOutcome {
	t.startOnce.Do(func() { close(t.started) })
	select {
	case <-t.release:
		return loop.ToolExecutionSucceeded{Result: run.ToolExecutionResult{Output: run.MustParseCanonicalJSON(`{"gate":"released"}`)}}
	case <-ctx.Done():
		return loop.ToolExecutionUnknown{Failure: run.ToolFailure{Class: run.FailureEffectUnknown, Message: "cancelled: " + ctx.Err().Error()}}
	}
}

func (t *gateTool) Release() { t.releaseOnce.Do(func() { close(t.release) }) }

type executorServer struct {
	exec   loop.Executor
	gate   *gateTool
	client *http.Client

	mu        sync.Mutex
	callbacks map[loop.AssignmentKey][]string
}

// deliverTo fans one Outcome out to every callback registered for key.
func (s *executorServer) deliverTo(key loop.AssignmentKey) loop.Deliver {
	return func(out loop.Outcome) {
		s.mu.Lock()
		cbs := s.callbacks[key]
		delete(s.callbacks, key)
		s.mu.Unlock()
		wire := encodeOutcome(out)
		var wg sync.WaitGroup
		for _, cb := range cbs {
			wg.Add(1)
			go func(cb string) {
				defer wg.Done()
				if err := postJSON(s.client, cb, wire, nil); err != nil {
					fmt.Fprintf(os.Stderr, "executor: outcome to %s: %v\n", cb, err)
				}
			}(cb)
		}
		wg.Wait()
	}
}

func (s *executorServer) addCallback(key loop.AssignmentKey, cb string) {
	s.mu.Lock()
	s.callbacks[key] = append(s.callbacks[key], cb)
	s.mu.Unlock()
}

func (s *executorServer) dropCallback(key loop.AssignmentKey, cb string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := s.callbacks[key][:0]
	for _, c := range s.callbacks[key] {
		if c != cb {
			kept = append(kept, c)
		}
	}
	if len(kept) == 0 {
		delete(s.callbacks, key)
	} else {
		s.callbacks[key] = kept
	}
}

func (s *executorServer) routes(mux *http.ServeMux) {
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, http.StatusOK, map[string]bool{"ok": true}) })
	mux.HandleFunc("/validate", func(w http.ResponseWriter, r *http.Request) {
		var a loop.Assignment
		if err := readJSON(r, &a); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		failure, err := s.exec.Validate(r.Context(), a)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"failure": failure})
	})
	mux.HandleFunc("/dispatch", func(w http.ResponseWriter, r *http.Request) {
		var req dispatchRequest
		if err := readJSON(r, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		key := req.Assignment.Key()
		s.addCallback(key, req.Callback)
		if err := s.exec.Dispatch(context.Background(), req.Assignment, s.deliverTo(key)); err != nil {
			s.dropCallback(key, req.Callback)
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/attach", func(w http.ResponseWriter, r *http.Request) {
		var req dispatchRequest
		if err := readJSON(r, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		key := req.Assignment.Key()
		s.addCallback(key, req.Callback)
		attached, err := s.exec.Attach(context.Background(), req.Assignment, s.deliverTo(key))
		if err != nil {
			s.dropCallback(key, req.Callback)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !attached {
			s.dropCallback(key, req.Callback)
		}
		writeJSON(w, http.StatusOK, map[string]bool{"attached": attached})
	})
	mux.HandleFunc("/cancel", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			RunID run.RunID `json:"runId"`
		}
		if err := readJSON(r, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.exec.Cancel(context.Background(), req.RunID); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	// Test control: has the gate tool started executing; release it.
	mux.HandleFunc("/started", func(w http.ResponseWriter, _ *http.Request) {
		select {
		case <-s.gate.started:
			w.WriteHeader(http.StatusOK)
		case <-time.After(5 * time.Second):
			http.Error(w, "gate tool has not started", http.StatusGatewayTimeout)
		}
	})
	mux.HandleFunc("/release", func(w http.ResponseWriter, _ *http.Request) {
		s.gate.Release()
		w.WriteHeader(http.StatusOK)
	})
}

// runExecutor is the executor role's main.
func runExecutor() int {
	root, listen := os.Getenv(envRoot), os.Getenv(envListen)
	content, err := filestore.NewContentStore(root, runmod.FrozenAuthority, filestore.ContentStoreOptions{})
	if err != nil {
		return fail(err)
	}
	gate := newGateTool()
	cat, err := host.NewCatalog(map[run.ModelRef]loop.ModelInvoker{scriptedModelRef: scriptedModel{}}, gate)
	if err != nil {
		return fail(err)
	}
	exec, err := host.NewLocalExecutor(cat, content, nil, false)
	if err != nil {
		return fail(err)
	}
	srv := &executorServer{exec: exec, gate: gate, client: &http.Client{Timeout: 15 * time.Second},
		callbacks: make(map[loop.AssignmentKey][]string)}
	mux := http.NewServeMux()
	srv.routes(mux)
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return fail(err)
	}
	go func() { _ = http.Serve(ln, mux) }()
	fmt.Println(readyLine)
	select {}
}

func fail(err error) int {
	fmt.Fprintln(os.Stderr, strings.TrimSpace(err.Error()))
	return 1
}
