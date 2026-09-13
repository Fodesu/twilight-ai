package cloudtest

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"

	"github.com/felinics/twilight/agent/decision"
	"github.com/felinics/twilight/agent/host"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/filestore"
	runmod "github.com/felinics/twilight/agent/session/run"
	"github.com/felinics/twilight/agent/turn"
)

// The authority process: the fact and decision layers over a shared file
// store, with the effect layer behind a remote Executor. It constructs no
// model client and no tool implementation; the Profile it registers names the
// model by ref and the tool by its frozen definition only.

// warnings collects Ports.Warn so a test can observe host-level failures --
// in particular a fenced late settlement -- through /status.
type warnings struct {
	mu   sync.Mutex
	list []string
}

func (w *warnings) add(err error) {
	w.mu.Lock()
	w.list = append(w.list, err.Error())
	w.mu.Unlock()
}

func (w *warnings) snapshot() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.list...)
}

type authorityServer struct {
	h    *host.Host
	sid  session.SessionID
	warn *warnings

	mu      sync.Mutex
	session *host.Session
}

func (a *authorityServer) current() *host.Session {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.session
}

func (a *authorityServer) routes(mux *http.ServeMux, remote *remoteExecutor) {
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, http.StatusOK, map[string]bool{"ok": true}) })
	mux.HandleFunc("/outcome", remote.handleOutcome)
	mux.HandleFunc("/submit", func(w http.ResponseWriter, r *http.Request) {
		s := a.current()
		if s == nil {
			http.Error(w, "session not open", http.StatusServiceUnavailable)
			return
		}
		var req struct {
			Text string `json:"text"`
		}
		if err := readJSON(r, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ref, err := s.Submit(r.Context(), req.Text)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"turnId": string(ref.TurnID)})
	})
	mux.HandleFunc("/resume", func(w http.ResponseWriter, _ *http.Request) {
		s := a.current()
		if s == nil {
			http.Error(w, "session not open", http.StatusServiceUnavailable)
			return
		}
		go func() {
			if _, _, err := s.Resume(context.Background()); err != nil {
				a.warn.add(fmt.Errorf("resume: %w", err))
			}
		}()
		w.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		s := a.current()
		if s == nil {
			http.Error(w, "session not open", http.StatusServiceUnavailable)
			return
		}
		status, err := s.Status(r.Context())
		reply := statusReply{Recovered: s.Recovered, Warnings: a.warn.snapshot()}
		if err != nil {
			// A fenced owner's Writer reports ownership_lost on every access;
			// that is an observation, so it travels in the reply.
			reply.Error = err.Error()
		} else {
			reply.Active, reply.Failed = status.Active, status.Failed
		}
		writeJSON(w, http.StatusOK, reply)
	})
	mux.HandleFunc("/projection", func(w http.ResponseWriter, r *http.Request) {
		raw, err := a.projection(r.Context(), r.URL.Query().Get("id"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(raw)
	})
}

type statusReply struct {
	Active    turn.TurnID   `json:"active"`
	Failed    []turn.TurnID `json:"failed"`
	Recovered int           `json:"recovered"`
	Warnings  []string      `json:"warnings"`
	// Error is set when the Session facade itself failed, as it does once the
	// owner has been fenced.
	Error string `json:"error,omitempty"`
}

// projection encodes a projection state with its own StateCodec: the same
// bytes an observer folding from the Store produces (EXT-PRJ-4).
func (a *authorityServer) projection(ctx context.Context, id string) ([]byte, error) {
	switch id {
	case "context":
		state, _, err := a.h.Projection(ctx, a.sid, chatlog.ContextProjectionID, chatlog.ContextProjection.Version)
		if err != nil {
			return nil, err
		}
		v, err := chatlog.ContextProjection.StateCodec.Encode(state)
		return v.Bytes(), err
	case "surface":
		state, _, err := a.h.Projection(ctx, a.sid, turn.SurfaceProjectionID, turn.SurfaceProjection.Version)
		if err != nil {
			return nil, err
		}
		v, err := turn.SurfaceProjection.StateCodec.Encode(state)
		return v.Bytes(), err
	}
	return nil, fmt.Errorf("unknown projection %q", id)
}

// cloudProfile is the decision identity the authority registers: model by
// ref, the gate tool by frozen definition, default planner and policy.
func cloudProfile() (turn.Profile, error) {
	def, err := run.FreezeToolDefinition(gateDefinition())
	if err != nil {
		return turn.Profile{}, err
	}
	p := turn.Profile{SchemaVersion: 1, Model: scriptedModelRef,
		Tools:   []turn.PublicTool{{Ref: gateToolRef, Definition: def, Policy: run.DirectExecution}},
		Planner: decision.PlannerContextV1, Policy: decision.PolicyDefaultV1}
	return p, turn.ValidateProfile(&p)
}

// runAuthority is the authority role's main.
func runAuthority() int {
	ctx := context.Background()
	root, listen := os.Getenv(envRoot), os.Getenv(envListen)
	sid := session.SessionID(os.Getenv(envSession))
	takeover := os.Getenv(envTakeover) == "1"

	store, err := filestore.New(root)
	if err != nil {
		return fail(err)
	}
	content, err := filestore.NewContentStore(root, runmod.FrozenAuthority, filestore.ContentStoreOptions{})
	if err != nil {
		return fail(err)
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return fail(err)
	}
	callback := "http://" + ln.Addr().String() + "/outcome"
	remote := newRemoteExecutor(os.Getenv(envExecutor), callback)
	warn := &warnings{}
	h, err := host.New(host.Ports{Store: store, Content: content, Executor: remote,
		Ownership: session.OpenOptions{Takeover: takeover}, Warn: warn.add})
	if err != nil {
		return fail(err)
	}
	profile, err := cloudProfile()
	if err != nil {
		return fail(err)
	}
	ref, err := h.Profiles.Register("cloud", profile)
	if err != nil {
		return fail(err)
	}
	a := &authorityServer{h: h, sid: sid, warn: warn}
	mux := http.NewServeMux()
	a.routes(mux, remote)
	// Serve before opening: a takeover's reattached Outcome arrives at the
	// callback endpoint, and the executor must be able to reach it.
	go func() { _ = http.Serve(ln, mux) }()
	go remote.monitor(ctx)

	s, err := h.OpenSession(ctx, sid, host.SessionOptions{Profile: ref})
	if err != nil {
		return fail(err)
	}
	a.mu.Lock()
	a.session = s
	a.mu.Unlock()
	fmt.Println(readyLine)
	select {}
}
