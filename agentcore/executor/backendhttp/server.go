// Package backendhttp is the wire between a Worker and a remote
// ExecutionBackend (CLD-WIR-1): Server exposes one backend over HTTP, Client
// implements executor.ExecutionBackend against such a server. The Go
// contract on both sides is executor.ExecutionBackend as it is; the wire
// alone splits Outcome into a read (/outcome, 204 while unsettled) and a
// notice stream (/notices), so no request is held open for the length of
// an execution, and the Client's blocking Outcome is built from the two.
//
// The Server keeps no ledger: an in-flight table of the refs it started and
// the Outcome each reached, in memory. A server that restarts forgets them,
// and the backend behind it answers Attach with missing for every ref it
// held, which is what the Worker's recovery expects of a stateless backend
// (RUN-EXE-3, RUN-EXE-9).
package backendhttp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	stdhttp "net/http"
	"strconv"
	"sync"
	"time"

	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/executor/protocol"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
)

// DefaultMaxBodyBytes bounds a request body when Server.MaxBodyBytes is zero.
const DefaultMaxBodyBytes int64 = 16 << 20

// Server exposes Backend over HTTP. Progress, when set, serves /progress:
// the hub the backend publishes its frames into (RUN-EXE-12). Every endpoint
// accepts POST only.
type Server struct {
	Backend executor.ExecutionBackend
	// Progress is the backend process's own hub, the ProgressSink its
	// backend publishes into; nil serves no progress.
	Progress     effect.ProgressPort
	MaxBodyBytes int64

	once    sync.Once
	epoch   string
	mu      sync.Mutex
	settled map[string]*settledRef
	notices *noticeHub
}

// settledRef is one ref the Server started: its Outcome once the backend's
// blocking read returned, or the read's error.
type settledRef struct {
	done    chan struct{}
	outcome effect.Outcome
	err     error
}

type refRequest struct {
	Ref string `json:"ref"`
}

type startRequest struct {
	Ref        string            `json:"ref"`
	Assignment effect.Assignment `json:"assignment"`
}

type restartRequest struct {
	Previous   string            `json:"previous"`
	Assignment effect.Assignment `json:"assignment"`
}

type refResponse struct {
	Ref string `json:"ref"`
}

type statusResponse struct {
	Status effect.ExecutionStatus `json:"status"`
}

type validateResponse struct {
	Failure *run.ToolFailure `json:"failure,omitempty"`
}

func (s *Server) validate(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req startRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	failure, err := s.Backend.Validate(r.Context(), req.Assignment)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, validateResponse{Failure: failure})
}

type progressRequest struct {
	Key   effect.AssignmentKey `json:"key"`
	After uint64               `json:"after"`
}

type noticesRequest struct {
	Epoch string `json:"epoch,omitempty"`
	After uint64 `json:"after"`
}

// Notice is the Server's word that a ref reached its Outcome: read it with
// /outcome. Epoch names the Server incarnation, Sequence orders notices in
// it; the Client resubscribes with its last position and re-reads every ref
// it waits on when the epoch changes or the ring evicted its position.
type Notice struct {
	Ref      string `json:"ref"`
	Epoch    string `json:"epoch"`
	Sequence uint64 `json:"sequence"`
}

func (s *Server) init() {
	s.once.Do(func() {
		s.epoch = strconv.FormatInt(time.Now().UnixNano(), 36)
		s.settled = make(map[string]*settledRef)
		s.notices = newNoticeHub(s.epoch, 0)
	})
}

// Handler routes the backend protocol.
func (s *Server) Handler() stdhttp.Handler {
	s.init()
	mux := stdhttp.NewServeMux()
	mux.HandleFunc("POST /validate", s.validate)
	mux.HandleFunc("POST /prepare", s.prepare)
	mux.HandleFunc("POST /start", s.start)
	mux.HandleFunc("POST /restart", s.restart)
	mux.HandleFunc("POST /attach", s.attach)
	mux.HandleFunc("POST /status", s.status)
	mux.HandleFunc("POST /outcome", s.outcome)
	mux.HandleFunc("POST /cancel", s.cancel)
	mux.HandleFunc("POST /progress", s.progress)
	mux.HandleFunc("POST /notices", s.noticeStream)
	return mux
}

func (s *Server) prepare(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req startRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	ref, err := s.Backend.Prepare(r.Context(), req.Assignment)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, refResponse{Ref: ref})
}

// start hands the ref to the backend and, once the backend accepted it (or
// could not say), begins the blocking Outcome read that /outcome and
// /notices answer from. A definite refusal is 400; an uncertain start is 202
// like an accepted one, because the Client cannot tell the two apart any
// better than the Server can (ErrDispatchUnknown is the backend's own word).
func (s *Server) start(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req startRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	err := s.Backend.Start(r.Context(), req.Ref, req.Assignment)
	if err != nil && !errors.Is(err, effect.ErrDispatchUnknown) {
		stdhttp.Error(w, err.Error(), stdhttp.StatusBadRequest)
		return
	}
	s.watch(req.Ref)
	if err != nil {
		// The Client maps 202 with this header to ErrDispatchUnknown.
		w.Header().Set("X-Twilight-Start", "unknown")
	}
	w.WriteHeader(stdhttp.StatusAccepted)
}

// watch begins the backend's blocking Outcome read for ref, once.
func (s *Server) watch(ref string) {
	s.mu.Lock()
	if _, ok := s.settled[ref]; ok {
		s.mu.Unlock()
		return
	}
	entry := &settledRef{done: make(chan struct{})}
	s.settled[ref] = entry
	s.mu.Unlock()
	go func() {
		out, err := s.Backend.Outcome(context.Background(), ref)
		s.mu.Lock()
		entry.outcome, entry.err = out, err
		close(entry.done)
		s.mu.Unlock()
		s.notices.record(ref)
	}()
}

func (s *Server) restart(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req restartRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	ref, err := s.Backend.Restart(r.Context(), req.Previous, req.Assignment)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, refResponse{Ref: ref})
}

func (s *Server) attach(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req refRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	attachment, err := s.Backend.Attach(r.Context(), req.Ref)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, attachment)
}

func (s *Server) status(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req refRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	status, err := s.Backend.Status(r.Context(), req.Ref)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, statusResponse{Status: status})
}

// outcome is the read: 200 with the Outcome once the backend's read
// returned, 204 while it has not, 404 for a ref this Server never started
// (a ref the backend may still know is asked about through /attach).
func (s *Server) outcome(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req refRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	s.mu.Lock()
	entry, ok := s.settled[req.Ref]
	s.mu.Unlock()
	if !ok {
		stdhttp.Error(w, "backendhttp: unknown ref", stdhttp.StatusNotFound)
		return
	}
	select {
	case <-entry.done:
	default:
		w.WriteHeader(stdhttp.StatusNoContent)
		return
	}
	s.mu.Lock()
	out, err := entry.outcome, entry.err
	s.mu.Unlock()
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, protocol.EncodeOutcome(out))
}

func (s *Server) cancel(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req refRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	if err := s.Backend.Cancel(r.Context(), req.Ref); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(stdhttp.StatusNoContent)
}

func (s *Server) progress(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req progressRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	if s.Progress == nil {
		sse(w)
		return
	}
	send := sse(w)
	_ = s.Progress.Progress(r.Context(), req.Key, req.After, func(f effect.ProgressFrame) bool { return send(f) })
}

func (s *Server) noticeStream(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req noticesRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	var send func(v any) bool
	err := s.notices.subscribe(r.Context(), req.Epoch, req.After, func(n Notice) bool {
		if send == nil {
			send = sse(w)
		}
		return send(n)
	})
	if errors.Is(err, errNoticesEvicted) && send == nil {
		stdhttp.Error(w, err.Error(), stdhttp.StatusGone)
		return
	}
	if send == nil {
		sse(w)
	}
}

func (s *Server) readJSON(w stdhttp.ResponseWriter, r *stdhttp.Request, out any) bool {
	limit := s.MaxBodyBytes
	if limit <= 0 {
		limit = DefaultMaxBodyBytes
	}
	r.Body = stdhttp.MaxBytesReader(w, r.Body, limit)
	if err := json.NewDecoder(r.Body).Decode(out); err != nil {
		var tooLarge *stdhttp.MaxBytesError
		if errors.As(err, &tooLarge) {
			stdhttp.Error(w, err.Error(), stdhttp.StatusRequestEntityTooLarge)
			return false
		}
		stdhttp.Error(w, err.Error(), stdhttp.StatusBadRequest)
		return false
	}
	return true
}

func writeJSON(w stdhttp.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(stdhttp.StatusOK)
	_ = json.NewEncoder(w).Encode(value)
}

// writeError maps the backend's definitive answers to statuses the Client
// maps back: 404 is effect.ErrExecutionNotFound (a ref the backend does not
// hold); anything else is 500 and reaches the Client as a read failure.
func writeError(w stdhttp.ResponseWriter, err error) {
	status := stdhttp.StatusInternalServerError
	if errors.Is(err, effect.ErrExecutionNotFound) {
		status = stdhttp.StatusNotFound
	}
	stdhttp.Error(w, err.Error(), status)
}

// sse opens a server-sent event response and returns the writer of one
// `data:` line, which reports false once the client is gone.
func sse(w stdhttp.ResponseWriter) func(v any) bool {
	flusher, _ := w.(stdhttp.Flusher)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(stdhttp.StatusOK)
	if flusher != nil {
		flusher.Flush()
	}
	return func(v any) bool {
		line, err := json.Marshal(v)
		if err != nil {
			return true
		}
		if _, err := fmt.Fprintf(w, "data: %s\n\n", line); err != nil {
			return false
		}
		if flusher != nil {
			flusher.Flush()
		}
		return true
	}
}
