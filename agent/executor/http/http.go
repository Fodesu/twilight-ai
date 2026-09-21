package http

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"strings"

	"github.com/felinics/twilight/agent/executor"
	"github.com/felinics/twilight/agent/executor/protocol"
	"github.com/felinics/twilight/agent/executor/store"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/effect"
)

// Client is a message-oriented Executor over HTTP. It contains no callback
// endpoint: GetOutcome is a long-polling/read operation and can be retried
// after any network failure.
type Client struct {
	BaseURL string
	HTTP    *stdhttp.Client
}

func (c *Client) client() *stdhttp.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return stdhttp.DefaultClient
}

func (c *Client) Validate(ctx context.Context, a effect.Assignment) (*run.ToolFailure, error) {
	var response struct {
		Failure *run.ToolFailure `json:"failure"`
	}
	if err := c.post(ctx, "/validate", makeAssignmentRequest(a), &response); err != nil {
		return nil, err
	}
	return response.Failure, nil
}

func (c *Client) Dispatch(ctx context.Context, a effect.Assignment) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := c.post(ctx, "/dispatch", makeAssignmentRequest(a), nil)
	var responseErr *responseError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &responseErr) && responseErr.statusCode < stdhttp.StatusInternalServerError:
		// 4xx: the server answered and refused the Assignment; nothing
		// started (RUN-EXE-3).
		return err
	case errors.As(err, &responseErr) && responseErr.statusCode == stdhttp.StatusServiceUnavailable:
		// 503 is the server's own "not now": the Worker refused before the
		// barrier for a reason that may pass. An intermediary that did not
		// forward the request answers the same way for the same reason.
		return fmt.Errorf("%w: %w", effect.ErrDispatchRetryable, err)
	}
	// A transport failure or a gateway 5xx can follow acceptance. Preserve
	// the executing target for outcome observation and recovery.
	return fmt.Errorf("%w: %w", effect.ErrDispatchUnknown, err)
}

func (c *Client) Attach(ctx context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	var response effect.Attachment
	if err := c.post(ctx, "/attach", keyRequest{Key: key}, &response); err != nil {
		return effect.Attachment{}, err
	}
	return response, nil
}

func (c *Client) GetStatus(ctx context.Context, key effect.AssignmentKey) (effect.ExecutionStatus, error) {
	var response struct {
		Status effect.ExecutionStatus `json:"status"`
	}
	if err := c.post(ctx, "/status", keyRequest{Key: key}, &response); err != nil {
		return effect.ExecutionNotFound, err
	}
	return response.Status, nil
}

func (c *Client) GetOutcome(ctx context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
	var response protocol.OutcomeEnvelope
	if err := c.post(ctx, "/outcome", keyRequest{Key: key}, &response); err != nil {
		return effect.Outcome{}, err
	}
	return protocol.DecodeOutcome(&response), nil
}

// Progress is effect.ProgressPort over the server's event stream
// (RUN-EXE-12). The request is cancelled when fn stops or ctx ends.
func (c *Client) Progress(ctx context.Context, key effect.AssignmentKey, after uint64, fn func(effect.ProgressFrame) bool) error {
	if strings.TrimSpace(c.BaseURL) == "" {
		return errors.New("executor/http: empty executor URL")
	}
	body, err := json.Marshal(progressRequest{Key: key, After: after})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodPost, strings.TrimRight(c.BaseURL, "/")+"/progress", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		message, _ := io.ReadAll(resp.Body)
		if resp.StatusCode == stdhttp.StatusNotFound {
			return effect.ErrExecutionNotFound
		}
		return &responseError{status: resp.Status, statusCode: resp.StatusCode, body: strings.TrimSpace(string(message))}
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64<<10), 16<<20)
	for scanner.Scan() {
		line := scanner.Bytes()
		if !bytes.HasPrefix(line, []byte("data: ")) {
			continue
		}
		var f effect.ProgressFrame
		if err := json.Unmarshal(bytes.TrimPrefix(line, []byte("data: ")), &f); err != nil {
			continue
		}
		if !fn(f) {
			return nil
		}
	}
	if err := scanner.Err(); err != nil && ctx.Err() == nil {
		return err
	}
	return ctx.Err()
}

func (c *Client) Cancel(ctx context.Context, key effect.AssignmentKey) error {
	return c.post(ctx, "/cancel", keyRequest{Key: key}, nil)
}

// Takeover asks the Worker to acquire an expired execution record. This is a
// control-plane operation and is intentionally not part of effect.ExecutionPort.
func (c *Client) Takeover(ctx context.Context, key effect.AssignmentKey) error {
	return c.post(ctx, "/takeover", keyRequest{Key: key}, nil)
}

// Reconcile asks the Worker to adopt every execution record whose lease
// expired, returning the number of records handed to Takeover. This is a
// control-plane operation and is intentionally not part of effect.ExecutionPort.
func (c *Client) Reconcile(ctx context.Context) (int, error) {
	var response struct {
		Adopted int `json:"adopted"`
	}
	if err := c.post(ctx, "/reconcile", struct{}{}, &response); err != nil {
		return 0, err
	}
	return response.Adopted, nil
}

// Dispose settles an execution record as Unknown without re-dispatching it,
// so the authority disposes the Run target on its next read. This is a
// control-plane operation and is intentionally not part of effect.ExecutionPort.
func (c *Client) Dispose(ctx context.Context, key effect.AssignmentKey) error {
	return c.post(ctx, "/dispose", keyRequest{Key: key}, nil)
}

func (c *Client) post(ctx context.Context, path string, in, out any) error {
	if strings.TrimSpace(c.BaseURL) == "" {
		return errors.New("executor/http: empty executor URL")
	}
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodPost, strings.TrimRight(c.BaseURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		message, _ := io.ReadAll(resp.Body)
		return &responseError{status: resp.Status, statusCode: resp.StatusCode, body: strings.TrimSpace(string(message))}
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// DefaultMaxBodyBytes bounds a request body when Server.MaxBodyBytes is zero.
const DefaultMaxBodyBytes int64 = 16 << 20

// Server exposes a Worker using only request/reply messages. A deployment may
// add authentication, TLS, routing and callback notifications around this
// handler; none of those are part of the Agent Core protocol. Every endpoint
// accepts POST only, and a body larger than MaxBodyBytes (zero takes
// DefaultMaxBodyBytes) is rejected before the Worker sees the request.
type Server struct {
	Worker       *executor.Worker
	MaxBodyBytes int64
}

type responseError struct {
	status     string
	statusCode int
	body       string
}

func (e *responseError) Error() string {
	if e.body == "" {
		return "executor/http: " + e.status
	}
	return "executor/http: " + e.status + ": " + e.body
}

type assignmentRequest struct {
	ProtocolVersion  uint16            `json:"protocolVersion"`
	Assignment       effect.Assignment `json:"assignment"`
	AssignmentDigest run.Digest        `json:"assignmentDigest"`
}

type keyRequest struct {
	Key effect.AssignmentKey `json:"key"`
}

// progressRequest subscribes to an execution's frames after a sequence.
type progressRequest struct {
	Key   effect.AssignmentKey `json:"key"`
	After uint64               `json:"after"`
}

func (s *Server) Handler() stdhttp.Handler {
	mux := stdhttp.NewServeMux()
	mux.HandleFunc("POST /validate", s.validate)
	mux.HandleFunc("POST /dispatch", s.dispatch)
	mux.HandleFunc("POST /attach", s.attach)
	mux.HandleFunc("POST /status", s.status)
	mux.HandleFunc("POST /outcome", s.outcome)
	mux.HandleFunc("POST /cancel", s.cancel)
	mux.HandleFunc("POST /takeover", s.takeover)
	mux.HandleFunc("POST /reconcile", s.reconcile)
	mux.HandleFunc("POST /dispose", s.dispose)
	mux.HandleFunc("POST /progress", s.progress)
	return mux
}

// progress streams an execution's frames as server-sent events
// (RUN-EXE-12): one `data:` line per frame, flushed as it arrives, until the
// Worker ends the stream or the client goes away.
func (s *Server) progress(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req progressRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	flusher, _ := w.(stdhttp.Flusher)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(stdhttp.StatusOK)
	if flusher != nil {
		flusher.Flush()
	}
	_ = s.Worker.Progress(r.Context(), req.Key, req.After, func(f effect.ProgressFrame) bool {
		line, err := json.Marshal(f)
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
	})
}

func makeAssignmentRequest(a effect.Assignment) assignmentRequest {
	digest, _ := a.Digest()
	return assignmentRequest{ProtocolVersion: protocol.ProtocolVersion, Assignment: a, AssignmentDigest: digest}
}

func validateAssignmentRequest(req *assignmentRequest) error {
	if req.ProtocolVersion != protocol.ProtocolVersion {
		return fmt.Errorf("executor/http: unsupported protocol version %d", req.ProtocolVersion)
	}
	digest, err := req.Assignment.Digest()
	if err != nil {
		return err
	}
	if req.AssignmentDigest != digest {
		return fmt.Errorf("executor/http: assignment digest mismatch")
	}
	return nil
}

func (s *Server) validate(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req assignmentRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	if err := validateAssignmentRequest(&req); err != nil {
		writeError(w, err)
		return
	}
	failure, err := s.Worker.Validate(r.Context(), req.Assignment)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, struct {
		Failure *run.ToolFailure `json:"failure,omitempty"`
	}{failure})
}

func (s *Server) dispatch(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req assignmentRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	if err := validateAssignmentRequest(&req); err != nil {
		stdhttp.Error(w, err.Error(), stdhttp.StatusBadRequest)
		return
	}
	// The Worker has persisted acceptance and observes the backend even
	// when the backend's dispatch acknowledgement is uncertain. Every other
	// Dispatch error is a Known answer the client must be able to tell apart
	// from a lost response (RUN-EXE-3): a definite rejection of the
	// Assignment is 400 (a conflicting replay 409), a refusal the Worker
	// itself may lift later is 503. 5xx other than 503 never come from here.
	if err := s.Worker.Dispatch(r.Context(), req.Assignment); err != nil && !errors.Is(err, effect.ErrDispatchUnknown) {
		status := stdhttp.StatusBadRequest
		switch {
		case errors.Is(err, store.ErrAssignmentConflict):
			status = stdhttp.StatusConflict
		case errors.Is(err, effect.ErrDispatchRetryable):
			status = stdhttp.StatusServiceUnavailable
		}
		stdhttp.Error(w, err.Error(), status)
		return
	}
	w.WriteHeader(stdhttp.StatusAccepted)
}

func (s *Server) attach(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req keyRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	attachment, err := s.Worker.Attach(r.Context(), req.Key)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, attachment)
}

func (s *Server) status(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req keyRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	status, err := s.Worker.GetStatus(r.Context(), req.Key)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, struct {
		Status effect.ExecutionStatus `json:"status"`
	}{status})
}

func (s *Server) outcome(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req keyRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	outcome, err := s.Worker.GetOutcomeEnvelope(r.Context(), req.Key)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, outcome)
}

func (s *Server) cancel(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req keyRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	if err := s.Worker.Cancel(r.Context(), req.Key); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(stdhttp.StatusAccepted)
}

func (s *Server) takeover(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req keyRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	if err := s.Worker.Takeover(r.Context(), req.Key); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(stdhttp.StatusAccepted)
}

func (s *Server) reconcile(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	n, err := s.Worker.Reconcile(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, struct {
		Adopted int `json:"adopted"`
	}{n})
}

func (s *Server) dispose(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req keyRequest
	if !s.readJSON(w, r, &req) {
		return
	}
	if err := s.Worker.Dispose(r.Context(), req.Key); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(stdhttp.StatusAccepted)
}

// readJSON decodes the request body within the Server's size bound: a body
// past it is 413, any other decoding failure 400.
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

func writeError(w stdhttp.ResponseWriter, err error) {
	status := stdhttp.StatusInternalServerError
	if errors.Is(err, effect.ErrExecutionNotFound) {
		status = stdhttp.StatusNotFound
	}
	if errors.Is(err, store.ErrAssignmentConflict) || errors.Is(err, store.ErrLeaseLost) {
		status = stdhttp.StatusConflict
	}
	if errors.Is(err, effect.ErrOutcomeNotReady) {
		// 204 responses carry no body; writing one violates the HTTP contract.
		w.WriteHeader(stdhttp.StatusNoContent)
		return
	}
	stdhttp.Error(w, err.Error(), status)
}

var _ effect.ExecutionPort = (*Client)(nil)
