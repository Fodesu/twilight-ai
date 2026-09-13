package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"strings"

	"github.com/felinics/twilight/agent/executor"
	"github.com/felinics/twilight/agent/executor/store"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/effect"
	"github.com/felinics/twilight/agent/run/protocol"
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
	return c.post(ctx, "/dispatch", makeAssignmentRequest(a), nil)
}

func (c *Client) Attach(ctx context.Context, key effect.AssignmentKey) (bool, error) {
	var response struct {
		Attached bool `json:"attached"`
	}
	if err := c.post(ctx, "/attach", keyRequest{Key: key}, &response); err != nil {
		return false, err
	}
	return response.Attached, nil
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
	return protocol.DecodeOutcome(response), nil
}

func (c *Client) Cancel(ctx context.Context, key effect.AssignmentKey) error {
	return c.post(ctx, "/cancel", keyRequest{Key: key}, nil)
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
		if len(message) == 0 {
			message = []byte(resp.Status)
		}
		return fmt.Errorf("executor/http: %s: %s", resp.Status, strings.TrimSpace(string(message)))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// Server exposes a Worker using only request/reply messages. A deployment may
// add authentication, TLS, routing and callback notifications around this
// handler; none of those are part of the Agent Core protocol.
type Server struct{ Worker *executor.Worker }

type assignmentRequest struct {
	ProtocolVersion  uint16            `json:"protocolVersion"`
	Assignment       effect.Assignment `json:"assignment"`
	AssignmentDigest run.Digest        `json:"assignmentDigest"`
}

type keyRequest struct {
	Key effect.AssignmentKey `json:"key"`
}

func (s *Server) Handler() stdhttp.Handler {
	mux := stdhttp.NewServeMux()
	mux.HandleFunc("/validate", s.validate)
	mux.HandleFunc("/dispatch", s.dispatch)
	mux.HandleFunc("/attach", s.attach)
	mux.HandleFunc("/status", s.status)
	mux.HandleFunc("/outcome", s.outcome)
	mux.HandleFunc("/cancel", s.cancel)
	return mux
}

func makeAssignmentRequest(a effect.Assignment) assignmentRequest {
	digest, _ := a.Digest()
	return assignmentRequest{ProtocolVersion: protocol.ProtocolVersion, Assignment: a, AssignmentDigest: digest}
}

func validateAssignmentRequest(req assignmentRequest) error {
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
	if !readJSON(w, r, &req) {
		return
	}
	if err := validateAssignmentRequest(req); err != nil {
		writeError(w, err)
		return
	}
	failure, err := s.Worker.Validate(r.Context(), req.Assignment)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, struct {
		Failure *run.ToolFailure `json:"failure,omitempty"`
	}{failure})
}

func (s *Server) dispatch(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req assignmentRequest
	if !readJSON(w, r, &req) {
		return
	}
	if err := validateAssignmentRequest(req); err != nil {
		writeError(w, err)
		return
	}
	if err := s.Worker.Dispatch(r.Context(), req.Assignment); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(stdhttp.StatusAccepted)
}

func (s *Server) attach(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req keyRequest
	if !readJSON(w, r, &req) {
		return
	}
	attached, err := s.Worker.Attach(r.Context(), req.Key)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, struct {
		Attached bool `json:"attached"`
	}{attached})
}

func (s *Server) status(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req keyRequest
	if !readJSON(w, r, &req) {
		return
	}
	status, err := s.Worker.GetStatus(r.Context(), req.Key)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, struct {
		Status effect.ExecutionStatus `json:"status"`
	}{status})
}

func (s *Server) outcome(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req keyRequest
	if !readJSON(w, r, &req) {
		return
	}
	outcome, err := s.Worker.GetOutcomeEnvelope(r.Context(), req.Key)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, stdhttp.StatusOK, outcome)
}

func (s *Server) cancel(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var req keyRequest
	if !readJSON(w, r, &req) {
		return
	}
	if err := s.Worker.Cancel(r.Context(), req.Key); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(stdhttp.StatusAccepted)
}

func readJSON(w stdhttp.ResponseWriter, r *stdhttp.Request, out any) bool {
	if err := json.NewDecoder(r.Body).Decode(out); err != nil {
		writeError(w, err)
		return false
	}
	return true
}

func writeJSON(w stdhttp.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w stdhttp.ResponseWriter, err error) {
	status := stdhttp.StatusInternalServerError
	if errors.Is(err, effect.ErrExecutionNotFound) {
		status = stdhttp.StatusNotFound
	}
	if errors.Is(err, store.ErrAssignmentConflict) {
		status = stdhttp.StatusConflict
	}
	if errors.Is(err, effect.ErrOutcomeNotReady) {
		status = stdhttp.StatusNoContent
	}
	stdhttp.Error(w, err.Error(), status)
}

var _ effect.Port = (*Client)(nil)
