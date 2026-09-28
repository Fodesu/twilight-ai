package sdk

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"unicode/utf8"
)

const (
	// apiErrorMaxBodyBytes caps how much of a non-2xx response body an
	// APIError retains; a misbehaving upstream cannot make an error value
	// carry an unbounded buffer.
	apiErrorMaxBodyBytes = 64 << 10
	// apiErrorMaxBodyDisplay caps the body excerpt Error() appends.
	apiErrorMaxBodyDisplay = 1024
)

// APIError is the single error type for a non-2xx HTTP response from an
// upstream provider, across every provider surface (chat generation and
// streaming, speech, transcription, embeddings, model listings and health
// probes). Provider paths wrap it with %w, so a caller above a provider can
// classify a failure (rate limit, authentication, outage) with errors.As and
// read StatusCode and Message, instead of parsing Error() output.
type APIError struct {
	StatusCode int    `json:"statusCode"`
	Status     string `json:"status"`
	Message    string `json:"message"`
	RawBody    []byte `json:"-"`
}

func (e *APIError) Error() string {
	msg := e.Message
	if msg == "" {
		msg = http.StatusText(e.StatusCode)
	}
	if msg == "" {
		msg = e.Status
	}
	out := fmt.Sprintf("api error %d: %s", e.StatusCode, msg)
	if len(e.RawBody) > 0 {
		out += " [body: " + truncateAPIErrorBody(e.RawBody) + "]"
	}
	return out
}

// NewAPIError builds the APIError for a non-2xx HTTP response whose body has
// already been read, extracting the upstream's message when the body carries
// one of the recognized JSON shapes ({"error":{"message":...}} or
// {"message":...}); data trailing the JSON value is ignored. body is retained
// capped at apiErrorMaxBodyBytes.
func NewAPIError(statusCode int, status string, body []byte) *APIError {
	if len(body) > apiErrorMaxBodyBytes {
		body = body[:apiErrorMaxBodyBytes]
	}
	e := &APIError{StatusCode: statusCode, Status: status, RawBody: body}
	var parsed struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
		Message string `json:"message"`
	}
	if err := json.NewDecoder(bytes.NewReader(body)).Decode(&parsed); err == nil {
		switch {
		case parsed.Error.Message != "":
			e.Message = parsed.Error.Message
		case parsed.Message != "":
			e.Message = parsed.Message
		}
	}
	return e
}

func truncateAPIErrorBody(b []byte) string {
	if len(b) <= apiErrorMaxBodyDisplay {
		return string(b)
	}
	b = b[:apiErrorMaxBodyDisplay]
	for !utf8.Valid(b) {
		b = b[:len(b)-1]
	}
	return string(b) + "...(truncated)"
}
