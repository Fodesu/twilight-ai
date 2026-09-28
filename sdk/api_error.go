package sdk

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"unicode/utf8"
)

const (
	// apiErrorMaxBodyBytes caps how much of a non-2xx response body an
	// APIError retains; a misbehaving upstream cannot make an error value
	// carry an unbounded buffer.
	apiErrorMaxBodyBytes = 64 << 10
	// apiErrorReadBudget bounds the transient read used for message
	// extraction, so a multi-megabyte upstream error page still costs
	// bounded memory; retention stays at apiErrorMaxBodyBytes.
	apiErrorReadBudget = 1 << 20
	// apiErrorMaxBodyDisplay caps the body excerpt Error() appends. Sized to
	// keep whole the 1-3 KB error documents that message extraction misses
	// (nested Google/Azure shapes); Message stays the uncapped primary
	// channel and RawBody retains 64 KiB for programmatic access.
	apiErrorMaxBodyDisplay = 4096
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
// already been read. The upstream message is extracted from the full body
// before retention is capped, and the retained RawBody is a copy capped at
// apiErrorMaxBodyBytes, so the error never pins the caller's buffer.
func NewAPIError(statusCode int, status string, body []byte) *APIError {
	e := &APIError{StatusCode: statusCode, Status: status}
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
	if len(body) > apiErrorMaxBodyBytes {
		body = body[:apiErrorMaxBodyBytes]
	}
	e.RawBody = bytes.Clone(body)
	return e
}

// NewAPIErrorFromResponse builds the APIError for a non-2xx HTTP response.
// It reads the body up to apiErrorReadBudget for message extraction, retains
// at most apiErrorMaxBodyBytes, and drains up to another apiErrorMaxBodyBytes
// so a keep-alive connection stays reusable; larger bodies are abandoned and
// the connection closed.
func NewAPIErrorFromResponse(resp *http.Response) *APIError {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, apiErrorReadBudget))
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, apiErrorMaxBodyBytes))
	return NewAPIError(resp.StatusCode, resp.Status, body)
}

// truncateAPIErrorBody returns b as a string, capped at
// apiErrorMaxBodyDisplay bytes of complete runes plus a truncation marker.
// Invalid bytes decode as width-1 units and pass through; only a rune that
// would straddle the budget is dropped.
func truncateAPIErrorBody(b []byte) string {
	if len(b) <= apiErrorMaxBodyDisplay {
		return string(b)
	}
	size := 0
	for size < apiErrorMaxBodyDisplay {
		_, w := utf8.DecodeRune(b[size:])
		if size+w > apiErrorMaxBodyDisplay {
			break
		}
		size += w
	}
	return string(b[:size]) + "...(truncated)"
}
