package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/turn"
)

// writeError classifies the kernel and turn errors the handlers surface; an
// unknown Session or a malformed request must not read as a server fault.
func TestWriteErrorStatus(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"turn conflict", fmt.Errorf("submit: %w", turn.ErrConflict), http.StatusConflict},
		{"commit conflict", &session.Error{Code: session.ErrConflict, Operation: "append"}, http.StatusConflict},
		{"owned", &session.Error{Code: session.ErrOwned, Operation: "open"}, http.StatusConflict},
		{"ownership lost", &session.Error{Code: session.ErrOwnershipLost, Operation: "append"}, http.StatusConflict},
		{"not found", &session.Error{Code: session.ErrNotFound, Operation: "read"}, http.StatusNotFound},
		{"invalid", &session.Error{Code: session.ErrInvalid, Operation: "read"}, http.StatusBadRequest},
		{"other", errors.New("boom"), http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			writeError(rec, tc.err)
			if rec.Code != tc.want {
				t.Fatalf("writeError(%v) = %d, want %d", tc.err, rec.Code, tc.want)
			}
		})
	}
}
