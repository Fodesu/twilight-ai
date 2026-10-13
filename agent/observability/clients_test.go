package observability_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	ownerhttp "github.com/felinics/twilight/agent/app/http"
	"github.com/felinics/twilight/agent/executor/backendhttp"
	executorhttp "github.com/felinics/twilight/agent/executor/http"
	"github.com/felinics/twilight/agent/observability"
	wshttp "github.com/felinics/twilight/agent/workspace/http"
	"github.com/felinics/twilight/agentcore/run/effect"
)

func TestClientsForwardRequestCorrelation(t *testing.T) {
	const parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	for _, tc := range []struct {
		name string
		call func(context.Context, string) error
	}{
		{"owner", func(ctx context.Context, url string) error {
			return (&ownerhttp.Client{BaseURL: url}).Ensure(ctx, "session")
		}},
		{"executor", func(ctx context.Context, url string) error {
			return (&executorhttp.Client{BaseURL: url}).Dispatch(ctx, effect.Assignment{})
		}},
		{"backend", func(ctx context.Context, url string) error {
			return (&backendhttp.Client{BaseURL: url}).Start(ctx, "ref", effect.Assignment{})
		}},
		{"workspace", func(ctx context.Context, url string) error {
			_, err := (&wshttp.Client{BaseURL: url}).Snapshot(ctx, "workspace")
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.Header.Get("X-Request-ID"); got != "request-test" {
					t.Errorf("request ID: %q", got)
				}
				if got := r.Header.Get("traceparent"); got != parent {
					t.Errorf("traceparent: %q", got)
				}
				if r.Header.Get("baggage") != "" {
					t.Error("baggage leaked")
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, "{}")
			}))
			defer peer.Close()
			o := observability.New(slog.New(slog.NewJSONHandler(io.Discard, nil)))
			entry := o.HTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := tc.call(r.Context(), peer.URL); err != nil {
					t.Errorf("client: %v", err)
				}
				w.WriteHeader(204)
			}))
			r := httptest.NewRequest("POST", "/entry", nil)
			r.Header.Set("X-Request-ID", "request-test")
			r.Header.Set("traceparent", parent)
			r.Header.Set("baggage", "secret=value")
			entry.ServeHTTP(httptest.NewRecorder(), r)
		})
	}
}
