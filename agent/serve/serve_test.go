package serve

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type probeComponent struct{ probe func(context.Context) error }

func (c probeComponent) Handler() http.Handler           { return http.NotFoundHandler() }
func (c probeComponent) Close(context.Context) error     { return nil }
func (c probeComponent) Ready(ctx context.Context) error { return c.probe(ctx) }

type plainComponent struct{}

func (plainComponent) Handler() http.Handler       { return http.NotFoundHandler() }
func (plainComponent) Close(context.Context) error { return nil }

func TestProbeRoutes(t *testing.T) {
	ready := false
	h := Handler(http.NotFoundHandler(), func() bool { return ready })
	for _, tc := range []struct {
		path   string
		status int
	}{{"/healthz", 204}, {"/readyz", 503}, {"/metrics", 200}} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", tc.path, nil))
		if w.Code != tc.status {
			t.Errorf("%s: %d want %d", tc.path, w.Code, tc.status)
		}
	}
	ready = true
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/readyz", nil))
	if w.Code != 204 {
		t.Fatalf("ready returned %d", w.Code)
	}
}

func TestComponentReadiness(t *testing.T) {
	failure := errors.New("database unavailable")
	for _, result := range []error{nil, failure} {
		c := probeComponent{probe: func(ctx context.Context) error {
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > 2*time.Second {
				t.Error("probe not bounded")
			}
			return result
		}}
		if got := checkReadiness(context.Background(), c, Options{}); !errors.Is(got, result) {
			t.Fatalf("check: %v want %v", got, result)
		}
	}
	if err := checkReadiness(context.Background(), plainComponent{}, Options{}); err != nil {
		t.Fatal(err)
	}
}

func TestReadinessGateDoesNotCallDependency(t *testing.T) {
	c := probeComponent{probe: func(context.Context) error { t.Error("dependency called behind closed gate"); return nil }}
	if err := checkReadiness(context.Background(), c, Options{Ready: func() bool { return false }}); err == nil {
		t.Fatal("gate ignored")
	}
}

func TestReadinessHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := probeComponent{probe: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	if err := checkReadiness(ctx, c, Options{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}
