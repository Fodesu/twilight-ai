package observability

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/session"
)

func quietObserver() *Observer { return New(slog.New(slog.NewJSONHandler(io.Discard, nil))) }

func scrape(t *testing.T, o *Observer) string {
	t.Helper()
	w := httptest.NewRecorder()
	o.MetricsHandler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("scrape: %d %s", w.Code, w.Body.String())
	}
	return w.Body.String()
}

func TestHTTPUsesBoundedLabelsAndNoBodies(t *testing.T) {
	var logs bytes.Buffer
	o := New(slog.New(slog.NewJSONHandler(&logs, nil)))
	component := http.NewServeMux()
	component.HandleFunc("POST /sessions/{sid}/commands", func(w http.ResponseWriter, r *http.Request) {
		if RequestID(r.Context()) != "test-request" {
			t.Error("missing request context")
		}
		w.WriteHeader(http.StatusAccepted)
	})
	outer := http.NewServeMux()
	outer.Handle("/", component)
	handler := o.HTTP(outer)
	for _, id := range []string{"unique-a", "unique-b"} {
		r := httptest.NewRequest(http.MethodPost, "/sessions/"+id+"/commands?api_key=secret-query", strings.NewReader("secret-body"))
		r.Header.Set("Authorization", "Bearer secret-header")
		r.Header.Set("X-Request-ID", "test-request")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Header().Get("X-Request-ID") != "test-request" {
			t.Fatal("response has no correlation")
		}
	}
	for i := range 20 {
		r := httptest.NewRequest("CUSTOM"+fmt.Sprint(i), "/random-"+fmt.Sprint(i), nil)
		handler.ServeHTTP(httptest.NewRecorder(), r)
	}
	metrics := scrape(t, o)
	if !strings.Contains(metrics, `twilight_http_requests_total{method="POST",route="POST /sessions/{sid}/commands",status="202"} 2`) {
		t.Fatalf("missing template route metric:\n%s", metrics)
	}
	if !strings.Contains(metrics, `twilight_http_requests_total{method="OTHER",route="unmatched",status="404"} 20`) {
		t.Fatalf("unknown routes/methods not bounded:\n%s", metrics)
	}
	for _, secret := range []string{"secret-query", "secret-body", "secret-header", "random-", "CUSTOM", "unique-a", "unique-b"} {
		if strings.Contains(metrics, secret) {
			t.Errorf("metric label leaked %s", secret)
		}
	}
	for _, secret := range []string{"secret-query", "secret-body", "secret-header"} {
		if strings.Contains(logs.String(), secret) {
			t.Errorf("log leaked %s", secret)
		}
	}
	if !strings.Contains(logs.String(), `"session_id":"unique-a"`) {
		t.Fatal("missing session log field")
	}
}

func TestSSEFlushesBeforeCompletion(t *testing.T) {
	o := quietObserver()
	finished := make(chan struct{})
	server := httptest.NewServer(o.HTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: first\n\n")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("Flusher was lost")
			return
		}
		flusher.Flush()
		<-r.Context().Done()
	})))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil || line != "data: first\n" {
		t.Fatalf("stream buffered: %q %v", line, err)
	}
	if !strings.Contains(scrape(t, o), `twilight_http_requests_in_flight{method="GET"} 1`) {
		t.Fatal("live stream not counted")
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("stream did not cancel")
	}
}

func TestHTTPPreservesOptionalInterfaces(t *testing.T) {
	o := quietObserver()
	handler := o.HTTP(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, ok := w.(http.Flusher); !ok {
			t.Error("lost Flusher")
		}
		if _, ok := w.(http.Hijacker); ok {
			t.Error("invented Hijacker")
		}
		w.WriteHeader(204)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
}

func TestMetricsScrapeIsNotInstrumented(t *testing.T) {
	o := quietObserver()
	h := o.HTTP(o.MetricsHandler())
	for range 2 {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/metrics", nil))
	}
	if strings.Contains(scrape(t, o), "twilight_http_requests_total{") {
		t.Fatal("scrapes counted as application requests")
	}
}

func TestOperationClassificationDoesNotLogRemoteBody(t *testing.T) {
	var logs bytes.Buffer
	o := New(slog.New(slog.NewJSONHandler(&logs, nil)))
	err := fmt.Errorf("%w: secret-remote-body", effect.ErrDispatchUnknown)
	o.Begin(context.Background(), "dispatch", "effect_id", "unique-effect")(err)
	metrics := scrape(t, o)
	if !strings.Contains(metrics, `twilight_operations_total{operation="dispatch",result="dispatch_unknown"} 1`) {
		t.Fatal("unknown dispatch not recorded")
	}
	if strings.Contains(metrics, "unique-effect") || strings.Contains(logs.String(), "secret-remote-body") {
		t.Fatal("sensitive/unbounded data leaked")
	}
}

func TestResult(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{nil, "ok"}, {effect.ErrDispatchUnknown, "dispatch_unknown"}, {effect.ErrDispatchRetryable, "retryable"},
		{effect.ErrOutcomeNotReady, "not_ready"}, {effect.ErrExecutionNotFound, "not_found"}, {effect.ErrOutcomeUnavailable, "unavailable"},
		{&session.Error{Code: session.ErrOwned}, "owned"}, {&session.Error{Code: session.ErrOwnershipLost}, "fenced"},
		{&ledger.Error{Code: ledger.CodeOwnershipLost}, "fenced"}, {context.Canceled, "canceled"}, {context.DeadlineExceeded, "timeout"}, {errors.New("arbitrary"), "error"},
	} {
		if got := Result(tc.err); got != tc.want {
			t.Errorf("Result(%v)=%s want %s", tc.err, got, tc.want)
		}
	}
}
