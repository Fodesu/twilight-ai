package observability

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCorrelationPropagation(t *testing.T) {
	const parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("X-Request-ID", "request-123")
	r.Header.Set("traceparent", parent)
	r.Header.Set("baggage", "secret=value")
	r = Correlate(r)
	out, _ := http.NewRequestWithContext(r.Context(), "POST", "http://worker/dispatch", nil)
	Inject(out)
	if out.Header.Get("X-Request-ID") != "request-123" || out.Header.Get("traceparent") != parent {
		t.Fatalf("correlation lost: %v", out.Header)
	}
	if out.Header.Get("baggage") != "" {
		t.Fatal("baggage propagated")
	}
	attrs := Correlation(r.Context())
	if len(attrs) != 4 || attrs[3] != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("trace log fields: %v", attrs)
	}
}

func TestInvalidCorrelationReplaced(t *testing.T) {
	for _, id := range []string{"", "has spaces", "line\nbreak", strings.Repeat("a", 129), "中文"} {
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("X-Request-ID", id)
		r.Header.Set("traceparent", "not-a-trace")
		r = Correlate(r)
		got := RequestID(r.Context())
		if !validRequestID(got) || got == id {
			t.Fatalf("invalid ID not replaced: %q", got)
		}
		if len(Correlation(r.Context())) != 2 {
			t.Fatal("invalid trace accepted")
		}
	}
}
