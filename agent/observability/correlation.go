package observability

import (
	"context"
	"net/http"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type requestIDKey struct{}

// Correlate accepts a bounded printable request ID or generates one. W3C trace
// context is extracted for correlation/forwarding only: this is not a tracer
// provider and does not emit spans. Baggage is deliberately not propagated.
func Correlate(r *http.Request) *http.Request {
	id := r.Header.Get("X-Request-ID")
	if !validRequestID(id) {
		id = uuid.NewString()
	}
	ctx := context.WithValue(r.Context(), requestIDKey{}, id)
	ctx = (propagation.TraceContext{}).Extract(ctx, propagation.HeaderCarrier(r.Header))
	return r.WithContext(ctx)
}

func validRequestID(id string) bool {
	if len(id) == 0 || len(id) > 128 {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return true
}

func RequestID(ctx context.Context) string { id, _ := ctx.Value(requestIDKey{}).(string); return id }

// Inject forwards correlation to internal HTTP peers. A background recovery
// need not inherit the original request; its durable effect key links the logs.
func Inject(req *http.Request) {
	if id := RequestID(req.Context()); id != "" {
		req.Header.Set("X-Request-ID", id)
	}
	(propagation.TraceContext{}).Inject(req.Context(), propagation.HeaderCarrier(req.Header))
}

func Correlation(ctx context.Context) []any {
	attrs := []any{}
	if id := RequestID(ctx); id != "" {
		attrs = append(attrs, "request_id", id)
	}
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		attrs = append(attrs, "trace_id", sc.TraceID().String())
	}
	return attrs
}
