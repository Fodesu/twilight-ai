// Package observability supplies process-local operational evidence, not durable
// business facts. IDs belong in logs; metric labels are bounded vocabularies.
package observability

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felixge/httpsnoop"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Observer owns a registry, so multiple test servers do not register duplicate
// collectors in Prometheus's global registry. Default is shared by a binary.
type Observer struct {
	registry          *prometheus.Registry
	logger            *slog.Logger
	requests          *prometheus.CounterVec
	duration          *prometheus.HistogramVec
	inflight          *prometheus.GaugeVec
	inflightByClass   *prometheus.GaugeVec
	operations        *prometheus.CounterVec
	operationDuration *prometheus.HistogramVec
	eventFrames       *prometheus.CounterVec
	warnings          prometheus.Counter
	ready             prometheus.Gauge
}

var Default = New(nil)

// New uses logger, or the current slog default if nil.
func New(logger *slog.Logger) *Observer {
	o := &Observer{
		registry: prometheus.NewRegistry(), logger: logger,
		requests:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: "twilight_http_requests_total", Help: "Completed HTTP requests (streams count on close)."}, []string{"method", "route", "status"}),
		duration:          prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "twilight_http_request_duration_seconds", Help: "HTTP lifetime, including the entire lifetime of SSE streams.", Buckets: prometheus.DefBuckets}, []string{"method", "route"}),
		inflight:          prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "twilight_http_requests_in_flight", Help: "Active HTTP requests, including SSE streams."}, []string{"method"}),
		inflightByClass:   prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "twilight_http_requests_in_flight_by_class", Help: "Active HTTP requests by endpoint class: probe, rpc or stream. Includes stream handshakes, not only accepted SSE connections."}, []string{"class"}),
		operations:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "twilight_operations_total", Help: "Completed session and execution boundary operations by result."}, []string{"operation", "result"}),
		operationDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "twilight_operation_duration_seconds", Help: "Session and execution boundary operation latency.", Buckets: prometheus.DefBuckets}, []string{"operation"}),
		eventFrames:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "twilight_event_frames_total", Help: "SSE frames written, not client acknowledgements. A from-cursor stream includes catch-up and live frames."}, []string{"mode", "kind"}),
		warnings:          prometheus.NewCounter(prometheus.CounterOpts{Name: "twilight_background_warnings_total", Help: "Owner background warnings."}),
		ready:             prometheus.NewGauge(prometheus.GaugeOpts{Name: "twilight_ready", Help: "Result of the most recent readiness check (not a continuous dependency monitor)."}),
	}
	for _, class := range []string{"probe", "rpc", "stream"} {
		o.inflightByClass.WithLabelValues(class).Set(0)
	}
	o.registry.MustRegister(o.requests, o.duration, o.inflight, o.inflightByClass, o.operations, o.operationDuration, o.eventFrames, o.warnings, o.ready,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return o
}

func (o *Observer) log() *slog.Logger {
	if o.logger != nil {
		return o.logger
	}
	return slog.Default()
}

// MetricsHandler exposes only this process's registry.
func (o *Observer) MetricsHandler() http.Handler {
	return promhttp.HandlerFor(o.registry, promhttp.HandlerOpts{})
}

// HTTP preserves the ResponseWriter's optional interfaces, including Flusher
// and Hijacker. No request/response bodies, query strings or credentials are logged.
func (o *Observer) HTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = Correlate(r)
		w.Header().Set("X-Request-ID", RequestID(r.Context()))
		if r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		method := metricMethod(r.Method)
		o.inflight.WithLabelValues(method).Inc()
		defer o.inflight.WithLabelValues(method).Dec()
		class := requestClass(r)
		o.inflightByClass.WithLabelValues(class).Inc()
		defer o.inflightByClass.WithLabelValues(class).Dec()
		m := httpsnoop.CaptureMetrics(next, w, r)
		route := r.Pattern
		if route == "" || route == "/" {
			route = "unmatched"
		}
		o.requests.WithLabelValues(method, route, strconv.Itoa(m.Code)).Inc()
		o.duration.WithLabelValues(method, route).Observe(m.Duration.Seconds())
		if (r.URL.Path == "/healthz" || r.URL.Path == "/readyz") && m.Code < 400 {
			return
		}
		attrs := []any{"event", "http_completed", "class", class, "method", method, "route", route, "status", m.Code, "duration_seconds", m.Duration.Seconds(), "response_bytes", m.Written}
		if sid := r.PathValue("sid"); sid != "" {
			attrs = append(attrs, "session_id", sid)
		}
		o.log().Log(r.Context(), httpLevel(m.Code), "HTTP request completed", append(Correlation(r.Context()), attrs...)...)
	})
}

func metricMethod(method string) string {
	switch method {
	case "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "CONNECT", "TRACE":
		return method
	default:
		return "OTHER"
	}
}
func httpLevel(status int) slog.Level {
	if status >= 500 {
		return slog.LevelError
	}
	if status >= 400 {
		return slog.LevelWarn
	}
	return slog.LevelInfo
}

// Begin returns a completion function. operation must be a fixed literal from
// the caller, never a session ID, execution key or arbitrary error message.
func (o *Observer) Begin(ctx context.Context, operation string, attrs ...any) func(error) {
	start := time.Now()
	return func(err error) {
		result := Result(err)
		o.operations.WithLabelValues(operation, result).Inc()
		o.operationDuration.WithLabelValues(operation).Observe(time.Since(start).Seconds())
		fields := append(Correlation(ctx), "event", operation, "result", result, "duration_seconds", time.Since(start).Seconds())
		fields = append(fields, attrs...)
		level := slog.LevelInfo
		if result == "error" || result == "dispatch_unknown" {
			level = slog.LevelWarn
		}
		// Do not log err.Error(): remote errors can contain model/tool bodies.
		o.log().Log(ctx, level, "Operation completed", fields...)
	}
}

// Result classifies protocol errors without putting unbounded messages in labels.
func Result(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, effect.ErrDispatchUnknown):
		return "dispatch_unknown"
	case errors.Is(err, effect.ErrDispatchRetryable):
		return "retryable"
	case errors.Is(err, effect.ErrOutcomeNotReady):
		return "not_ready"
	case errors.Is(err, effect.ErrExecutionNotFound):
		return "not_found"
	case errors.Is(err, effect.ErrOutcomeUnavailable):
		return "unavailable"
	case session.IsCode(err, session.ErrOwned):
		return "owned"
	case session.IsCode(err, session.ErrOwnershipLost), errors.Is(err, &ledger.Error{Code: ledger.CodeOwnershipLost}):
		return "fenced"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return "error"
	}
}

// Warn connects the owner's background failures to logs instead of discarding
// them. Callers should not include prompts, tool output or credentials in errors.
func (o *Observer) Warn(err error) {
	o.warnings.Inc()
	o.log().Warn("Owner background work failed", "event", "background_warning", "result", Result(err), "error", err)
}

func (o *Observer) Readiness(err error) {
	if err == nil {
		o.ready.Set(1)
		return
	}
	o.ready.Set(0)
	o.log().Warn("Readiness check failed", "event", "readiness_failed", "result", Result(err))
}

// ExecutionAttrs identifies an effect across owner, worker and backend hops.
// There is no synthetic execution ID: the durable identity is this tuple.
func ExecutionAttrs(key effect.AssignmentKey) []any {
	return []any{"session_id", key.Session, "run_id", key.RunID, "effect_id", key.Effect}
}
