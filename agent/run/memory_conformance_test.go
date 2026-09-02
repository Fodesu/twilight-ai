package run_test

import (
	"testing"
	"time"

	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/run/runtimetest"
)

// MemoryStore-backed Runtime is the in-process reference; it runs the same
// shared suite durable adapters import (RUN-CMP-2).
// Dependency chain: run_test -> runtimetest -> run (the net/http/httptest
// layout), so the loop is broken by the external test package.
func TestMemoryStoreRuntimeConformance(t *testing.T) {
	runtimetest.RunConformance(t, func() run.Runtime { return run.NewRuntime(run.NewMemoryStore()) })
}

func TestMemoryStoreRuntimeRecovery(t *testing.T) {
	runtimetest.RunRecoveryConformance(t, func(now func() time.Time, ttl time.Duration) run.Runtime {
		return run.NewRuntimeWithOptions(run.NewMemoryStore(), run.RuntimeOptions{LeaseTTL: ttl, Now: now})
	})
}

func TestMemoryStoreRuntimeLeaseRenewal(t *testing.T) {
	runtimetest.RunLeaseRenewalConformance(t, func(now func() time.Time, ttl time.Duration) run.Runtime {
		return run.NewRuntimeWithOptions(run.NewMemoryStore(), run.RuntimeOptions{LeaseTTL: ttl, Now: now})
	})
}
