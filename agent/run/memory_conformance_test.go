package run_test

import (
	"testing"

	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/run/runtimetest"
)

// MemoryRuntime is the reference implementation of the Runtime contract; it
// runs the same shared suite durable adapters import (RUN-CMP-2).
// Dependency chain: run_test -> runtimetest -> run (the net/http/httptest
// layout), so the loop is broken by the external test package.
func TestMemoryRuntimeConformance(t *testing.T) {
	runtimetest.RunConformance(t, func() run.Runtime { return run.NewMemoryRuntime() })
}
