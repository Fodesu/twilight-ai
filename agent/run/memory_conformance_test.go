package run_test

import (
	"testing"

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
