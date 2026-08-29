package run_test

import (
	"testing"

	agent "github.com/memohai/twilight-ai/agent/run"
	"github.com/memohai/twilight-ai/agent/run/runtimetest"
)

func TestMemoryRuntimeConformance(t *testing.T) {
	runtimetest.Run(t, func(t testing.TB, initial agent.MachineState) agent.Runtime {
		t.Helper()
		return agent.NewMemoryRuntime(initial)
	})
}
