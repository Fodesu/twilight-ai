package agent_test

import (
	"testing"

	"github.com/memohai/twilight-ai/agent"
	"github.com/memohai/twilight-ai/agent/runtimetest"
)

func TestMemoryRuntimeConformance(t *testing.T) {
	runtimetest.Run(t, func(t testing.TB, initial agent.MachineState) agent.Runtime {
		t.Helper()
		return agent.NewMemoryRuntime(initial)
	})
}
