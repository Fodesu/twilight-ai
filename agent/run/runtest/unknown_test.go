package runtest_test

import (
	"testing"

	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/run/runtest"
)

func TestUnknownToolOutcomeContinues(t *testing.T) {
	f := runtest.New(t)
	f.Unknown("echo")
	f.Model(runtest.ToolCalls("echo", "c1"), runtest.Text("recovered"))
	f.Run()
	f.RequireCompleted("recovered")
	f.RequireCallFailed("c1", run.ToolOutcomeUnknown)
	f.RequireFailureClass(run.FailureEffectUnknown)
}

func TestUnknownToolOutcomeLeavesSiblingRunning(t *testing.T) {
	f := runtest.New(t)
	f.Unknown("lost")
	f.Tool("echo", run.DirectExecution)
	f.Model(runtest.Calls(runtest.Call("lost", "c1"), runtest.Call("echo", "c2")), runtest.Text("done"))
	f.Run()
	f.RequireCompleted("done")
	f.RequireRan("lost")
	f.RequireRan("echo")
	f.RequireCallFailed("c1", run.ToolOutcomeUnknown)
	f.RequirePlannerSawTool("c2", `{"x":1}`)
}
