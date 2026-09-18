package loop_test

import (
	"testing"

	"github.com/felinics/twilight/agent/run"
)

func TestUnknownToolOutcomeContinues(t *testing.T) {
	f := newFeature(t)
	f.Unknown("echo")
	f.Model(ToolCalls("echo", "c1"), Text("recovered"))
	f.Run()
	f.RequireCompleted("recovered")
	f.RequireCallFailed("c1", run.ToolOutcomeUnknown)
	f.RequireFailureClass(run.FailureEffectUnknown)
}

func TestUnknownToolOutcomeLeavesSiblingRunning(t *testing.T) {
	f := newFeature(t)
	f.Unknown("lost")
	f.Tool("echo", run.DirectExecution)
	f.Model(Calls(Call("lost", "c1"), Call("echo", "c2")), Text("done"))
	f.Run()
	f.RequireCompleted("done")
	f.RequireRan("lost")
	f.RequireRan("echo")
	f.RequireCallFailed("c1", run.ToolOutcomeUnknown)
	f.RequireBuilderSawTool("c2", `{"x":1}`)
}
