package runtest_test

import (
	"testing"

	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/run/runtest"
)

func TestModelCallCompletes(t *testing.T) {
	f := runtest.New(t)
	f.Model(runtest.Text("hello"))
	f.Run()
	f.RequireCompleted("hello")
}

func TestToolRoundTripCompletes(t *testing.T) {
	f := runtest.New(t)
	f.Tool("echo", run.DirectExecution)
	f.Model(runtest.ToolCalls("echo", "c1"), runtest.Text("done"))
	f.Run()
	f.RequireCompleted("done")
	f.RequireRan("echo")
	f.RequireModelCalls(2)
	f.RequireUsage(3)
	f.RequireFactOpened()
	f.RequirePlannerSawTool("c1", `{"x":1}`)
}

func TestKnownToolFailureContinues(t *testing.T) {
	f := runtest.New(t)
	f.KnownFailure("echo", run.FailureExecution)
	f.Model(runtest.ToolCalls("echo", "c1"), runtest.Text("recovered"))
	f.Run()
	f.RequireCompleted("recovered")
	f.RequireFailureClass(run.FailureExecution)
}

func TestUnknownToolRefContinues(t *testing.T) {
	f := runtest.New(t)
	f.Model(runtest.ToolCalls("ghost", "c1"), runtest.Text("moved on"))
	f.Run()
	f.RequireCompleted("moved on")
	f.RequireFailureClass(run.FailureToolLookup)
}
