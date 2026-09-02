package runtest_test

import (
	"testing"

	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/run/runtest"
)

func TestCancelStopsIdleRun(t *testing.T) {
	f := runtest.New(t)
	f.Cancel()
	f.RequireStopped()
	f.RequireNoUncertain()
	f.Run()
	f.RequireStopped()
}

func TestCancelProjectsExecutingTool(t *testing.T) {
	f := runtest.New(t)
	f.Tool("echo", run.DirectExecution)
	f.ExecutingTool("echo", "c1")
	f.Cancel()
	f.RequireStopped()
	f.RequireUncertainCall("c1")
}

func TestCancelProjectsExecutingModel(t *testing.T) {
	f := runtest.New(t)
	f.ExecutingModel()
	f.Cancel()
	f.RequireStopped()
	f.RequireUncertainModel()
}
