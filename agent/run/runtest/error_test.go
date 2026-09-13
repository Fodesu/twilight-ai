package runtest_test

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agent/run/runtest"
)

func TestCatalogResolveErrorLeavesRunActive(t *testing.T) {
	missing := errors.New("missing provider")
	f := runtest.New(t)
	f.ModelResolveError(missing)
	f.RunError(missing)
	f.RequireActive()
	// The unstartable attempt is withdrawn: the Run is Open for a fresh plan
	// once a model is available (RUN-LOP-3).
	f.RequireOpen()
}

func TestContextCancelBeforeRunLeavesRunActive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f := runtest.New(t)
	f.Model(runtest.Text("resumed"))
	f.Context(ctx)
	f.RunError(context.Canceled)
	f.RequireActive()
	f.Context(context.Background())
	f.Run()
	f.RequireCompleted("resumed")
}
