package loop

import (
	"context"
	"testing"

	. "github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/sdk"
)

func TestLoopPrepareRejectionDoesNotLivelock(t *testing.T) {
	rt := loopRuntime(t)
	interpreter, err := New(fakeCatalog{&fakeInvoker{}}, fakeToolCatalog{},
		badPlanner{}, ExecutionPolicy{}, false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = interpreter.Run(context.Background(), rt, "run-1", nil)
	if err == nil {
		t.Fatal("prepare livelock not surfaced")
	}
}

// badPlanner never consumes pending inputs, so its Prepare is always rejected.
type badPlanner struct{}

func (badPlanner) Plan(_ context.Context, _ PlanningHint) (RequestPlan, error) {
	return RequestPlan{Model: testModel, Request: sdk.Request{Model: string(testModel)}}, nil
}
