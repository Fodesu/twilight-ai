package loop

import (
	"context"

	run "github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/writer"
)

// boundRuntime binds a run.Runtime to the Session one Loop.Run drives, so the
// interpreter stays RunID-addressed internally.
type boundRuntime struct {
	rt run.Runtime
	// w is the Session's Writer: the ownership capability every commit of
	// the drive goes through (AUTH-OWN-2).
	w writer.Writer
}

func (b boundRuntime) sid() session.SessionID { return b.w.SessionID() }

func (b boundRuntime) Load(ctx context.Context, runID run.RunID) (run.RuntimeSnapshot, error) {
	return b.rt.Load(ctx, b.w, runID)
}

func (b boundRuntime) Commit(ctx context.Context, req run.CommitRequest) (run.CommitResult, error) {
	return b.rt.Commit(ctx, b.w, req)
}

func (b boundRuntime) FrozenRequest(ctx context.Context, digest run.Digest) (run.ModelRequest, error) {
	return b.rt.FrozenRequest(ctx, digest)
}
