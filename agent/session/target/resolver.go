package target

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
)

// ErrUnbound reports a tool effect of a Session with no bound target under
// a Resolver that requires one.
var ErrUnbound = errors.New("target: session has no bound target")

// Resolver is the loop.TargetResolver over the binding (RUN-LOP-9,
// APP-TGT-1): a tool effect resolves to the Session's current target at the
// moment the effect starts and a model effect to none. An unbound Session
// gives its tool effects no target unless Required, in which case the
// effect does not start.
type Resolver struct {
	Projections extension.ProjectionReader
	Required    bool
}

var _ loop.TargetResolver = (*Resolver)(nil)

// ResolveTarget implements loop.TargetResolver.
func (r *Resolver) ResolveTarget(ctx context.Context, ec loop.EffectContext) (*run.TargetRef, error) {
	if ec.Kind != loop.AssignmentTool {
		return nil, nil
	}
	cur, err := Read(ctx, r.Projections, session.SessionID(ec.Session))
	if err != nil {
		return nil, err
	}
	if cur.Target == nil {
		if r.Required {
			return nil, fmt.Errorf("%w: %s", ErrUnbound, ec.Session)
		}
		return nil, nil
	}
	bound := *cur.Target
	return &bound, nil
}

// Read loads the Session's current target through r; Target is nil while
// the Session is unbound.
func Read(ctx context.Context, r extension.ProjectionReader, sid session.SessionID) (Current, error) {
	state, _, err := r.Load(ctx, sid, ProjectionID, Projection.Version)
	if err != nil {
		return Current{}, err
	}
	return current(state)
}
