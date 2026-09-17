package turn

import (
	"context"
	"fmt"

	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
	"github.com/felinics/twilight/agent/session/writer"
)

// ReadSurface loads the turn surface of one Session through r.
func ReadSurface(ctx context.Context, r extension.ProjectionReader, sid session.SessionID) (TurnSurface, error) {
	state, _, err := r.Load(ctx, sid, SurfaceProjectionID, SurfaceProjection.Version)
	if err != nil {
		return TurnSurface{}, err
	}
	return state.(TurnSurface), nil
}

// RequireNoActiveTurn is the turn layer's quiescence precondition for a
// commit of another domain, evaluated inside the Writer's critical section:
// ErrConflict while a Turn is active (HST-CKP-1).
func RequireNoActiveTurn(v writer.View) error {
	state, err := v.Projection(SurfaceProjectionID, SurfaceProjection.Version)
	if err != nil {
		return err
	}
	surface := state.(TurnSurface)
	if active, ok := surface.Active(); ok {
		return fmt.Errorf("%w: turn %s is active", ErrConflict, active.TurnID)
	}
	return nil
}
