package runmod

import (
	"context"
	"fmt"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/runtime"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
	"github.com/felinics/twilight/agentcore/session/writer"
)

// RequireNoActiveRun is the Run module's quiescence precondition for a
// Session-level operation evaluated inside the Writer's critical section:
// an error while any Run of the Session is active. Terminal Runs leave the machine projection (RUN-CMT-2), so an
// empty Active set is the whole answer: no Run is executing, awaiting an
// effect or holding an unresolved outcome.
func RequireNoActiveRun(view writer.View) error {
	m, err := loadMachine(view)
	if err != nil {
		return err
	}
	for id := range m.Active {
		return fmt.Errorf("runmod: run %s is active", id)
	}
	return nil
}

// SchemaOf returns the protocol Schema of an active Run as the machine
// projection recorded it at creation (RUN-CMT-8): the version every command
// addressed to the Run must carry. It reads through the given projection
// reader, the Writer's own for command planning.
func SchemaOf(ctx context.Context, reader extension.ProjectionReader, sid session.SessionID, runID run.RunID) (schema.Schema, error) {
	state, _, err := reader.Load(ctx, sid, MachineProjectionID, MachineProjection.Version)
	if err != nil {
		return schema.Schema{}, err
	}
	m, ok := state.(Machine)
	if !ok {
		return schema.Schema{}, fmt.Errorf("runmod: machine projection is %T", state)
	}
	v, ok := m.Schemas[runID]
	if !ok {
		return schema.Schema{}, runtime.ErrRunNotFound
	}
	return schema.For(v)
}
