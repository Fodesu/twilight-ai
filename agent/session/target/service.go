package target

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/writer"
)

// Commands are the binding's commands, each through the Writer the caller
// owns (AUTH-OWN-2). They are the only writer of target facts.
type Commands struct {
	Now func() time.Time
}

// Guard is a caller-supplied precondition evaluated inside Bind's commit
// critical section, on the View Bind reads. The application injects the
// quiescence rule of the turn domain (turn.RequireNoActiveTurn, APP-TGT-1),
// which this package cannot import.
type Guard func(v writer.View) error

// Bind makes ref the Session's current target (APP-TGT-1): every tool
// effect started afterwards resolves to it (RUN-LOP-9). Binding the target
// that is already current writes nothing, so a retried Bind is idempotent.
// The guard runs first, on the same View.
func (s *Commands) Bind(ctx context.Context, w writer.Writer, ref run.TargetRef, guard Guard) error {
	if ref.Kind == "" || ref.ID == "" {
		return errors.New("target: bind requires a target kind and id")
	}
	res, err := w.Commit(ctx, func(v writer.View) (*writer.SemanticGroup, error) {
		if guard != nil {
			if err := guard(v); err != nil {
				return nil, err
			}
		}
		cur, err := load(v)
		if err != nil {
			return nil, err
		}
		if cur.Target != nil && *cur.Target == ref {
			return nil, nil
		}
		// The commit is identified by the ledger position it lands at. A
		// retry that finds ref already current writes nothing above; one
		// that finds another commit at the position re-plans on the new
		// View. The SessionID keeps a fork child's binds apart from the
		// commits it inherits.
		id := session.CommitID(fmt.Sprintf("target-bound/%s/%d", w.SessionID(), v.Head().Next))
		return &writer.SemanticGroup{CommitID: id,
			Batches: []writer.TypedBatch{{Stream: Stream, Events: []writer.TypedEvent{{
				Type: TypeBound, RecordedAtUnixMilli: s.Now().UnixMilli(), Value: BoundPayload{Target: ref},
			}}}}}, nil
	})
	if err != nil {
		return err
	}
	switch res.Outcome {
	case writer.CommitApplied, writer.CommitAlreadyApplied, writer.CommitNoop:
		return nil
	default:
		return fmt.Errorf("target: bind: %s: %s", res.Outcome, res.Detail)
	}
}

// load reads the current target from a Writer's View inside a commit.
func load(v writer.View) (Current, error) {
	state, err := v.Projection(ProjectionID, Projection.Version)
	if err != nil {
		return Current{}, err
	}
	return current(state)
}
