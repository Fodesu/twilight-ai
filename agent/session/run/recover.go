package runmod

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/canonical"
	"github.com/felinics/twilight/agent/run/reconcile"
	"github.com/felinics/twilight/agent/run/runtime"
	"github.com/felinics/twilight/agent/session/writer"
)

// Reconciler decides the takeover disposition of one Run's Executing targets
// (RUN-CMT-7); reconcile.Reconciler is the implementation.
type Reconciler interface {
	Reconcile(ctx context.Context, store runtime.RunStore, snapshot *runtime.Snapshot, claim run.ExecutionClaim) (int, error)
}

// RecoverInterrupted is the Session-level takeover (RUN-CMT-7): every active
// Run of the Session is reconciled under the takeover claim of the Writer's
// Epoch, so the same owner repeats idempotently. The host calls it once after
// opening the Writer and before driving any Run. A nil Reconciler disposes
// every Executing target. It returns the number of accepted recovery commands.
func (s *SessionRunStore) RecoverInterrupted(ctx context.Context, w writer.Writer, rec Reconciler) (int, error) {
	if err := runtime.CheckContext(ctx); err != nil {
		return 0, err
	}
	if w == nil {
		return 0, errors.New("runmod: recovery requires the session's writer")
	}
	if rec == nil {
		rec = &reconcile.Reconciler{}
	}
	sid := w.SessionID()
	state, _, err := w.Projections().Load(ctx, sid, MachineProjectionID, MachineProjection.Version)
	if err != nil {
		return 0, err
	}
	m, ok := state.(Machine)
	if !ok {
		return 0, fmt.Errorf("runmod: machine projection is %T", state)
	}
	store := s.Bind(w)
	claim := canonical.DeriveTakeoverClaim(store.Scope(), uint64(w.Epoch()))
	n := 0
	for runID := range m.Active {
		snapshot, _ := m.snapshot(runID)
		accepted, err := rec.Reconcile(ctx, store, &snapshot, claim)
		n += accepted
		if err != nil {
			return n, err
		}
	}
	return n, nil
}
