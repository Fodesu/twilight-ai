package run

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ExecutionLease is the Store occupancy record for one live start. Grant is
// the capability returned to the owner Loop. Deadline zero means the lease
// does not expire (process-lifetime occupancy).
type ExecutionLease struct {
	Claim          ExecutionClaim
	Grant          ExecutionGrant
	StartCommandID CommandID
	Deadline       time.Time
}

// StoredRun is one Run's persisted view. Store implementations clone on the
// way out of View and isolate Update mutations until fn returns nil.
type StoredRun struct {
	Header      RunHeader
	State       MachineState
	Revision    uint64
	Watermark   uint64
	Log         []TransitionRecord
	Transitions map[CommandID]TransitionRecord
	Leases      map[string]ExecutionLease
	StartGrants map[CommandID]ExecutionGrant
}

// Store persists Runs for one Runtime. It does not call EvaluateCommit,
// Decide, or Evolve. SQLite and Postgres adapters implement this contract;
// MemoryStore is the in-process implementation.
type Store interface {
	Create(ctx context.Context, header RunHeader) (created bool, existing RunHeader, err error)
	View(ctx context.Context, id RunID) (StoredRun, error)
	Update(ctx context.Context, id RunID, fn func(*StoredRun) error) error
	ListIDs(ctx context.Context) ([]RunID, error)
}

func cloneStoredRun(s StoredRun) StoredRun {
	out := StoredRun{
		Header:    cloneRunHeader(s.Header),
		State:     cloneMachineState(&s.State),
		Revision:  s.Revision,
		Watermark: s.Watermark,
	}
	if s.Log != nil {
		out.Log = cloneTransitionRecords(s.Log)
	}
	if s.Transitions != nil {
		out.Transitions = make(map[CommandID]TransitionRecord, len(s.Transitions))
		for id, rec := range s.Transitions {
			out.Transitions[id] = cloneTransitionRecord(&rec)
		}
	}
	if s.Leases != nil {
		out.Leases = make(map[string]ExecutionLease, len(s.Leases))
		for k, v := range s.Leases {
			out.Leases[k] = v
		}
	}
	if s.StartGrants != nil {
		out.StartGrants = make(map[CommandID]ExecutionGrant, len(s.StartGrants))
		for k, v := range s.StartGrants {
			out.StartGrants[k] = v
		}
	}
	return out
}

func (s *StoredRun) forgetStartGrant(grant ExecutionGrant) {
	if grant == "" || s.StartGrants == nil {
		return
	}
	for commandID, candidate := range s.StartGrants {
		if candidate == grant {
			delete(s.StartGrants, commandID)
		}
	}
}

func (s *StoredRun) clearLeases() {
	s.Leases = make(map[string]ExecutionLease)
	s.StartGrants = make(map[CommandID]ExecutionGrant)
}

// Rebuild is an optional diagnostic (RUN-CMT-2): it refolds the state from
// the transition log and replaces the stored state with the fold result.
// Runtime.Commit remains the normal state transition path. A log shorter than
// the last committed revision returns ErrLogTruncated (audit gap). It returns
// true when the refolded state differs from the stored state, which identifies
// an Evolve bug, out-of-band write, or storage corruption.
func Rebuild(ctx context.Context, store Store, runID RunID) (rebuilt bool, err error) {
	if store == nil {
		return false, errors.New("agent: rebuild: nil store")
	}
	var diverged bool
	err = store.Update(ctx, runID, func(stored *StoredRun) error {
		folded, maxRevision, foldErr := FoldTransitions(cloneMachineState(&stored.Header.InitialState), stored.Log)
		if foldErr != nil {
			return foldErr
		}
		if maxRevision < stored.Watermark {
			return fmt.Errorf("%w: log ends at %d, watermark %d", ErrLogTruncated, maxRevision, stored.Watermark)
		}
		diverged = stored.Revision != maxRevision || !statesEquivalent(&stored.State, &folded)
		stored.State = folded
		stored.Revision = maxRevision
		return nil
	})
	return diverged, err
}
