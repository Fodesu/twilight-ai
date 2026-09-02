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

// RunHead is the current authority position of one Run: the immutable header,
// the stored MachineState snapshot at Revision, and the live leases. It never
// carries the transition log; Runtime reads the log separately through LoadLog
// only when it needs to verify or rebuild.
type RunHead struct {
	Header   RunHeader
	State    MachineState
	Revision uint64
	Leases   map[string]ExecutionLease
}

// LeaseOps is the lease change applied atomically with one appended
// transition. Put upserts by lease key, Delete removes by key, Clear removes
// every lease of the Run before Put is applied.
type LeaseOps struct {
	Put    map[string]ExecutionLease
	Delete []string
	Clear  bool
}

// Append is the write Runtime hands back from a Commit critical section: the
// next transition, the snapshot after it, and the lease change. The Store
// persists all three atomically. ExpectedRevision must equal the head
// revision the section observed; a mismatch is a Runtime bug and returns
// ErrAppendConflict.
type Append struct {
	ExpectedRevision uint64
	Transition       TransitionRecord
	State            MachineState
	Leases           LeaseOps
}

// ExpiredLease is one lease whose deadline has passed, addressed by Run and
// lease key so the recovery scanner can load only the affected Runs.
type ExpiredLease struct {
	RunID RunID
	Key   string
	Lease ExecutionLease
}

// ErrAppendConflict reports that a write observed a revision other than the
// one it was built against. Inside a Commit section this cannot happen; it
// guards ReplaceSnapshot and adapter bugs.
var ErrAppendConflict = errors.New("agent: store: append revision conflict")

// RunTx is the view a Commit critical section has of one Run. Both reads see
// the same authority version, and nothing else can advance the Run until the
// section returns.
type RunTx interface {
	Head() RunHead
	LookupTransition(CommandID) (TransitionRecord, bool, error)
}

// Store persists Runs for one Runtime as an append-only transition log plus a
// derived MachineState snapshot and lease table. It does not call
// EvaluateCommit, Decide, or Evolve. The log is never rewritten: the only
// write paths are Create, the Append returned from a Commit section, lease
// maintenance, and ReplaceSnapshot for the derived snapshot. Every Run is a
// single-writer aggregate: Commit serializes all writers of one Run
// (RUN-CMT-2), so Runtime evaluates against a head that cannot move under it.
type Store interface {
	// Create stores the header and Revision-0 snapshot once per RunID. A second
	// Create for the same RunID returns created=false and the stored header.
	Create(ctx context.Context, header RunHeader) (created bool, existing RunHeader, err error)
	// LoadHead returns the header, current snapshot, revision and leases in one
	// consistent read. It does not touch the log.
	LoadHead(ctx context.Context, id RunID) (RunHead, error)
	// LoadLog returns every TransitionRecord with Revision >= from, ordered by
	// revision. from=1 returns the complete log.
	LoadLog(ctx context.Context, id RunID, from uint64) ([]TransitionRecord, error)
	// LoadRecord returns the head and the complete log in one consistent
	// read: a concurrent Commit is either fully visible in both or
	// in neither. Runtime.Record and Rebuild use it.
	LoadRecord(ctx context.Context, id RunID) (RunHead, []TransitionRecord, error)
	// Commit runs fn inside the Run's critical section. When fn returns a
	// non-nil Append, the Store persists it in the same section; a nil Append
	// with a nil error means fn decided without writing (replay or rejection
	// mapped by the caller). fn's error aborts the section unchanged.
	Commit(ctx context.Context, id RunID, fn func(RunTx) (*Append, error)) error
	// RenewLease moves the deadline of the lease at key forward when its grant
	// still equals grant; a missing or re-issued lease returns ErrStaleRuntime.
	RenewLease(ctx context.Context, id RunID, key string, grant ExecutionGrant, deadline time.Time) error
	// ExpiredLeases lists leases whose non-zero deadline is at or before
	// before, across all Runs.
	ExpiredLeases(ctx context.Context, before time.Time) ([]ExpiredLease, error)
	// ReplaceSnapshot overwrites the derived snapshot at revision without
	// touching the log. Rebuild is its only caller.
	ReplaceSnapshot(ctx context.Context, id RunID, revision uint64, state *MachineState) error
}

func cloneRunHead(h *RunHead) RunHead {
	out := RunHead{
		Header:   cloneRunHeader(h.Header),
		State:    cloneMachineState(&h.State),
		Revision: h.Revision,
	}
	if h.Leases != nil {
		out.Leases = make(map[string]ExecutionLease, len(h.Leases))
		for k, v := range h.Leases {
			out.Leases[k] = v
		}
	}
	return out
}

func applyLeaseOps(leases map[string]ExecutionLease, ops LeaseOps) map[string]ExecutionLease {
	out := make(map[string]ExecutionLease, len(leases)+len(ops.Put))
	if !ops.Clear {
		for k, v := range leases {
			out[k] = v
		}
	}
	for _, k := range ops.Delete {
		delete(out, k)
	}
	for k, v := range ops.Put {
		out[k] = v
	}
	return out
}

// ValidateAppend checks the structural preconditions every Store enforces
// before writing: the Append was built against the current head, the
// transition revision is contiguous, and RunIDs agree. Store adapters call it
// inside their Commit section.
func ValidateAppend(id RunID, head *RunHead, a *Append) error {
	if head.Revision != a.ExpectedRevision {
		return fmt.Errorf("%w: expected %d, at %d", ErrAppendConflict, a.ExpectedRevision, head.Revision)
	}
	if a.Transition.RunID != id || a.Transition.Revision != a.ExpectedRevision+1 {
		return fmt.Errorf("agent: store: transition %s@%d does not follow %s@%d", a.Transition.RunID, a.Transition.Revision, id, a.ExpectedRevision)
	}
	if a.State.RunID != id {
		return fmt.Errorf("agent: store: snapshot RunID %q does not match %q", a.State.RunID, id)
	}
	return nil
}

// Rebuild is a diagnostic (RUN-CMT-2): it refolds the state from the complete
// transition log and replaces the derived snapshot with the fold result. The
// log is never modified. A log shorter than the head revision returns
// ErrLogTruncated (audit gap). It returns true when the refolded state differs
// from the stored snapshot, which identifies an Evolve bug, an out-of-band
// write, or storage corruption.
func Rebuild(ctx context.Context, store Store, runID RunID) (rebuilt bool, err error) {
	if store == nil {
		return false, errors.New("agent: rebuild: nil store")
	}
	head, log, err := store.LoadRecord(ctx, runID)
	if err != nil {
		return false, err
	}
	folded, maxRevision, err := FoldRun(&head.Header, log)
	if err != nil {
		return false, err
	}
	if maxRevision < head.Revision {
		return false, fmt.Errorf("%w: log ends at %d, head revision %d", ErrLogTruncated, maxRevision, head.Revision)
	}
	if maxRevision > head.Revision {
		return false, fmt.Errorf("agent: rebuild: log ends at %d beyond head revision %d", maxRevision, head.Revision)
	}
	if statesEquivalent(&head.State, &folded) {
		return false, nil
	}
	if err := store.ReplaceSnapshot(ctx, runID, head.Revision, &folded); err != nil {
		return true, err
	}
	return true, nil
}
