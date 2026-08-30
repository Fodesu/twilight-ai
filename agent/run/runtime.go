package run

import "context"

// Runtime is the RunID-addressed access and atomic commit boundary. Create
// establishes immutable Revision-0 state; Load, Commit, and Record operate on
// exactly one Run. MachineState remains execution authority; planning, queues,
// and tool entry points are not hidden methods.
type Runtime interface {
	Create(context.Context, NewRun) (CreateResult, error)
	Load(context.Context, RunID) (RuntimeSnapshot, error)
	Commit(context.Context, CommitRequest) (CommitResult, error)
	Record(context.Context, RunID) (RunRecord, error)
}

type RuntimeSnapshot struct {
	State MachineState
	// Revision counts accepted transitions; the initial state is 0.
	Revision uint64
}

type CommitRequest struct {
	BaseRevision uint64
	Grant        ExecutionGrant
	Command      CommandEnvelope
}

type CommitStatus uint8

const (
	CommitAccepted CommitStatus = iota
	CommitAlreadyApplied
)

type CommitResult struct {
	Status   CommitStatus
	Snapshot RuntimeSnapshot
	// Events is the complete event group of the transition, for Accepted and
	// AlreadyApplied alike.
	Events []AgentEvent
	// Grant is returned only for an Accepted start command; empty otherwise.
	Grant ExecutionGrant
}
