package run

import "context"

// Runtime is the RunID-addressed authority and atomic commit boundary. Create
// establishes immutable Revision-0 state; Load, Commit, and Record operate on
// exactly one Run. MachineState is the semantic state view; planning, queues,
// and tool entry points stay outside this interface.
type Runtime interface {
	Create(context.Context, NewRun) (CreateResult, error)
	Load(context.Context, RunID) (RuntimeSnapshot, error)
	Commit(context.Context, CommitRequest) (CommitResult, error)
	Record(context.Context, RunID) (RunRecord, error)
	// RecoverExpired grantless-commits recovery for expired execution leases.
	// Hosts call it on a timer; Loop does not.
	RecoverExpired(context.Context) (int, error)
}

type RuntimeSnapshot struct {
	// State is a detached in-process view, not a portable persistence format.
	// Durable implementations rebuild through RunHeader + TransitionRecords;
	// any optimized stored snapshot uses an implementation-private codec.
	State MachineState
	// Revision counts accepted transitions; the initial state is 0.
	Revision uint64
	// SchemaVersion is the Run header protocol version. Loop and Application
	// select ProtocolFor(SchemaVersion) once; they must not stamp a process-global
	// currentSchemaVersion (RUN-CMT-7).
	SchemaVersion uint16
}

// Protocol returns the protocol implementation frozen on this Run's header.
func (s RuntimeSnapshot) Protocol() (Protocol, error) {
	return ProtocolFor(s.SchemaVersion)
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
	// Grant is returned for an Accepted start command and for an exact replay
	// while that start is still live; after settlement or terminalization the
	// replay remains idempotent but Grant is empty.
	Grant ExecutionGrant
}
