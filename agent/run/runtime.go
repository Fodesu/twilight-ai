package run

import (
	"context"

	"github.com/memohai/twilight/agent/es"
	"github.com/memohai/twilight/agent/session"
)

// RunPosition is the stream position of a Run's last twilight/run/ event.
// Only the Run's own events move it; other modules' commits in the same
// Session leave it untouched, which is what makes Prepare's hard CAS
// insensitive to concurrent chatlog or turn writes (RUN-CMT-4).
type RunPosition struct {
	Revision es.Revision `json:"revision"`
	Index    uint16      `json:"index"`
}

// Runtime is the Run command entry (RUN-CMT-1): addressed by (SessionID,
// RunID), it evaluates commands inside the Session critical section and
// appends facts, companion content and attached events as one SessionCommit.
// Runs are created by the Coordinator's Start group; there is no Create.
type Runtime interface {
	Load(context.Context, session.SessionID, RunID) (RuntimeSnapshot, error)
	Commit(context.Context, session.SessionID, CommitRequest) (CommitResult, error)
	Record(context.Context, session.SessionID, RunID) (RunRecord, error)
	// FrozenRequest returns the request body a Prepared or Executing ModelStep
	// names by RequestDigest (RUN-WIR-4); a missing body is ErrFrozenValueMissing.
	FrozenRequest(context.Context, Digest) (ModelRequest, error)
	// RenewLease extends the lease behind grant on the Executing target
	// (stepID alone for a ModelStep, stepID+callID for a tool call).
	RenewLease(ctx context.Context, sessionID session.SessionID, runID RunID, stepID StepID, callID CallID, grant ExecutionGrant) error
	// RecoverExpired grantless-commits recovery for expired execution leases.
	// Hosts call it on a timer; Loop does not.
	RecoverExpired(context.Context) (int, error)
}

type RuntimeSnapshot struct {
	// State is a detached in-process view.
	State MachineState
	// Position is the Run's last event position at read time.
	Position RunPosition
	// Head is the Session head at read time.
	Head session.Head
	// SchemaVersion is created.SchemaVersion; Loop and Application select
	// ProtocolFor(SchemaVersion) once.
	SchemaVersion uint16
}

// Protocol returns the protocol frozen at the Run's creation.
func (s RuntimeSnapshot) Protocol() (Protocol, error) {
	return ProtocolFor(s.SchemaVersion)
}

// ModuleEvent is a typed event of another module (chatlog, turn) that the
// Runtime appends after the Run facts in the same commit. The module
// implementation encodes it through the Registry.
type ModuleEvent struct {
	Type  session.EventType
	Value any
}

// CompanionRequest is what the Runtime hands the Companion after Evolve:
// the command (with its transient content), the facts and the new state.
type CompanionRequest struct {
	Session             session.SessionID
	Owner               OwnerID
	RunID               RunID
	Command             AgentCommand
	Facts               []Fact
	State               MachineState
	RecordedAtUnixMilli int64
}

// Companion maps Run facts and the command's transient content to the
// conversation events that travel in the same commit (TRN-CMP). Map must be
// a deterministic pure function.
type Companion interface {
	Version() string
	Map(CompanionRequest) ([]ModuleEvent, error)
}

type CommitRequest struct {
	// Base is the Position the caller loaded. PrepareModelRequest treats it
	// as a hard CAS; other commands rebase call-locally (RUN-CMT-4).
	Base    RunPosition
	Grant   ExecutionGrant
	Command CommandEnvelope
	// Attach are caller events appended after the companion events; they must
	// not be twilight/run/ events.
	Attach []ModuleEvent
}

type CommitStatus uint8

const (
	CommitAccepted CommitStatus = iota
	CommitAlreadyApplied
)

type CommitResult struct {
	Status   CommitStatus
	Snapshot RuntimeSnapshot
	// Commit is the complete SessionCommit: run facts, companion, attach.
	Commit session.SessionCommit
	// Grant is returned for an Accepted start and for an exact replay while
	// that start is still live; otherwise empty.
	Grant ExecutionGrant
}

// RunRecord is one verified read of a Run: every twilight/run/ event of the
// RunID in stream order, folded and compared with the projection.
type RunRecord struct {
	Created  session.EventPosition
	Snapshot RuntimeSnapshot
	Events   []session.SessionEvent
	Facts    []Fact
}
