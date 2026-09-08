package run

import (
	"context"
	"errors"

	"github.com/memohai/twilight/agent/session"
)

// RunPosition is the Seq of a Run's last twilight/run/ event. Only the Run's
// own events move it; other modules' rows in the same Session leave it
// untouched, which is what makes Prepare's hard CAS insensitive to concurrent
// chatlog or turn writes (RUN-CMT-4).
type RunPosition = session.Seq

// ErrOwnershipLost reports that the Session Writer behind the Runtime was
// superseded (RUN-CMT-6). It is terminal for the caller: no further command of
// this process can reach the stream.
var ErrOwnershipLost = errors.New("agent: session ownership lost")

// Runtime is the Run command entry (RUN-CMT-1): addressed by (SessionID,
// RunID), it evaluates commands inside the Session Writer and appends facts,
// companion content and attached events as one group. Runs are created by the
// Coordinator's Start group; there is no Create.
type Runtime interface {
	Load(context.Context, session.SessionID, RunID) (RuntimeSnapshot, error)
	Commit(context.Context, session.SessionID, CommitRequest) (CommitResult, error)
	Record(context.Context, session.SessionID, RunID) (RunRecord, error)
	// FrozenRequest returns the request body a Prepared or Executing ModelStep
	// names by RequestDigest (RUN-WIR-4); a missing body is ErrFrozenValueMissing.
	FrozenRequest(context.Context, Digest) (ModelRequest, error)
	// RecoverInterrupted is the takeover disposition (RUN-CMT-7): one recovery
	// command per Executing target of the Session. The host calls it once after
	// opening the Writer and before driving any Run; it returns the number of
	// accepted commands.
	RecoverInterrupted(context.Context, session.SessionID) (int, error)
}

type RuntimeSnapshot struct {
	// State is a detached in-process view.
	State MachineState
	// Position is the Run's last event Seq at read time.
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
// Runtime appends after the Run facts in the same group. The module
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
// conversation events that travel in the same group (TRN-CMP). Map must be a
// deterministic pure function.
type Companion interface {
	Version() string
	Map(CompanionRequest) ([]ModuleEvent, error)
}

type CommitRequest struct {
	// Base is the Position the caller loaded. PrepareModelRequest treats it
	// as a hard CAS; other commands rebase call-locally and may pass zero
	// (RUN-CMT-4).
	Base    RunPosition
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
	// Events is the complete group: run facts, companion, attach.
	Events []session.SessionEvent
}

// RunRecord is one verified read of a Run: every twilight/run/ event of the
// RunID in Seq order, folded and compared with the projection.
type RunRecord struct {
	Created  session.Seq
	Snapshot RuntimeSnapshot
	Events   []session.SessionEvent
	Facts    []Fact
}
