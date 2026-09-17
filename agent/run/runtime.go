package run

import (
	"context"
	"errors"

	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/writer"
)

// RunPosition is the StreamSeq of a Run's last twilight/run/ event. Only the
// Run's own events move it; other modules' events in the same Session leave
// it untouched, which is what makes Prepare's hard CAS insensitive to
// concurrent chatlog or turn writes (RUN-CMT-4).
type RunPosition = session.StreamSeq

// ErrOwnershipLost reports that the Session Writer behind the Runtime was
// superseded (RUN-CMT-6). It is terminal for the caller: no further command of
// this process can reach the stream.
var ErrOwnershipLost = errors.New("agent: session ownership lost")

// Runtime is the Run command entry (RUN-CMT-1): addressed by (SessionID,
// RunID), it evaluates commands inside the Session Writer and appends the
// facts and the caller's attached events as one group. The bodies the facts
// name by digest (RUN-WIR-4) are frozen before the group is appended. Runs are
// created by the Coordinator's Start group; there is no Create.
type Runtime interface {
	// Load is the owner's view of a Run for the next command: it reads the
	// Writer's transactional projections, so a Loop holding a superseded
	// Writer plans against its own epoch's state and is fenced at commit
	// (RUN-LOP-5). Record is the read: it folds from the Store by SessionID
	// and needs no ownership (AUTH-OWN-2).
	Load(context.Context, writer.Writer, RunID) (RuntimeSnapshot, error)
	Record(context.Context, session.SessionID, RunID) (RunRecord, error)
	// Commit is a command: it takes the Session's Writer, the caller's
	// ownership capability, and commits through it (AUTH-OWN-2).
	Commit(context.Context, writer.Writer, CommitRequest) (CommitResult, error)
	// FrozenRequest returns the request body a Prepared or Executing ModelStep
	// names by RequestDigest (RUN-WIR-4); a missing body is ErrFrozenValueMissing.
	FrozenRequest(context.Context, Digest) (ModelRequest, error)
	// RecoverInterrupted is the takeover disposition (RUN-CMT-7). For every
	// Executing target of the Session it first asks reattach whether the
	// attempt that started it is still producing an Outcome; a target it can
	// reattach stays Executing and its Outcome settles under the original
	// Claim, every other target gets one recovery command. A nil reattach
	// disposes everything. The host calls it once after opening the Writer and
	// before driving any Run; it returns the number of accepted recovery
	// commands.
	RecoverInterrupted(context.Context, writer.Writer, Reattacher) (int, error)
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
// caller attaches to a command: a genuine fact of that module produced by the
// same semantic operation (an input delivered, a Turn stopped), appended
// after the Run facts in the same group. The module implementation encodes it
// through the Registry. Nothing derived from the Run facts travels this way:
// conversation and Turn state are projections of the facts themselves.
type ModuleEvent struct {
	Type  session.EventType
	Value any
}

type CommitRequest struct {
	// Base is the Position the caller loaded. PrepareModelRequest treats it
	// as a hard CAS; other commands rebase call-locally and may pass zero
	// (RUN-CMT-4).
	Base    RunPosition
	Command CommandEnvelope
	// Attach are caller events appended after the Run facts; they must not be
	// twilight/run/ events.
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
	// Events is the complete commit in batch order: run facts, then attach.
	Events []session.Event
}

// RunRecord is one verified read of a Run: every twilight/run/ event of the
// RunID in stream order, folded and compared with the projection.
type RunRecord struct {
	Created  session.StreamSeq
	Snapshot RuntimeSnapshot
	Events   []session.Event
	Facts    []Fact
}
