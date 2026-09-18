package run

import (
	"context"
	"errors"
)

// Scope is the opaque identity of the store one Run lives in: a Session in
// Twilight. Run never interprets it. It only scopes what has to stay distinct
// across stores -- execution keys handed to a shared executor, the takeover
// claim of a new owner, the prompt builder's read of the surrounding
// conversation -- and the adapter that realizes RunStore converts it to and
// from its own identity type.
type Scope string

// RunPosition is the index of a Run's last fact in the Run's own stream.
// Only the Run's own facts move it; whatever else the surrounding store
// appends leaves it untouched, which is what makes Prepare's hard CAS
// insensitive to concurrent writes of other modules (RUN-CMT-4).
type RunPosition uint64

// ErrOwnershipLost reports that the write capability behind a RunStore was
// superseded (RUN-CMT-6). It is terminal for the caller: no further command of
// this process can reach the stream.
var ErrOwnershipLost = errors.New("agent: run store ownership lost")

// RunStore is the transactional port of the Run core (RUN-CMT-1): the store
// the Runs of one Scope live in, already bound to the caller's write
// capability. It speaks only Run types; how a command reaches durable
// storage, how facts are encoded on the wire and how the bound capability
// fences a stale owner are the adapter's business (agent/session/run). Runs
// are created by the owning module's creation commit; there is no Create.
type RunStore interface {
	// Scope is the store this port is bound to.
	Scope() Scope
	// Load is the owner's view of a Run for the next command. A port whose
	// capability was superseded still answers from its own epoch's state and
	// is fenced at Commit (RUN-LOP-5).
	Load(context.Context, RunID) (RuntimeSnapshot, error)
	// Commit evaluates one command (RUN-CMT-3) and appends its facts as one
	// atomic group. Replays are answered from the store's command index
	// without re-deciding (RUN-CMT-5).
	Commit(context.Context, CommitRequest) (CommitResult, error)
	// FrozenRequest returns the request body a Prepared or Executing ModelStep
	// names by RequestDigest (RUN-WIR-4); a missing body is ErrFrozenValueMissing.
	FrozenRequest(context.Context, Digest) (ModelRequest, error)
}

// RuntimeSnapshot is one Run as a command sees it.
type RuntimeSnapshot struct {
	// State is a detached in-process view.
	State MachineState
	// Position is the Run's last fact position at read time.
	Position RunPosition
	// SchemaVersion is the Session segment's, read from the version of the
	// Run's facts; Loop and Application select SchemaFor(SchemaVersion) once.
	SchemaVersion uint16
}

// Schema returns the schema the Run's segment declares.
func (s RuntimeSnapshot) Schema() (Schema, error) { //nolint:gocritic // hugeParam: RuntimeSnapshot is handed around by value; a pointer receiver would refuse the common snapshot.Schema() on a temporary
	return SchemaFor(s.SchemaVersion)
}

type CommitRequest struct {
	// Base is the Position the caller loaded. PrepareModelRequest treats it
	// as a hard CAS; other commands rebase call-locally and may pass zero
	// (RUN-CMT-4).
	Base    RunPosition
	Command CommandEnvelope
}

type CommitStatus uint8

const (
	CommitAccepted CommitStatus = iota
	CommitAlreadyApplied
)

type CommitResult struct {
	Status   CommitStatus
	Snapshot RuntimeSnapshot
	// Facts are the Run facts the command produced, in stream order. On a
	// replay they are the facts of the original commit.
	Facts []Fact
}
