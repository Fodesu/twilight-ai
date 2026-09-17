// Package session is the commit-ledger kernel of a Twilight Session
// (docs/design/agent-session.md). It owns the header, the Commit as the
// atomic unit of append, logical streams within commits, Session-level
// writer ownership with epoch fencing, the commit digest chain and ordered
// reads. Payloads are opaque canonical JSON that Session modules encode and
// interpret.
package session

import (
	"fmt"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/jsonstable"
)

type (
	SessionID string
	CommitID  string
	EventType string
	// Epoch is the writer ownership generation of a stream, from 1.
	Epoch uint64
)

// ProtocolVersion1 is the kernel wire version of the commit ledger: events
// carry no transaction metadata and the Commit is the atomic, chained unit
// that may span logical streams. It covers header fields and the
// commit/batch digest preimages only; payload versions are carried by
// modules (SES-VER-1). It ships with the feat/agent-runtime branch; the
// earlier row model it replaced never left the branch.
const ProtocolVersion1 uint16 = 1

// SessionHeader is the immutable creation record of a commit segment
// (agent-session.md section 8): the node of the lineage DAG the Session that
// created it appends to. SessionID names that creating Session; HeaderDigest
// is the segment's identity (SegmentIDOf). Readers of a Session see the
// header of the segment its root names.
type SessionHeader struct {
	ProtocolVersion    uint16           `json:"protocolVersion"`
	SessionID          SessionID        `json:"sessionId"`
	CreatedAtUnixMilli int64            `json:"createdAtUnixMilli"`
	ParentFork         *ForkPoint       `json:"parentFork,omitempty"` // nil for a root segment; the edge to the parent otherwise
	CausationID        es.CausationID   `json:"causationId,omitempty"`
	Metadata           jsonstable.Value `json:"metadata,omitempty"`
	HeaderDigest       es.Digest        `json:"headerDigest"`
}

// ForkPoint is the edge from a segment to its parent in the lineage DAG: the
// parent segment and the last commit of it the child inherits. The child's
// own commits are numbered from Seq+1 and chained from Digest; readers see
// the inherited prefix [0, Seq] followed by them. The prefix is immutable
// (append-only), so the edge is a stable reference, and it is covered by the
// child's header digest.
type ForkPoint struct {
	Parent SegmentID `json:"parent"`
	Seq    CommitSeq `json:"seq"`
	Digest es.Digest `json:"digest"`
}

// ErrorCode classifies kernel failures (SES 7).
type ErrorCode string

const (
	ErrInvalid       ErrorCode = "invalid"
	ErrNotFound      ErrorCode = "not_found"
	ErrConflict      ErrorCode = "conflict"
	ErrCorrupt       ErrorCode = "corrupt"
	ErrOwned         ErrorCode = "owned"
	ErrOwnershipLost ErrorCode = "ownership_lost"
	// ErrHandleFailed: a previous Append of this Handle failed while writing
	// or persisting, so what reached storage is unknown. The Handle refuses
	// further Appends; the caller reopens and Open reads the log as it is
	// (SES-APP-1).
	ErrHandleFailed       ErrorCode = "handle_failed"
	ErrUnsupportedProfile ErrorCode = "unsupported_profile"
	ErrUnsupported        ErrorCode = "unsupported"
)

// Error is the kernel's discriminable error value.
type Error struct {
	Code      ErrorCode
	Operation string
	SessionID SessionID
	CommitID  CommitID
	Detail    string
}

func (e *Error) Error() string {
	s := fmt.Sprintf("session: %s: %s", e.Operation, e.Code)
	if e.SessionID != "" {
		s += fmt.Sprintf(" session=%s", e.SessionID)
	}
	if e.CommitID != "" {
		s += fmt.Sprintf(" commit=%s", e.CommitID)
	}
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	return s
}

// Is lets callers match on the code: errors.Is(err, &Error{Code: ErrNotFound}).
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	return t.Code == e.Code && (t.Operation == "" || t.Operation == e.Operation)
}

func newError(code ErrorCode, op string, sid SessionID, detail string) *Error {
	return &Error{Code: code, Operation: op, SessionID: sid, Detail: detail}
}

// IsCode reports whether err is a kernel Error with the given code.
func IsCode(err error, code ErrorCode) bool {
	var e *Error
	for err != nil {
		if ce, ok := err.(*Error); ok {
			e = ce
			break
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			break
		}
		err = u.Unwrap()
	}
	return e != nil && e.Code == code
}
