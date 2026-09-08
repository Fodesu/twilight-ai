// Package session is the append-only log kernel of a Twilight Session
// (docs/design/agent-session.md, edition 2). It owns the header, one row per
// event, group-atomic append, Session-level writer ownership with epoch
// fencing, the per-row digest chain and ordered reads. Payloads are opaque
// canonical JSON that Session modules encode and interpret.
package session

import (
	"fmt"

	"github.com/memohai/twilight/agent/es"
	"github.com/memohai/twilight/agent/jsonstable"
)

type (
	SessionID string
	CommitID  string
	EventType string
	// Seq is the row number inside one stream, contiguous from 0.
	Seq uint64
	// Epoch is the writer ownership generation of a stream, from 1.
	Epoch uint64
)

// ProtocolVersion1 is the current pre-release kernel wire version. It covers
// header fields, row fields, digest preimages and the group completeness rule
// only; payload versions are carried by modules (SES-VER-1).
const ProtocolVersion1 uint16 = 1

// SessionHeader is the immutable creation record of a stream.
type SessionHeader struct {
	ProtocolVersion    uint16           `json:"protocolVersion"`
	SessionID          SessionID        `json:"sessionId"`
	CreatedAtUnixMilli int64            `json:"createdAtUnixMilli"`
	ParentFork         *ForkPoint       `json:"parentFork,omitempty"` // v1: always nil (appendix A)
	CausationID        es.CausationID   `json:"causationId,omitempty"`
	Metadata           jsonstable.Value `json:"metadata,omitempty"`
	HeaderDigest       es.Digest        `json:"headerDigest"`
}

// ForkPoint is reserved for appendix A; v1 rejects non-nil values.
type ForkPoint struct {
	ParentSessionID SessionID `json:"parentSessionId"`
	Seq             Seq       `json:"seq"`
	Digest          es.Digest `json:"digest"`
}

// SessionEvent is one committed row (SES-WIR-1). Rows written by one Append
// share CommitID; Index orders them and Last marks the group's end. Digest
// covers every other field plus the previous row's Digest (SES-WIR-2).
type SessionEvent struct {
	Seq                 Seq              `json:"seq"`
	CommitID            CommitID         `json:"commitId"`
	Index               uint16           `json:"index"`
	Last                bool             `json:"last"`
	Type                EventType        `json:"type"`
	RecordedAtUnixMilli int64            `json:"recordedAtUnixMilli"`
	SourceSeqs          []Seq            `json:"sourceSeqs,omitempty"`
	Ignorable           bool             `json:"ignorable,omitempty"`
	Payload             jsonstable.Value `json:"payload"`
	Digest              es.Digest        `json:"digest"`
}

// UncommittedEvent is what a producer hands to Append.
type UncommittedEvent struct {
	Type                EventType
	RecordedAtUnixMilli int64
	SourceSeqs          []Seq
	Ignorable           bool
	Payload             jsonstable.Value
}

// Group is one atomic append: a non-empty event list under one CommitID.
type Group struct {
	CommitID CommitID
	Events   []UncommittedEvent
}

// Head is the stream position after the last row: the next Seq to assign and
// the last row's Digest. The empty stream head is {0, HeaderDigest}.
type Head struct {
	Next   Seq       `json:"next"`
	Digest es.Digest `json:"digest"`
}

// ErrorCode classifies kernel failures (SES 7).
type ErrorCode string

const (
	ErrInvalid            ErrorCode = "invalid"
	ErrNotFound           ErrorCode = "not_found"
	ErrConflict           ErrorCode = "conflict"
	ErrCorrupt            ErrorCode = "corrupt"
	ErrOwned              ErrorCode = "owned"
	ErrOwnershipLost      ErrorCode = "ownership_lost"
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
