// Package session is the Event Sourcing kernel of a Twilight Session
// (docs/design/agent-session.md). It owns the envelope, ordering, commit,
// critical section, snapshot and control-plane KV mechanics; payloads are
// opaque canonical JSON that Session modules encode and interpret.
package session

import (
	"fmt"

	"github.com/memohai/twilight/agent/es"
	"github.com/memohai/twilight/agent/jsonstable"
)

type (
	SessionID        string
	CommitID         string
	EventID          string
	EventType        string
	ProjectionKey    string
	CursorToken      string
	ControlNamespace string
)

// ProtocolVersion1 is the current pre-release kernel wire version. It covers
// header, envelope, commit, snapshot envelope and digest profile only;
// payload versions are carried by modules (SES-VER-1).
const ProtocolVersion1 uint16 = 1

// SessionHeader is the immutable creation record of a stream (SES-WIR-1).
type SessionHeader struct {
	ProtocolVersion uint16           `json:"protocolVersion"`
	SessionID       SessionID        `json:"sessionId"`
	ParentFork      *ForkPoint       `json:"parentFork,omitempty"` // v1: always nil (appendix A)
	CausationID     es.CausationID   `json:"causationId,omitempty"`
	Metadata        jsonstable.Value `json:"metadata,omitempty"`
	HeaderDigest    es.Digest        `json:"headerDigest"`
}

// ForkPoint is reserved for appendix A; v1 rejects non-nil values.
type ForkPoint struct {
	ParentSessionID SessionID   `json:"parentSessionId"`
	Revision        es.Revision `json:"revision"`
	HeadDigest      es.Digest   `json:"headDigest"`
}

// SessionEvent is one committed event. Index orders events inside a commit;
// EventDigest additionally covers SessionID, Revision and Index.
type SessionEvent struct {
	EventID             EventID          `json:"eventId"`
	Index               uint16           `json:"index"`
	Type                EventType        `json:"type"`
	RecordedAtUnixMilli int64            `json:"recordedAtUnixMilli"`
	SourceEvents        []EventID        `json:"sourceEvents,omitempty"`
	Payload             jsonstable.Value `json:"payload"`
	EventDigest         es.Digest        `json:"eventDigest"`
}

// UncommittedEvent is what a producer hands to an append port.
type UncommittedEvent struct {
	EventID             EventID
	Type                EventType
	RecordedAtUnixMilli int64
	SourceEvents        []EventID
	Payload             jsonstable.Value
}

// SessionCommit is one atomic append: the unit of ordering and of the digest
// chain. Revision 1 chains to HeaderDigest, later commits to the previous
// CommitDigest.
type SessionCommit struct {
	ProtocolVersion uint16         `json:"protocolVersion"`
	SessionID       SessionID      `json:"sessionId"`
	Revision        es.Revision    `json:"revision"`
	PreviousDigest  es.Digest      `json:"previousDigest"`
	CommitID        CommitID       `json:"commitId"`
	CausationID     es.CausationID `json:"causationId,omitempty"`
	CorrelationID   string         `json:"correlationId,omitempty"`
	Events          []SessionEvent `json:"events"`
	CommitDigest    es.Digest      `json:"commitDigest"`
}

// Head is the stream position after the last commit. The empty stream head
// is {0, HeaderDigest}.
type Head struct {
	Revision es.Revision `json:"revision"`
	Digest   es.Digest   `json:"digest"`
}

// EventPosition addresses one committed event exactly.
type EventPosition struct {
	Revision    es.Revision `json:"revision"`
	Index       uint16      `json:"index"`
	EventDigest es.Digest   `json:"eventDigest"`
}

// Snapshot is a discardable projection cache (SES-SNP-1). Through.Digest is
// the coverage proof: it must equal the CommitDigest at Through.Revision.
type Snapshot struct {
	ProtocolVersion   uint16           `json:"protocolVersion"`
	SessionID         SessionID        `json:"sessionId"`
	ProjectionKey     ProjectionKey    `json:"projectionKey"`
	ProjectionVersion uint16           `json:"projectionVersion"`
	Through           Head             `json:"through"`
	State             jsonstable.Value `json:"state"`
	SnapshotDigest    es.Digest        `json:"snapshotDigest"`
}

// ControlEntry is one control-plane KV row (SES-API-3). The kernel never
// interprets Value and never deletes an entry because its deadline passed.
type ControlEntry struct {
	SessionID         SessionID
	Namespace         ControlNamespace
	Key               string
	Value             []byte
	DeadlineUnixMilli int64
}

// ErrorCode classifies kernel failures (SES 4).
type ErrorCode string

const (
	ErrInvalid            ErrorCode = "invalid"
	ErrNotFound           ErrorCode = "not_found"
	ErrConflict           ErrorCode = "conflict"
	ErrCorrupt            ErrorCode = "corrupt"
	ErrUnsupportedProfile ErrorCode = "unsupported_profile"
	ErrUnsupported        ErrorCode = "unsupported"
	ErrUnavailable        ErrorCode = "unavailable"
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
