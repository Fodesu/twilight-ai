package session

import (
	"context"

	"github.com/memohai/twilight/agent/es"
	"github.com/memohai/twilight/agent/jsonstable"
)

// CreateRequest establishes a stream. Field-identical repeats are idempotent;
// a different request for the same SessionID is a Conflict (SES-WIR-1).
type CreateRequest struct {
	ProtocolVersion uint16
	SessionID       SessionID
	CausationID     es.CausationID
	Metadata        jsonstable.Value
}

// AppendRequest is one atomic commit to append (SES 5).
type AppendRequest struct {
	SessionID     SessionID
	ExpectedHead  Head
	CommitID      CommitID
	CausationID   es.CausationID
	CorrelationID string
	Events        []UncommittedEvent
}

type AppendDisposition string

const (
	AppendApplied        AppendDisposition = "applied"
	AppendAlreadyApplied AppendDisposition = "already_applied"
	AppendHeadConflict   AppendDisposition = "head_conflict"
	AppendCommitConflict AppendDisposition = "commit_conflict"
	AppendInvalid        AppendDisposition = "invalid"
)

type AppendResult struct {
	Disposition AppendDisposition
	Commit      *SessionCommit
	ActualHead  Head
	// Detail explains an Invalid disposition.
	Detail string
}

// SessionTx is the read/write view inside CommitIn's critical section
// (SES-API-2). Every method takes effect in the same transaction as the
// commit the fn decides to append.
type SessionTx interface {
	Head() Head
	LookupCommit(CommitID) (SessionCommit, bool, error)
	// Tail returns the commits after `after`; with a non-empty types filter
	// only commits carrying at least one event whose Type has one of the
	// prefixes are returned.
	Tail(after Head, types []EventType) ([]SessionCommit, error)
	LoadSnapshot(ProjectionKey, uint16) (SnapshotResult, error)
	SaveSnapshot(Snapshot) error
	ControlGet(ControlNamespace, string) (ControlEntry, bool, error)
	ControlPut(ControlNamespace, string, []byte, int64) error
	ControlDelete(ControlNamespace, string) error
}

// CommitInFn decides, inside the critical section, what to append. nil means
// append nothing; snapshot and KV writes already made through tx still commit.
type CommitInFn func(SessionTx) (*AppendRequest, error)

type ReplayCursor struct {
	After *EventPosition
	Token CursorToken
}

type ReplayRequest struct {
	SessionID SessionID
	Types     []EventType // empty = all; otherwise EventType prefix filter
	Cursor    *ReplayCursor
	Limit     uint32
}

type ReplayPage struct {
	Header  SessionHeader
	Commits []SessionCommit
	Next    *ReplayCursor
	Head    Head
}

type SnapshotRequest struct {
	SessionID         SessionID
	ProjectionKey     ProjectionKey
	ProjectionVersion uint16
}
type SnapshotResult struct {
	Snapshot *Snapshot
	Found    bool
}
type SaveSnapshotRequest struct{ Snapshot Snapshot }
type SaveSnapshotResult struct {
	Snapshot Snapshot
	Replaced bool
}

// Store is the kernel port (SES 4). Commit and CommitIn are the only append
// entries; both persist the commit, the new head and any same-call snapshot
// and control-plane writes atomically.
type Store interface {
	Create(context.Context, CreateRequest) (SessionHeader, error)
	Header(context.Context, SessionID) (SessionHeader, error)
	Head(context.Context, SessionID) (Head, error)
	LookupCommit(context.Context, SessionID, CommitID) (SessionCommit, bool, error)
	Commit(context.Context, AppendRequest) (AppendResult, error)
	CommitIn(context.Context, SessionID, CommitInFn) (AppendResult, error)
	Replay(context.Context, ReplayRequest) (ReplayPage, error)
	LoadSnapshot(context.Context, SnapshotRequest) (SnapshotResult, error)
	SaveSnapshot(context.Context, SaveSnapshotRequest) (SaveSnapshotResult, error)
	ControlGet(context.Context, SessionID, ControlNamespace, string) (ControlEntry, bool, error)
	ControlPut(context.Context, SessionID, ControlNamespace, string, []byte, int64) error
	// ControlCompareAndPut writes only when the entry exists and its current
	// Value equals expected bytewise; it reports whether it wrote.
	ControlCompareAndPut(context.Context, SessionID, ControlNamespace, string, []byte, []byte, int64) (bool, error)
	ControlDelete(context.Context, SessionID, ControlNamespace, string) error
	// ControlScan enumerates across all Sessions by key prefix; fn false stops.
	ControlScan(context.Context, ControlNamespace, string, func(ControlEntry) (bool, error)) error
	// ControlExpired enumerates entries whose deadline is non-zero and before
	// beforeUnixMilli, across all Sessions; fn false stops.
	ControlExpired(context.Context, ControlNamespace, int64, func(ControlEntry) (bool, error)) error
}

// HasTypePrefix reports whether typ matches one of the prefixes (empty list
// matches everything).
func HasTypePrefix(typ EventType, prefixes []EventType) bool {
	if len(prefixes) == 0 {
		return true
	}
	for _, p := range prefixes {
		if len(typ) >= len(p) && typ[:len(p)] == p {
			return true
		}
	}
	return false
}

// CommitMatchesTypes reports whether a commit carries at least one event
// whose Type has one of the prefixes.
func CommitMatchesTypes(c *SessionCommit, prefixes []EventType) bool {
	if len(prefixes) == 0 {
		return true
	}
	for i := range c.Events {
		if HasTypePrefix(c.Events[i].Type, prefixes) {
			return true
		}
	}
	return false
}
