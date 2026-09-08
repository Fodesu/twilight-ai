package session

import (
	"context"
	"time"

	"github.com/memohai/twilight/agent/es"
	"github.com/memohai/twilight/agent/jsonstable"
)

// CreateRequest establishes a stream. Field-identical repeats are idempotent;
// a different request for the same SessionID is a Conflict.
type CreateRequest struct {
	ProtocolVersion    uint16
	SessionID          SessionID
	CreatedAtUnixMilli int64
	CausationID        es.CausationID
	Metadata           jsonstable.Value
}

// OpenOptions configures writer ownership (SES-OWN-1). TTL zero means the
// ownership lives only as long as the process or connection (file-lock
// semantics); non-zero means the writer must Heartbeat within TTL or another
// Open may take over.
type OpenOptions struct {
	TTL time.Duration
}

// Writer is the kernel's ownership handle returned by Store.Open. Append and
// Heartbeat carry its Epoch; a Writer whose Epoch has been superseded gets
// ErrOwnershipLost and writes nothing (SES-OWN-2).
type Writer interface {
	SessionID() SessionID
	Epoch() Epoch
	Head() Head
	// Append persists one group atomically and returns the sealed rows
	// (SES-APP-1). It rejects empty groups, duplicate CommitIDs, non-canonical
	// or non-object payloads, invalid identities and a stale Epoch (SES-APP-3).
	Append(context.Context, Group) ([]SessionEvent, error)
	Heartbeat(context.Context) error
	Close(context.Context) error
}

// ReadRequest reads rows from From (inclusive), optionally filtered by
// EventType prefix and limited to whole groups (SES-REP-1).
type ReadRequest struct {
	SessionID SessionID
	From      Seq
	Types     []EventType // empty = all; otherwise EventType prefix filter
	Limit     uint32      // 0 = unlimited; truncation only at a group boundary
}

// ReadPage is the result of one Read. Head is the stream head at read time;
// HasMore reports whether rows beyond the returned ones matched.
type ReadPage struct {
	Header  SessionHeader
	Events  []SessionEvent
	Head    Head
	HasMore bool
}

// Store is the kernel port (SES 4 to 6).
type Store interface {
	Create(context.Context, CreateRequest) (SessionHeader, error)
	Header(context.Context, SessionID) (SessionHeader, error)
	Open(context.Context, SessionID, OpenOptions) (Writer, error)
	Read(context.Context, ReadRequest) (ReadPage, error)
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
