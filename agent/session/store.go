package session

import (
	"context"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/jsonstable"
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

// OpenOptions configures writer ownership (SES-OWN-1). While a Handle is
// live, an Open without Takeover fails with ErrOwned; an Open with Takeover
// supersedes it — safety rests on Epoch fencing (SES-OWN-2), and when to take
// over is the caller's policy, above the kernel.
type OpenOptions struct {
	Takeover bool
}

// Head is the ledger head after the last commit: the next CommitSeq to
// assign and that commit's Digest. The empty ledger head is {0, HeaderDigest}.
type Head struct {
	Next   CommitSeq
	Digest es.Digest
}

// Proposal is one atomic append: the batches of a single commit under one
// CommitID. The commit may span several streams; the store lands every batch
// or none (SES-APP-1).
type Proposal struct {
	CommitID CommitID
	Batches  []StreamBatch
}

// Handle is the kernel's ownership handle returned by Store.Open. Append
// carries its Epoch; a Handle whose Epoch has been superseded gets
// ErrOwnershipLost and writes nothing (SES-OWN-2).
type Handle interface {
	SessionID() SessionID
	Epoch() Epoch
	Head() Head
	// Append persists one commit atomically and returns it sealed (SES-APP-1).
	// It rejects malformed CommitIDs, duplicate CommitIDs, malformed stream
	// refs and events, and a stale Epoch (SES-APP-3).
	Append(context.Context, Proposal) (Commit, error)
	// Committed reports whether CommitID is already in the ledger. Append must
	// reject a duplicate CommitID (SES-APP-3), so the kernel answers this from
	// the index it already keeps, without touching storage (SES-REP-3).
	Committed(CommitID) bool
	// LookupCommit returns a committed group. It reads it from storage when
	// the handle does not already hold it, so a caller that needs the commit
	// pays for it only on a hit (SES-REP-4).
	LookupCommit(CommitID) (Commit, bool, error)
	Close(context.Context) error
}

// StreamSeq is the position of an event inside its logical stream: the first
// event a stream ever receives is 0. It is derived from the ledger by
// counting a stream's events in CommitSeq order and is a read optimization
// only; CommitSeq is the canonical order.
type StreamSeq uint64

// CommitReadRequest reads whole commits from From (inclusive). Limit counts
// commits and never truncates inside one (SES-REP-1).
type CommitReadRequest struct {
	SessionID SessionID
	From      CommitSeq
	Limit     uint32 // 0 = unlimited
}

// CommitPage is the result of one ReadCommits. Head is the ledger head at
// read time; HasMore reports whether commits beyond the returned ones exist.
type CommitPage struct {
	Header  SessionHeader
	Commits []Commit
	Head    Head
	HasMore bool
}

// StreamReadRequest reads the events of one logical stream in CommitSeq
// order. From counts events within the stream, starting at 0 for the first
// event the stream ever received.
type StreamReadRequest struct {
	SessionID SessionID
	Stream    StreamRef
	From      StreamSeq
	Limit     uint32 // 0 = unlimited
}

// StreamPage is the result of one ReadStream. Head is the ledger head at
// read time; HasMore reports whether events beyond the returned ones exist.
type StreamPage struct {
	Header  SessionHeader
	Stream  StreamRef
	Events  []Event
	Head    Head
	HasMore bool
}

// Store is the kernel port (SES 4 to 6).
type Store interface {
	Create(context.Context, CreateRequest) (SessionHeader, error)
	Header(context.Context, SessionID) (SessionHeader, error)
	Open(context.Context, SessionID, OpenOptions) (Handle, error)
	ReadCommits(context.Context, CommitReadRequest) (CommitPage, error)
	ReadStream(context.Context, StreamReadRequest) (StreamPage, error)
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
