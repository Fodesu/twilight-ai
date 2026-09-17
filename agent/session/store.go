package session

import (
	"context"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/jsonstable"
)

// CreateRequest establishes a Session: a root naming a new segment. A
// repeat for an existing SessionID whose ProtocolVersion, resolved parent
// edge, CausationID, Metadata and CreatedAtUnixMilli match the existing
// Session is idempotent; any difference is a Conflict. Fork makes the new
// segment a child of another Session's history (SES-FRK-1): that Session
// must be live in the same Store, its ancestry must hold commit Seq, and it
// must share the protocol version; otherwise Create fails and writes
// nothing. The segment's nonce is always the kernel's to draw: a caller
// never names a writable node, so no two roots can be made to share one
// (SES-FRK-4).
type CreateRequest struct {
	ProtocolVersion    uint16
	SessionID          SessionID
	CreatedAtUnixMilli int64
	Fork               *ForkOrigin
	CausationID        es.CausationID
	Metadata           jsonstable.Value
}

// ForkOrigin names the point a fork inherits: a Session and a CommitSeq of
// its stitched history. The Ledger resolves it to the segment that
// contributes that commit and records the edge as SegmentHeader.Parent.
type ForkOrigin struct {
	Session SessionID
	Seq     CommitSeq
}

// OpenOptions configures writer ownership (SES-OWN-1). While a Handle is
// live, an Open without Takeover fails with ErrOwned; an Open with Takeover
// supersedes it — safety rests on Epoch fencing (SES-OWN-2), and when to take
// over is the caller's policy, above the kernel.
type OpenOptions struct {
	Takeover bool
}

// Head is the ledger head after the last commit: the next CommitSeq to
// assign and that commit's Digest. A segment with no commits of its own has
// head LedgerSeed(header): {0, HeaderDigest} for a root segment,
// {Parent.Seq+1, Parent.Digest} for a child.
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
// commits and never truncates inside one (SES-REP-1). On a fork the sequence
// read is the inherited parent prefix followed by the Session's own commits
// (SES-FRK-2).
type CommitReadRequest struct {
	SessionID SessionID
	From      CommitSeq
	Limit     uint32 // 0 = unlimited
}

// CommitPage is the result of one ReadCommits. Header is the tip segment's;
// Head is the ledger head at read time; HasMore reports whether commits
// beyond the returned ones exist.
type CommitPage struct {
	Header  SegmentHeader
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
	Header  SegmentHeader
	Stream  StreamRef
	Events  []Event
	Head    Head
	HasMore bool
}

// CollectReport is what one Collect reclaimed (SES-GC-2): the segments it
// removed entirely and, for segments some root still reaches, the new
// Head.Next after their unreachable suffix was dropped.
type CollectReport struct {
	Removed   []SegmentID
	Truncated map[SegmentID]CommitSeq
}

// Store is the kernel port (SES 4 to 6, 8, 9). A Session is a root into the
// lineage DAG: it names the segment it appends to, and reads the stitched
// history of that segment's ancestry. Delete drops the root; Collect
// reclaims the nodes no root reaches.
type Store interface {
	// Create establishes a root and its tip segment and returns the tip's
	// header.
	Create(context.Context, CreateRequest) (SegmentHeader, error)
	// Header returns the header of the Session's tip segment.
	Header(context.Context, SessionID) (SegmentHeader, error)
	// Record returns the Session's root.
	Record(context.Context, SessionID) (SessionRecord, error)
	Open(context.Context, SessionID, OpenOptions) (Handle, error)
	ReadCommits(context.Context, CommitReadRequest) (CommitPage, error)
	ReadStream(context.Context, StreamReadRequest) (StreamPage, error)
	// Delete drops the Session's root (SES-GC-1): the Session is no longer
	// found, opened, read or forked; its SessionID is free again at once.
	// The segments it reached stay nodes of the DAG for as long as another
	// root reaches them. An owned Session is ErrOwned.
	Delete(context.Context, SessionID) error
	// Collect reclaims every node and suffix no root reaches (SES-GC-2). It
	// is idempotent and safe while Sessions are open: nothing a root reaches
	// is touched.
	Collect(context.Context) (CollectReport, error)
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
