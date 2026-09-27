package session

import (
	"context"
	"time"
)

// SegmentStore is the adapter port for nodes: independent, append-only
// segments and their indexes. It knows nothing of Sessions, forks or
// reachability.
type SegmentStore interface {
	// Segment returns a node; ErrNotFound when absent.
	Segment(context.Context, SegmentID) (Segment, error)
	// ReadSegment returns the segment's own commits from CommitSeq from
	// (absolute), at most limit (0 = unlimited), its head, and whether more
	// own commits follow. A torn tail is never returned.
	ReadSegment(ctx context.Context, id SegmentID, from CommitSeq, limit uint32) ([]Commit, Head, bool, error)
	// ReadSegmentStream returns, from the segment's own commits with Seq at
	// or past from, those that carry a batch of stream, in Seq order, at
	// most limit of them (0 = unlimited), and whether more follow
	// (SES-REP-2). It is ReadSegment narrowed to one stream, so an adapter
	// that indexes streams per commit (the CommitIndex's stream counts,
	// SES-REP-5) reads only the commits that matter. Commits are returned
	// whole; the caller extracts the stream's batch.
	ReadSegmentStream(ctx context.Context, id SegmentID, stream StreamRef, from CommitSeq, limit uint32) ([]Commit, bool, error)
	// Locate reports whether the segment holds CommitID as its own commit,
	// and at which Seq, from the segment's CommitIndex alone (SES-REP-3/5);
	// LookupCommit reads the commit (SES-REP-4).
	Locate(context.Context, SegmentID, CommitID) (CommitSeq, bool, error)
	LookupCommit(context.Context, SegmentID, CommitID) (Commit, bool, error)
	// StreamHead returns the number of events the segment's own commits
	// with Seq below before wrote to one logical stream, from the segment's
	// CommitIndex alone (SES-REP-3): the StreamSeq the stream's next event
	// takes as of that head, 0 when none of those commits wrote to it.
	StreamHead(ctx context.Context, id SegmentID, stream StreamRef, before CommitSeq) (StreamSeq, error)
	// Summarize returns the summary of the segment's CommitIndex as the
	// adapter keeps it, and the segment's current head (SES-REP-5): what
	// Open checks the index by, without the entries. After a crash the
	// index may lag the commits, which the kernel detects with
	// IndexSummary.Valid and repairs through PutIndex.
	Summarize(context.Context, SegmentID) (IndexSummary, Head, error)
	// Index returns the segment's whole CommitIndex and its head
	// (SES-REP-5); Collect reads it to name the commits a truncation drops.
	Index(context.Context, SegmentID) (CommitIndex, Head, error)
	// PutIndex replaces the segment's CommitIndex with one the kernel rebuilt
	// from the commits.
	PutIndex(context.Context, SegmentID, CommitIndex) error
	// Append persists a commit whose Seq is the segment head, under a Lease
	// the adapter checks atomically with the write: the Lease must be
	// current for its Session and that Session's Tip must be the segment
	// (SES-OWN-2). A superseded Lease gets ErrOwnershipLost and writes
	// nothing. The adapter only ever inserts: no operation of this port
	// rewrites or removes a commit a root still reaches (SES-APP-5).
	Append(context.Context, Lease, SegmentID, Commit) error
}

// RootStore is the adapter port for roots: a Session's record and its
// writer ownership (SES-OWN).
type RootStore interface {
	// Record returns a root; ErrNotFound when absent.
	Record(context.Context, SessionID) (SessionRecord, error)
	// Acquire takes writer ownership of a root (SES-OWN-1): ErrOwned while a
	// Lease is live unless Takeover, which supersedes it with the next Epoch.
	// Whether the current Lease has expired, and the expiry of the new one
	// (now plus OpenOptions.LeaseDuration), are judged by the adapter's
	// clock (SES-OWN-6): a shared database is its own clock, so every
	// replica agrees. Repair of a torn tail in the root's segment happens
	// here.
	Acquire(context.Context, SessionID, OpenOptions) (Lease, error)
	// Renew moves the Lease's expiry to the adapter's now plus duration when
	// the Lease is still the root's current one (SES-OWN-1) and returns the
	// new expiry; a superseded Lease is ErrOwnershipLost and nothing
	// changes. A non-positive duration is a lease that never expires.
	Renew(context.Context, Lease, time.Duration) (int64, error)
	// Release ends a Lease; a superseded Lease is a no-op.
	Release(context.Context, Lease) error
	// LeaseOf returns the root's current Lease (SES-OWN-5): ok is false
	// when the root has no holder (never opened, or released); a root that
	// does not exist is ErrNotFound. A read: nothing changes, and an expired
	// Lease is returned as it is for the caller to judge against its clock.
	LeaseOf(context.Context, SessionID) (Lease, bool, error)
	// ListLeases returns the Lease of every root that has a holder
	// (SES-OWN-5), expired ones included.
	ListLeases(context.Context) ([]Lease, error)
	// ExpiredLeases returns held Leases expired by the adapter's clock
	// (SES-OWN-6), soonest expired first, at most limit of them (0 for
	// all); never-expiring Leases are never returned (SES-OWN-5). It is the
	// read a replica pool recovers dead owners' Sessions by (APP-ACT-3),
	// and an adapter indexes it.
	ExpiredLeases(ctx context.Context, limit int) ([]Lease, error)
}

// MaintenanceStore is the adapter port for the operations that change the
// set of nodes and roots (SES-GC-4): enumeration for reachability, node
// creation with its root, truncation and removal, root deletion. They share
// one consistency domain with the other two ports.
type MaintenanceStore interface {
	// ListSegments returns every node.
	ListSegments(context.Context) ([]SegmentID, error)
	// ListRecords returns every root.
	ListRecords(context.Context) ([]SessionRecord, error)
	// CreateSession persists a new node, the endpoints its path records on
	// each span's segment, and the root that names the node (SES-FRK-1).
	// The root is the last write: a crash before it leaves a segment and
	// endpoints no live root names, which Collect repairs. Never a root
	// without its segment. Both the SessionID and the SegmentID must be
	// new: ErrConflict when either exists, ErrDeleted when the SessionID
	// was deleted, so no two roots ever name one writable tip (SES-FRK-4).
	// When the segment's header names a Parent, the parent segment must
	// exist at the moment of the write and the adapter must keep it from
	// being removed while the edge stands (SES-GC-4): a vanished parent is
	// ErrNotFound.
	CreateSession(context.Context, Segment, SessionRecord) error
	// RemoveEndpoint drops sid's endpoint on the segment and returns the
	// endpoints that remain. A missing cover record returns an empty slice
	// and a nil error. ReplaceEndpoints sets the segment's endpoints to covers;
	// Collect uses it to make stored endpoints match live paths. A missing
	// segment is ErrNotFound and nothing is written.
	RemoveEndpoint(ctx context.Context, id SegmentID, sid SessionID) ([]Endpoint, error)
	ReplaceEndpoints(ctx context.Context, id SegmentID, covers []Endpoint) error
	// TruncateSegment drops the segment's own commits after through and
	// returns the new head. RemoveSegment deletes the node when nothing
	// references it, atomically with that check (SES-GC-4): a root whose Tip is
	// the segment or a segment whose Parent edge names it makes the removal
	// ErrReferenced and nothing is removed. The adapter enforces this with
	// its own consistency (a foreign key, a check under the store lock), so
	// a Create racing a Collect on another replica cannot leave a child on
	// a removed parent.
	TruncateSegment(ctx context.Context, id SegmentID, through CommitSeq) (Head, error)
	RemoveSegment(context.Context, SegmentID) error
	// DeleteRecord marks a root deleted (SES-GC-1): the Session is no longer
	// found, and its SessionID stays taken (CreateSession on it is
	// ErrDeleted), so a command in flight for the old Session can never
	// reach a new one under the same name. ErrOwned while a Lease is live;
	// a deleted root is ErrNotFound. Record and ListRecords do not return
	// deleted roots.
	DeleteRecord(context.Context, SessionID) error
}

// Backend is what an adapter implements: the three ports over one
// consistency domain, so Append can check a Lease atomically and
// CreateSession can check its parent atomically (SES-GC-4).
type Backend interface {
	SegmentStore
	RootStore
	MaintenanceStore
}
