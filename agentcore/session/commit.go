package session

import (
	"github.com/felinics/twilight/agentcore/ledger"
)

// The commit vocabulary is the kernel's (agentcore/ledger): a Commit is the
// batches of one operation, a Proposal the same before the store assigns a
// position. session aliases them; the store-level shape checks are the
// ledger's, domain semantics are the Module Framework's upstream (EXT-STR-1).

// CommitSeq is the position of one Commit in the ledger and the canonical
// total order of the authority. Per-stream local positions are read
// optimizations derived from the ledger, never a second ordering.
type CommitSeq = ledger.CommitSeq

// Head is the ledger head after the last commit: the next CommitSeq to
// assign. A segment with no commits of its own has head header.Seed():
// 0 for a root segment, Parent.Seq+1 for a child.
type Head = ledger.Head

// Event is one committed payload. Unlike the v1 row it carries no
// transaction metadata: canonical order comes from CommitSeq plus the
// event's position inside its batch.
type Event = ledger.Event

// Commit is one atomic unit of the ledger. One Append persists exactly one
// Commit; the Commit may span several streams, and the store either lands
// every batch or none (SES-APP-1). Seq is its position, CommitID the
// identity of the operation that produced it (SES-APP-4); the store never
// rewrites or removes a commit, so the two name it for good.
type Commit = ledger.Commit

// Proposal is one atomic append: the batches of a single commit under one
// CommitID, before the store assigns Seq. The commit may span several
// streams; the store lands every batch or none (SES-APP-1).
type Proposal = ledger.Proposal

// ValidateBatches checks the shape of one proposed commit before it is
// stored: non-empty batches, unique streams within the commit, valid
// attribution and canonical payloads. Duplicate CommitIDs and epoch fencing
// are store duties.
func ValidateBatches(batches []EventBatch) error { return ledger.ValidateBatches(batches) }

// ValidateEvent checks one event before it is stored.
func ValidateEvent(e Event) error { return ledger.ValidateEvent(e) }

// CommitRef names one commit in the lineage tree: the commit at Seq of a
// segment, by its place in the stitched sequence. As SegmentHeader.Parent
// it is the edge from a child segment to the last commit it inherits: the
// child's own commits are numbered from Seq+1 and readers see the prefix
// [0, Seq] followed by them. History is append-only, so (Segment, Seq)
// names one commit for good and the edge is a stable reference (SES-FRK-1).
type CommitRef struct {
	Segment SegmentID `json:"segment"`
	Seq     CommitSeq `json:"seq"`
}

// Validate checks the shape of a parent edge. A nil edge is a root segment;
// otherwise it names a parent segment. Whether the parent still holds the
// commit is checked again inside CreateSession (SES-GC-4).
func (edge *CommitRef) Validate() error {
	if edge == nil {
		return nil
	}
	return validIdentity("Parent.Segment", string(edge.Segment))
}
