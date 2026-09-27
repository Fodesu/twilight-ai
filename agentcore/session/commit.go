package session

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/jsonstable"
)

// CommitSeq is the position of one Commit in the ledger and the canonical
// total order of the authority. Per-stream local positions are read
// optimizations derived from the ledger, never a second ordering.
type CommitSeq uint64

// Head is the ledger head after the last commit: the next CommitSeq to
// assign. A segment with no commits of its own has head header.Seed():
// 0 for a root segment, Parent.Seq+1 for a child.
type Head struct {
	Next CommitSeq
}

// Event is one committed payload. Unlike the v1 row it carries no transaction
// metadata: canonical order comes from CommitSeq plus the event's position
// inside its batch.
type Event struct {
	Type                EventType        `json:"type"`
	RecordedAtUnixMilli int64            `json:"recordedAtUnixMilli"`
	Payload             jsonstable.Value `json:"payload"`
}

// Position is the ledger position of one event: the commit it landed in and
// its index among that commit's events in batch order. Positions order every
// event of a Session totally, so a projection that needs to order what it
// derives records the position of the event that produced it instead of
// keeping a counter of its own.
type Position struct {
	Commit CommitSeq `json:"commit"`
	Index  uint32    `json:"index"`
}

// Less reports whether p precedes q in the ledger.
func (p Position) Less(q Position) bool {
	if p.Commit != q.Commit {
		return p.Commit < q.Commit
	}
	return p.Index < q.Index
}

// StreamBatch is the ordered slice of one commit that belongs to one stream.
type StreamBatch struct {
	Stream StreamRef `json:"stream"`
	Events []Event   `json:"events"`
}

// Commit is one atomic unit of the ledger. One Append persists exactly one
// Commit; the Commit may span several streams, and the store either lands
// every batch or none (SES-APP-1). Seq is its position, CommitID the
// identity of the operation that produced it (SES-APP-4); the store never
// rewrites or removes a commit, so the two name it for good.
type Commit struct {
	Seq      CommitSeq     `json:"seq"`
	CommitID CommitID      `json:"commitId"`
	Batches  []StreamBatch `json:"batches"`
}

// Proposal is one atomic append: the batches of a single commit under one
// CommitID, before the store assigns Seq. The commit may span several
// streams; the store lands every batch or none (SES-APP-1).
type Proposal struct {
	CommitID CommitID
	Batches  []StreamBatch
}

// At returns the proposal as a commit positioned at seq. It does not
// validate; callers validate the proposal before assigning a position.
func (p Proposal) At(seq CommitSeq) Commit {
	return Commit{Seq: seq, CommitID: p.CommitID, Batches: p.Batches}
}

// Validate checks the shape of a proposal: a valid CommitID and well-formed
// batches. Seq is not part of a proposal.
func (p Proposal) Validate() error {
	c := p.At(0)
	return c.Validate()
}

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

// Validate checks one event before it is stored.
func (e *Event) Validate() error {
	return validateEventShape(e.Type, e.Payload)
}

// ValidateBatches checks the shape of one proposed commit before it is stored:
// non-empty batches, unique streams within the commit, valid attribution and
// canonical payloads. Duplicate CommitIDs and epoch fencing are store duties.
func ValidateBatches(batches []StreamBatch) error {
	if len(batches) == 0 {
		return errors.New("commit without batches")
	}
	seen := make(map[StreamRef]struct{}, len(batches))
	for i := range batches {
		b := &batches[i]
		if err := ValidateStreamRef(b.Stream); err != nil {
			return fmt.Errorf("batch %d: %w", i, err)
		}
		if _, dup := seen[b.Stream]; dup {
			return fmt.Errorf("batch %d: stream %s appears twice in one commit", i, b.Stream)
		}
		seen[b.Stream] = struct{}{}
		if len(b.Events) == 0 {
			return fmt.Errorf("batch %d: no events", i)
		}
		for j := range b.Events {
			if err := b.Events[j].Validate(); err != nil {
				return fmt.Errorf("batch %d event %d: %w", i, j, err)
			}
		}
	}
	return nil
}

// Validate checks the shape of a commit before it is stored: a valid
// CommitID and well-formed batches. Seq and duplicate CommitIDs are the
// store's checks (SES-APP-3).
func (c *Commit) Validate() error {
	if err := validIdentity("CommitID", string(c.CommitID)); err != nil {
		return err
	}
	return ValidateBatches(c.Batches)
}

// Validate checks the shape of a parent edge. A nil edge is a root segment;
// otherwise it names a parent segment. Whether the parent holds the commit
// is the loaded Session's check at Create (SES-FRK-1).
func (edge *CommitRef) Validate() error {
	if edge == nil {
		return nil
	}
	return validIdentity("Parent.Segment", string(edge.Segment))
}
