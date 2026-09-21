package session

import (
	"fmt"

	"github.com/felinics/twilight/agent/es"
)

// CommitIndex is the index of one segment's own commits by CommitID
// (SES-REP-5): a component of the segment, persisted next to its commits and
// kept current by the adapter on every Append and Truncate. Its invariant is
// that Entries are exactly the segment's own commits in Seq order from
// LedgerSeed(header).Next and Through is the head after the last of them, so
// the kernel validates it against the segment head with two O(1) checks
// (Valid) and rebuilds it from the commits only when they fail. It answers
// Committed, StreamHead and the byte-range lookup of LookupCommit without a
// read of the commits themselves.
type CommitIndex struct {
	// Through is the segment head the index covers.
	Through Head `json:"through"`
	// Entries are the indexed commits in Seq order.
	Entries []IndexEntry `json:"entries"`
}

// IndexEntry is one indexed commit: its identity, position, digest and the
// event count of each logical stream it wrote, so StreamHead is a sum over
// entries (SES-REP-3).
type IndexEntry struct {
	CommitID CommitID      `json:"commitId"`
	Seq      CommitSeq     `json:"seq"`
	Digest   es.Digest     `json:"digest"`
	Streams  []StreamCount `json:"streams,omitempty"`
}

// StreamCount is the number of events one commit wrote to one stream.
type StreamCount struct {
	Stream StreamRef `json:"stream"`
	Events uint32    `json:"events"`
}

// IndexEntryOf derives the entry of a sealed commit.
func IndexEntryOf(c *Commit) IndexEntry {
	e := IndexEntry{CommitID: c.CommitID, Seq: c.Seq, Digest: c.Digest}
	if len(c.Batches) > 0 {
		e.Streams = make([]StreamCount, len(c.Batches))
		for i := range c.Batches {
			e.Streams[i] = StreamCount{Stream: c.Batches[i].Stream, Events: uint32(len(c.Batches[i].Events))} //nolint:gosec // a batch holds far fewer than MaxUint32 events
		}
	}
	return e
}

// BuildCommitIndex derives the index of a segment from its own commits, which
// are contiguous from LedgerSeed(header).Next.
func BuildCommitIndex(header SegmentHeader, commits []Commit) CommitIndex {
	idx := CommitIndex{Through: LedgerSeed(header)}
	if len(commits) > 0 {
		idx.Entries = make([]IndexEntry, 0, len(commits))
	}
	for i := range commits {
		idx.Extend(&commits[i])
	}
	return idx
}

// Extend appends one sealed commit that continues the index at Through.
func (x *CommitIndex) Extend(c *Commit) {
	x.Entries = append(x.Entries, IndexEntryOf(c))
	x.Through = Head{Next: c.Seq + 1, Digest: c.Digest}
}

// Truncate drops the entries after through and moves Through back to the
// last kept entry, or to seed when none is kept.
func (x *CommitIndex) Truncate(seed Head, through CommitSeq) {
	keep := 0
	for keep < len(x.Entries) && x.Entries[keep].Seq <= through {
		keep++
	}
	x.Entries = x.Entries[:keep]
	if keep == 0 {
		x.Through = seed
		return
	}
	last := &x.Entries[keep-1]
	x.Through = Head{Next: last.Seq + 1, Digest: last.Digest}
}

// Valid reports whether the index covers exactly the segment's commits from
// seed to head: Through equals head and the entry count equals the distance
// from seed. These are the only checks Open makes; an index that passes them
// is used, one that fails is rebuilt from the commits (SES-REP-5).
func (x *CommitIndex) Valid(seed, head Head) bool {
	if x.Through != head || head.Next < seed.Next {
		return false
	}
	if uint64(len(x.Entries)) != uint64(head.Next-seed.Next) {
		return false
	}
	if n := len(x.Entries); n > 0 {
		last := &x.Entries[n-1]
		if last.Seq+1 != head.Next || last.Digest != head.Digest || x.Entries[0].Seq != seed.Next {
			return false
		}
	}
	return x.checkContiguous(seed) == nil
}

// checkContiguous verifies entry Seqs run from seed without gaps or repeats.
func (x *CommitIndex) checkContiguous(seed Head) error {
	for i := range x.Entries {
		if want := seed.Next + CommitSeq(i); x.Entries[i].Seq != want { //nolint:gosec // i < len(Entries)
			return fmt.Errorf("index entry %d has seq %d, want %d", i, x.Entries[i].Seq, want)
		}
	}
	return nil
}

// Locate returns the entry of id, if indexed. It is a linear scan; a Handle
// builds a map from the entries it holds.
func (x *CommitIndex) Locate(id CommitID) (IndexEntry, bool) {
	for i := range x.Entries {
		if x.Entries[i].CommitID == id {
			return x.Entries[i], true
		}
	}
	return IndexEntry{}, false
}

// Clone returns an independent copy.
func (x CommitIndex) Clone() CommitIndex {
	out := x
	out.Entries = make([]IndexEntry, len(x.Entries))
	for i := range x.Entries {
		out.Entries[i] = x.Entries[i]
		out.Entries[i].Streams = append([]StreamCount(nil), x.Entries[i].Streams...)
	}
	return out
}
