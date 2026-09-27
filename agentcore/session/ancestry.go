package session

import (
	"context"
	"fmt"
)

// Ancestry is the unique path of a Session through the lineage tree: its
// segments from the root down to the tip, each with the range of the
// stitched sequence it contributes. Every read, lookup and reachability
// question is answered on this value; nothing walks the store recursively.
type Ancestry struct {
	// Segments are ordered root first; the last is the tip a Session
	// appends to.
	Segments []AncestrySegment
}

// AncestrySegment is one node on the path with the stitched positions it
// contributes: its own commits from Seed up to and including Through (the
// next segment's anchor), or up to its head for the tip.
type AncestrySegment struct {
	Segment Segment
	// From is the first stitched CommitSeq the segment contributes.
	From CommitSeq
	// Through is the last stitched CommitSeq it contributes; the tip
	// contributes through its head, reported as ^CommitSeq(0).
	Through CommitSeq
}

// Tip is the segment the Session appends to.
func (a *Ancestry) Tip() Segment { return a.Segments[len(a.Segments)-1].Segment }

// Header is the tip's creation record: the Session's public header.
func (a *Ancestry) Header() SegmentHeader { return a.Tip().Header }

// Owner returns the segment that contributes the commit at seq.
func (a *Ancestry) Owner(seq CommitSeq) (AncestrySegment, bool) {
	for i := range a.Segments {
		if s := &a.Segments[i]; seq >= s.From && seq <= s.Through {
			return *s, true
		}
	}
	return AncestrySegment{}, false
}

// Load resolves the Ancestry of the segment tip: it follows parent edges to
// the root through the SegmentStore, loading each node once.
func LoadAncestry(ctx context.Context, store SegmentStore, tip SegmentID) (*Ancestry, error) {
	var chain []Segment
	seen := map[SegmentID]bool{}
	for id := tip; ; {
		if seen[id] {
			return nil, &Error{Code: ErrCorrupt, Operation: "ancestry", Detail: fmt.Sprintf("segment %s is its own ancestor", id)}
		}
		seen[id] = true
		seg, err := store.Segment(ctx, id)
		if err != nil {
			return nil, err
		}
		chain = append(chain, seg)
		parent := seg.Parent()
		if parent == nil {
			break
		}
		id = parent.Segment
	}
	a := &Ancestry{Segments: make([]AncestrySegment, len(chain))}
	for i := range chain {
		seg := chain[len(chain)-1-i] // root first
		s := AncestrySegment{Segment: seg, From: seg.Seed().Next, Through: ^CommitSeq(0)}
		if i+1 < len(chain) {
			s.Through = chain[len(chain)-2-i].Parent().Seq
		}
		a.Segments[i] = s
	}
	return a, nil
}

// Read returns the stitched commits of the Ancestry from from (inclusive),
// at most limit (0 = unlimited), the tip's head and whether more follow. It
// iterates the explicit path; each segment is read once for its range.
func (a *Ancestry) Read(ctx context.Context, store SegmentStore, from CommitSeq, limit uint32) ([]Commit, Head, bool, error) {
	var out []Commit
	var head Head
	tip := len(a.Segments) - 1
	for i := range a.Segments {
		s := &a.Segments[i]
		start := from
		if start < s.From {
			start = s.From
		}
		if i < tip && start > s.Through {
			continue
		}
		var want uint32
		if limit > 0 {
			remaining := int(limit) - len(out)
			if remaining <= 0 {
				head, err := a.tipHead(ctx, store)
				return out, head, true, err
			}
			want = Limit32(uint64(remaining))
			if i < tip && CommitSeq(want) > s.Through-start+1 {
				want = Limit32(uint64(s.Through - start + 1))
			}
		} else if i < tip {
			want = Limit32(uint64(s.Through - start + 1))
		}
		commits, segHead, more, err := store.ReadSegment(ctx, s.Segment.ID, start, want)
		if err != nil {
			return nil, Head{}, false, err
		}
		if i == tip {
			head = segHead
			out = append(out, commits...)
			return out, head, more, nil
		}
		for _, c := range commits {
			if c.Seq > s.Through {
				break
			}
			out = append(out, c)
		}
		if AtLimit(len(out), limit) {
			// Anything after the last returned commit exists by construction:
			// either the rest of this segment's range or the segments below.
			last := out[len(out)-1].Seq
			head, err := a.tipHead(ctx, store)
			return out, head, last < s.Through || i < tip, err
		}
	}
	return out, head, false, nil
}

// ReadStream returns, in stitched order, the commits of the Ancestry that
// carry a batch of stream: every segment's under LineageSession, the tip's
// own under LineageSegment (SES-FRK-5). Each segment is read through
// SegmentStore.ReadSegmentStream within the range it contributes.
func (a *Ancestry) ReadStream(ctx context.Context, store SegmentStore, stream StreamRef, lineage StreamLineage) ([]Commit, error) {
	var out []Commit
	tip := len(a.Segments) - 1
	first := 0
	if lineage == LineageSegment {
		first = tip
	}
	for i := first; i <= tip; i++ {
		s := &a.Segments[i]
		commits, _, err := store.ReadSegmentStream(ctx, s.Segment.ID, stream, s.From, 0)
		if err != nil {
			return nil, err
		}
		for _, c := range commits {
			if i < tip && c.Seq > s.Through {
				break
			}
			out = append(out, c)
		}
	}
	return out, nil
}

// tipHead reads the tip's head without reading commits.
func (a *Ancestry) tipHead(ctx context.Context, store SegmentStore) (Head, error) {
	_, head, _, err := store.ReadSegment(ctx, a.Tip().ID, ^CommitSeq(0), 1)
	return head, err
}

// Contains reports whether id is a commit of the Ancestry: one of the tip's
// own commits or an inherited one within its anchor range (SES-FRK-3). It
// consults the segments' indexes only (SES-REP-5).
func (a *Ancestry) Contains(ctx context.Context, store SegmentStore, id CommitID) (bool, error) {
	return locateIn(ctx, store, a.Segments, id)
}

// ContainsInherited is Contains over the Ancestry without its tip.
func (a *Ancestry) ContainsInherited(ctx context.Context, store SegmentStore, id CommitID) (bool, error) {
	return locateIn(ctx, store, a.Segments[:len(a.Segments)-1], id)
}

// Lookup reads a commit of the Ancestry by CommitID.
func (a *Ancestry) Lookup(ctx context.Context, store SegmentStore, id CommitID) (Commit, bool, error) {
	return lookupIn(ctx, store, a.Segments, id)
}

// LookupInherited reads an inherited commit by CommitID: the Ancestry without
// its tip. The inherited prefix is immutable, so reading it from storage at
// any time gives the same answer.
func (a *Ancestry) LookupInherited(ctx context.Context, store SegmentStore, id CommitID) (Commit, bool, error) {
	return lookupIn(ctx, store, a.Segments[:len(a.Segments)-1], id)
}

func lookupIn(ctx context.Context, store SegmentStore, segments []AncestrySegment, id CommitID) (Commit, bool, error) {
	for i := len(segments) - 1; i >= 0; i-- {
		s := segments[i]
		c, ok, err := store.LookupCommit(ctx, s.Segment.ID, id)
		if err != nil {
			return Commit{}, false, err
		}
		if ok && c.Seq >= s.From && c.Seq <= s.Through {
			return c, true, nil
		}
	}
	return Commit{}, false, nil
}

// locateIn reports whether id is a commit of segments within their anchor
// ranges, from the segments' indexes alone.
func locateIn(ctx context.Context, store SegmentStore, segments []AncestrySegment, id CommitID) (bool, error) {
	for i := len(segments) - 1; i >= 0; i-- {
		s := &segments[i]
		seq, ok, err := store.Locate(ctx, s.Segment.ID, id)
		if err != nil {
			return false, err
		}
		if ok && seq >= s.From && seq <= s.Through {
			return true, nil
		}
	}
	return false, nil
}

// Reachable computes, from every node and the roots that are live, the last
// stitched CommitSeq each node must keep (SES-GC-2): a node that is some
// root's tip keeps everything; a node reached only through edges keeps up to
// the largest anchor Seq any reaching edge carries. Nodes absent from the
// result are unreachable. Edges are followed transitively, so a node
// referenced only by unreachable nodes is unreachable.
func Reachable(nodes map[SegmentID]Segment, roots []SessionRecord) map[SegmentID]CommitSeq {
	const all = ^CommitSeq(0)
	need := make(map[SegmentID]CommitSeq, len(nodes))
	for _, r := range roots {
		id := r.Tip
		seg, ok := nodes[id]
		if !ok {
			continue
		}
		need[id] = all
		for edge := seg.Parent(); edge != nil; {
			if cur, ok := need[edge.Segment]; !ok || edge.Seq > cur {
				need[edge.Segment] = edge.Seq
			}
			parent, ok := nodes[edge.Segment]
			if !ok {
				break
			}
			edge = parent.Parent()
		}
	}
	return need
}
