package session

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/felinics/twilight/agent/es"
)

// The Session lineage is a DAG (agent-session.md section 8): immutable
// commit segments are its nodes, a segment's parent anchor is an edge, and a
// Session is a root that names the segment it appends to. These are the
// domain types; the Ledger implements every operation over them and the
// adapters store them.

// SegmentID identifies one commit segment independently of any Session. It
// is the digest of the segment's creation record, so a segment recreated
// with a different record is a different node.
type SegmentID string

// LedgerRef names one position in the DAG: a commit of a segment, by its
// place in the stitched sequence and by its digest. As SessionHeader.Parent
// it is the edge from a child segment to the last commit it inherits: the
// child's own commits are numbered from Seq+1 and chained from Digest, and
// readers see the prefix [0, Seq] followed by them. The prefix is immutable,
// so the edge is a stable reference, and it is covered by the child's header
// digest.
type LedgerRef struct {
	Segment SegmentID `json:"segment"`
	Seq     CommitSeq `json:"seq"`
	Digest  es.Digest `json:"digest"`
}

// Segment is a node: an immutable creation record whose commits chain from
// LedgerSeed(Header). Header.Parent is the edge to the parent segment; a
// root segment has none.
type Segment struct {
	ID     SegmentID
	Header SegmentHeader
}

// SegmentIDOf derives a segment's identity from its sealed creation record.
func SegmentIDOf(h SegmentHeader) SegmentID { return SegmentID(h.HeaderDigest) }

// NewNonce returns a fresh segment nonce: 128 random bits, hex encoded.
func NewNonce() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("session: nonce: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// Parent returns the edge to the parent segment, or nil for a root.
func (s Segment) Parent() *LedgerRef {
	if s.Header.Parent == nil {
		return nil
	}
	edge := *s.Header.Parent
	return &edge
}

// Seed is the head of the segment while it holds no commits of its own.
func (s Segment) Seed() Head { return LedgerSeed(s.Header) }

// SessionRecord is a root: a Session's identity, the segment it appends to
// (its tip) and the Session's own metadata. Dropping the record is deleting
// the Session; the tip stays a node of the DAG for as long as any root
// reaches it. Two roots never share a tip (SES-FRK-4): a fork gets a new
// child segment, so writers of different Sessions never append to one node.
type SessionRecord struct {
	ID                 SessionID `json:"sessionId"`
	Tip                SegmentID `json:"tip"`
	CreatedAtUnixMilli int64     `json:"createdAtUnixMilli"`
}

// Lease is writer ownership of one Session root (SES-OWN-1/2): the adapter
// fences every append with it.
type Lease struct {
	Session SessionID
	Epoch   Epoch
}

// LedgerStore is the adapter port for nodes: independent, append-only
// segments. It knows nothing of Sessions, forks or reachability.
type LedgerStore interface {
	// Segment returns a node; ErrNotFound when absent.
	Segment(context.Context, SegmentID) (Segment, error)
	// ListSegments returns every node.
	ListSegments(context.Context) ([]SegmentID, error)
	// ReadSegment returns the segment's own commits from CommitSeq from
	// (absolute), at most limit (0 = unlimited), its head, and whether more
	// own commits follow. A torn tail is never returned.
	ReadSegment(ctx context.Context, id SegmentID, from CommitSeq, limit uint32) ([]Commit, Head, bool, error)
	// Contains reports whether the segment holds CommitID as its own commit
	// (SES-REP-3); LookupCommit reads it (SES-REP-4).
	Contains(context.Context, SegmentID, CommitID) (bool, error)
	LookupCommit(context.Context, SegmentID, CommitID) (Commit, bool, error)
	// Append persists a commit the Ledger sealed against the segment head,
	// under a Lease the adapter checks atomically with the write: the Lease
	// must be current for its Session and that Session's Tip must be the
	// segment (SES-OWN-2). A superseded Lease gets ErrOwnershipLost and
	// writes nothing.
	Append(context.Context, Lease, SegmentID, Commit) error
	// TruncateSegment drops the segment's own commits after through and
	// returns the new head; RemoveSegment deletes the node.
	TruncateSegment(ctx context.Context, id SegmentID, through CommitSeq) (Head, error)
	RemoveSegment(context.Context, SegmentID) error
}

// SessionStore is the adapter port for roots: Session records and their
// writer ownership.
type SessionStore interface {
	// Record returns a root; ErrNotFound when absent.
	Record(context.Context, SessionID) (SessionRecord, error)
	// ListRecords returns every root.
	ListRecords(context.Context) ([]SessionRecord, error)
	// Acquire takes writer ownership of a root (SES-OWN-1): ErrOwned while a
	// Lease is live unless Takeover, which supersedes it with the next Epoch.
	// Repair of a torn tail in the root's segment happens here.
	Acquire(context.Context, SessionID, OpenOptions) (Lease, error)
	// Release ends a Lease; a superseded Lease is a no-op.
	Release(context.Context, Lease) error
	// DeleteRecord drops a root (SES-GC-1); ErrOwned while a Lease is live.
	DeleteRecord(context.Context, SessionID) error
}

// Backend is what an adapter implements: both ports, sharing one
// consistency domain so Append can check a Lease atomically, plus the one
// write that spans them.
type Backend interface {
	LedgerStore
	SessionStore
	// CreateSession persists a new node and the root that names it as one
	// durable step (SES-FRK-1): never a root without its segment, never a
	// segment a Collect could see without its root. Both must be new:
	// ErrConflict when the SessionID or the SegmentID exists, so no two
	// roots ever name one writable tip (SES-FRK-4).
	CreateSession(context.Context, Segment, SessionRecord) error
	// AdvanceTip persists a new node with its own first commits (sealed by
	// the Ledger from the node's seed) and moves the root of the Lease's
	// Session from tip from to the node, as one durable step under the
	// Lease (SES-ADV-2): the Lease must be current, the root's Tip must
	// still be from (ErrConflict otherwise) and the node must be new. A
	// crash before the publication point leaves at most a node no root
	// names, which Collect reclaims; the root is never seen pointing at a
	// node that lacks its bootstrap commits.
	AdvanceTip(ctx context.Context, lease Lease, seg Segment, bootstrap []Commit, from SegmentID) error
}

// Ancestry is the explicit path of a Session through the DAG: its segments
// from the root down to the tip, each with the range of the stitched
// sequence it contributes. Every read, lookup and reachability question is
// answered on this value; nothing walks the store recursively.
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
	for _, s := range a.Segments {
		if seq >= s.From && seq <= s.Through {
			return s, true
		}
	}
	return AncestrySegment{}, false
}

// Load resolves the Ancestry of the segment tip: it follows parent edges to
// the root through the LedgerStore, loading each node once.
func LoadAncestry(ctx context.Context, store LedgerStore, tip SegmentID) (*Ancestry, error) {
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
func (a *Ancestry) Read(ctx context.Context, store LedgerStore, from CommitSeq, limit uint32) ([]Commit, Head, bool, error) {
	var out []Commit
	var head Head
	tip := len(a.Segments) - 1
	for i, s := range a.Segments {
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

// tipHead reads the tip's head without reading commits.
func (a *Ancestry) tipHead(ctx context.Context, store LedgerStore) (Head, error) {
	_, head, _, err := store.ReadSegment(ctx, a.Tip().ID, ^CommitSeq(0), 1)
	return head, err
}

// Contains reports whether id is a commit of the Ancestry: one of the tip's
// own commits or an inherited one within its anchor range (SES-FRK-3).
func (a *Ancestry) Contains(ctx context.Context, store LedgerStore, id CommitID) (bool, error) {
	_, ok, err := a.Lookup(ctx, store, id)
	return ok, err
}

// Lookup reads a commit of the Ancestry by CommitID.
func (a *Ancestry) Lookup(ctx context.Context, store LedgerStore, id CommitID) (Commit, bool, error) {
	return lookupIn(ctx, store, a.Segments, id)
}

// LookupInherited reads an inherited commit by CommitID: the Ancestry without
// its tip. The inherited prefix is immutable, so reading it from storage at
// any time gives the same answer.
func (a *Ancestry) LookupInherited(ctx context.Context, store LedgerStore, id CommitID) (Commit, bool, error) {
	return lookupIn(ctx, store, a.Segments[:len(a.Segments)-1], id)
}

func lookupIn(ctx context.Context, store LedgerStore, segments []AncestrySegment, id CommitID) (Commit, bool, error) {
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
