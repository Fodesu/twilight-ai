package session

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// Ledger is the kernel's Store over a Backend (SES 4 to 6, 8, 9): the
// Session lineage tree in code. Roots (SessionRecord) name the segment they
// append to as their tip; segments (Segment) point to their parents through
// CommitRef edges; a Session's history is the stitched Ancestry of its segment. Fork
// adds a node and an edge; Delete drops a root; Collect reclaims what no
// root reaches. Every adapter gets these semantics from here and implements
// none of them.
type Ledger struct {
	be        Backend
	segmentID func() (SegmentID, error)
	// graph serializes this process's operations that change the set of
	// roots and nodes; across processes the adapter's own consistency
	// (CreateSession's parent check, RemoveSegment's reference check) is
	// the authority (SES-GC-4).
	// graph serializes the operations that change the set of roots and
	// nodes (Create, Delete, Collect) against each other (SES-GC-4): a
	// Create's check that its parent is live, and its write, cannot
	// interleave with a Collect that would reclaim that parent or the new
	// node. Append and reads never take it; a live root's segments are never
	// touched by Collect. The lock is per process: a Backend shared by
	// several processes must provide this exclusion itself.
	graph sync.Mutex
}

// LedgerOption configures a Ledger.
type LedgerOption func(*Ledger)

// WithSegmentIDSource replaces the segment identity generator. Production
// uses NewSegmentID; wire fixtures inject a deterministic source so the
// bytes they freeze are reproducible.
func WithSegmentIDSource(src func() (SegmentID, error)) LedgerOption {
	return func(l *Ledger) { l.segmentID = src }
}

// NewLedger returns the Store over be.
func NewLedger(be Backend, opts ...LedgerOption) *Ledger {
	l := &Ledger{be: be, segmentID: NewSegmentID}
	for _, o := range opts {
		o(l)
	}
	return l
}

// resolve loads a live Session's root and the Ancestry of its segment.
func (l *Ledger) resolve(ctx context.Context, sid SessionID) (SessionRecord, *Ancestry, error) {
	root, err := l.be.Record(ctx, sid)
	if err != nil {
		return SessionRecord{}, nil, err
	}
	a, err := LoadAncestry(ctx, l.be, root.Tip)
	if err != nil {
		return SessionRecord{}, nil, err
	}
	return root, a, nil
}

// --- create -----------------------------------------------------------------------

func (l *Ledger) Create(ctx context.Context, req CreateRequest) (SegmentHeader, error) {
	if err := ctx.Err(); err != nil {
		return SegmentHeader{}, err
	}
	l.graph.Lock()
	defer l.graph.Unlock()
	if err := validIdentity("SessionID", string(req.SessionID)); err != nil {
		return SegmentHeader{}, newError(ErrInvalid, "create", req.SessionID, err.Error())
	}
	header := SegmentHeader{CausationID: req.CausationID, Ext: req.Ext.Clone()}
	if req.Fork != nil {
		// The edge names the segment that contributes the inherited commit,
		// wherever in the parent's ancestry it lives (SES-FRK-1).
		if req.Fork.Session == req.SessionID {
			return SegmentHeader{}, newError(ErrInvalid, "create", req.SessionID, "a session cannot fork itself")
		}
		_, parent, err := l.resolve(ctx, req.Fork.Session)
		if err != nil {
			if IsCode(err, ErrNotFound) {
				return SegmentHeader{}, newError(ErrNotFound, "create", req.SessionID, fmt.Sprintf("parent session %s not found", req.Fork.Session))
			}
			return SegmentHeader{}, err
		}
		// The edge names a position in the parent's history (SES-FRK-1).
		owner, ok := parent.Owner(req.Fork.Seq)
		if !ok {
			return SegmentHeader{}, newError(ErrInvalid, "create", req.SessionID, fmt.Sprintf("parent %s has no commit %d", req.Fork.Session, req.Fork.Seq))
		}
		commits, _, _, err := l.be.ReadSegment(ctx, owner.Segment.ID, req.Fork.Seq, 1)
		if err != nil {
			return SegmentHeader{}, err
		}
		if len(commits) != 1 || commits[0].Seq != req.Fork.Seq {
			return SegmentHeader{}, newError(ErrInvalid, "create", req.SessionID, fmt.Sprintf("parent %s has no commit %d", req.Fork.Session, req.Fork.Seq))
		}
		header.Parent = &CommitRef{Segment: owner.Segment.ID, Seq: req.Fork.Seq}
	}
	// Idempotency is judged on what the request determines about the
	// segment, not on its identity or clock: the ID is drawn fresh each time
	// and the creation time is the first writer's (SES-CRT-1).
	if existing, err := l.be.Record(ctx, req.SessionID); err == nil {
		seg, err := l.be.Segment(ctx, existing.Tip)
		if err != nil {
			return SegmentHeader{}, err
		}
		if sameCreation(header, seg.Header) {
			return seg.Header, nil
		}
		return SegmentHeader{}, newError(ErrConflict, "create", req.SessionID, "session exists with a different creation record")
	} else if !IsCode(err, ErrNotFound) && !IsCode(err, ErrDeleted) {
		return SegmentHeader{}, err
	}
	id, err := l.segmentID()
	if err != nil {
		return SegmentHeader{}, err
	}
	header.ID = id
	if err := ValidateHeader(header); err != nil {
		return SegmentHeader{}, err
	}
	segment := Segment{ID: header.ID, Header: header}
	root := SessionRecord{ID: req.SessionID, Tip: segment.ID, CreatedAtUnixMilli: req.CreatedAtUnixMilli}
	if err := l.be.CreateSession(ctx, segment, root); err != nil {
		// The parent was checked above, but another replica's Collect may
		// have removed it since (SES-GC-4): the adapter's own check inside
		// the write is the authority, and the fork is refused as of a
		// parent that is gone.
		if header.Parent != nil && IsCode(err, ErrNotFound) {
			return SegmentHeader{}, newError(ErrNotFound, "create", req.SessionID, fmt.Sprintf("parent session %s not found", req.Fork.Session))
		}
		return SegmentHeader{}, err
	}
	return header, nil
}

// sameCreation reports whether req would create the Session that exists:
// same resolved edge, same causation, same extensions. The creation time
// is the first writer's and does not decide it, so a retried Create with a
// fresh clock is a replay (SES-CRT-1).
func sameCreation(want, have SegmentHeader) bool {
	if (have.Parent == nil) != (want.Parent == nil) || (have.Parent != nil && *have.Parent != *want.Parent) {
		return false
	}
	return have.CausationID == want.CausationID && have.Ext.Equal(want.Ext)
}

func (l *Ledger) Header(ctx context.Context, sid SessionID) (SegmentHeader, error) {
	if err := ctx.Err(); err != nil {
		return SegmentHeader{}, err
	}
	root, err := l.be.Record(ctx, sid)
	if err != nil {
		return SegmentHeader{}, err
	}
	seg, err := l.be.Segment(ctx, root.Tip)
	if err != nil {
		return SegmentHeader{}, err
	}
	return seg.Header, nil
}

func (l *Ledger) Record(ctx context.Context, sid SessionID) (SessionRecord, error) {
	if err := ctx.Err(); err != nil {
		return SessionRecord{}, err
	}
	return l.be.Record(ctx, sid)
}

// LeaseOf is Store.LeaseOf (SES-OWN-5): the adapter's reading of the root's
// current holder.
func (l *Ledger) LeaseOf(ctx context.Context, sid SessionID) (Lease, bool, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, false, err
	}
	return l.be.LeaseOf(ctx, sid)
}

// ListLeases is Store.ListLeases (SES-OWN-5).
func (l *Ledger) ListLeases(ctx context.Context) ([]Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return l.be.ListLeases(ctx)
}

// ExpiredLeases is Store.ExpiredLeases (SES-OWN-5/6).
func (l *Ledger) ExpiredLeases(ctx context.Context, limit int) ([]Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return l.be.ExpiredLeases(ctx, limit)
}

// ExpiredLeasesOf selects from leases what ExpiredLeases returns: the
// shared filter of adapters without an expiry index.
func ExpiredLeasesOf(leases []Lease, beforeUnixMilli int64, limit int) []Lease {
	var out []Lease
	for _, l := range leases {
		if l.UntilUnixMilli != 0 && l.UntilUnixMilli <= beforeUnixMilli {
			out = append(out, l)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UntilUnixMilli != out[j].UntilUnixMilli {
			return out[i].UntilUnixMilli < out[j].UntilUnixMilli
		}
		return out[i].Session < out[j].Session
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// --- open -------------------------------------------------------------------------

func (l *Ledger) Open(ctx context.Context, sid SessionID, opts OpenOptions) (Handle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root, a, err := l.resolve(ctx, sid)
	if err != nil {
		return nil, err
	}
	tip := a.Tip()
	lease, err := l.be.Acquire(ctx, sid, opts)
	if err != nil {
		return nil, err
	}
	// Acquire repaired a torn tail; the segment's CommitIndex (SES-REP-5) is
	// checked against the head and rebuilt when it lags, but the handle
	// holds none of it: membership and stream heads are answered by the
	// backend's index on demand (SES-REP-3), so Open costs the same for a
	// tip of ten commits and one of a million.
	head, err := l.checkIndex(ctx, tip)
	if err != nil {
		_ = l.be.Release(ctx, lease)
		return nil, err
	}
	return &ledgerHandle{l: l, root: root, ancestry: a, lease: lease, opts: opts, head: head, streams: make(map[StreamRef]StreamSeq)}, nil
}

// checkIndex returns the segment's head after checking its CommitIndex by
// summary. An index that fails Valid against the head (absent, lagging
// after a crash, or cut) is rebuilt from the segment's own commits and
// written back (SES-REP-5); nothing is read otherwise.
func (l *Ledger) checkIndex(ctx context.Context, seg Segment) (Head, error) {
	summary, head, err := l.be.Summarize(ctx, seg.ID)
	if err != nil {
		return Head{}, err
	}
	if summary.Valid(seg.Seed(), head) {
		return head, nil
	}
	commits, head, _, err := l.be.ReadSegment(ctx, seg.ID, seg.Seed().Next, 0)
	if err != nil {
		return Head{}, err
	}
	if err := l.be.PutIndex(ctx, seg.ID, BuildCommitIndex(seg.Header, commits)); err != nil {
		return Head{}, err
	}
	return head, nil
}

// --- read -------------------------------------------------------------------------

func (l *Ledger) ReadCommits(ctx context.Context, req CommitReadRequest) (CommitPage, error) {
	if err := ctx.Err(); err != nil {
		return CommitPage{}, err
	}
	_, a, err := l.resolve(ctx, req.SessionID)
	if err != nil {
		return CommitPage{}, err
	}
	commits, head, more, err := a.Read(ctx, l.be, req.From, req.Limit)
	if err != nil {
		return CommitPage{}, err
	}
	return CommitPage{Header: a.Header(), Commits: commits, Head: head, HasMore: more}, nil
}

func (l *Ledger) ReadStream(ctx context.Context, req StreamReadRequest) (StreamPage, error) {
	if err := ctx.Err(); err != nil {
		return StreamPage{}, err
	}
	if err := ValidateStreamRef(req.Stream); err != nil {
		return StreamPage{}, newError(ErrInvalid, "read_stream", req.SessionID, err.Error())
	}
	if err := ValidateStreamLineage(req.Lineage); err != nil {
		return StreamPage{}, newError(ErrInvalid, "read_stream", req.SessionID, err.Error())
	}
	_, a, err := l.resolve(ctx, req.SessionID)
	if err != nil {
		return StreamPage{}, err
	}
	// Stream positions count the stream's events from the first commit the
	// read sees (SES-REP-2): the stitched history under LineageSession, the
	// tip segment's own commits under LineageSegment (SES-FRK-5). The kernel
	// applies the mode the read names; the stream's owning module declared
	// which one its domain is. Each segment is read through the adapter's
	// stream index (ReadSegmentStream), so only commits carrying the stream
	// travel; the stitching across the ancestry stays here.
	commits, err := a.ReadStream(ctx, l.be, req.Stream, req.Lineage)
	if err != nil {
		return StreamPage{}, err
	}
	head, err := a.tipHead(ctx, l.be)
	if err != nil {
		return StreamPage{}, err
	}
	page := StreamPage{Header: a.Header(), Stream: req.Stream, Head: head}
	page.Events, page.HasMore = StreamEvents(commits, req.Stream, req.From, req.Limit)
	return page, nil
}

// StreamEvents walks commits in order and returns the events of stream from
// position from, at most limit (0 = unlimited); more reports whether events
// beyond the returned ones exist. It is the one StreamSeq derivation
// (SES-REP-2).
func StreamEvents(commits []Commit, stream StreamRef, from StreamSeq, limit uint32) (events []Event, more bool) {
	var pos StreamSeq
	for i := range commits {
		for j := range commits[i].Batches {
			b := &commits[i].Batches[j]
			if b.Stream != stream {
				continue
			}
			for _, e := range b.Events {
				if pos < from {
					pos++
					continue
				}
				if AtLimit(len(events), limit) {
					return events, true
				}
				events = append(events, e)
				pos++
			}
		}
	}
	return events, false
}

// --- delete and collect (SES-GC) ----------------------------------------------------

func (l *Ledger) Delete(ctx context.Context, sid SessionID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.graph.Lock()
	defer l.graph.Unlock()
	return l.be.DeleteRecord(ctx, sid)
}

func (l *Ledger) Collect(ctx context.Context) (CollectReport, error) {
	if err := ctx.Err(); err != nil {
		return CollectReport{}, err
	}
	l.graph.Lock()
	defer l.graph.Unlock()
	ids, err := l.be.ListSegments(ctx)
	if err != nil {
		return CollectReport{}, err
	}
	nodes := make(map[SegmentID]Segment, len(ids))
	for _, id := range ids {
		seg, err := l.be.Segment(ctx, id)
		if err != nil {
			if IsCode(err, ErrNotFound) {
				continue
			}
			return CollectReport{}, err
		}
		nodes[id] = seg
	}
	roots, err := l.be.ListRecords(ctx)
	if err != nil {
		return CollectReport{}, err
	}
	need := Reachable(nodes, roots)
	report := CollectReport{Truncated: map[SegmentID]CommitSeq{}, Dropped: map[SegmentID][]CommitID{}}
	// Unreachable nodes go children first: the adapter refuses to remove a
	// node a child's edge still names (SES-GC-4), so a parent is removed
	// only once every unreachable child of it is gone.
	for _, id := range removalOrder(nodes, need) {
		// Reachable was computed from a snapshot; a root or a child created
		// since keeps the node, and the adapter says so (SES-GC-4). Such a
		// node is left for a later Collect.
		if err := l.be.RemoveSegment(ctx, id); err != nil {
			if IsCode(err, ErrReferenced) {
				continue
			}
			return report, err
		}
		report.Removed = append(report.Removed, id)
	}
	for id := range nodes {
		through, reached := need[id]
		if !reached {
			continue
		}
		if through == ^CommitSeq(0) {
			continue // a root's tip keeps everything
		}
		_, head, _, err := l.be.ReadSegment(ctx, id, through+1, 1)
		if err != nil {
			return report, err
		}
		if head.Next <= through+1 {
			continue
		}
		// The dropped commits are named before they go, from the index.
		idx, _, err := l.be.Index(ctx, id)
		if err != nil {
			return report, err
		}
		var dropped []CommitID
		for i := range idx.Entries {
			if idx.Entries[i].Seq > through {
				dropped = append(dropped, idx.Entries[i].CommitID)
			}
		}
		newHead, err := l.be.TruncateSegment(ctx, id, through)
		if err != nil {
			return report, err
		}
		if len(dropped) > 0 {
			report.Dropped[id] = dropped
		}
		report.Truncated[id] = newHead.Next
	}
	return report, nil
}

// removalOrder lists the unreachable nodes so that every node comes before
// its parent: a child's edge keeps its parent from being removed.
func removalOrder(nodes map[SegmentID]Segment, need map[SegmentID]CommitSeq) []SegmentID {
	depth := func(id SegmentID) int {
		d := 0
		for seg, ok := nodes[id]; ok && seg.Header.Parent != nil; seg, ok = nodes[seg.Header.Parent.Segment] {
			d++
			if d > len(nodes) {
				break // a cycle cannot exist (SES-LIN-1); guard the walk anyway
			}
		}
		return d
	}
	var out []SegmentID
	for id := range nodes {
		if _, reached := need[id]; !reached {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		di, dj := depth(out[i]), depth(out[j])
		if di != dj {
			return di > dj
		}
		return out[i] < out[j]
	})
	return out
}

func cloneCommit(c Commit) Commit {
	out := c
	out.Batches = cloneBatches(c.Batches)
	return out
}

func cloneBatches(batches []StreamBatch) []StreamBatch {
	out := make([]StreamBatch, len(batches))
	for i := range batches {
		out[i] = batches[i]
		out[i].Events = append([]Event(nil), batches[i].Events...)
	}
	return out
}

var _ Store = (*Ledger)(nil)
