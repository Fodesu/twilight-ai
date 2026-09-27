package session

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// Ledger is the kernel's Store over a Backend (SES 4 to 6, 8, 9): the
// Session lineage tree in code. A root stores the path of spans it reads
// and names the last span's segment as its tip. Segments stay append-only
// logs and keep one parent edge, recorded when the segment is created.
// Each segment also stores the right endpoints of the live paths that cover
// it. Fork copies the parent's path, closes the span that contains the fork
// seq, and appends a new open segment. Delete tombstones the root and drops
// its endpoints; Collect rebuilds endpoints from the live paths. Every
// adapter gets these semantics from here and implements none of them.
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
	var parent *Session
	if req.Fork != nil {
		// The edge names the segment that contributes the inherited commit,
		// wherever on the parent's path it lives (SES-FRK-1).
		if req.Fork.Session == req.SessionID {
			return SegmentHeader{}, newError(ErrInvalid, "create", req.SessionID, "a session cannot fork itself")
		}
		var err error
		parent, err = l.Load(ctx, req.Fork.Session)
		if err != nil {
			if IsCode(err, ErrNotFound) {
				return SegmentHeader{}, newError(ErrNotFound, "create", req.SessionID, fmt.Sprintf("parent session %s not found", req.Fork.Session))
			}
			return SegmentHeader{}, err
		}
		// The edge names a position in the parent's history (SES-FRK-1).
		// EdgeAt reads the commit, so an open span cannot accept a seq the
		// segment does not hold.
		edge, ok, err := parent.EdgeAt(ctx, req.Fork.Seq)
		if err != nil {
			return SegmentHeader{}, err
		}
		if !ok {
			return SegmentHeader{}, newError(ErrInvalid, "create", req.SessionID, fmt.Sprintf("parent %s has no commit %d", req.Fork.Session, req.Fork.Seq))
		}
		header.Parent = &edge
	}
	// Idempotency is judged on what the request determines about the
	// segment, not on its identity or clock: the ID is drawn fresh each time
	// and the creation time is the first writer's (SES-CRT-1).
	if seg, err := l.tipSegment(ctx, req.SessionID); err == nil {
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
	if err := header.Validate(); err != nil {
		return SegmentHeader{}, err
	}
	path, err := creationPath(parent, header, req.SessionID)
	if err != nil {
		return SegmentHeader{}, err
	}
	segment := Segment{Header: header}
	root := SessionRecord{ID: req.SessionID, Tip: segment.ID(), CreatedAtUnixMilli: req.CreatedAtUnixMilli, Path: path}
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

// creationPath is the path stored on a new root. A fork keeps the parent's
// spans through the one that contains the fork seq, closes that span, and
// appends an open span for the new segment. The closed span's segment is
// the parent edge EdgeAt already resolved.
func creationPath(parent *Session, header SegmentHeader, sid SessionID) (Path, error) {
	if header.Parent == nil {
		path := Path{{Segment: header.ID, From: 0, End: OpenBound()}}
		if err := path.Validate(header.ID); err != nil {
			return nil, newError(ErrCorrupt, "create", sid, err.Error())
		}
		return path, nil
	}
	if parent == nil {
		return nil, newError(ErrCorrupt, "create", sid, "fork has no parent session")
	}
	base := parent.Record().Path
	if len(base) == 0 {
		base = pathFromLoaded(parent.Loaded())
	}
	path, edge, err := base.Branch(header.Parent.Seq, header.ID)
	if err != nil {
		return nil, newError(ErrCorrupt, "create", sid, err.Error())
	}
	if edge != *header.Parent {
		return nil, newError(ErrCorrupt, "create", sid, "fork edge does not match the stored path")
	}
	if err := path.Validate(header.ID); err != nil {
		return nil, newError(ErrCorrupt, "create", sid, err.Error())
	}
	return path, nil
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
	seg, err := l.tipSegment(ctx, sid)
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
	s, err := l.Load(ctx, sid)
	if err != nil {
		return nil, err
	}
	lease, err := l.be.Acquire(ctx, sid, opts)
	if err != nil {
		return nil, err
	}
	// Acquire repaired a torn tail; the segment's CommitIndex (SES-REP-5) is
	// checked against the head and rebuilt when it lags, but the handle
	// holds none of it: membership and stream heads are answered by the
	// backend's index on demand (SES-REP-3), so Open costs the same for a
	// tip of ten commits and one of a million.
	head, err := s.repairTip(ctx)
	if err != nil {
		_ = l.be.Release(ctx, lease)
		return nil, err
	}
	return &ledgerHandle{session: s, lease: lease, opts: opts, head: head, streams: make(map[StreamRef]StreamSeq)}, nil
}

// --- read -------------------------------------------------------------------------

func (l *Ledger) ReadCommits(ctx context.Context, req CommitReadRequest) (CommitPage, error) {
	if err := ctx.Err(); err != nil {
		return CommitPage{}, err
	}
	s, err := l.Load(ctx, req.SessionID)
	if err != nil {
		return CommitPage{}, err
	}
	return s.ReadCommits(ctx, req.From, req.Limit)
}

func (l *Ledger) ReadStream(ctx context.Context, req StreamReadRequest) (StreamPage, error) {
	if err := ctx.Err(); err != nil {
		return StreamPage{}, err
	}
	// Validate before loading so a malformed read is ErrInvalid even when the
	// Session is absent. Stream positions count the stream's events from the
	// first commit the read sees (SES-REP-2).
	if err := validateStreamRead(req.SessionID, req.Stream, req.Lineage); err != nil {
		return StreamPage{}, err
	}
	s, err := l.Load(ctx, req.SessionID)
	if err != nil {
		return StreamPage{}, err
	}
	return s.collectStream(ctx, req.Stream, req.Lineage, req.From, req.Limit)
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

func (l *Ledger) Delete(ctx context.Context, sid SessionID) (CollectReport, error) {
	if err := ctx.Err(); err != nil {
		return CollectReport{}, err
	}
	l.graph.Lock()
	defer l.graph.Unlock()
	root, err := l.be.Record(ctx, sid)
	if err != nil {
		return CollectReport{}, err
	}
	// Resolve the path before the tombstone. ErrOwned and ErrNotFound from
	// DeleteRecord then leave every endpoint where it was.
	path, err := l.sessionPath(ctx, root)
	if err != nil {
		return CollectReport{}, err
	}
	if err := l.be.DeleteRecord(ctx, sid); err != nil {
		return CollectReport{}, err
	}
	report := CollectReport{Truncated: map[SegmentID]CommitSeq{}, Dropped: map[SegmentID][]CommitID{}}
	// Tip first: a segment is removed only after the child edge that names
	// its parent has been removed with the child.
	for i := len(path) - 1; i >= 0; i-- {
		if err := l.reclaim(ctx, path[i].Segment, sid, &report); err != nil {
			return report, err
		}
	}
	return report, nil
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
	need := make(map[SegmentID][]Endpoint)
	for i := range roots {
		path, err := l.sessionPath(ctx, roots[i])
		if err != nil {
			return CollectReport{}, err
		}
		for _, span := range path {
			need[span.Segment] = append(need[span.Segment], Endpoint{Session: roots[i].ID, End: span.End})
		}
	}
	report := CollectReport{Truncated: map[SegmentID]CommitSeq{}, Dropped: map[SegmentID][]CommitID{}}
	reached := make(map[SegmentID]Bound, len(need))
	for id, covers := range need {
		if _, ok := nodes[id]; !ok {
			continue
		}
		if err := l.be.ReplaceEndpoints(ctx, id, covers); err != nil {
			if IsCode(err, ErrNotFound) {
				continue
			}
			return report, err
		}
		cov, ok := MaxBound(covers)
		if !ok {
			continue
		}
		reached[id] = cov
		if err := l.clip(ctx, id, cov, &report); err != nil {
			return report, err
		}
	}
	// Segments no live path covers go children first: the adapter refuses to
	// remove a node a child's edge still names (SES-GC-4).
	for _, id := range removalOrder(nodes, reached) {
		if err := l.be.RemoveSegment(ctx, id); err != nil {
			if IsCode(err, ErrReferenced) {
				continue
			}
			return report, err
		}
		report.Removed = append(report.Removed, id)
	}
	return report, nil
}

// reclaim drops sid's endpoint on id. An empty set removes the segment. A
// closed maximum truncates the segment to that commit. ErrReferenced leaves
// the segment for a later Collect.
func (l *Ledger) reclaim(ctx context.Context, id SegmentID, sid SessionID, report *CollectReport) error {
	covers, err := l.be.RemoveEndpoint(ctx, id, sid)
	if err != nil {
		return err
	}
	if len(covers) == 0 {
		if err := l.be.RemoveSegment(ctx, id); err != nil {
			if IsCode(err, ErrReferenced) {
				return nil
			}
			return err
		}
		report.Removed = append(report.Removed, id)
		return nil
	}
	cov, ok := MaxBound(covers)
	if !ok {
		return nil
	}
	return l.clip(ctx, id, cov, report)
}

// clip drops id's commits after cov. An open coverage retains the live head.
func (l *Ledger) clip(ctx context.Context, id SegmentID, cov Bound, report *CollectReport) error {
	if cov.Open {
		return nil
	}
	_, head, _, err := l.be.ReadSegment(ctx, id, cov.Through+1, 1)
	if err != nil {
		return err
	}
	if head.Next <= cov.Through+1 {
		return nil
	}
	idx, _, err := l.be.Index(ctx, id)
	if err != nil {
		return err
	}
	var dropped []CommitID
	for i := range idx.Entries {
		if idx.Entries[i].Seq > cov.Through {
			dropped = append(dropped, idx.Entries[i].CommitID)
		}
	}
	newHead, err := l.be.TruncateSegment(ctx, id, cov.Through)
	if err != nil {
		return err
	}
	if len(dropped) > 0 {
		report.Dropped[id] = dropped
	}
	report.Truncated[id] = newHead.Next
	return nil
}

// removalOrder lists the segments no live path covers so that every segment
// comes before its parent: a child's edge keeps its parent from being removed.
func removalOrder(nodes map[SegmentID]Segment, need map[SegmentID]Bound) []SegmentID {
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
