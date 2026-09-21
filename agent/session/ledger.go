package session

import (
	"context"
	"fmt"
	"sync"
)

// Ledger is the kernel's Store over a Backend (SES 4 to 6, 8, 9): the
// Session lineage tree in code. Roots (SessionRecord) name the segment they
// append to as their tip; segments (Segment) chain to their parents through
// LedgerRef edges; a Session's history is the stitched Ancestry of its segment. Fork
// adds a node and an edge; Delete drops a root; Collect reclaims what no
// root reaches. Every adapter gets these semantics from here and implements
// none of them.
type Ledger struct {
	be    Backend
	nonce func() (string, error)
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

// WithNonceSource replaces the segment nonce generator. Production uses
// NewNonce; wire fixtures inject a deterministic source so the bytes they
// freeze are reproducible.
func WithNonceSource(src func() (string, error)) LedgerOption {
	return func(l *Ledger) { l.nonce = src }
}

// NewLedger returns the Store over be.
func NewLedger(be Backend, opts ...LedgerOption) *Ledger {
	l := &Ledger{be: be, nonce: NewNonce}
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
	profile, err := LedgerProfileFor(req.ProtocolVersion)
	if err != nil {
		return SegmentHeader{}, &Error{Code: ErrUnsupportedProfile, Operation: "create", SessionID: req.SessionID}
	}
	if err := validIdentity("SessionID", string(req.SessionID)); err != nil {
		return SegmentHeader{}, newError(ErrInvalid, "create", req.SessionID, err.Error())
	}
	header := SegmentHeader{ProtocolVersion: req.ProtocolVersion, CausationID: req.CausationID, Metadata: req.Metadata}
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
		if parent.Header().ProtocolVersion != req.ProtocolVersion {
			return SegmentHeader{}, &Error{Code: ErrUnsupportedProfile, Operation: "create", SessionID: req.SessionID,
				Detail: fmt.Sprintf("parent %s is protocol v%d", req.Fork.Session, parent.Header().ProtocolVersion)}
		}
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
		header.Parent = &LedgerRef{Segment: owner.Segment.ID, Seq: req.Fork.Seq, Digest: commits[0].Digest}
	}
	// Idempotency is judged on what the request determines, not on the
	// segment identity: the nonce is drawn fresh each time, so a repeat is
	// recognized by matching every requested field (SES-CRT-1).
	if existing, err := l.be.Record(ctx, req.SessionID); err == nil {
		seg, err := l.be.Segment(ctx, existing.Tip)
		if err != nil {
			return SegmentHeader{}, err
		}
		if sameCreation(req, header, existing, seg.Header) {
			return seg.Header, nil
		}
		return SegmentHeader{}, newError(ErrConflict, "create", req.SessionID, "session exists with a different creation record")
	} else if !IsCode(err, ErrNotFound) {
		return SegmentHeader{}, err
	}
	if header.Nonce, err = l.nonce(); err != nil {
		return SegmentHeader{}, err
	}
	digest, err := profile.HeaderDigest(header)
	if err != nil {
		return SegmentHeader{}, err
	}
	header.HeaderDigest = digest
	if err := profile.ValidateHeader(header); err != nil {
		return SegmentHeader{}, err
	}
	segment := Segment{ID: SegmentIDOf(header), Header: header}
	root := SessionRecord{ID: req.SessionID, Tip: segment.ID, CreatedAtUnixMilli: req.CreatedAtUnixMilli}
	if err := l.be.CreateSession(ctx, segment, root); err != nil {
		return SegmentHeader{}, err
	}
	return header, nil
}

// sameCreation reports whether req would create exactly the Session that
// exists: same protocol, same resolved edge, same causation and metadata,
// same creation time.
func sameCreation(req CreateRequest, want SegmentHeader, root SessionRecord, have SegmentHeader) bool {
	if have.ProtocolVersion != want.ProtocolVersion || root.CreatedAtUnixMilli != req.CreatedAtUnixMilli {
		return false
	}
	if (have.Parent == nil) != (want.Parent == nil) || (have.Parent != nil && *have.Parent != *want.Parent) {
		return false
	}
	return have.CausationID == want.CausationID && have.Metadata.String() == want.Metadata.String()
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
	profile, err := LedgerProfileFor(tip.Header.ProtocolVersion)
	if err != nil {
		return nil, err
	}
	// Corruption detection happens before ownership is established
	// (SES-REP-1); reads trust the store.
	own, _, _, err := l.be.ReadSegment(ctx, tip.ID, tip.Seed().Next, 0)
	if err != nil {
		return nil, err
	}
	if err := ValidateLedger(profile, tip.Header, own); err != nil {
		return nil, err
	}
	lease, err := l.be.Acquire(ctx, sid, opts)
	if err != nil {
		return nil, err
	}
	// Acquire repaired a torn tail, so the own commits are re-read for the
	// handle's index (SES-REP-3); the inherited prefix is immutable.
	own, head, _, err := l.be.ReadSegment(ctx, tip.ID, tip.Seed().Next, 0)
	if err != nil {
		_ = l.be.Release(ctx, lease)
		return nil, err
	}
	h := &ledgerHandle{l: l, root: root, ancestry: a, profile: profile, lease: lease, head: head,
		own: make(map[CommitID]struct{}, len(own)), streams: make(map[StreamRef]StreamSeq)}
	for i := range own {
		h.own[own[i].CommitID] = struct{}{}
		h.countStreams(&own[i])
	}
	return h, nil
}

// ledgerHandle is the ownership handle over one root. It answers membership
// of the tip's own commits from the index it built at Open and extends with
// each Append (SES-REP-3): what it knows is exactly what it wrote or read
// under its own lease, so a superseded handle never learns of a successor's
// commits and reaches the fence at Append. The inherited prefix is immutable
// and is read from storage.
type ledgerHandle struct {
	mu       sync.Mutex
	l        *Ledger
	root     SessionRecord
	ancestry *Ancestry
	profile  LedgerProfile
	lease    Lease
	head     Head
	own      map[CommitID]struct{}
	// streams counts the events of each logical stream the tip segment
	// holds, so StreamHead is answered without a read (SES-REP-3).
	streams map[StreamRef]StreamSeq
	// failed is set once an Append's durable outcome is unknown (SES-APP-1):
	// the handle then answers nothing about the ledger, because what reached
	// storage is exactly what it cannot know. The caller reopens.
	failed error
}

func (w *ledgerHandle) SessionID() SessionID { return w.root.ID }
func (w *ledgerHandle) Epoch() Epoch         { return w.lease.Epoch }

func (w *ledgerHandle) Head() Head {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.head
}

// Committed is SES-REP-3: the handle's own index plus the inherited prefix
// (SES-FRK-3).
func (w *ledgerHandle) Committed(id CommitID) bool {
	w.mu.Lock()
	failed := w.failed
	_, own := w.own[id]
	w.mu.Unlock()
	if failed != nil {
		return false
	}
	if own {
		return true
	}
	_, inherited, err := w.ancestry.LookupInherited(context.Background(), w.l.be, id)
	return err == nil && inherited
}

// countStreams extends the stream index with one own commit; w.mu is held or
// the handle is still being built.
func (w *ledgerHandle) countStreams(c *Commit) {
	for i := range c.Batches {
		w.streams[c.Batches[i].Stream] += StreamSeq(len(c.Batches[i].Events))
	}
}

func (w *ledgerHandle) StreamHead(stream StreamRef) (StreamSeq, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failed != nil {
		return 0, false
	}
	n, ok := w.streams[stream]
	return n, ok
}

// LookupCommit is SES-REP-4: an own commit is read from the tip segment, an
// inherited one from the segment that holds it.
func (w *ledgerHandle) LookupCommit(id CommitID) (Commit, bool, error) {
	w.mu.Lock()
	failed := w.failed
	_, own := w.own[id]
	w.mu.Unlock()
	if failed != nil {
		return Commit{}, false, failed
	}
	if own {
		c, ok, err := w.l.be.LookupCommit(context.Background(), w.root.Tip, id)
		if err != nil || !ok {
			return Commit{}, false, err
		}
		return c, true, nil
	}
	return w.ancestry.LookupInherited(context.Background(), w.l.be, id)
}

func (w *ledgerHandle) Append(ctx context.Context, p Proposal) (Commit, error) {
	if err := ctx.Err(); err != nil {
		return Commit{}, err
	}
	sid := w.root.ID
	if err := validIdentity("CommitID", string(p.CommitID)); err != nil {
		return Commit{}, newError(ErrInvalid, "append", sid, err.Error())
	}
	if err := ValidateBatches(p.Batches); err != nil {
		return Commit{}, newError(ErrInvalid, "append", sid, err.Error())
	}
	if w.Committed(p.CommitID) {
		return Commit{}, &Error{Code: ErrConflict, Operation: "append", SessionID: sid, CommitID: p.CommitID, Detail: "CommitID already in the ledger"}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failed != nil {
		return Commit{}, w.failed
	}
	c := Commit{Seq: w.head.Next, CommitID: p.CommitID, Epoch: w.lease.Epoch, Intent: p.Intent, Batches: cloneBatches(p.Batches)}
	if err := SealCommit(w.profile, w.head.Digest, w.root.Tip, &c); err != nil {
		return Commit{}, err
	}
	if err := w.l.be.Append(ctx, w.lease, w.root.Tip, c); err != nil {
		if IsCode(err, ErrHandleFailed) {
			w.failed = err
		}
		return Commit{}, err
	}
	w.head = Head{Next: c.Seq + 1, Digest: c.Digest}
	w.own[c.CommitID] = struct{}{}
	w.countStreams(&c)
	return cloneCommit(c), nil
}

func (w *ledgerHandle) Close(ctx context.Context) error { return w.l.be.Release(ctx, w.lease) }

// committedLocked is Committed with w.mu held.
func (w *ledgerHandle) committedLocked(id CommitID) bool {
	if _, own := w.own[id]; own {
		return true
	}
	_, inherited, err := w.ancestry.LookupInherited(context.Background(), w.l.be, id)
	return err == nil && inherited
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
	// Stream positions count the stream's events from the first commit the
	// read sees (SES-REP-2): the stitched history under LineageSession, the
	// tip segment's own commits under LineageSegment (SES-FRK-5). The kernel
	// applies the mode the read names; the stream's owning module declared
	// which one its domain is.
	all, err := l.ReadCommits(ctx, CommitReadRequest{SessionID: req.SessionID})
	if err != nil {
		return StreamPage{}, err
	}
	commits := all.Commits
	if req.Lineage == LineageSegment && all.Header.Parent != nil {
		own := commits[:0:0]
		for _, c := range commits {
			if c.Seq > all.Header.Parent.Seq {
				own = append(own, c)
			}
		}
		commits = own
	}
	page := StreamPage{Header: all.Header, Stream: req.Stream, Head: all.Head}
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
	report := CollectReport{Truncated: map[SegmentID]CommitSeq{}}
	for id := range nodes {
		through, reached := need[id]
		if !reached {
			if err := l.be.RemoveSegment(ctx, id); err != nil {
				return report, err
			}
			report.Removed = append(report.Removed, id)
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
		newHead, err := l.be.TruncateSegment(ctx, id, through)
		if err != nil {
			return report, err
		}
		report.Truncated[id] = newHead.Next
	}
	return report, nil
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
