package session

import (
	"context"
	"fmt"
	"sync"
)

// Ledger is the kernel's Store over a Backend (SES 4 to 6, 8, 9): the
// Session lineage DAG in code. Roots (SessionRecord) name the segment they
// append to; segments (Segment) chain to their parents through ForkPoint
// edges; a Session's history is the stitched Ancestry of its segment. Fork
// adds a node and an edge; Delete drops a root; Collect reclaims what no
// root reaches. Every adapter gets these semantics from here and implements
// none of them.
type Ledger struct {
	be Backend
}

// NewLedger returns the Store over be.
func NewLedger(be Backend) *Ledger { return &Ledger{be: be} }

// resolve loads a live Session's root and the Ancestry of its segment.
func (l *Ledger) resolve(ctx context.Context, sid SessionID) (SessionRecord, *Ancestry, error) {
	root, err := l.be.Record(ctx, sid)
	if err != nil {
		return SessionRecord{}, nil, err
	}
	a, err := LoadAncestry(ctx, l.be, root.Segment)
	if err != nil {
		return SessionRecord{}, nil, err
	}
	return root, a, nil
}

// --- create -----------------------------------------------------------------------

func (l *Ledger) Create(ctx context.Context, req CreateRequest) (SessionHeader, error) {
	if err := ctx.Err(); err != nil {
		return SessionHeader{}, err
	}
	profile, err := LedgerProfileFor(req.ProtocolVersion)
	if err != nil {
		return SessionHeader{}, &Error{Code: ErrUnsupportedProfile, Operation: "create", SessionID: req.SessionID}
	}
	header := SessionHeader{ProtocolVersion: req.ProtocolVersion, SessionID: req.SessionID, CreatedAtUnixMilli: req.CreatedAtUnixMilli,
		CausationID: req.CausationID, Metadata: req.Metadata}
	if req.Fork != nil {
		// The edge names the segment that contributes the inherited commit,
		// wherever in the parent's ancestry it lives (SES-FRK-1).
		if req.Fork.Session == req.SessionID {
			return SessionHeader{}, newError(ErrInvalid, "create", req.SessionID, "a session cannot fork itself")
		}
		_, parent, err := l.resolve(ctx, req.Fork.Session)
		if err != nil {
			if IsCode(err, ErrNotFound) {
				return SessionHeader{}, newError(ErrNotFound, "create", req.SessionID, fmt.Sprintf("parent session %s not found", req.Fork.Session))
			}
			return SessionHeader{}, err
		}
		if parent.Header().ProtocolVersion != req.ProtocolVersion {
			return SessionHeader{}, &Error{Code: ErrUnsupportedProfile, Operation: "create", SessionID: req.SessionID,
				Detail: fmt.Sprintf("parent %s is protocol v%d", req.Fork.Session, parent.Header().ProtocolVersion)}
		}
		owner, ok := parent.Owner(req.Fork.Seq)
		if !ok {
			return SessionHeader{}, newError(ErrInvalid, "create", req.SessionID, fmt.Sprintf("parent %s has no commit %d", req.Fork.Session, req.Fork.Seq))
		}
		commits, _, _, err := l.be.ReadSegment(ctx, owner.Segment.ID, req.Fork.Seq, 1)
		if err != nil {
			return SessionHeader{}, err
		}
		if len(commits) != 1 || commits[0].Seq != req.Fork.Seq {
			return SessionHeader{}, newError(ErrInvalid, "create", req.SessionID, fmt.Sprintf("parent %s has no commit %d", req.Fork.Session, req.Fork.Seq))
		}
		header.ParentFork = &ForkPoint{Parent: owner.Segment.ID, Seq: req.Fork.Seq, Digest: commits[0].Digest}
	}
	digest, err := profile.HeaderDigest(header)
	if err != nil {
		return SessionHeader{}, err
	}
	header.HeaderDigest = digest
	if err := profile.ValidateHeader(header); err != nil {
		return SessionHeader{}, err
	}
	segment := Segment{ID: SegmentIDOf(header), Header: header}
	// Idempotency: the same request yields the same segment identity.
	if existing, err := l.be.Record(ctx, req.SessionID); err == nil {
		if existing.Segment == segment.ID {
			return header, nil
		}
		return SessionHeader{}, newError(ErrConflict, "create", req.SessionID, "session exists with a different header")
	} else if !IsCode(err, ErrNotFound) {
		return SessionHeader{}, err
	}
	if err := l.be.CreateSegment(ctx, segment); err != nil && !IsCode(err, ErrConflict) {
		return SessionHeader{}, err
	}
	if err := l.be.CreateRecord(ctx, SessionRecord{ID: req.SessionID, Segment: segment.ID}); err != nil {
		if IsCode(err, ErrConflict) {
			// A concurrent creator won; answer as the idempotent path would.
			if existing, rerr := l.be.Record(ctx, req.SessionID); rerr == nil && existing.Segment == segment.ID {
				return header, nil
			}
		}
		return SessionHeader{}, err
	}
	return header, nil
}

func (l *Ledger) Header(ctx context.Context, sid SessionID) (SessionHeader, error) {
	if err := ctx.Err(); err != nil {
		return SessionHeader{}, err
	}
	root, err := l.be.Record(ctx, sid)
	if err != nil {
		return SessionHeader{}, err
	}
	seg, err := l.be.Segment(ctx, root.Segment)
	if err != nil {
		return SessionHeader{}, err
	}
	return seg.Header, nil
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
	h := &ledgerHandle{l: l, root: root, ancestry: a, profile: profile, lease: lease, head: head, own: make(map[CommitID]struct{}, len(own))}
	for i := range own {
		h.own[own[i].CommitID] = struct{}{}
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
		c, ok, err := w.l.be.LookupCommit(context.Background(), w.root.Segment, id)
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
	c := Commit{Seq: w.head.Next, CommitID: p.CommitID, Epoch: w.lease.Epoch, Batches: cloneBatches(p.Batches)}
	if err := SealCommit(w.profile, w.head.Digest, sid, &c); err != nil {
		return Commit{}, err
	}
	if err := w.l.be.Append(ctx, w.lease, w.root.Segment, c); err != nil {
		if IsCode(err, ErrHandleFailed) {
			w.failed = err
		}
		return Commit{}, err
	}
	w.head = Head{Next: c.Seq + 1, Digest: c.Digest}
	w.own[c.CommitID] = struct{}{}
	return cloneCommit(c), nil
}

func (w *ledgerHandle) Close(ctx context.Context) error { return w.l.be.Release(ctx, w.lease) }

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
	// Stream positions count the stream's events from the first commit the
	// Session sees, inherited prefix included (SES-REP-2).
	all, err := l.ReadCommits(ctx, CommitReadRequest{SessionID: req.SessionID})
	if err != nil {
		return StreamPage{}, err
	}
	page := StreamPage{Header: all.Header, Stream: req.Stream, Head: all.Head}
	page.Events, page.HasMore = StreamEvents(all.Commits, req.Stream, req.From, req.Limit)
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
				if limit > 0 && uint32(len(events)) >= limit {
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
	return l.be.DeleteRecord(ctx, sid)
}

func (l *Ledger) Collect(ctx context.Context) (CollectReport, error) {
	if err := ctx.Err(); err != nil {
		return CollectReport{}, err
	}
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
