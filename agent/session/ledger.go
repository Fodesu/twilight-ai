package session

import (
	"context"
	"fmt"
)

// SegmentStore is the adapter port beneath the Ledger (SES-SCP-4): durable
// storage of independent, append-only commit segments, one per SessionID,
// with writer ownership per segment. An adapter knows nothing of forks,
// inherited prefixes or reachability; the Ledger implements those once over
// this port. A segment's own commits are numbered from LedgerSeed(header).Next.
type SegmentStore interface {
	// CreateSegment persists a header the Ledger has sealed and validated.
	// It is ErrConflict when a segment with that SessionID exists, live or
	// deleted.
	CreateSegment(context.Context, SessionHeader) error
	// SegmentHeader returns a segment's header and whether its root was
	// deleted; ErrNotFound when no segment exists.
	SegmentHeader(context.Context, SessionID) (SessionHeader, bool, error)
	// ListSegments returns every segment the store holds, live or deleted.
	ListSegments(context.Context) ([]SessionID, error)
	// ReadSegment returns the segment's own commits from CommitSeq from
	// (absolute), at most limit (0 = unlimited), its head, and whether more
	// own commits follow. A torn tail is never returned.
	ReadSegment(ctx context.Context, sid SessionID, from CommitSeq, limit uint32) ([]Commit, Head, bool, error)
	// OpenSegment takes writer ownership (SES-OWN-1/2) and repairs a torn
	// tail. The Ledger validates the chain before calling it.
	OpenSegment(context.Context, SessionID, OpenOptions) (SegmentHandle, error)
	// MarkDeleted records that the segment's root was dropped; ErrOwned
	// while a writer holds it.
	MarkDeleted(context.Context, SessionID) error
	// TruncateSegment drops the segment's own commits after through and
	// returns the new head.
	TruncateSegment(ctx context.Context, sid SessionID, through CommitSeq) (Head, error)
	// RemoveSegment deletes the segment entirely.
	RemoveSegment(context.Context, SessionID) error
}

// SegmentHandle is the adapter's ownership handle over one segment.
type SegmentHandle interface {
	Epoch() Epoch
	// Head is the segment's head: LedgerSeed(header) while it has no own
	// commits.
	Head() Head
	// Own reports whether id is one of the segment's own commits.
	Own(CommitID) bool
	// LookupOwn reads one own commit.
	LookupOwn(CommitID) (Commit, bool, error)
	// Append persists a commit the Ledger sealed against this handle's Epoch
	// and Head; a superseded handle gets ErrOwnershipLost and writes nothing.
	Append(context.Context, Commit) error
	Close(context.Context) error
}

// Ledger is the kernel's Store over a SegmentStore (SES 4 to 6, 8): the
// Session lineage DAG in code. A Session is a root reference into immutable
// segments: its own segment plus, through ParentFork, a prefix of its
// parent's, transitively. Fork adds a reference edge; Delete drops a root;
// Collect reclaims what no root reaches. Every adapter gets these semantics
// from here and implements none of them.
type Ledger struct {
	seg SegmentStore
}

// NewLedger returns the Store over seg.
func NewLedger(seg SegmentStore) *Ledger { return &Ledger{seg: seg} }

func (l *Ledger) profileOf(h SessionHeader) (LedgerProfile, error) {
	return LedgerProfileFor(h.ProtocolVersion)
}

// header resolves a live Session's header; a deleted root is not found.
func (l *Ledger) header(ctx context.Context, sid SessionID, op string, asSegment bool) (SessionHeader, error) {
	h, deleted, err := l.seg.SegmentHeader(ctx, sid)
	if err != nil {
		return SessionHeader{}, err
	}
	if deleted && !asSegment {
		return SessionHeader{}, newError(ErrNotFound, op, sid, "session deleted")
	}
	return h, nil
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
		ParentFork: cloneFork(req.ParentFork), CausationID: req.CausationID, Metadata: req.Metadata}
	digest, err := profile.HeaderDigest(header)
	if err != nil {
		return SessionHeader{}, err
	}
	header.HeaderDigest = digest
	if err := profile.ValidateHeader(header); err != nil {
		return SessionHeader{}, err
	}
	if fork := header.ParentFork; fork != nil {
		// The anchor must name a commit a live parent holds (SES-FRK-1). The
		// parent is append-only, so the commit cannot disappear before the
		// child is registered.
		page, err := l.readCommits(ctx, CommitReadRequest{SessionID: fork.ParentSessionID, From: fork.Seq, Limit: 1}, false)
		if err != nil {
			if IsCode(err, ErrNotFound) {
				return SessionHeader{}, newError(ErrNotFound, "create", header.SessionID, fmt.Sprintf("parent session %s not found", fork.ParentSessionID))
			}
			return SessionHeader{}, err
		}
		if err := CheckForkAnchor(header, page); err != nil {
			return SessionHeader{}, err
		}
	}
	if existing, deleted, err := l.seg.SegmentHeader(ctx, req.SessionID); err == nil {
		if deleted {
			return SessionHeader{}, newError(ErrConflict, "create", req.SessionID, "session deleted; its segment awaits collection")
		}
		if existing.HeaderDigest == header.HeaderDigest {
			return existing, nil
		}
		return SessionHeader{}, newError(ErrConflict, "create", req.SessionID, "session exists with a different header")
	} else if !IsCode(err, ErrNotFound) {
		return SessionHeader{}, err
	}
	if err := l.seg.CreateSegment(ctx, header); err != nil {
		if IsCode(err, ErrConflict) {
			// A concurrent creator won; answer as the idempotent path would.
			if existing, deleted, herr := l.seg.SegmentHeader(ctx, req.SessionID); herr == nil && !deleted && existing.HeaderDigest == header.HeaderDigest {
				return existing, nil
			}
		}
		return SessionHeader{}, err
	}
	return header, nil
}

// CheckForkAnchor is the Store-independent half of SES-FRK-1: the parent page
// read at ParentFork.Seq (Limit 1) must hold that commit with that digest and
// share the child's protocol version.
func CheckForkAnchor(child SessionHeader, parent CommitPage) error {
	fork := child.ParentFork
	if parent.Header.ProtocolVersion != child.ProtocolVersion {
		return &Error{Code: ErrUnsupportedProfile, Operation: "create", SessionID: child.SessionID,
			Detail: fmt.Sprintf("parent %s is protocol v%d", fork.ParentSessionID, parent.Header.ProtocolVersion)}
	}
	if len(parent.Commits) == 0 || parent.Commits[0].Seq != fork.Seq {
		return newError(ErrInvalid, "create", child.SessionID, fmt.Sprintf("parent %s has no commit %d", fork.ParentSessionID, fork.Seq))
	}
	if parent.Commits[0].Digest != fork.Digest {
		return newError(ErrInvalid, "create", child.SessionID, fmt.Sprintf("parent %s commit %d digest mismatch", fork.ParentSessionID, fork.Seq))
	}
	return nil
}

func cloneFork(f *ForkPoint) *ForkPoint {
	if f == nil {
		return nil
	}
	c := *f
	return &c
}

func (l *Ledger) Header(ctx context.Context, sid SessionID) (SessionHeader, error) {
	if err := ctx.Err(); err != nil {
		return SessionHeader{}, err
	}
	return l.header(ctx, sid, "header", false)
}

// --- open -------------------------------------------------------------------------

func (l *Ledger) Open(ctx context.Context, sid SessionID, opts OpenOptions) (Handle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	header, err := l.header(ctx, sid, "open", false)
	if err != nil {
		return nil, err
	}
	profile, err := l.profileOf(header)
	if err != nil {
		return nil, err
	}
	// Corruption detection happens before ownership is established
	// (SES-REP-1); reads trust the store.
	own, _, _, err := l.seg.ReadSegment(ctx, sid, LedgerSeed(header).Next, 0)
	if err != nil {
		return nil, err
	}
	if err := ValidateLedger(profile, header, own); err != nil {
		return nil, err
	}
	var prefix map[CommitID]prefixRef
	if header.ParentFork != nil {
		// The inherited prefix is immutable, so its CommitID index is built
		// once per Open (SES-FRK-3).
		if prefix, err = l.prefixIndex(ctx, header.ParentFork); err != nil {
			return nil, err
		}
	}
	seg, err := l.seg.OpenSegment(ctx, sid, opts)
	if err != nil {
		return nil, err
	}
	return &ledgerHandle{l: l, header: header, profile: profile, seg: seg, prefix: prefix}, nil
}

// prefixRef locates an inherited commit: the segment that holds it and its
// CommitSeq there.
type prefixRef struct {
	sid SessionID
	seq CommitSeq
}

func (l *Ledger) prefixIndex(ctx context.Context, fork *ForkPoint) (map[CommitID]prefixRef, error) {
	page, err := l.readCommits(ctx, CommitReadRequest{SessionID: fork.ParentSessionID, Limit: uint32(fork.Seq) + 1}, true)
	if err != nil {
		return nil, err
	}
	// The ancestor chain with each segment's own range start, so every
	// inherited commit is attributed to the segment that stores it.
	type ancestor struct {
		sid  SessionID
		seed CommitSeq
	}
	var chain []ancestor
	for sid := fork.ParentSessionID; sid != ""; {
		h, _, err := l.seg.SegmentHeader(ctx, sid)
		if err != nil {
			return nil, err
		}
		chain = append(chain, ancestor{sid: sid, seed: LedgerSeed(h).Next})
		if h.ParentFork == nil {
			break
		}
		sid = h.ParentFork.ParentSessionID
	}
	owner := func(seq CommitSeq) SessionID {
		for _, a := range chain {
			if seq >= a.seed {
				return a.sid
			}
		}
		return chain[len(chain)-1].sid
	}
	out := make(map[CommitID]prefixRef, len(page.Commits))
	for _, c := range page.Commits {
		if c.Seq > fork.Seq {
			break
		}
		out[c.CommitID] = prefixRef{sid: owner(c.Seq), seq: c.Seq}
	}
	return out, nil
}

type ledgerHandle struct {
	l       *Ledger
	header  SessionHeader
	profile LedgerProfile
	seg     SegmentHandle
	prefix  map[CommitID]prefixRef
}

func (w *ledgerHandle) SessionID() SessionID { return w.header.SessionID }
func (w *ledgerHandle) Epoch() Epoch         { return w.seg.Epoch() }
func (w *ledgerHandle) Head() Head           { return w.seg.Head() }

// Committed is SES-REP-3; the inherited prefix counts (SES-FRK-3).
func (w *ledgerHandle) Committed(id CommitID) bool {
	if w.seg.Own(id) {
		return true
	}
	_, inherited := w.prefix[id]
	return inherited
}

// LookupCommit is SES-REP-4; an inherited commit is read from its segment.
func (w *ledgerHandle) LookupCommit(id CommitID) (Commit, bool, error) {
	if c, ok, err := w.seg.LookupOwn(id); err != nil || ok {
		return c, ok, err
	}
	ref, inherited := w.prefix[id]
	if !inherited {
		return Commit{}, false, nil
	}
	commits, _, _, err := w.l.seg.ReadSegment(context.Background(), ref.sid, ref.seq, 1)
	if err != nil {
		return Commit{}, false, err
	}
	if len(commits) != 1 || commits[0].CommitID != id {
		return Commit{}, false, &Error{Code: ErrCorrupt, Operation: "lookup", SessionID: w.header.SessionID, CommitID: id,
			Detail: fmt.Sprintf("inherited commit not at %s@%d", ref.sid, ref.seq)}
	}
	return commits[0], true, nil
}

func (w *ledgerHandle) Append(ctx context.Context, p Proposal) (Commit, error) {
	if err := ctx.Err(); err != nil {
		return Commit{}, err
	}
	sid := w.header.SessionID
	if err := validIdentity("CommitID", string(p.CommitID)); err != nil {
		return Commit{}, newError(ErrInvalid, "append", sid, err.Error())
	}
	if err := ValidateBatches(p.Batches); err != nil {
		return Commit{}, newError(ErrInvalid, "append", sid, err.Error())
	}
	if w.seg.Own(p.CommitID) {
		return Commit{}, &Error{Code: ErrConflict, Operation: "append", SessionID: sid, CommitID: p.CommitID, Detail: "CommitID already in stream"}
	}
	if _, dup := w.prefix[p.CommitID]; dup {
		return Commit{}, &Error{Code: ErrConflict, Operation: "append", SessionID: sid, CommitID: p.CommitID, Detail: "CommitID already in the inherited prefix"}
	}
	head := w.seg.Head()
	c := Commit{Seq: head.Next, CommitID: p.CommitID, Epoch: w.seg.Epoch(), Batches: cloneBatches(p.Batches)}
	if err := SealCommit(w.profile, head.Digest, sid, &c); err != nil {
		return Commit{}, err
	}
	if err := w.seg.Append(ctx, c); err != nil {
		return Commit{}, err
	}
	return cloneCommit(c), nil
}

func (w *ledgerHandle) Close(ctx context.Context) error { return w.seg.Close(ctx) }

// --- read -------------------------------------------------------------------------

func (l *Ledger) ReadCommits(ctx context.Context, req CommitReadRequest) (CommitPage, error) {
	if err := ctx.Err(); err != nil {
		return CommitPage{}, err
	}
	return l.readCommits(ctx, req, false)
}

// readCommits stitches the inherited prefix (read through the parent, which
// stitches its own) in front of the Session's own commits (SES-FRK-2).
// asSegment also serves a deleted root, which is how a fork reaches its
// prefix after the parent was deleted.
func (l *Ledger) readCommits(ctx context.Context, req CommitReadRequest, asSegment bool) (CommitPage, error) {
	header, err := l.header(ctx, req.SessionID, "read", asSegment)
	if err != nil {
		return CommitPage{}, err
	}
	seed := LedgerSeed(header)
	page := CommitPage{Header: header}
	if fork := header.ParentFork; fork != nil && req.From <= fork.Seq {
		prefix, err := l.readCommits(ctx, CommitReadRequest{SessionID: fork.ParentSessionID, From: req.From, Limit: req.Limit}, true)
		if err != nil {
			return CommitPage{}, err
		}
		for _, c := range prefix.Commits {
			if c.Seq > fork.Seq {
				break
			}
			page.Commits = append(page.Commits, c)
		}
		if req.Limit > 0 && uint32(len(page.Commits)) >= req.Limit {
			_, head, _, err := l.seg.ReadSegment(ctx, req.SessionID, seed.Next, 1)
			if err != nil {
				return CommitPage{}, err
			}
			page.Head = head
			page.HasMore = page.Commits[len(page.Commits)-1].Seq < fork.Seq || head.Next > seed.Next
			return page, nil
		}
	}
	from := req.From
	if from < seed.Next {
		from = seed.Next
	}
	var limit uint32
	if req.Limit > 0 {
		limit = req.Limit - uint32(len(page.Commits))
	}
	own, head, more, err := l.seg.ReadSegment(ctx, req.SessionID, from, limit)
	if err != nil {
		return CommitPage{}, err
	}
	page.Head = head
	page.Commits = append(page.Commits, own...)
	page.HasMore = more
	return page, nil
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
	all, err := l.readCommits(ctx, CommitReadRequest{SessionID: req.SessionID}, false)
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
	if _, err := l.header(ctx, sid, "delete", false); err != nil {
		return err
	}
	return l.seg.MarkDeleted(ctx, sid)
}

func (l *Ledger) Collect(ctx context.Context) (CollectReport, error) {
	if err := ctx.Err(); err != nil {
		return CollectReport{}, err
	}
	ids, err := l.seg.ListSegments(ctx)
	if err != nil {
		return CollectReport{}, err
	}
	headers := make(map[SessionID]SessionHeader, len(ids))
	live := make(map[SessionID]bool, len(ids))
	for _, sid := range ids {
		h, deleted, err := l.seg.SegmentHeader(ctx, sid)
		if err != nil {
			if IsCode(err, ErrNotFound) {
				continue
			}
			return CollectReport{}, err
		}
		headers[sid], live[sid] = h, !deleted
	}
	need := Reachable(headers, live)
	report := CollectReport{Truncated: map[SessionID]CommitSeq{}}
	for sid := range headers {
		if live[sid] {
			continue
		}
		through, reached := need[sid]
		if !reached {
			if err := l.seg.RemoveSegment(ctx, sid); err != nil {
				return report, err
			}
			report.Removed = append(report.Removed, sid)
			continue
		}
		_, head, _, err := l.seg.ReadSegment(ctx, sid, through+1, 1)
		if err != nil {
			return report, err
		}
		if head.Next <= through+1 {
			continue
		}
		newHead, err := l.seg.TruncateSegment(ctx, sid, through)
		if err != nil {
			return report, err
		}
		report.Truncated[sid] = newHead.Next
	}
	return report, nil
}

// Reachable computes, from the headers of every segment and which of them
// are live roots, the last CommitSeq each segment must keep (SES-GC-2): a
// live root keeps everything; a segment reached only through forks keeps up
// to the largest Seq any reaching fork anchors at. Segments absent from the
// result are unreachable. Anchors are followed transitively, so a deleted
// segment referenced only by other deleted segments is unreachable.
func Reachable(headers map[SessionID]SessionHeader, live map[SessionID]bool) map[SessionID]CommitSeq {
	const all = ^CommitSeq(0)
	need := make(map[SessionID]CommitSeq, len(headers))
	for sid, h := range headers {
		if !live[sid] {
			continue
		}
		need[sid] = all
		fork := h.ParentFork
		for fork != nil {
			if cur, ok := need[fork.ParentSessionID]; !ok || fork.Seq > cur {
				need[fork.ParentSessionID] = fork.Seq
			}
			parent, ok := headers[fork.ParentSessionID]
			if !ok {
				break
			}
			fork = parent.ParentFork
		}
	}
	return need
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
