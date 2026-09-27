// Package session is the commit-ledger kernel of a Twilight Session
// (docs/design/agent-session.md). It owns the header, the Commit as the
// atomic unit of append, logical streams within commits, Session-level
// writer ownership with epoch fencing, and ordered reads over an append-only
// store. Payloads are opaque canonical JSON that Session modules encode and
// interpret.
package session

import "context"

type (
	SessionID string
	CommitID  string
	EventType string
	// Epoch is the writer ownership generation of a stream, from 1.
	Epoch uint64
)

// Session is one live root and its loaded path. Reads and fork-point
// resolution go through it. Appending is a Handle, which keeps the lease and
// the tip head on top of a Session. The value is tied to the Backend it was
// loaded from. A stored path is loaded by reading each span's segment; a root
// written before paths existed is assembled by walking parent edges.
type Session struct {
	root SessionRecord
	path *LoadedPath
	be   Backend
}

// ID is the Session's root identity.
func (s *Session) ID() SessionID { return s.root.ID }

// Record is the root: the tip segment, the stored path and the creation time.
func (s *Session) Record() SessionRecord { return s.root }

// Header is the tip segment's creation record.
func (s *Session) Header() SegmentHeader { return s.path.Header() }

// Tip is the segment the Session appends to.
func (s *Session) Tip() Segment { return s.path.Tip() }

// Loaded is the path from the root segment to the tip, with each segment read.
func (s *Session) Loaded() *LoadedPath { return s.path }

// Load reads a live Session's root and the loaded path of its tip.
func (l *Ledger) Load(ctx context.Context, sid SessionID) (*Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root, err := l.be.Record(ctx, sid)
	if err != nil {
		return nil, err
	}
	path, err := l.materialize(ctx, root)
	if err != nil {
		return nil, err
	}
	return &Session{root: root, path: path, be: l.be}, nil
}

// materialize turns the root's stored path into a loaded path. An empty path
// is a root written before paths were stored; its spans come from parent edges.
func (l *Ledger) materialize(ctx context.Context, root SessionRecord) (*LoadedPath, error) {
	if len(root.Path) == 0 {
		return loadPathFromEdges(ctx, l.be, root.Tip)
	}
	if err := root.Path.Validate(root.Tip); err != nil {
		return nil, newError(ErrCorrupt, "load", root.ID, err.Error())
	}
	a := &LoadedPath{Segments: make([]LoadedSpan, len(root.Path))}
	for i, span := range root.Path {
		seg, err := l.be.Segment(ctx, span.Segment)
		if err != nil {
			return nil, err
		}
		a.Segments[i] = LoadedSpan{Segment: seg, From: span.From, End: span.End}
	}
	return a, nil
}

// sessionPath is the root's stored path, or the path assembled from parent
// edges when the root has none.
func (l *Ledger) sessionPath(ctx context.Context, root SessionRecord) (Path, error) {
	if len(root.Path) == 0 {
		anc, err := loadPathFromEdges(ctx, l.be, root.Tip)
		if err != nil {
			return nil, err
		}
		return pathFromLoaded(anc), nil
	}
	if err := root.Path.Validate(root.Tip); err != nil {
		return nil, newError(ErrCorrupt, "load", root.ID, err.Error())
	}
	return root.Path, nil
}

// tipSegment reads the tip's creation record without walking its parents.
// Header and Create's idempotency check need nothing else.
func (l *Ledger) tipSegment(ctx context.Context, sid SessionID) (Segment, error) {
	root, err := l.be.Record(ctx, sid)
	if err != nil {
		return Segment{}, err
	}
	return l.be.Segment(ctx, root.Tip)
}

// ReadCommits returns the stitched commits of the Session from from,
// inclusive, at most limit (0 = unlimited).
func (s *Session) ReadCommits(ctx context.Context, from CommitSeq, limit uint32) (CommitPage, error) {
	commits, head, more, err := s.path.Read(ctx, s.be, from, limit)
	if err != nil {
		return CommitPage{}, err
	}
	return CommitPage{Header: s.Header(), Commits: commits, Head: head, HasMore: more}, nil
}

// ReadStream returns the events of one logical stream in the order the
// requested lineage sees them.
func (s *Session) ReadStream(ctx context.Context, stream StreamRef, lineage StreamLineage, from StreamSeq, limit uint32) (StreamPage, error) {
	if err := validateStreamRead(s.ID(), stream, lineage); err != nil {
		return StreamPage{}, err
	}
	return s.collectStream(ctx, stream, lineage, from, limit)
}

func (s *Session) collectStream(ctx context.Context, stream StreamRef, lineage StreamLineage, from StreamSeq, limit uint32) (StreamPage, error) {
	commits, err := s.path.ReadStream(ctx, s.be, stream, lineage)
	if err != nil {
		return StreamPage{}, err
	}
	head, err := s.path.tipHead(ctx, s.be)
	if err != nil {
		return StreamPage{}, err
	}
	page := StreamPage{Header: s.Header(), Stream: stream, Head: head}
	page.Events, page.HasMore = StreamEvents(commits, stream, from, limit)
	return page, nil
}

func validateStreamRead(sid SessionID, stream StreamRef, lineage StreamLineage) error {
	if err := ValidateStreamRef(stream); err != nil {
		return newError(ErrInvalid, "read_stream", sid, err.Error())
	}
	if err := ValidateStreamLineage(lineage); err != nil {
		return newError(ErrInvalid, "read_stream", sid, err.Error())
	}
	return nil
}

// EdgeAt resolves a stitched CommitSeq to the parent edge a fork records:
// the segment that contributes that commit, and the seq within it. ok is
// false when the Session has no such commit. The segment must still hold it
// (SES-FRK-1).
func (s *Session) EdgeAt(ctx context.Context, seq CommitSeq) (CommitRef, bool, error) {
	span, ok := s.path.At(seq)
	if !ok {
		return CommitRef{}, false, nil
	}
	commits, _, _, err := s.be.ReadSegment(ctx, span.Segment.ID(), seq, 1)
	if err != nil {
		return CommitRef{}, false, err
	}
	if len(commits) != 1 || commits[0].Seq != seq {
		return CommitRef{}, false, nil
	}
	return CommitRef{Segment: span.Segment.ID(), Seq: seq}, true, nil
}

// repairTip checks the tip segment's CommitIndex against its head and
// rebuilds the index when it lags (SES-REP-5). Acquire has already repaired
// a torn tail, so the head this returns is the head a new Handle starts from.
func (s *Session) repairTip(ctx context.Context) (Head, error) {
	tip := s.Tip()
	summary, head, err := s.be.Summarize(ctx, tip.ID())
	if err != nil {
		return Head{}, err
	}
	if summary.Valid(tip.Seed(), head) {
		return head, nil
	}
	commits, head, _, err := s.be.ReadSegment(ctx, tip.ID(), tip.Seed().Next, 0)
	if err != nil {
		return Head{}, err
	}
	if err := s.be.PutIndex(ctx, tip.ID(), BuildCommitIndex(tip.Header, commits)); err != nil {
		return Head{}, err
	}
	return head, nil
}
