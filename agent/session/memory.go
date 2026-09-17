package session

import (
	"context"
	"fmt"
	"sync"
)

// MemoryStore is the in-process reference Store: the Ledger over an
// in-memory SegmentStore. Ownership lasts until Close; an Open with Takeover
// supersedes a live owner, which is then fenced by its stale Epoch.
type MemoryStore struct {
	*Ledger
	seg *memorySegments
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	seg := &memorySegments{segments: make(map[SessionID]*memorySegment)}
	return &MemoryStore{Ledger: NewLedger(seg), seg: seg}
}

// Tamper mutates one stored commit in place. It exists so conformance can
// prove that the ledger check at Open detects corruption; production code
// never calls it. seq names one of the Session's own commits.
func (m *MemoryStore) Tamper(sid SessionID, seq CommitSeq, mutate func(*Commit)) {
	s, err := m.seg.segment(sid, "tamper")
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	seed := LedgerSeed(s.header)
	if seq >= seed.Next && int(seq-seed.Next) < len(s.commits) {
		mutate(&s.commits[seq-seed.Next])
	}
}

// CrashTail drops every stored commit after the first keep, simulating a
// crash whose last commit never became durable. It exists so conformance can
// prove that Open recovers to the last whole commit (SES-APP-2); production
// code never calls it.
func (m *MemoryStore) CrashTail(sid SessionID, keep int) error {
	s, err := m.seg.segment(sid, "crash_tail")
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if keep < 0 {
		keep = 0
	}
	if keep > len(s.commits) {
		keep = len(s.commits)
	}
	s.commits = s.commits[:keep]
	s.rebuildIndex()
	return nil
}

// --- segment store -----------------------------------------------------------------

// memorySegments is the in-memory SegmentStore: independent append-only
// segments with per-segment ownership. It implements no fork or reachability
// semantics; the Ledger does.
type memorySegments struct {
	mu       sync.RWMutex // guards the map
	segments map[SessionID]*memorySegment
}

type memorySegment struct {
	mu       sync.Mutex
	header   SessionHeader
	commits  []Commit         // own commits only, from LedgerSeed(header).Next
	byCommit map[CommitID]int // index into commits
	epoch    Epoch
	owner    *memoryHandle // nil when no live owner
	deleted  bool
}

func (m *memorySegments) segment(sid SessionID, op string) (*memorySegment, error) {
	m.mu.RLock()
	s, ok := m.segments[sid]
	m.mu.RUnlock()
	if !ok {
		return nil, newError(ErrNotFound, op, sid, "session not found")
	}
	return s, nil
}

func (s *memorySegment) head() Head {
	if len(s.commits) == 0 {
		return LedgerSeed(s.header)
	}
	last := &s.commits[len(s.commits)-1]
	return Head{Next: last.Seq + 1, Digest: last.Digest}
}

func (s *memorySegment) rebuildIndex() {
	s.byCommit = make(map[CommitID]int, len(s.commits))
	for i := range s.commits {
		s.byCommit[s.commits[i].CommitID] = i
	}
}

func (m *memorySegments) CreateSegment(ctx context.Context, header SessionHeader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.segments[header.SessionID]; exists {
		return newError(ErrConflict, "create", header.SessionID, "segment exists")
	}
	m.segments[header.SessionID] = &memorySegment{header: header, byCommit: make(map[CommitID]int)}
	return nil
}

func (m *memorySegments) SegmentHeader(ctx context.Context, sid SessionID) (SessionHeader, bool, error) {
	if err := ctx.Err(); err != nil {
		return SessionHeader{}, false, err
	}
	s, err := m.segment(sid, "header")
	if err != nil {
		return SessionHeader{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.header, s.deleted, nil
}

func (m *memorySegments) ListSegments(ctx context.Context) ([]SessionID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]SessionID, 0, len(m.segments))
	for sid := range m.segments {
		out = append(out, sid)
	}
	return out, nil
}

func (m *memorySegments) ReadSegment(ctx context.Context, sid SessionID, from CommitSeq, limit uint32) ([]Commit, Head, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, Head{}, false, err
	}
	s, err := m.segment(sid, "read")
	if err != nil {
		return nil, Head{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	head := s.head()
	seed := LedgerSeed(s.header)
	if from >= head.Next { // compared as CommitSeq: int(from) wraps above MaxInt
		return nil, head, false, nil
	}
	start := 0
	if from > seed.Next {
		start = int(from - seed.Next)
	}
	end := len(s.commits)
	more := false
	if limit > 0 && start+int(limit) < end {
		end = start + int(limit)
		more = true
	}
	out := make([]Commit, 0, end-start)
	for i := start; i < end; i++ {
		out = append(out, cloneCommit(s.commits[i]))
	}
	return out, head, more, nil
}

type memoryHandle struct {
	s     *memorySegment
	epoch Epoch
}

func (m *memorySegments) OpenSegment(ctx context.Context, sid SessionID, opts OpenOptions) (SegmentHandle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s, err := m.segment(sid, "open")
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owner != nil && !opts.Takeover {
		return nil, newError(ErrOwned, "open", sid, fmt.Sprintf("owned by epoch %d", s.epoch))
	}
	s.epoch++
	w := &memoryHandle{s: s, epoch: s.epoch}
	s.owner = w
	return w, nil
}

func (w *memoryHandle) Epoch() Epoch { return w.epoch }

func (w *memoryHandle) Head() Head {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	return w.s.head()
}

func (w *memoryHandle) Own(id CommitID) bool {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	_, ok := w.s.byCommit[id]
	return ok
}

func (w *memoryHandle) LookupOwn(id CommitID) (Commit, bool, error) {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	i, ok := w.s.byCommit[id]
	if !ok {
		return Commit{}, false, nil
	}
	return cloneCommit(w.s.commits[i]), true, nil
}

// Append persists a sealed commit under the handle's Epoch (SES-OWN-2).
func (w *memoryHandle) Append(ctx context.Context, c Commit) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	sid := w.s.header.SessionID
	if w.s.owner != w || w.s.epoch != w.epoch {
		return newError(ErrOwnershipLost, "append", sid, fmt.Sprintf("epoch %d superseded by %d", w.epoch, w.s.epoch))
	}
	if head := w.s.head(); c.Seq != head.Next || c.PrevDigest != head.Digest {
		return newError(ErrInvalid, "append", sid, "commit is not sealed against the segment head")
	}
	w.s.commits = append(w.s.commits, c)
	w.s.byCommit[c.CommitID] = len(w.s.commits) - 1
	return nil
}

func (w *memoryHandle) Close(ctx context.Context) error {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	if w.s.owner == w {
		w.s.owner = nil
	}
	return nil
}

func (m *memorySegments) MarkDeleted(ctx context.Context, sid SessionID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s, err := m.segment(sid, "delete")
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owner != nil {
		return newError(ErrOwned, "delete", sid, fmt.Sprintf("owned by epoch %d", s.epoch))
	}
	s.deleted = true
	return nil
}

func (m *memorySegments) TruncateSegment(ctx context.Context, sid SessionID, through CommitSeq) (Head, error) {
	if err := ctx.Err(); err != nil {
		return Head{}, err
	}
	s, err := m.segment(sid, "collect")
	if err != nil {
		return Head{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	keep := 0
	for keep < len(s.commits) && s.commits[keep].Seq <= through {
		keep++
	}
	s.commits = s.commits[:keep]
	s.rebuildIndex()
	return s.head(), nil
}

func (m *memorySegments) RemoveSegment(ctx context.Context, sid SessionID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.segments, sid)
	return nil
}

var _ Store = (*MemoryStore)(nil)
var _ SegmentStore = (*memorySegments)(nil)
