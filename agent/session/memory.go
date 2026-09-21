package session

import (
	"context"
	"fmt"
	"sync"
)

// MemoryStore is the in-process reference Store: the Ledger over an
// in-memory Backend. Ownership lasts until Close; an Open with Takeover
// supersedes a live owner, which is then fenced by its stale Epoch.
type MemoryStore struct {
	*Ledger
	be *memoryBackend
}

// NewMemoryStore returns an empty MemoryStore.
// Durable reports false: the store lives as long as the process.
func (*MemoryStore) Durable() bool { return false }

func NewMemoryStore(opts ...LedgerOption) *MemoryStore {
	be := &memoryBackend{segments: make(map[SegmentID]*memorySegment), roots: make(map[SessionID]*memoryRoot)}
	return &MemoryStore{Ledger: NewLedger(be, opts...), be: be}
}

// DropVerifiedMark forgets the tip segment's verified mark, so conformance
// can prove that Open then verifies from the seed (SES-REP-1); production
// code never calls it.
func (m *MemoryStore) DropVerifiedMark(sid SessionID) error {
	s := m.be.tipOf(sid)
	if s == nil {
		return newError(ErrNotFound, "drop_verified_mark", sid, "session not found")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.verified, s.hasVerified = Head{}, false
	return nil
}

// CutIndex keeps only the first keep entries of the tip segment's
// CommitIndex while its commits stay, so conformance can prove that Open
// detects a lagging or absent index and rebuilds it (SES-REP-5); production
// code never calls it. A negative keep drops the index entirely.
func (m *MemoryStore) CutIndex(sid SessionID, keep int) error {
	s := m.be.tipOf(sid)
	if s == nil {
		return newError(ErrNotFound, "cut_index", sid, "session not found")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if keep < 0 {
		s.index = CommitIndex{}
		return nil
	}
	if keep > len(s.index.Entries) {
		keep = len(s.index.Entries)
	}
	s.index.Entries = s.index.Entries[:keep]
	if keep == 0 {
		s.index.Through = LedgerSeed(s.header)
	} else {
		last := &s.index.Entries[keep-1]
		s.index.Through = Head{Next: last.Seq + 1, Digest: last.Digest}
	}
	return nil
}

// Tamper mutates one own commit of the Session's segment in place. It exists
// so conformance can prove that the ledger check at Open detects corruption;
// production code never calls it.
func (m *MemoryStore) Tamper(sid SessionID, seq CommitSeq, mutate func(*Commit)) {
	s := m.be.tipOf(sid)
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	seed := LedgerSeed(s.header)
	if seq >= seed.Next && seq-seed.Next < CommitSeq(len(s.commits)) {
		mutate(&s.commits[seq-seed.Next])
	}
}

// CrashTail drops every stored commit after the first keep, simulating a
// crash whose last commit never became durable. It exists so conformance can
// prove that Open recovers to the last whole commit (SES-APP-2); production
// code never calls it.
func (m *MemoryStore) CrashTail(sid SessionID, keep int) error {
	s := m.be.tipOf(sid)
	if s == nil {
		return newError(ErrNotFound, "crash_tail", sid, "session not found")
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

// --- backend -------------------------------------------------------------------

// memoryBackend is the in-memory Backend: segments (nodes) and roots, with
// writer ownership per root. It implements no fork or reachability
// semantics; the Ledger does.
type memoryBackend struct {
	mu       sync.RWMutex // guards both maps
	segments map[SegmentID]*memorySegment
	roots    map[SessionID]*memoryRoot
}

type memorySegment struct {
	mu      sync.Mutex
	header  SegmentHeader
	commits []Commit // own commits only, from LedgerSeed(header).Next
	// index is the segment's CommitIndex (SES-REP-5), extended with each
	// Append and cut with each Truncate; byCommit is its lookup map.
	index    CommitIndex
	byCommit map[CommitID]int // index into commits
	// verified is the head through which the segment was last verified
	// (SES-REP-1); hasVerified reports whether one is recorded.
	verified    Head
	hasVerified bool
}

type memoryRoot struct {
	record SessionRecord
	epoch  Epoch
	owned  bool
	owner  string
	until  int64 // lease expiry in unix millis; zero never expires
}

// live reports whether the root's lease is held and unexpired at now.
func (r *memoryRoot) live(now int64) bool {
	return r.owned && (r.until == 0 || r.until > now)
}

func (m *memoryBackend) segment(id SegmentID) (*memorySegment, error) {
	m.mu.RLock()
	s, ok := m.segments[id]
	m.mu.RUnlock()
	if !ok {
		return nil, &Error{Code: ErrNotFound, Operation: "segment", Detail: fmt.Sprintf("segment %s not found", id)}
	}
	return s, nil
}

// tipOf is the segment a live Session appends to, for the test hooks.
func (m *memoryBackend) tipOf(sid SessionID) *memorySegment {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.roots[sid]
	if !ok {
		return nil
	}
	return m.segments[r.record.Tip]
}

func (s *memorySegment) head() Head {
	if len(s.commits) == 0 {
		return LedgerSeed(s.header)
	}
	last := &s.commits[len(s.commits)-1]
	return Head{Next: last.Seq + 1, Digest: last.Digest}
}

func (s *memorySegment) rebuildIndex() {
	s.index = BuildCommitIndex(s.header, s.commits)
	s.byCommit = make(map[CommitID]int, len(s.commits))
	for i := range s.commits {
		s.byCommit[s.commits[i].CommitID] = i
	}
}

// --- LedgerStore -----------------------------------------------------------------

func (m *memoryBackend) Segment(ctx context.Context, id SegmentID) (Segment, error) {
	if err := ctx.Err(); err != nil {
		return Segment{}, err
	}
	s, err := m.segment(id)
	if err != nil {
		return Segment{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return Segment{ID: id, Header: s.header}, nil
}

func (m *memoryBackend) ListSegments(ctx context.Context) ([]SegmentID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]SegmentID, 0, len(m.segments))
	for id := range m.segments {
		out = append(out, id)
	}
	return out, nil
}

func (m *memoryBackend) ReadSegment(ctx context.Context, id SegmentID, from CommitSeq, limit uint32) ([]Commit, Head, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, Head{}, false, err
	}
	s, err := m.segment(id)
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
		start = IndexWithin(from-seed.Next, len(s.commits))
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

func (m *memoryBackend) Locate(ctx context.Context, id SegmentID, cid CommitID) (CommitSeq, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	s, err := m.segment(id)
	if err != nil {
		return 0, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	i, ok := s.byCommit[cid]
	if !ok {
		return 0, false, nil
	}
	return s.commits[i].Seq, true, nil
}

func (m *memoryBackend) Index(ctx context.Context, id SegmentID) (CommitIndex, Head, error) {
	if err := ctx.Err(); err != nil {
		return CommitIndex{}, Head{}, err
	}
	s, err := m.segment(id)
	if err != nil {
		return CommitIndex{}, Head{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.index.Clone(), s.head(), nil
}

func (m *memoryBackend) VerifiedMark(ctx context.Context, id SegmentID) (Head, bool, error) {
	if err := ctx.Err(); err != nil {
		return Head{}, false, err
	}
	s, err := m.segment(id)
	if err != nil {
		return Head{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.verified, s.hasVerified, nil
}

func (m *memoryBackend) PutVerifiedMark(ctx context.Context, id SegmentID, mark Head) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s, err := m.segment(id)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.verified, s.hasVerified = mark, true
	return nil
}

func (m *memoryBackend) PutIndex(ctx context.Context, id SegmentID, idx CommitIndex) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s, err := m.segment(id)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.index = idx.Clone()
	return nil
}

func (m *memoryBackend) LookupCommit(ctx context.Context, id SegmentID, cid CommitID) (Commit, bool, error) {
	if err := ctx.Err(); err != nil {
		return Commit{}, false, err
	}
	s, err := m.segment(id)
	if err != nil {
		return Commit{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	i, ok := s.byCommit[cid]
	if !ok {
		return Commit{}, false, nil
	}
	return cloneCommit(s.commits[i]), true, nil
}

// Append persists a sealed commit under the lease (SES-OWN-2): the lease
// check and the write happen under the backend lock, so a takeover cannot
// slip between them.
func (m *memoryBackend) Append(ctx context.Context, lease Lease, id SegmentID, c Commit) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.roots[lease.Session]
	if !ok || !r.owned || r.epoch != lease.Epoch {
		var current Epoch
		if ok {
			current = r.epoch
		}
		return newError(ErrOwnershipLost, "append", lease.Session, fmt.Sprintf("epoch %d superseded by %d", lease.Epoch, current))
	}
	if r.record.Tip != id {
		return newError(ErrInvalid, "append", lease.Session, "lease does not cover the segment")
	}
	s, ok := m.segments[id]
	if !ok {
		return &Error{Code: ErrNotFound, Operation: "append", SessionID: lease.Session, Detail: fmt.Sprintf("segment %s not found", id)}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if head := s.head(); c.Seq != head.Next || c.PrevDigest != head.Digest {
		return newError(ErrInvalid, "append", lease.Session, "commit is not sealed against the segment head")
	}
	s.commits = append(s.commits, c)
	s.byCommit[c.CommitID] = len(s.commits) - 1
	s.index.Extend(&c)
	return nil
}

func (m *memoryBackend) TruncateSegment(ctx context.Context, id SegmentID, through CommitSeq) (Head, error) {
	if err := ctx.Err(); err != nil {
		return Head{}, err
	}
	s, err := m.segment(id)
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

func (m *memoryBackend) RemoveSegment(ctx context.Context, id SegmentID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.segments, id)
	return nil
}

// --- Backend ---------------------------------------------------------------------

// CreateSession lands the node and the root under one lock (SES-FRK-1).
func (m *memoryBackend) CreateSession(ctx context.Context, seg Segment, rec SessionRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.roots[rec.ID]; exists {
		return newError(ErrConflict, "create", rec.ID, "session exists")
	}
	if _, exists := m.segments[seg.ID]; exists {
		return newError(ErrConflict, "create", rec.ID, fmt.Sprintf("segment %s exists", seg.ID))
	}
	m.segments[seg.ID] = &memorySegment{header: seg.Header, byCommit: make(map[CommitID]int), index: CommitIndex{Through: LedgerSeed(seg.Header)}}
	m.roots[rec.ID] = &memoryRoot{record: rec}
	return nil
}

// AdvanceTip lands the new empty node and moves the root under one lock
// (SES-ADV-1), fenced by the lease.
func (m *memoryBackend) AdvanceTip(ctx context.Context, lease Lease, seg Segment, from SegmentID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.roots[lease.Session]
	if !ok || !r.owned || r.epoch != lease.Epoch {
		var current Epoch
		if ok {
			current = r.epoch
		}
		return newError(ErrOwnershipLost, "advance", lease.Session, fmt.Sprintf("epoch %d superseded by %d", lease.Epoch, current))
	}
	if r.record.Tip != from {
		return newError(ErrConflict, "advance", lease.Session, fmt.Sprintf("tip is %s, not %s", r.record.Tip, from))
	}
	if _, exists := m.segments[seg.ID]; exists {
		return newError(ErrConflict, "advance", lease.Session, fmt.Sprintf("segment %s exists", seg.ID))
	}
	m.segments[seg.ID] = &memorySegment{header: seg.Header, byCommit: make(map[CommitID]int), index: CommitIndex{Through: LedgerSeed(seg.Header)}}
	r.record.Tip = seg.ID
	return nil
}

// --- SessionStore ----------------------------------------------------------------

func (m *memoryBackend) Record(ctx context.Context, sid SessionID) (SessionRecord, error) {
	if err := ctx.Err(); err != nil {
		return SessionRecord{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.roots[sid]
	if !ok {
		return SessionRecord{}, newError(ErrNotFound, "record", sid, "session not found")
	}
	return r.record, nil
}

func (m *memoryBackend) ListRecords(ctx context.Context) ([]SessionRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]SessionRecord, 0, len(m.roots))
	for _, r := range m.roots {
		out = append(out, r.record)
	}
	return out, nil
}

func (m *memoryBackend) Acquire(ctx context.Context, sid SessionID, opts OpenOptions) (Lease, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.roots[sid]
	if !ok {
		return Lease{}, newError(ErrNotFound, "open", sid, "session not found")
	}
	now := opts.Now()
	if r.live(now.UnixMilli()) && !opts.Takeover {
		return Lease{}, newError(ErrOwned, "open", sid, fmt.Sprintf("owned by epoch %d (%s) until %d", r.epoch, r.owner, r.until))
	}
	r.epoch++
	r.owned = true
	r.owner = opts.Owner
	r.until = opts.LeaseUntil(now)
	return Lease{Session: sid, Epoch: r.epoch, Owner: r.owner, UntilUnixMilli: r.until}, nil
}

func (m *memoryBackend) Renew(ctx context.Context, lease Lease, until int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.roots[lease.Session]
	if !ok || !r.owned || r.epoch != lease.Epoch {
		var current Epoch
		if ok {
			current = r.epoch
		}
		return newError(ErrOwnershipLost, "renew", lease.Session, fmt.Sprintf("epoch %d superseded by %d", lease.Epoch, current))
	}
	r.until = until
	return nil
}

func (m *memoryBackend) Release(ctx context.Context, lease Lease) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.roots[lease.Session]; ok && r.owned && r.epoch == lease.Epoch {
		r.owned = false
	}
	return nil
}

func (m *memoryBackend) DeleteRecord(ctx context.Context, sid SessionID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.roots[sid]
	if !ok {
		return newError(ErrNotFound, "delete", sid, "session not found")
	}
	if r.owned {
		return newError(ErrOwned, "delete", sid, fmt.Sprintf("owned by epoch %d", r.epoch))
	}
	delete(m.roots, sid)
	return nil
}

var _ Store = (*MemoryStore)(nil)
var _ Backend = (*memoryBackend)(nil)
