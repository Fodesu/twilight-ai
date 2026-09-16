package session

import (
	"context"
	"fmt"
	"sync"
)

// MemoryStore is the in-process reference Store. Ownership lasts until Close;
// an Open with Takeover supersedes a live owner, which is then fenced by its
// stale Epoch.
type MemoryStore struct {
	profile  LedgerProfile
	mu       sync.RWMutex // guards sessions map
	sessions map[SessionID]*memorySession
}

type memorySession struct {
	mu       sync.Mutex
	header   SessionHeader
	commits  []Commit
	byCommit map[CommitID]int // index into commits
	epoch    Epoch
	owner    *memoryHandle // nil when no live owner
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{profile: ProfileV1(), sessions: make(map[SessionID]*memorySession)}
}

func (m *MemoryStore) session(sid SessionID, op string) (*memorySession, error) {
	m.mu.RLock()
	s, ok := m.sessions[sid]
	m.mu.RUnlock()
	if !ok {
		return nil, newError(ErrNotFound, op, sid, "session not found")
	}
	return s, nil
}

func (m *MemoryStore) Create(ctx context.Context, req CreateRequest) (SessionHeader, error) {
	if err := ctx.Err(); err != nil {
		return SessionHeader{}, err
	}
	if req.ProtocolVersion != m.profile.Version() {
		return SessionHeader{}, &Error{Code: ErrUnsupportedProfile, Operation: "create", SessionID: req.SessionID}
	}
	header := SessionHeader{ProtocolVersion: req.ProtocolVersion, SessionID: req.SessionID, CreatedAtUnixMilli: req.CreatedAtUnixMilli, CausationID: req.CausationID, Metadata: req.Metadata}
	digest, err := m.profile.HeaderDigest(header)
	if err != nil {
		return SessionHeader{}, err
	}
	header.HeaderDigest = digest
	if err := m.profile.ValidateHeader(header); err != nil {
		return SessionHeader{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.sessions[req.SessionID]; ok {
		if existing.header.HeaderDigest == header.HeaderDigest {
			return existing.header, nil
		}
		return SessionHeader{}, newError(ErrConflict, "create", req.SessionID, "session exists with a different header")
	}
	m.sessions[req.SessionID] = &memorySession{header: header, byCommit: make(map[CommitID]int)}
	return header, nil
}

func (m *MemoryStore) Header(ctx context.Context, sid SessionID) (SessionHeader, error) {
	if err := ctx.Err(); err != nil {
		return SessionHeader{}, err
	}
	s, err := m.session(sid, "header")
	if err != nil {
		return SessionHeader{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.header, nil
}

func (s *memorySession) head() Head {
	if len(s.commits) == 0 {
		return Head{Next: 0, Digest: s.header.HeaderDigest}
	}
	last := &s.commits[len(s.commits)-1]
	return Head{Next: last.Seq + 1, Digest: last.Digest}
}

// --- ownership ------------------------------------------------------------------

type memoryHandle struct {
	store *MemoryStore
	s     *memorySession
	epoch Epoch
}

func (m *MemoryStore) Open(ctx context.Context, sid SessionID, opts OpenOptions) (Handle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s, err := m.session(sid, "open")
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owner != nil && !opts.Takeover {
		return nil, newError(ErrOwned, "open", sid, fmt.Sprintf("owned by epoch %d", s.epoch))
	}
	// Corruption detection happens here, before ownership is established
	// (SES-REP-1); reads trust the store.
	if err := ValidateLedger(m.profile, s.header, s.commits); err != nil {
		return nil, err
	}
	s.epoch++
	w := &memoryHandle{store: m, s: s, epoch: s.epoch}
	s.owner = w
	return w, nil
}

func (w *memoryHandle) SessionID() SessionID { return w.s.header.SessionID }
func (w *memoryHandle) Epoch() Epoch         { return w.epoch }

func (w *memoryHandle) Head() Head {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	return w.s.head()
}

// current reports whether w still owns the stream; the caller holds s.mu.
func (w *memoryHandle) current(op string) error {
	if w.s.owner != w || w.s.epoch != w.epoch {
		return newError(ErrOwnershipLost, op, w.s.header.SessionID, fmt.Sprintf("epoch %d superseded by %d", w.epoch, w.s.epoch))
	}
	return nil
}

// Committed is SES-REP-3: the CommitID index Append already keeps answers
// membership directly.
func (w *memoryHandle) Committed(id CommitID) bool {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	_, ok := w.s.byCommit[id]
	return ok
}

// LookupCommit is SES-REP-4: the session holds every commit, so a hit copies
// it instead of reading storage.
func (w *memoryHandle) LookupCommit(id CommitID) (Commit, bool, error) {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	i, ok := w.s.byCommit[id]
	if !ok {
		return Commit{}, false, nil
	}
	return cloneCommit(w.s.commits[i]), true, nil
}

func (w *memoryHandle) Close(ctx context.Context) error {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	if w.s.owner == w {
		w.s.owner = nil
	}
	return nil
}

// --- append -----------------------------------------------------------------------

func (w *memoryHandle) Append(ctx context.Context, p Proposal) (Commit, error) {
	if err := ctx.Err(); err != nil {
		return Commit{}, err
	}
	sid := w.s.header.SessionID
	if err := validIdentity("CommitID", string(p.CommitID)); err != nil {
		return Commit{}, newError(ErrInvalid, "append", sid, err.Error())
	}
	if err := ValidateBatches(p.Batches); err != nil {
		return Commit{}, newError(ErrInvalid, "append", sid, err.Error())
	}
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	if err := w.current("append"); err != nil {
		return Commit{}, err
	}
	if _, dup := w.s.byCommit[p.CommitID]; dup {
		return Commit{}, &Error{Code: ErrConflict, Operation: "append", SessionID: sid, CommitID: p.CommitID, Detail: "CommitID already in stream"}
	}
	head := w.s.head()
	c := Commit{Seq: head.Next, CommitID: p.CommitID, Epoch: w.epoch, Batches: cloneBatches(p.Batches)}
	if err := SealCommit(w.store.profile, head.Digest, sid, &c); err != nil {
		return Commit{}, err
	}
	w.s.commits = append(w.s.commits, c)
	w.s.byCommit[p.CommitID] = len(w.s.commits) - 1
	return cloneCommit(c), nil
}

// --- read --------------------------------------------------------------------------

func (m *MemoryStore) ReadCommits(ctx context.Context, req CommitReadRequest) (CommitPage, error) {
	if err := ctx.Err(); err != nil {
		return CommitPage{}, err
	}
	s, err := m.session(req.SessionID, "read")
	if err != nil {
		return CommitPage{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	page := CommitPage{Header: s.header, Head: s.head()}
	if int(req.From) >= len(s.commits) {
		return page, nil
	}
	start := int(req.From)
	end := len(s.commits)
	if req.Limit > 0 && start+int(req.Limit) < end {
		end = start + int(req.Limit)
		page.HasMore = true
	}
	page.Commits = make([]Commit, 0, end-start)
	for i := start; i < end; i++ {
		page.Commits = append(page.Commits, cloneCommit(s.commits[i]))
	}
	return page, nil
}

func (m *MemoryStore) ReadStream(ctx context.Context, req StreamReadRequest) (StreamPage, error) {
	if err := ctx.Err(); err != nil {
		return StreamPage{}, err
	}
	if err := ValidateStreamRef(req.Stream); err != nil {
		return StreamPage{}, newError(ErrInvalid, "read_stream", req.SessionID, err.Error())
	}
	s, err := m.session(req.SessionID, "read_stream")
	if err != nil {
		return StreamPage{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	page := StreamPage{Header: s.header, Stream: req.Stream, Head: s.head()}
	var pos StreamSeq
	for i := range s.commits {
		for j := range s.commits[i].Batches {
			b := &s.commits[i].Batches[j]
			if b.Stream != req.Stream {
				continue
			}
			for _, e := range b.Events {
				if pos < req.From {
					pos++
					continue
				}
				if req.Limit > 0 && uint32(len(page.Events)) >= req.Limit {
					page.HasMore = true
					return page, nil
				}
				page.Events = append(page.Events, e)
				pos++
			}
		}
	}
	return page, nil
}

// Tamper mutates one stored commit in place. It exists so conformance can
// prove that the ledger check at Open detects corruption; production code
// never calls it.
func (m *MemoryStore) Tamper(sid SessionID, seq CommitSeq, mutate func(*Commit)) {
	s, err := m.session(sid, "tamper")
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if int(seq) < len(s.commits) {
		mutate(&s.commits[seq])
	}
}

// CrashTail drops every stored commit after the first keep, simulating a
// crash whose last commit never became durable. It exists so conformance can
// prove that Open recovers to the last whole commit (SES-APP-2); production
// code never calls it.
func (m *MemoryStore) CrashTail(sid SessionID, keep int) error {
	s, err := m.session(sid, "crash_tail")
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

// rebuildIndex recomputes the CommitID index after a crash truncation, so the
// dropped CommitIDs are admissible again.
func (s *memorySession) rebuildIndex() {
	s.byCommit = make(map[CommitID]int, len(s.commits))
	for i := range s.commits {
		s.byCommit[s.commits[i].CommitID] = i
	}
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

var _ Store = (*MemoryStore)(nil)
