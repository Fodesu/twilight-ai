package session

import (
	"context"
	"fmt"
	"sync"
)

// MemoryStore is the in-process reference Store. Ownership lasts until Close;
// an Open with Takeover supersedes a live owner, which is then fenced by its
// stale Epoch. A fork holds only its own commits; reads stitch the parent's
// inherited prefix in front of them (SES-FRK-2).
type MemoryStore struct {
	profile  LedgerProfile
	mu       sync.RWMutex // guards sessions map
	sessions map[SessionID]*memorySession
}

type memorySession struct {
	mu       sync.Mutex
	header   SessionHeader
	commits  []Commit         // own commits only, from LedgerSeed(header).Next
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
	header := SessionHeader{ProtocolVersion: req.ProtocolVersion, SessionID: req.SessionID, CreatedAtUnixMilli: req.CreatedAtUnixMilli,
		ParentFork: cloneFork(req.ParentFork), CausationID: req.CausationID, Metadata: req.Metadata}
	digest, err := m.profile.HeaderDigest(header)
	if err != nil {
		return SessionHeader{}, err
	}
	header.HeaderDigest = digest
	if err := m.profile.ValidateHeader(header); err != nil {
		return SessionHeader{}, err
	}
	if header.ParentFork != nil {
		// The anchor must name a commit the parent holds (SES-FRK-1). The
		// parent is read outside the sessions lock; it is append-only, so the
		// commit cannot disappear before the child is registered.
		if err := m.checkFork(ctx, header); err != nil {
			return SessionHeader{}, err
		}
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

// checkFork verifies a fork anchor against the parent: same protocol, and the
// parent's (possibly itself inherited) commit at Seq carries Digest.
func (m *MemoryStore) checkFork(ctx context.Context, header SessionHeader) error {
	fork := header.ParentFork
	page, err := m.ReadCommits(ctx, CommitReadRequest{SessionID: fork.ParentSessionID, From: fork.Seq, Limit: 1})
	if err != nil {
		if IsCode(err, ErrNotFound) {
			return newError(ErrNotFound, "create", header.SessionID, fmt.Sprintf("parent session %s not found", fork.ParentSessionID))
		}
		return err
	}
	return CheckForkAnchor(header, page)
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
		return LedgerSeed(s.header)
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
// membership directly; on a fork the inherited prefix counts too (SES-FRK-3).
func (w *memoryHandle) Committed(id CommitID) bool {
	w.s.mu.Lock()
	_, ok := w.s.byCommit[id]
	fork := w.s.header.ParentFork
	w.s.mu.Unlock()
	if ok || fork == nil {
		return ok
	}
	_, found := w.store.lookupPrefix(fork, id)
	return found
}

// LookupCommit is SES-REP-4: the session holds every commit, so a hit copies
// it instead of reading storage.
func (w *memoryHandle) LookupCommit(id CommitID) (Commit, bool, error) {
	w.s.mu.Lock()
	i, ok := w.s.byCommit[id]
	if ok {
		c := cloneCommit(w.s.commits[i])
		w.s.mu.Unlock()
		return c, true, nil
	}
	fork := w.s.header.ParentFork
	w.s.mu.Unlock()
	if fork == nil {
		return Commit{}, false, nil
	}
	c, found := w.store.lookupPrefix(fork, id)
	return c, found, nil
}

// lookupPrefix finds id among the commits a fork inherits: the parent's own
// commits up to fork.Seq, then the parent's own inherited prefix.
func (m *MemoryStore) lookupPrefix(fork *ForkPoint, id CommitID) (Commit, bool) {
	for fork != nil {
		p, err := m.session(fork.ParentSessionID, "lookup")
		if err != nil {
			return Commit{}, false
		}
		p.mu.Lock()
		i, ok := p.byCommit[id]
		var c Commit
		if ok && p.commits[i].Seq <= fork.Seq {
			c = cloneCommit(p.commits[i])
		} else {
			ok = false
		}
		next := p.header.ParentFork
		p.mu.Unlock()
		if ok {
			return c, true
		}
		fork = next
	}
	return Commit{}, false
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
	// A CommitID the inherited prefix holds is a duplicate too (SES-FRK-3);
	// the prefix is immutable, so this check needs no lock on w.s.
	if fork := w.s.header.ParentFork; fork != nil {
		if _, dup := w.store.lookupPrefix(fork, p.CommitID); dup {
			return Commit{}, &Error{Code: ErrConflict, Operation: "append", SessionID: sid, CommitID: p.CommitID, Detail: "CommitID already in the inherited prefix"}
		}
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
	header, head := s.header, s.head()
	own := make([]Commit, len(s.commits))
	for i := range s.commits {
		own[i] = cloneCommit(s.commits[i])
	}
	s.mu.Unlock()
	page := CommitPage{Header: header, Head: head}
	if req.From >= head.Next {
		return page, nil
	}
	// The inherited prefix comes first (SES-FRK-2); the parent stitches its
	// own prefix in turn, so a fork of a fork reads through both.
	if fork := header.ParentFork; fork != nil && req.From <= fork.Seq {
		prefix, err := m.ReadCommits(ctx, CommitReadRequest{SessionID: fork.ParentSessionID, From: req.From, Limit: req.Limit})
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
			// The limit was reached inside the prefix: more follows when the
			// prefix continues or the fork has commits of its own.
			last := page.Commits[len(page.Commits)-1].Seq
			page.HasMore = last < fork.Seq || len(own) > 0
			return page, nil
		}
	}
	seed := LedgerSeed(header)
	start := 0
	if req.From > seed.Next {
		start = int(req.From - seed.Next)
	}
	for i := start; i < len(own); i++ {
		if req.Limit > 0 && uint32(len(page.Commits)) >= req.Limit {
			page.HasMore = true
			break
		}
		page.Commits = append(page.Commits, own[i])
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
	// Stream positions count the stream's events from the first commit the
	// Session sees, inherited prefix included.
	all, err := m.ReadCommits(ctx, CommitReadRequest{SessionID: req.SessionID})
	if err != nil {
		return StreamPage{}, err
	}
	page := StreamPage{Header: all.Header, Stream: req.Stream, Head: all.Head}
	page.Events, page.HasMore = StreamEvents(all.Commits, req.Stream, req.From, req.Limit)
	return page, nil
}

// StreamEvents walks commits in order and returns the events of stream from
// position from, at most limit (0 = unlimited); more reports whether events
// beyond the returned ones exist. It is the one StreamSeq derivation the
// adapters share (SES-REP-2).
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

// Tamper mutates one stored commit in place. It exists so conformance can
// prove that the ledger check at Open detects corruption; production code
// never calls it. seq names one of the Session's own commits.
func (m *MemoryStore) Tamper(sid SessionID, seq CommitSeq, mutate func(*Commit)) {
	s, err := m.session(sid, "tamper")
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
