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
	profile  ProtocolProfile
	mu       sync.RWMutex // guards sessions map
	sessions map[SessionID]*memorySession
}

type memorySession struct {
	mu       sync.Mutex
	header   SessionHeader
	rows     []SessionEvent
	byCommit map[CommitID][2]int // [first, last] row index of the group
	epoch    Epoch
	owner    *memoryHandle // nil when no live owner
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{profile: ProfileV1(), sessions: make(map[SessionID]*memorySession)}
}

func (m *MemoryStore) Profile() ProtocolProfile { return m.profile }

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
	m.sessions[req.SessionID] = &memorySession{header: header, byCommit: make(map[CommitID][2]int)}
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
	if len(s.rows) == 0 {
		return Head{Next: 0, Digest: s.header.HeaderDigest}
	}
	last := &s.rows[len(s.rows)-1]
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
	// (SES-REP-1); Read trusts the store.
	if err := ValidateChain(m.profile, s.header, s.rows); err != nil {
		return nil, err
	}
	// A crash can leave one incomplete tail group. Drop it before the head is
	// established, exactly as the file adapter truncates the file: otherwise
	// Head.Next would sit inside the torn group and the next Append would
	// extend a group that can never get its Last row, welding two CommitIDs
	// into one group that Read would hand back as a single group (SES-APP-2).
	s.dropIncompleteTail()
	s.epoch++
	w := &memoryHandle{store: m, s: s, epoch: s.epoch}
	s.owner = w
	return w, nil
}

// dropIncompleteTail removes a trailing group whose Last row never landed and
// rebuilds the CommitID index from the surviving rows, so a retry of the
// dropped CommitID is admissible again.
func (s *memorySession) dropIncompleteTail() {
	keep := len(s.rows)
	for keep > 0 && !s.rows[keep-1].Last {
		keep--
	}
	if keep == len(s.rows) {
		return
	}
	s.rows = s.rows[:keep]
	s.rebuildIndex()
}

// rebuildIndex recomputes the CommitID to row-range index from rows.
func (s *memorySession) rebuildIndex() {
	s.byCommit = make(map[CommitID][2]int)
	for i := 0; i < len(s.rows); {
		end := i
		for end < len(s.rows) && !s.rows[end].Last {
			end++
		}
		s.byCommit[s.rows[i].CommitID] = [2]int{i, end}
		i = end + 1
	}
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

// Committed is SES-REP-3: the row index the kernel keeps to reject a duplicate
// CommitID answers membership directly.
func (w *memoryHandle) Committed(id CommitID) bool {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	_, ok := w.s.byCommit[id]
	return ok
}

// LookupCommit is SES-REP-4: the session holds every row, so a hit copies the
// group's span instead of reading storage.
func (w *memoryHandle) LookupCommit(id CommitID) ([]SessionEvent, bool, error) {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	span, ok := w.s.byCommit[id]
	if !ok {
		return nil, false, nil
	}
	return cloneRows(w.s.rows[span[0] : span[1]+1]), true, nil
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

func (w *memoryHandle) Append(ctx context.Context, g Group) ([]SessionEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sid := w.s.header.SessionID
	if g.CommitID == "" {
		return nil, newError(ErrInvalid, "append", sid, "empty CommitID")
	}
	if err := validIdentity("CommitID", string(g.CommitID)); err != nil {
		return nil, newError(ErrInvalid, "append", sid, err.Error())
	}
	if len(g.Events) == 0 {
		return nil, newError(ErrInvalid, "append", sid, "empty group")
	}
	if len(g.Events) > int(^uint16(0)) {
		return nil, newError(ErrInvalid, "append", sid, "group too large")
	}
	for i := range g.Events {
		if err := ValidateUncommitted(&g.Events[i]); err != nil {
			return nil, newError(ErrInvalid, "append", sid, fmt.Sprintf("event %d: %v", i, err))
		}
	}
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	if err := w.current("append"); err != nil {
		return nil, err
	}
	if _, dup := w.s.byCommit[g.CommitID]; dup {
		return nil, &Error{Code: ErrConflict, Operation: "append", SessionID: sid, CommitID: g.CommitID, Detail: "CommitID already in stream"}
	}
	head := w.s.head()
	prev := head.Digest
	rows := make([]SessionEvent, len(g.Events))
	for i := range g.Events {
		e := &g.Events[i]
		row := SessionEvent{Seq: head.Next + Seq(i), CommitID: g.CommitID, Index: uint16(i), Last: i == len(g.Events)-1,
			Type: e.Type, RecordedAtUnixMilli: e.RecordedAtUnixMilli, SourceSeqs: append([]Seq(nil), e.SourceSeqs...), Ignorable: e.Ignorable, Payload: e.Payload}
		d, err := w.store.profile.EventDigest(prev, sid, row)
		if err != nil {
			return nil, err
		}
		row.Digest = d
		prev = d
		rows[i] = row
	}
	first := len(w.s.rows)
	w.s.rows = append(w.s.rows, rows...)
	w.s.byCommit[g.CommitID] = [2]int{first, first + len(rows) - 1}
	return cloneRows(rows), nil
}

// --- read --------------------------------------------------------------------------

func (m *MemoryStore) Read(ctx context.Context, req ReadRequest) (ReadPage, error) {
	if err := ctx.Err(); err != nil {
		return ReadPage{}, err
	}
	s, err := m.session(req.SessionID, "read")
	if err != nil {
		return ReadPage{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	page := ReadPage{Header: s.header, Head: s.head()}
	if int(req.From) > len(s.rows) {
		return page, nil
	}
	// Start at a group boundary at or before From so no partial group leaks.
	start := int(req.From)
	for start > 0 && start < len(s.rows) && s.rows[start].Index != 0 {
		start--
	}
	for i := start; i < len(s.rows); {
		end := i
		for end < len(s.rows) && !s.rows[end].Last {
			end++
		}
		if end >= len(s.rows) {
			break // incomplete tail group is never exposed (SES-APP-2)
		}
		var matched []SessionEvent
		for j := i; j <= end; j++ {
			if s.rows[j].Seq >= req.From && HasTypePrefix(s.rows[j].Type, req.Types) {
				matched = append(matched, s.rows[j])
			}
		}
		if len(matched) > 0 {
			// Limit counts rows but only truncates between groups; the first
			// group is always returned so a caller can make progress.
			if req.Limit > 0 && len(page.Events) > 0 && len(page.Events)+len(matched) > int(req.Limit) {
				page.HasMore = true
				break
			}
			page.Events = append(page.Events, cloneRows(matched)...)
		}
		i = end + 1
	}
	return page, nil
}

// Tamper mutates one stored row in place. It exists so conformance can prove
// that the chain check at Open detects corruption; production code never
// calls it.
func (m *MemoryStore) Tamper(sid SessionID, seq Seq, mutate func(*SessionEvent)) {
	s, err := m.session(sid, "tamper")
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if int(seq) < len(s.rows) {
		mutate(&s.rows[seq])
	}
}

// CrashTail drops every stored row after the first keep, simulating a crash
// whose last group never finished landing. It exists so conformance can prove
// that Open recovers to the last complete group (SES-APP-2); production code
// never calls it.
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
	if keep > len(s.rows) {
		keep = len(s.rows)
	}
	s.rows = s.rows[:keep]
	return nil
}

func cloneRows(rows []SessionEvent) []SessionEvent {
	out := make([]SessionEvent, len(rows))
	for i := range rows {
		out[i] = rows[i]
		out[i].SourceSeqs = append([]Seq(nil), rows[i].SourceSeqs...)
	}
	return out
}

var _ Store = (*MemoryStore)(nil)
