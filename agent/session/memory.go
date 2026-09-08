package session

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MemoryStore is the in-process reference Store. Ownership follows the
// database-adapter shape: an Open with TTL records a deadline that Heartbeat
// pushes and that a later Open may pass; TTL zero ownership lasts until Close.
type MemoryStore struct {
	profile  ProtocolProfile
	now      func() time.Time
	mu       sync.RWMutex // guards sessions map
	sessions map[SessionID]*memorySession
}

type memorySession struct {
	mu       sync.Mutex
	header   SessionHeader
	rows     []SessionEvent
	byCommit map[CommitID][2]int // [first, last] row index of the group
	epoch    Epoch
	owner    *memoryWriter // nil when no live owner
	deadline time.Time     // zero when the owner has no TTL
}

// NewMemoryStore returns a MemoryStore using the wall clock.
func NewMemoryStore() *MemoryStore { return NewMemoryStoreWithClock(time.Now) }

// NewMemoryStoreWithClock lets tests drive ownership deadlines.
func NewMemoryStoreWithClock(now func() time.Time) *MemoryStore {
	return &MemoryStore{profile: ProfileV1(), now: now, sessions: make(map[SessionID]*memorySession)}
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

type memoryWriter struct {
	store *MemoryStore
	s     *memorySession
	epoch Epoch
	ttl   time.Duration
}

func (m *MemoryStore) Open(ctx context.Context, sid SessionID, opts OpenOptions) (Writer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opts.TTL < 0 {
		return nil, newError(ErrInvalid, "open", sid, "negative TTL")
	}
	s, err := m.session(sid, "open")
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owner != nil {
		if s.deadline.IsZero() || m.now().Before(s.deadline) {
			return nil, newError(ErrOwned, "open", sid, fmt.Sprintf("owned by epoch %d", s.epoch))
		}
	}
	s.epoch++
	w := &memoryWriter{store: m, s: s, epoch: s.epoch, ttl: opts.TTL}
	s.owner = w
	if opts.TTL > 0 {
		s.deadline = m.now().Add(opts.TTL)
	} else {
		s.deadline = time.Time{}
	}
	return w, nil
}

func (w *memoryWriter) SessionID() SessionID { return w.s.header.SessionID }
func (w *memoryWriter) Epoch() Epoch         { return w.epoch }

func (w *memoryWriter) Head() Head {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	return w.s.head()
}

// current reports whether w still owns the stream; the caller holds s.mu.
func (w *memoryWriter) current(op string) error {
	if w.s.owner != w || w.s.epoch != w.epoch {
		return newError(ErrOwnershipLost, op, w.s.header.SessionID, fmt.Sprintf("epoch %d superseded by %d", w.epoch, w.s.epoch))
	}
	return nil
}

func (w *memoryWriter) Heartbeat(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	if err := w.current("heartbeat"); err != nil {
		return err
	}
	if w.ttl > 0 {
		w.s.deadline = w.store.now().Add(w.ttl)
	}
	return nil
}

func (w *memoryWriter) Close(ctx context.Context) error {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	if w.s.owner == w {
		w.s.owner = nil
		w.s.deadline = time.Time{}
	}
	return nil
}

// --- append -----------------------------------------------------------------------

func (w *memoryWriter) Append(ctx context.Context, g Group) ([]SessionEvent, error) {
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
	// The Memory adapter verifies the whole chain on every read; a durable
	// adapter verifies the chain only on unfiltered reads (SES-REP-1).
	if err := ValidateChain(m.profile, s.header, s.rows); err != nil {
		return ReadPage{}, err
	}
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
// that the chain check detects corruption; production code never calls it.
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

func cloneRows(rows []SessionEvent) []SessionEvent {
	out := make([]SessionEvent, len(rows))
	for i := range rows {
		out[i] = rows[i]
		out[i].SourceSeqs = append([]Seq(nil), rows[i].SourceSeqs...)
	}
	return out
}

var _ Store = (*MemoryStore)(nil)
