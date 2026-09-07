package session

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/memohai/twilight/agent/es"
)

// MemoryStore is the in-process reference Store. Each Session has its own
// critical section; the control-plane KV and snapshot tables share the
// Session's lock so a CommitIn transaction is atomic with respect to every
// other operation on the same Session.
type MemoryStore struct {
	profile  ProtocolProfile
	mu       sync.RWMutex // guards sessions map
	sessions map[SessionID]*memorySession
}

type memorySession struct {
	mu        sync.Mutex
	header    SessionHeader
	commits   []SessionCommit
	byID      map[CommitID]int
	snapshots map[snapshotKey]Snapshot
	control   map[ControlNamespace]map[string]ControlEntry
	eventIDs  map[EventID]struct{}
}

type snapshotKey struct {
	key     ProjectionKey
	version uint16
}

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
	header := SessionHeader{ProtocolVersion: req.ProtocolVersion, SessionID: req.SessionID, CausationID: req.CausationID, Metadata: req.Metadata}
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
	m.sessions[req.SessionID] = &memorySession{
		header:    header,
		byID:      make(map[CommitID]int),
		snapshots: make(map[snapshotKey]Snapshot),
		control:   make(map[ControlNamespace]map[string]ControlEntry),
		eventIDs:  make(map[EventID]struct{}),
	}
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

func (m *MemoryStore) Head(ctx context.Context, sid SessionID) (Head, error) {
	if err := ctx.Err(); err != nil {
		return Head{}, err
	}
	s, err := m.session(sid, "head")
	if err != nil {
		return Head{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.head(), nil
}

func (s *memorySession) head() Head {
	if len(s.commits) == 0 {
		return Head{Revision: 0, Digest: s.header.HeaderDigest}
	}
	last := s.commits[len(s.commits)-1]
	return Head{Revision: last.Revision, Digest: last.CommitDigest}
}

func (m *MemoryStore) LookupCommit(ctx context.Context, sid SessionID, id CommitID) (SessionCommit, bool, error) {
	if err := ctx.Err(); err != nil {
		return SessionCommit{}, false, err
	}
	s, err := m.session(sid, "lookup_commit")
	if err != nil {
		return SessionCommit{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lookup(id)
}

func (s *memorySession) lookup(id CommitID) (SessionCommit, bool, error) {
	i, ok := s.byID[id]
	if !ok {
		return SessionCommit{}, false, nil
	}
	return cloneCommit(&s.commits[i]), true, nil
}

// --- transactions -----------------------------------------------------------

// memoryTx stages snapshot and KV writes; they are merged only when the
// transaction completes, so a failing fn leaves nothing behind.
type memoryTx struct {
	s         *memorySession
	snapshots map[snapshotKey]Snapshot
	control   map[ControlNamespace]map[string]*ControlEntry // nil entry = delete
	used      bool
}

func (s *memorySession) begin() *memoryTx {
	return &memoryTx{s: s, snapshots: make(map[snapshotKey]Snapshot), control: make(map[ControlNamespace]map[string]*ControlEntry)}
}

func (tx *memoryTx) Head() Head { return tx.s.head() }

func (tx *memoryTx) LookupCommit(id CommitID) (SessionCommit, bool, error) { return tx.s.lookup(id) }

func (tx *memoryTx) Tail(after Head, types []EventType) ([]SessionCommit, error) {
	return tx.s.tail(after, types)
}

func (s *memorySession) tail(after Head, types []EventType) ([]SessionCommit, error) {
	if after.Revision > 0 {
		if int(after.Revision) > len(s.commits) {
			return nil, newError(ErrInvalid, "tail", s.header.SessionID, "after is beyond head")
		}
		if s.commits[after.Revision-1].CommitDigest != after.Digest {
			return nil, newError(ErrInvalid, "tail", s.header.SessionID, "after digest is not on this stream")
		}
	} else if after.Digest != "" && after.Digest != s.header.HeaderDigest {
		return nil, newError(ErrInvalid, "tail", s.header.SessionID, "after digest is not this header")
	}
	var out []SessionCommit
	for i := int(after.Revision); i < len(s.commits); i++ {
		if CommitMatchesTypes(&s.commits[i], types) {
			out = append(out, cloneCommit(&s.commits[i]))
		}
	}
	return out, nil
}

func (tx *memoryTx) LoadSnapshot(key ProjectionKey, version uint16) (SnapshotResult, error) {
	k := snapshotKey{key, version}
	if snap, ok := tx.snapshots[k]; ok {
		return SnapshotResult{Snapshot: &snap, Found: true}, nil
	}
	return tx.s.loadSnapshot(k), nil
}

func (s *memorySession) loadSnapshot(k snapshotKey) SnapshotResult {
	snap, ok := s.snapshots[k]
	if !ok {
		return SnapshotResult{}
	}
	return SnapshotResult{Snapshot: &snap, Found: true}
}

func (tx *memoryTx) SaveSnapshot(snap Snapshot) error {
	if err := tx.s.checkSnapshot(&snap); err != nil {
		return err
	}
	tx.snapshots[snapshotKey{snap.ProjectionKey, snap.ProjectionVersion}] = snap
	return nil
}

func (s *memorySession) checkSnapshot(snap *Snapshot) error {
	if snap.SessionID != s.header.SessionID {
		return newError(ErrInvalid, "save_snapshot", s.header.SessionID, "snapshot session mismatch")
	}
	if snap.ProjectionKey == "" || snap.State.IsZero() {
		return newError(ErrInvalid, "save_snapshot", s.header.SessionID, "empty projection key or state")
	}
	return nil
}

func (tx *memoryTx) ControlGet(ns ControlNamespace, key string) (ControlEntry, bool, error) {
	if staged, ok := tx.control[ns][key]; ok {
		if staged == nil {
			return ControlEntry{}, false, nil
		}
		return cloneEntry(*staged), true, nil
	}
	return tx.s.controlGet(ns, key)
}

func (s *memorySession) controlGet(ns ControlNamespace, key string) (ControlEntry, bool, error) {
	e, ok := s.control[ns][key]
	if !ok {
		return ControlEntry{}, false, nil
	}
	return cloneEntry(e), true, nil
}

func (tx *memoryTx) ControlPut(ns ControlNamespace, key string, value []byte, deadline int64) error {
	if ns == "" || key == "" {
		return newError(ErrInvalid, "control_put", tx.s.header.SessionID, "empty namespace or key")
	}
	if tx.control[ns] == nil {
		tx.control[ns] = make(map[string]*ControlEntry)
	}
	e := ControlEntry{SessionID: tx.s.header.SessionID, Namespace: ns, Key: key, Value: append([]byte(nil), value...), DeadlineUnixMilli: deadline}
	tx.control[ns][key] = &e
	return nil
}

func (tx *memoryTx) ControlDelete(ns ControlNamespace, key string) error {
	if tx.control[ns] == nil {
		tx.control[ns] = make(map[string]*ControlEntry)
	}
	tx.control[ns][key] = nil
	return nil
}

// merge applies the staged writes; called with the session lock held after
// the append (if any) succeeded.
func (tx *memoryTx) merge() {
	for k, snap := range tx.snapshots {
		tx.s.snapshots[k] = snap
	}
	for ns, entries := range tx.control {
		if tx.s.control[ns] == nil {
			tx.s.control[ns] = make(map[string]ControlEntry)
		}
		for key, e := range entries {
			if e == nil {
				delete(tx.s.control[ns], key)
			} else {
				tx.s.control[ns][key] = *e
			}
		}
	}
}

func (m *MemoryStore) CommitIn(ctx context.Context, sid SessionID, fn CommitInFn) (AppendResult, error) {
	if err := ctx.Err(); err != nil {
		return AppendResult{}, err
	}
	if fn == nil {
		return AppendResult{}, newError(ErrInvalid, "commit_in", sid, "nil fn")
	}
	s, err := m.session(sid, "commit_in")
	if err != nil {
		return AppendResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx := s.begin()
	req, err := fn(tx)
	if err != nil {
		return AppendResult{}, err
	}
	if req == nil {
		tx.merge()
		return AppendResult{Disposition: AppendApplied, ActualHead: s.head()}, nil
	}
	if req.SessionID != sid {
		return AppendResult{Disposition: AppendInvalid, ActualHead: s.head(), Detail: "append session mismatch"}, nil
	}
	if req.ExpectedHead != s.head() {
		return AppendResult{Disposition: AppendInvalid, ActualHead: s.head(), Detail: "ExpectedHead does not match the head inside the critical section"}, nil
	}
	res, err := m.append(s, req)
	if err != nil {
		return AppendResult{}, err
	}
	if res.Disposition == AppendApplied || res.Disposition == AppendAlreadyApplied {
		tx.merge()
	}
	return res, nil
}

func (m *MemoryStore) Commit(ctx context.Context, req AppendRequest) (AppendResult, error) {
	if err := ctx.Err(); err != nil {
		return AppendResult{}, err
	}
	s, err := m.session(req.SessionID, "commit")
	if err != nil {
		return AppendResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Idempotent replay is decided before the head compare so a retry after
	// a lost response returns AlreadyApplied even if the head has moved.
	if existing, ok, _ := s.lookup(req.CommitID); ok {
		return m.replayDisposition(s, &existing, &req)
	}
	if req.ExpectedHead != s.head() {
		return AppendResult{Disposition: AppendHeadConflict, ActualHead: s.head()}, nil
	}
	return m.append(s, &req)
}

func (m *MemoryStore) replayDisposition(s *memorySession, existing *SessionCommit, req *AppendRequest) (AppendResult, error) {
	have, err := fingerprintCommit(m.profile, existing)
	if err != nil {
		return AppendResult{}, err
	}
	want, err := m.profile.FingerprintAppend(*req)
	if err != nil {
		return AppendResult{}, err
	}
	if have == want {
		c := cloneCommit(existing)
		return AppendResult{Disposition: AppendAlreadyApplied, Commit: &c, ActualHead: s.head()}, nil
	}
	return AppendResult{Disposition: AppendCommitConflict, ActualHead: s.head()}, nil
}

// append seals and stores one commit; the caller holds the session lock and
// has already checked ExpectedHead.
func (m *MemoryStore) append(s *memorySession, req *AppendRequest) (AppendResult, error) {
	if existing, ok, _ := s.lookup(req.CommitID); ok {
		return m.replayDisposition(s, &existing, req)
	}
	if req.CommitID == "" {
		return AppendResult{Disposition: AppendInvalid, ActualHead: s.head(), Detail: "empty CommitID"}, nil
	}
	if len(req.Events) == 0 {
		return AppendResult{Disposition: AppendInvalid, ActualHead: s.head(), Detail: "empty event group"}, nil
	}
	seen := make(map[EventID]struct{}, len(req.Events))
	for i := range req.Events {
		e := &req.Events[i]
		if err := validateUncommitted(e); err != nil {
			return AppendResult{Disposition: AppendInvalid, ActualHead: s.head(), Detail: fmt.Sprintf("event %d: %v", i, err)}, nil
		}
		if _, dup := seen[e.EventID]; dup {
			return AppendResult{Disposition: AppendInvalid, ActualHead: s.head(), Detail: "duplicate EventID in group"}, nil
		}
		if _, dup := s.eventIDs[e.EventID]; dup {
			return AppendResult{Disposition: AppendInvalid, ActualHead: s.head(), Detail: "EventID already in stream"}, nil
		}
		seen[e.EventID] = struct{}{}
		for _, src := range e.SourceEvents {
			if _, ok := s.eventIDs[src]; !ok {
				if _, inGroup := seen[src]; !inGroup {
					return AppendResult{Disposition: AppendInvalid, ActualHead: s.head(), Detail: "SourceEvents references an unknown EventID"}, nil
				}
			}
		}
	}
	head := s.head()
	commit := SessionCommit{
		ProtocolVersion: m.profile.Version(),
		SessionID:       req.SessionID,
		Revision:        head.Revision + 1,
		PreviousDigest:  head.Digest,
		CommitID:        req.CommitID,
		CausationID:     req.CausationID,
		CorrelationID:   req.CorrelationID,
		Events:          make([]SessionEvent, len(req.Events)),
	}
	for i := range req.Events {
		e := &req.Events[i]
		ev := SessionEvent{EventID: e.EventID, Index: uint16(i), Type: e.Type, RecordedAtUnixMilli: e.RecordedAtUnixMilli,
			SourceEvents: append([]EventID(nil), e.SourceEvents...), Payload: e.Payload}
		d, err := m.profile.EventDigest(commit.SessionID, commit.Revision, ev)
		if err != nil {
			return AppendResult{}, err
		}
		ev.EventDigest = d
		commit.Events[i] = ev
	}
	d, err := m.profile.CommitDigest(commit)
	if err != nil {
		return AppendResult{}, err
	}
	commit.CommitDigest = d
	s.commits = append(s.commits, commit)
	s.byID[commit.CommitID] = len(s.commits) - 1
	for i := range commit.Events {
		s.eventIDs[commit.Events[i].EventID] = struct{}{}
	}
	out := cloneCommit(&commit)
	return AppendResult{Disposition: AppendApplied, Commit: &out, ActualHead: s.head()}, nil
}

// --- replay -----------------------------------------------------------------

func (m *MemoryStore) Replay(ctx context.Context, req ReplayRequest) (ReplayPage, error) {
	if err := ctx.Err(); err != nil {
		return ReplayPage{}, err
	}
	s, err := m.session(req.SessionID, "replay")
	if err != nil {
		return ReplayPage{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	start := 0
	if req.Cursor != nil && req.Cursor.After != nil {
		after := req.Cursor.After
		if after.Revision == 0 || int(after.Revision) > len(s.commits) {
			return ReplayPage{}, newError(ErrInvalid, "replay", req.SessionID, "cursor revision is not on this stream")
		}
		c := &s.commits[after.Revision-1]
		if int(after.Index) >= len(c.Events) || c.Events[after.Index].EventDigest != after.EventDigest {
			return ReplayPage{}, newError(ErrInvalid, "replay", req.SessionID, "cursor event is not on this stream")
		}
		if int(after.Index) != len(c.Events)-1 {
			return ReplayPage{}, newError(ErrInvalid, "replay", req.SessionID, "cursor must address the last event of a complete commit")
		}
		start = int(after.Revision)
	}
	page := ReplayPage{Header: s.header, Head: s.head()}
	var prev es.Digest
	if start == 0 {
		prev = s.header.HeaderDigest
	} else {
		prev = s.commits[start-1].CommitDigest
	}
	for i := start; i < len(s.commits); i++ {
		c := &s.commits[i]
		if len(req.Types) == 0 {
			if c.Revision != es.Revision(i+1) || c.PreviousDigest != prev {
				return ReplayPage{}, newError(ErrCorrupt, "replay", req.SessionID, fmt.Sprintf("chain broken at revision %d", i+1))
			}
			if err := m.profile.ValidateCommit(*c); err != nil {
				return ReplayPage{}, err
			}
			prev = c.CommitDigest
		}
		if !CommitMatchesTypes(c, req.Types) {
			continue
		}
		page.Commits = append(page.Commits, cloneCommit(c))
		if req.Limit > 0 && uint32(len(page.Commits)) >= req.Limit && i+1 < len(s.commits) {
			last := &c.Events[len(c.Events)-1]
			page.Next = &ReplayCursor{After: &EventPosition{Revision: c.Revision, Index: last.Index, EventDigest: last.EventDigest}}
			break
		}
	}
	return page, nil
}

// --- snapshots ----------------------------------------------------------------

func (m *MemoryStore) LoadSnapshot(ctx context.Context, req SnapshotRequest) (SnapshotResult, error) {
	if err := ctx.Err(); err != nil {
		return SnapshotResult{}, err
	}
	s, err := m.session(req.SessionID, "load_snapshot")
	if err != nil {
		return SnapshotResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadSnapshot(snapshotKey{req.ProjectionKey, req.ProjectionVersion}), nil
}

func (m *MemoryStore) SaveSnapshot(ctx context.Context, req SaveSnapshotRequest) (SaveSnapshotResult, error) {
	if err := ctx.Err(); err != nil {
		return SaveSnapshotResult{}, err
	}
	s, err := m.session(req.Snapshot.SessionID, "save_snapshot")
	if err != nil {
		return SaveSnapshotResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkSnapshot(&req.Snapshot); err != nil {
		return SaveSnapshotResult{}, err
	}
	k := snapshotKey{req.Snapshot.ProjectionKey, req.Snapshot.ProjectionVersion}
	_, replaced := s.snapshots[k]
	s.snapshots[k] = req.Snapshot
	return SaveSnapshotResult{Snapshot: req.Snapshot, Replaced: replaced}, nil
}

// --- control-plane KV -----------------------------------------------------------

func (m *MemoryStore) ControlGet(ctx context.Context, sid SessionID, ns ControlNamespace, key string) (ControlEntry, bool, error) {
	if err := ctx.Err(); err != nil {
		return ControlEntry{}, false, err
	}
	s, err := m.session(sid, "control_get")
	if err != nil {
		return ControlEntry{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.controlGet(ns, key)
}

func (m *MemoryStore) ControlPut(ctx context.Context, sid SessionID, ns ControlNamespace, key string, value []byte, deadline int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s, err := m.session(sid, "control_put")
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx := s.begin()
	if err := tx.ControlPut(ns, key, value, deadline); err != nil {
		return err
	}
	tx.merge()
	return nil
}

func (m *MemoryStore) ControlCompareAndPut(ctx context.Context, sid SessionID, ns ControlNamespace, key string, expected, value []byte, deadline int64) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s, err := m.session(sid, "control_cas")
	if err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.control[ns][key]
	if !ok || string(cur.Value) != string(expected) {
		return false, nil
	}
	tx := s.begin()
	if err := tx.ControlPut(ns, key, value, deadline); err != nil {
		return false, err
	}
	tx.merge()
	return true, nil
}

func (m *MemoryStore) ControlDelete(ctx context.Context, sid SessionID, ns ControlNamespace, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s, err := m.session(sid, "control_delete")
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.control[ns], key)
	return nil
}

func (m *MemoryStore) ControlScan(ctx context.Context, ns ControlNamespace, prefix string, fn func(ControlEntry) (bool, error)) error {
	return m.scan(ctx, ns, func(e ControlEntry) bool { return strings.HasPrefix(e.Key, prefix) }, fn)
}

func (m *MemoryStore) ControlExpired(ctx context.Context, ns ControlNamespace, before int64, fn func(ControlEntry) (bool, error)) error {
	return m.scan(ctx, ns, func(e ControlEntry) bool { return e.DeadlineUnixMilli != 0 && e.DeadlineUnixMilli < before }, fn)
}

func (m *MemoryStore) scan(ctx context.Context, ns ControlNamespace, match func(ControlEntry) bool, fn func(ControlEntry) (bool, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.RLock()
	sids := make([]SessionID, 0, len(m.sessions))
	for sid := range m.sessions {
		sids = append(sids, sid)
	}
	m.mu.RUnlock()
	sort.Slice(sids, func(i, j int) bool { return sids[i] < sids[j] })
	for _, sid := range sids {
		s, err := m.session(sid, "control_scan")
		if err != nil {
			continue
		}
		s.mu.Lock()
		var matched []ControlEntry
		for _, e := range s.control[ns] {
			if match(e) {
				matched = append(matched, cloneEntry(e))
			}
		}
		s.mu.Unlock()
		sort.Slice(matched, func(i, j int) bool { return matched[i].Key < matched[j].Key })
		for _, e := range matched {
			cont, err := fn(e)
			if err != nil {
				return err
			}
			if !cont {
				return nil
			}
		}
	}
	return nil
}

func cloneCommit(c *SessionCommit) SessionCommit {
	out := *c
	out.Events = make([]SessionEvent, len(c.Events))
	for i := range c.Events {
		out.Events[i] = c.Events[i]
		out.Events[i].SourceEvents = append([]EventID(nil), c.Events[i].SourceEvents...)
	}
	return out
}

func cloneEntry(e ControlEntry) ControlEntry {
	e.Value = append([]byte(nil), e.Value...)
	return e
}

var _ Store = (*MemoryStore)(nil)
