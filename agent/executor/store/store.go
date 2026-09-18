package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/effect"
	"github.com/felinics/twilight/agent/run/protocol"
)

var (
	ErrAssignmentConflict = errors.New("executor/store: assignment conflict")
	ErrLeaseLost          = errors.New("executor/store: execution lease lost")
	ErrStateConflict      = errors.New("executor/store: state conflict")
)

func legalTransition(from, to effect.ExecutionStatus) bool {
	if to == effect.ExecutionCancelRequested {
		return from == effect.ExecutionAccepted || from == effect.ExecutionDispatching || from == effect.ExecutionRunning
	}
	switch from {
	case effect.ExecutionAccepted:
		return to == effect.ExecutionDispatching
	case effect.ExecutionDispatching:
		return to == effect.ExecutionRunning
	case effect.ExecutionRunning:
		// This transition is only used by explicit control-plane retry after
		// backend reconciliation found no attachable execution.
		return to == effect.ExecutionDispatching
	default:
		return false
	}
}

// ExecutionRef is the Executor's physical binding of one attempt
// (RUN-EXE-9): the provider (backend) the record was handed to and that
// backend's opaque handle. It is persisted before the execution starts and
// never leaves the Executor: Agent Core addresses executions by
// AssignmentKey.
type ExecutionRef struct {
	Provider string `json:"provider"`
	Ref      string `json:"ref"`
}

// Record is the worker's durable execution record. Assignment is immutable
// and carries the execution payload; ExecutionRef is set before Start;
// State and Outcome move monotonically to a terminal state. Owner and
// FencingEpoch protect takeover.
type Record struct {
	Assignment       effect.Assignment `json:"assignment"`
	AssignmentDigest run.Digest        `json:"assignmentDigest"`
	ExecutionRef     ExecutionRef      `json:"executionRef"`
	// Superseded lists the ExecutionRefs of earlier generations of this
	// attempt, oldest first: a takeover that found the physical execution
	// missing restarted it under a new Ref (RUN-EXE-9). The audit trail
	// keeps every Ref the attempt ever bound to.
	Superseded          []ExecutionRef            `json:"superseded,omitempty"`
	State               effect.ExecutionStatus    `json:"state"`
	Owner               string                    `json:"owner,omitempty"`
	FencingEpoch        uint64                    `json:"fencingEpoch,omitempty"`
	LeaseUntilUnixMilli int64                     `json:"leaseUntilUnixMilli,omitempty"`
	Outcome             *protocol.OutcomeEnvelope `json:"outcome,omitempty"`
}

type Store interface {
	Create(context.Context, Record) (Record, bool, error)
	Get(context.Context, effect.AssignmentKey) (Record, bool, error)
	Put(context.Context, Record) error
	PutOwned(context.Context, Record, string, uint64) error
	TransitionOwned(context.Context, effect.AssignmentKey, string, uint64, effect.ExecutionStatus, effect.ExecutionStatus) error
	Acquire(context.Context, effect.AssignmentKey, string, time.Duration) (Record, bool, error)
	Renew(context.Context, effect.AssignmentKey, string, uint64, time.Duration) error
	LeaseOwned(context.Context, effect.AssignmentKey, string, uint64) (bool, error)
	List(context.Context) ([]Record, error)
}

// MemoryStore is useful for conformance tests and embedded deployments.
type MemoryStore struct {
	mu       sync.Mutex
	records  map[effect.AssignmentKey]Record
	now      func() time.Time
	terminal []effect.AssignmentKey
	retain   int
}

type MemoryStoreOptions struct {
	Now func() time.Time
	// RetainTerminal bounds the terminal records kept for idempotent reads
	// (RUN-EXE-3); zero selects DefaultRetainTerminal. Records still executing
	// are never dropped. A dropped record reads as missing, and a Dispatch of
	// its key is a new execution.
	RetainTerminal int
}

// DefaultRetainTerminal is the MemoryStore's terminal-record bound.
const DefaultRetainTerminal = 1024

func NewMemoryStore(options ...MemoryStoreOptions) *MemoryStore {
	now := time.Now
	if len(options) > 0 && options[0].Now != nil {
		now = options[0].Now
	}
	retain := DefaultRetainTerminal
	if len(options) > 0 && options[0].RetainTerminal > 0 {
		retain = options[0].RetainTerminal
	}
	return &MemoryStore{records: make(map[effect.AssignmentKey]Record), now: now, retain: retain}
}

// noteLocked records a write: a record that became terminal joins the
// eviction order and the oldest terminal records beyond the bound are
// dropped. s.mu must be held.
func (s *MemoryStore) noteLocked(key effect.AssignmentKey, was, now effect.ExecutionStatus) {
	if now.Terminal() && !was.Terminal() {
		s.terminal = append(s.terminal, key)
	}
	for len(s.terminal) > s.retain {
		delete(s.records, s.terminal[0])
		s.terminal = s.terminal[1:]
	}
}

func (s *MemoryStore) Create(_ context.Context, record Record) (Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.records[record.Assignment.Key()]; ok {
		if old.AssignmentDigest != record.AssignmentDigest {
			return old, false, ErrAssignmentConflict
		}
		return old, false, nil
	}
	s.records[record.Assignment.Key()] = record
	return record, true, nil
}

func (s *MemoryStore) Get(_ context.Context, key effect.AssignmentKey) (Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[key]
	return r, ok, nil
}

func (s *MemoryStore) Put(_ context.Context, record Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.records[record.Assignment.Key()]
	if ok && old.AssignmentDigest != record.AssignmentDigest {
		return ErrAssignmentConflict
	}
	s.records[record.Assignment.Key()] = record
	s.noteLocked(record.Assignment.Key(), old.State, record.State)
	return nil
}

func (s *MemoryStore) PutOwned(_ context.Context, record Record, owner string, epoch uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.records[record.Assignment.Key()]
	if !ok {
		return effect.ErrExecutionNotFound
	}
	if old.AssignmentDigest != record.AssignmentDigest {
		return ErrAssignmentConflict
	}
	if old.Owner != owner || old.FencingEpoch != epoch || old.LeaseUntilUnixMilli <= s.now().UnixMilli() {
		return ErrLeaseLost
	}
	s.records[record.Assignment.Key()] = record
	s.noteLocked(record.Assignment.Key(), old.State, record.State)
	return nil
}

func (s *MemoryStore) TransitionOwned(_ context.Context, key effect.AssignmentKey, owner string, epoch uint64, from, to effect.ExecutionStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[key]
	if !ok {
		return effect.ErrExecutionNotFound
	}
	if r.Owner != owner || r.FencingEpoch != epoch || r.LeaseUntilUnixMilli <= s.now().UnixMilli() {
		return ErrLeaseLost
	}
	if r.State != from || !legalTransition(from, to) {
		return ErrStateConflict
	}
	r.State = to
	s.records[key] = r
	return nil
}

func (s *MemoryStore) Acquire(_ context.Context, key effect.AssignmentKey, owner string, ttl time.Duration) (Record, bool, error) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[key]
	if !ok {
		return Record{}, false, effect.ErrExecutionNotFound
	}
	if protocol.StatusTerminal(r.State) {
		return r, false, nil
	}
	expired := r.LeaseUntilUnixMilli <= now.UnixMilli()
	if r.Owner != "" && r.Owner != owner && !expired {
		return r, false, nil
	}
	if r.Owner != owner || r.FencingEpoch == 0 || expired {
		r.FencingEpoch++
	}
	r.Owner = owner
	r.LeaseUntilUnixMilli = now.Add(ttl).UnixMilli()
	s.records[key] = r
	return r, true, nil
}

func (s *MemoryStore) Renew(_ context.Context, key effect.AssignmentKey, owner string, epoch uint64, ttl time.Duration) error {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[key]
	if !ok {
		return effect.ErrExecutionNotFound
	}
	if protocol.StatusTerminal(r.State) || r.Owner != owner || r.FencingEpoch != epoch {
		return ErrLeaseLost
	}
	r.LeaseUntilUnixMilli = now.Add(ttl).UnixMilli()
	s.records[key] = r
	return nil
}

func (s *MemoryStore) LeaseOwned(_ context.Context, key effect.AssignmentKey, owner string, epoch uint64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[key]
	if !ok {
		return false, effect.ErrExecutionNotFound
	}
	return !protocol.StatusTerminal(r.State) && r.Owner == owner && r.FencingEpoch == epoch && r.LeaseUntilUnixMilli > s.now().UnixMilli(), nil
}

func (s *MemoryStore) List(_ context.Context) ([]Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Record, 0, len(s.records))
	for _, r := range s.records {
		out = append(out, r)
	}
	return out, nil
}

// FileStore is a single-worker durable store. Each record is replaced with an
// fsync + atomic rename. Its lease operations are only process-safe because
// the mutex is local; use a transactional shared store when multiple Workers
// may acquire the same Assignment.
type FileStore struct {
	mu   sync.Mutex
	root string
	now  func() time.Time
}

type FileStoreOptions struct {
	Now func() time.Time
}

func NewFileStore(root string, options ...FileStoreOptions) (*FileStore, error) {
	if root == "" {
		return nil, errors.New("executor/store: empty store root")
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, err
	}
	now := time.Now
	if len(options) > 0 && options[0].Now != nil {
		now = options[0].Now
	}
	return &FileStore{root: root, now: now}, nil
}

func recordFile(root string, key effect.AssignmentKey) string {
	d, _ := es.DigestCanonical(key)
	return filepath.Join(root, string(d)[len("sha256:"):]+".json")
}

func (s *FileStore) Create(ctx context.Context, record Record) (Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok, err := s.getLocked(ctx, record.Assignment.Key()); err != nil {
		return Record{}, false, err
	} else if ok {
		if old.AssignmentDigest != record.AssignmentDigest {
			return old, false, ErrAssignmentConflict
		}
		return old, false, nil
	}
	if err := s.putLocked(ctx, record); err != nil {
		return Record{}, false, err
	}
	return record, true, nil
}

func (s *FileStore) Get(ctx context.Context, key effect.AssignmentKey) (Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getLocked(ctx, key)
}

func (s *FileStore) Put(ctx context.Context, record Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok, err := s.getLocked(ctx, record.Assignment.Key())
	if err != nil {
		return err
	}
	if ok && old.AssignmentDigest != record.AssignmentDigest {
		return ErrAssignmentConflict
	}
	return s.putLocked(ctx, record)
}

func (s *FileStore) PutOwned(ctx context.Context, record Record, owner string, epoch uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok, err := s.getLocked(ctx, record.Assignment.Key())
	if err != nil {
		return err
	}
	if !ok {
		return effect.ErrExecutionNotFound
	}
	if old.AssignmentDigest != record.AssignmentDigest {
		return ErrAssignmentConflict
	}
	if old.Owner != owner || old.FencingEpoch != epoch || old.LeaseUntilUnixMilli <= s.now().UnixMilli() {
		return ErrLeaseLost
	}
	return s.putLocked(ctx, record)
}

func (s *FileStore) TransitionOwned(ctx context.Context, key effect.AssignmentKey, owner string, epoch uint64, from, to effect.ExecutionStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok, err := s.getLocked(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		return effect.ErrExecutionNotFound
	}
	if r.Owner != owner || r.FencingEpoch != epoch || r.LeaseUntilUnixMilli <= s.now().UnixMilli() {
		return ErrLeaseLost
	}
	if r.State != from || !legalTransition(from, to) {
		return ErrStateConflict
	}
	r.State = to
	return s.putLocked(ctx, r)
}

func (s *FileStore) Acquire(ctx context.Context, key effect.AssignmentKey, owner string, ttl time.Duration) (Record, bool, error) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok, err := s.getLocked(ctx, key)
	if err != nil {
		return Record{}, false, err
	}
	if !ok {
		return Record{}, false, effect.ErrExecutionNotFound
	}
	if protocol.StatusTerminal(r.State) {
		return r, false, nil
	}
	expired := r.LeaseUntilUnixMilli <= now.UnixMilli()
	if r.Owner != "" && r.Owner != owner && !expired {
		return r, false, nil
	}
	if r.Owner != owner || r.FencingEpoch == 0 || expired {
		r.FencingEpoch++
	}
	r.Owner = owner
	r.LeaseUntilUnixMilli = now.Add(ttl).UnixMilli()
	if err := s.putLocked(ctx, r); err != nil {
		return Record{}, false, err
	}
	return r, true, nil
}

func (s *FileStore) Renew(ctx context.Context, key effect.AssignmentKey, owner string, epoch uint64, ttl time.Duration) error {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok, err := s.getLocked(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		return effect.ErrExecutionNotFound
	}
	if protocol.StatusTerminal(r.State) || r.Owner != owner || r.FencingEpoch != epoch {
		return ErrLeaseLost
	}
	r.LeaseUntilUnixMilli = now.Add(ttl).UnixMilli()
	return s.putLocked(ctx, r)
}

func (s *FileStore) LeaseOwned(ctx context.Context, key effect.AssignmentKey, owner string, epoch uint64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok, err := s.getLocked(ctx, key)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, effect.ErrExecutionNotFound
	}
	return !protocol.StatusTerminal(r.State) && r.Owner == owner && r.FencingEpoch == epoch && r.LeaseUntilUnixMilli > s.now().UnixMilli(), nil
}

func (s *FileStore) List(ctx context.Context) ([]Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return nil, err
	}
	out := make([]Record, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(s.root, entry.Name()))
		if err != nil {
			return nil, err
		}
		var r Record
		if err := json.Unmarshal(raw, &r); err != nil {
			// One corrupt record must not hide the adoptable records from
			// Reconcile; the control plane disposes irrecoverable ones.
			continue
		}
		out = append(out, r)
	}
	return out, nil
}

func (s *FileStore) getLocked(_ context.Context, key effect.AssignmentKey) (Record, bool, error) {
	raw, err := os.ReadFile(recordFile(s.root, key))
	if errors.Is(err, os.ErrNotExist) {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, err
	}
	var r Record
	if err := json.Unmarshal(raw, &r); err != nil {
		return Record{}, false, err
	}
	return r, true, nil
}

func (s *FileStore) putLocked(_ context.Context, record Record) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.root, ".record-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, recordFile(s.root, record.Assignment.Key())); err != nil {
		return err
	}
	// Persist the directory entry as well as the record bytes; otherwise a
	// power loss can lose the rename even though the file was fsynced.
	dir, err := os.Open(s.root)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
