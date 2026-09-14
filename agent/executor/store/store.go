package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
)

// Record is the worker's durable execution record. Assignment is immutable
// and carries the execution payload; State and Outcome move monotonically to
// a terminal state. Owner and FencingEpoch protect takeover.
type Record struct {
	Assignment          effect.Assignment         `json:"assignment"`
	AssignmentDigest    run.Digest                `json:"assignmentDigest"`
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
	Acquire(context.Context, effect.AssignmentKey, string, time.Time, time.Duration) (Record, bool, error)
	Renew(context.Context, effect.AssignmentKey, string, uint64, time.Time, time.Duration) error
	List(context.Context) ([]Record, error)
}

// MemoryStore is useful for conformance tests and embedded deployments.
type MemoryStore struct {
	mu      sync.Mutex
	records map[effect.AssignmentKey]Record
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{records: make(map[effect.AssignmentKey]Record)}
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
	if old.Owner != owner || old.FencingEpoch != epoch {
		return ErrLeaseLost
	}
	s.records[record.Assignment.Key()] = record
	return nil
}

func (s *MemoryStore) Acquire(_ context.Context, key effect.AssignmentKey, owner string, now time.Time, ttl time.Duration) (Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[key]
	if !ok {
		return Record{}, false, effect.ErrExecutionNotFound
	}
	if protocol.StatusTerminal(r.State) {
		return r, false, nil
	}
	if r.Owner != "" && r.Owner != owner && r.LeaseUntilUnixMilli > now.UnixMilli() {
		return r, false, nil
	}
	if r.Owner != owner || r.FencingEpoch == 0 {
		r.FencingEpoch++
	}
	r.Owner = owner
	r.LeaseUntilUnixMilli = now.Add(ttl).UnixMilli()
	s.records[key] = r
	return r, true, nil
}

func (s *MemoryStore) Renew(_ context.Context, key effect.AssignmentKey, owner string, epoch uint64, now time.Time, ttl time.Duration) error {
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
}

func NewFileStore(root string) (*FileStore, error) {
	if root == "" {
		return nil, errors.New("executor/store: empty store root")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &FileStore{root: root}, nil
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
	if old.Owner != owner || old.FencingEpoch != epoch {
		return ErrLeaseLost
	}
	return s.putLocked(ctx, record)
}

func (s *FileStore) Acquire(ctx context.Context, key effect.AssignmentKey, owner string, now time.Time, ttl time.Duration) (Record, bool, error) {
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
	if r.Owner != "" && r.Owner != owner && r.LeaseUntilUnixMilli > now.UnixMilli() {
		return r, false, nil
	}
	if r.Owner != owner || r.FencingEpoch == 0 {
		r.FencingEpoch++
	}
	r.Owner = owner
	r.LeaseUntilUnixMilli = now.Add(ttl).UnixMilli()
	if err := s.putLocked(ctx, r); err != nil {
		return Record{}, false, err
	}
	return r, true, nil
}

func (s *FileStore) Renew(ctx context.Context, key effect.AssignmentKey, owner string, epoch uint64, now time.Time, ttl time.Duration) error {
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
			return nil, fmt.Errorf("executor/store: decode %s: %w", entry.Name(), err)
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
