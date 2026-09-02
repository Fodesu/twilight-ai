package run

import (
	"context"
	"errors"
	"sync"
	"time"
)

// MemoryStore is the in-process Store. Collection lock protects the Run map;
// each Run has an independent lock. Lease deadlines are stored; a zero
// deadline never expires.
type MemoryStore struct {
	mu   sync.RWMutex
	runs map[RunID]*memoryRun
}

type memoryRun struct {
	mu          sync.Mutex
	header      RunHeader
	state       MachineState
	revision    uint64
	watermark   uint64
	transitions map[CommandID]TransitionRecord
	log         []TransitionRecord
	leases      map[string]ExecutionLease
	startGrants map[CommandID]ExecutionGrant
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{runs: make(map[RunID]*memoryRun)}
}

func lockMemory(ctx context.Context, mu *sync.Mutex) error {
	for {
		if mu.TryLock() {
			return nil
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *MemoryStore) entry(runID RunID) (*memoryRun, error) {
	if s == nil {
		return nil, errors.New("agent: memory store: nil store")
	}
	s.mu.RLock()
	entry := s.runs[runID]
	s.mu.RUnlock()
	if entry == nil {
		return nil, ErrRunNotFound
	}
	return entry, nil
}

func (e *memoryRun) stored() StoredRun {
	return StoredRun{
		Header:      e.header,
		State:       e.state,
		Revision:    e.revision,
		Watermark:   e.watermark,
		Log:         e.log,
		Transitions: e.transitions,
		Leases:      e.leases,
		StartGrants: e.startGrants,
	}
}

func (e *memoryRun) apply(s StoredRun) {
	e.header = s.Header
	e.state = s.State
	e.revision = s.Revision
	e.watermark = s.Watermark
	e.log = s.Log
	e.transitions = s.Transitions
	e.leases = s.Leases
	e.startGrants = s.StartGrants
}

func (s *MemoryStore) Create(ctx context.Context, header RunHeader) (bool, RunHeader, error) {
	if err := checkContext(ctx); err != nil {
		return false, RunHeader{}, err
	}
	if s == nil {
		return false, RunHeader{}, errors.New("agent: memory store: nil store")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := checkContext(ctx); err != nil {
		return false, RunHeader{}, err
	}
	if existing := s.runs[header.RunID]; existing != nil {
		return false, cloneRunHeader(existing.header), nil
	}
	stored := cloneRunHeader(header)
	s.runs[header.RunID] = &memoryRun{
		header:      stored,
		state:       cloneMachineState(&stored.InitialState),
		transitions: make(map[CommandID]TransitionRecord),
		leases:      make(map[string]ExecutionLease),
		startGrants: make(map[CommandID]ExecutionGrant),
	}
	return true, cloneRunHeader(stored), nil
}

func (s *MemoryStore) View(ctx context.Context, id RunID) (StoredRun, error) {
	if err := checkContext(ctx); err != nil {
		return StoredRun{}, err
	}
	entry, err := s.entry(id)
	if err != nil {
		return StoredRun{}, err
	}
	if err := lockMemory(ctx, &entry.mu); err != nil {
		return StoredRun{}, err
	}
	defer entry.mu.Unlock()
	if err := checkContext(ctx); err != nil {
		return StoredRun{}, err
	}
	return cloneStoredRun(entry.stored()), nil
}

func (s *MemoryStore) Update(ctx context.Context, id RunID, fn func(*StoredRun) error) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	entry, err := s.entry(id)
	if err != nil {
		return err
	}
	if err := lockMemory(ctx, &entry.mu); err != nil {
		return err
	}
	defer entry.mu.Unlock()
	if err := checkContext(ctx); err != nil {
		return err
	}
	cur := cloneStoredRun(entry.stored())
	if err := fn(&cur); err != nil {
		return err
	}
	entry.apply(cur)
	return nil
}

func (s *MemoryStore) ListIDs(ctx context.Context) ([]RunID, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, errors.New("agent: memory store: nil store")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]RunID, 0, len(s.runs))
	for id := range s.runs {
		ids = append(ids, id)
	}
	return ids, nil
}
