package run

import (
	"context"
	"errors"
	"sync"
	"time"
)

// MemoryStore is the in-process Store. The collection lock protects the Run
// map; each Run has an independent lock. Lease deadlines are stored; a zero
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
	log         []TransitionRecord
	transitions map[CommandID]int // index into log
	leases      map[string]ExecutionLease
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

// locked runs fn with the Run's lock held.
func (s *MemoryStore) locked(ctx context.Context, id RunID, fn func(*memoryRun) error) error {
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
	return fn(entry)
}

func (e *memoryRun) head() RunHead {
	return cloneRunHead(&RunHead{Header: e.header, State: e.state, Revision: e.revision, Leases: e.leases})
}

//nolint:gocritic // hugeParam: Store.Create takes RunHeader by value as the persisted creation record.
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
		transitions: make(map[CommandID]int),
		leases:      make(map[string]ExecutionLease),
	}
	return true, cloneRunHeader(stored), nil
}

func (s *MemoryStore) LoadHead(ctx context.Context, id RunID) (RunHead, error) {
	var head RunHead
	err := s.locked(ctx, id, func(e *memoryRun) error {
		head = e.head()
		return nil
	})
	return head, err
}

func (s *MemoryStore) LoadLog(ctx context.Context, id RunID, from uint64) ([]TransitionRecord, error) {
	var out []TransitionRecord
	err := s.locked(ctx, id, func(e *memoryRun) error {
		for i := range e.log {
			if e.log[i].Revision >= from {
				out = append(out, cloneTransitionRecord(&e.log[i]))
			}
		}
		return nil
	})
	return out, err
}

func (s *MemoryStore) LoadRecord(ctx context.Context, id RunID) (RunHead, []TransitionRecord, error) {
	var head RunHead
	var log []TransitionRecord
	err := s.locked(ctx, id, func(e *memoryRun) error {
		head = e.head()
		log = cloneTransitionRecords(e.log)
		return nil
	})
	return head, log, err
}

type memoryTx struct{ e *memoryRun }

func (t memoryTx) Head() RunHead { return t.e.head() }

func (t memoryTx) LookupTransition(command CommandID) (TransitionRecord, bool, error) {
	i, ok := t.e.transitions[command]
	if !ok {
		return TransitionRecord{}, false, nil
	}
	return cloneTransitionRecord(&t.e.log[i]), true, nil
}

func (s *MemoryStore) Commit(ctx context.Context, id RunID, fn func(RunTx) (*Append, error)) error {
	if fn == nil {
		return errors.New("agent: memory store: nil commit fn")
	}
	return s.locked(ctx, id, func(e *memoryRun) error {
		a, err := fn(memoryTx{e})
		if err != nil || a == nil {
			return err
		}
		head := RunHead{Revision: e.revision}
		if err := ValidateAppend(id, &head, a); err != nil {
			return err
		}
		if _, dup := e.transitions[a.Transition.CommandID]; dup {
			return ErrCommandConflict
		}
		record := cloneTransitionRecord(&a.Transition)
		e.log = append(e.log, record)
		e.transitions[record.CommandID] = len(e.log) - 1
		e.state = cloneMachineState(&a.State)
		e.revision = a.Transition.Revision
		e.leases = applyLeaseOps(e.leases, a.Leases)
		return nil
	})
}

func (s *MemoryStore) RenewLease(ctx context.Context, id RunID, key string, grant ExecutionGrant, deadline time.Time) error {
	return s.locked(ctx, id, func(e *memoryRun) error {
		lease, ok := e.leases[key]
		if !ok || grant == "" || lease.Grant != grant {
			return ErrStaleRuntime
		}
		lease.Deadline = deadline
		e.leases[key] = lease
		return nil
	})
}

func (s *MemoryStore) ExpiredLeases(ctx context.Context, before time.Time) ([]ExpiredLease, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, errors.New("agent: memory store: nil store")
	}
	s.mu.RLock()
	entries := make([]*memoryRun, 0, len(s.runs))
	ids := make([]RunID, 0, len(s.runs))
	for id, e := range s.runs {
		entries = append(entries, e)
		ids = append(ids, id)
	}
	s.mu.RUnlock()
	var out []ExpiredLease
	for i, e := range entries {
		if err := lockMemory(ctx, &e.mu); err != nil {
			return nil, err
		}
		for key, lease := range e.leases {
			if !lease.Deadline.IsZero() && !lease.Deadline.After(before) {
				out = append(out, ExpiredLease{RunID: ids[i], Key: key, Lease: lease})
			}
		}
		e.mu.Unlock()
	}
	return out, nil
}

func (s *MemoryStore) ReplaceSnapshot(ctx context.Context, id RunID, revision uint64, state *MachineState) error {
	return s.locked(ctx, id, func(e *memoryRun) error {
		if e.revision != revision {
			return ErrAppendConflict
		}
		if state.RunID != id {
			return errors.New("agent: memory store: snapshot RunID mismatch")
		}
		e.state = cloneMachineState(state)
		return nil
	})
}
