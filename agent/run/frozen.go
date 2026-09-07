package run

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// FrozenValueStore is the content-addressed side store for frozen bodies that
// facts name by digest only (RUN-WIR-4): today the ModelRequest of a Prepared
// step. Put is idempotent; a body may be dropped once the step that named it
// has settled, so readers must treat a missing body as a distinct condition.
type FrozenValueStore interface {
	Put(ctx context.Context, digest Digest, value []byte) error
	Get(ctx context.Context, digest Digest) ([]byte, bool, error)
}

// ErrFrozenValueMissing reports that a body named by a fact is no longer in
// the FrozenValueStore. Recovery cannot resend the request; the caller decides
// whether to retry the attempt with a fresh plan.
var ErrFrozenValueMissing = errors.New("agent: frozen value missing")

// MemoryFrozenValues is the in-process FrozenValueStore. Tests that simulate a
// process restart share one instance across Runtimes, as a durable adapter
// would share its table.
type MemoryFrozenValues struct {
	mu     sync.RWMutex
	values map[Digest][]byte
}

func NewMemoryFrozenValues() *MemoryFrozenValues {
	return &MemoryFrozenValues{values: make(map[Digest][]byte)}
}

func (m *MemoryFrozenValues) Put(ctx context.Context, digest Digest, value []byte) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if digest == "" {
		return errors.New("agent: frozen values: empty digest")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.values[digest]; exists {
		return nil
	}
	m.values[digest] = append([]byte(nil), value...)
	return nil
}

func (m *MemoryFrozenValues) Get(ctx context.Context, digest Digest) ([]byte, bool, error) {
	if err := checkContext(ctx); err != nil {
		return nil, false, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, ok := m.values[digest]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

// Delete drops one body; adapters call it when the naming step has settled.
func (m *MemoryFrozenValues) Delete(digest Digest) {
	m.mu.Lock()
	delete(m.values, digest)
	m.mu.Unlock()
}

// encodeFrozenRequest renders the canonical bytes stored for a request and
// verifies they digest to the name the fact will carry.
func encodeFrozenRequest(req *ModelRequest, want Digest) ([]byte, error) {
	got, err := digestRequestV1(*req)
	if err != nil {
		return nil, err
	}
	if got != want {
		return nil, fmt.Errorf("agent: frozen request: body digest %s does not match %s", got, want)
	}
	return marshalCanonical(req)
}

// decodeFrozenRequest restores a request body and checks it still digests to
// the name it was stored under.
func decodeFrozenRequest(raw []byte, want Digest) (ModelRequest, error) {
	var req ModelRequest
	if err := decodeStrictJSON(raw, &req); err != nil {
		return ModelRequest{}, fmt.Errorf("agent: frozen request: %w", err)
	}
	got, err := digestRequestV1(req)
	if err != nil {
		return ModelRequest{}, err
	}
	if got != want {
		return ModelRequest{}, fmt.Errorf("agent: frozen request: stored body digest %s does not match %s", got, want)
	}
	return req, nil
}
