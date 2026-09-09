package filestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/memohai/twilight/agent/es"
)

// frozenDir is the store directory under the root. The leading "%" keeps it
// disjoint from every session directory: encodeID only ever emits "%" as part
// of a %XX escape, so no SessionID encodes to a name starting with "%f".
const frozenDir = "%frozen"

// FrozenValues is the file-backed run.FrozenValueStore: one file per digest
// under <root>/%frozen, written atomically. Two instances over the same root
// see each other's bodies, so a restarted process can replay the frozen
// request of an interrupted ModelStep (RUN-WIR-4).
type FrozenValues struct {
	dir string
}

// NewFrozenValues opens (creating if needed) the frozen-value store under
// root. The root may be shared with New's session store.
func NewFrozenValues(root string) (*FrozenValues, error) {
	if root == "" {
		return nil, errors.New("filestore: frozen values: empty root")
	}
	dir := filepath.Join(root, frozenDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("filestore: frozen values: %w", err)
	}
	return &FrozenValues{dir: dir}, nil
}

func (f *FrozenValues) path(digest es.Digest) string {
	return filepath.Join(f.dir, encodeID(string(digest)))
}

// Put stores one body under its digest. It is idempotent; a re-Put whose
// bytes differ from the stored file reports corruption instead of silently
// keeping either copy (the digest names the content).
func (f *FrozenValues) Put(ctx context.Context, digest es.Digest, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if digest == "" {
		return errors.New("filestore: frozen values: empty digest")
	}
	path := f.path(digest)
	if existing, err := os.ReadFile(path); err == nil {
		if !bytes.Equal(existing, value) {
			return fmt.Errorf("filestore: frozen values: stored body for %s differs from the new value", digest)
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return writeAtomic(path, value)
}

// Get returns the stored body; a missing digest is (nil, false, nil).
func (f *FrozenValues) Get(ctx context.Context, digest es.Digest) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if digest == "" {
		return nil, false, nil
	}
	raw, err := os.ReadFile(f.path(digest))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return raw, true, nil
}

// Delete drops one body; the Runtime calls it when a withdrawn step ends the
// body's useful life. Best effort: a missing file is already deleted.
func (f *FrozenValues) Delete(digest es.Digest) {
	if digest == "" {
		return
	}
	_ = os.Remove(f.path(digest))
}
