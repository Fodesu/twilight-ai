package runmod

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/felinics/twilight/agent/artifact"
	"github.com/felinics/twilight/agent/run"
)

// FrozenAuthority is the cas Authority of frozen model request bodies: the
// run layer's FrozenValueStore is one Authority of the artifact ContentStore,
// not a second content-addressed store (RUN-WIR-4).
const FrozenAuthority artifact.Authority = run.FrozenAuthority

// FrozenMediaType is the media type frozen bodies are stored under; the cas
// store binds it to the key, so every Put and Get of a body agrees on it.
const FrozenMediaType = "application/vnd.twilight.frozen-request+json"

// frozenValues realizes run.FrozenValueStore over a cas ContentStore. A body's
// digest is the SHA-256 of its bytes (run.EncodeFrozenRequest), which is
// exactly the cas Key, so no index maps digests to refs: Get rebuilds the Ref
// from the digest alone. Bodies are EventBound content: the fact that names
// them keeps them; retention is the artifact layer's policy, so the adapter
// offers no Delete.
type frozenValues struct {
	store artifact.ContentStore
}

// FrozenValues adapts a cas ContentStore serving FrozenAuthority to the run
// layer's FrozenValueStore port. A store of another Authority answers every
// Get with ErrUnauthorized, which surfaces as an error rather than a miss.
func FrozenValues(store artifact.ContentStore) run.FrozenValueStore {
	return &frozenValues{store: store}
}

// FrozenValuesInMemory is the in-process FrozenValueStore: a memory cas store
// under FrozenAuthority behind the adapter. Tests that simulate a process
// restart share one instance across Runtimes, as a durable store would share
// its files.
func FrozenValuesInMemory() run.FrozenValueStore {
	store, err := artifact.NewMemoryContentStore(FrozenAuthority, artifact.MemoryContentStoreOptions{})
	if err != nil {
		panic(err) // the authority is a constant; only an empty one fails
	}
	return FrozenValues(store)
}

func (f *frozenValues) Put(ctx context.Context, digest run.Digest, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ref, err := frozenRef(digest)
	if err != nil {
		return err
	}
	if key, _ := artifact.CASKey(value); key != ref.Key {
		return fmt.Errorf("agent: frozen values: body digests to %s, not to its name %s", key, digest)
	}
	_, err = f.store.Put(ctx, artifact.PutRequest{MediaType: FrozenMediaType, Reader: bytes.NewReader(value), Durability: artifact.EventBound})
	return err
}

func (f *frozenValues) Get(ctx context.Context, digest run.Digest) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if digest == "" {
		return nil, false, nil
	}
	ref, err := frozenRef(digest)
	if err != nil {
		return nil, false, err
	}
	rc, _, err := f.store.Open(ctx, ref)
	if err != nil {
		var aerr *artifact.Error
		if errors.As(err, &aerr) && aerr.Code == artifact.ErrNotFound {
			return nil, false, nil
		}
		return nil, false, err
	}
	defer rc.Close()
	raw, err := io.ReadAll(rc)
	if err != nil {
		return nil, false, err
	}
	return raw, true, nil
}

// frozenRef is the cas Ref of the body named by digest: Key and Integrity are
// both the digest, MediaType and Durability are the adapter's constants.
func frozenRef(digest run.Digest) (artifact.Ref, error) {
	algorithm, value, ok := strings.Cut(string(digest), ":")
	if !ok || algorithm != artifact.IntegritySHA256 || value == "" {
		return artifact.Ref{}, fmt.Errorf("agent: frozen values: digest %q is not sha256:<hex>", digest)
	}
	return artifact.Ref{Scheme: artifact.SchemeCAS, Authority: FrozenAuthority, Key: artifact.Key(digest), MediaType: FrozenMediaType,
		Integrity: &artifact.Integrity{Algorithm: algorithm, Value: value}, Durability: artifact.EventBound}, nil
}
