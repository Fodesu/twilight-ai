package runmod

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agent/artifact"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session/filestore"
	"github.com/felinics/twilight/sdk"
)

// frozenBody freezes one request and renders the bytes the store keeps.
func frozenBody(t *testing.T, text string) (run.Digest, []byte) {
	t.Helper()
	req, err := run.FreezeModelRequest(sdk.Request{Model: "m-1", Messages: []sdk.Message{sdk.UserMessage(text)}})
	if err != nil {
		t.Fatal(err)
	}
	digest, err := run.SchemaV1().Canonical.DigestRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := run.SchemaV1().Bodies.EncodeRequest(&req, digest)
	if err != nil {
		t.Fatal(err)
	}
	return digest, raw
}

func fileFrozen(t *testing.T, root string) run.FrozenValueStore {
	t.Helper()
	store, err := filestore.NewContentStore(root, FrozenAuthority, filestore.ContentStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := FrozenValues(store, artifact.NewMemoryBindingStore())
	if err != nil {
		t.Fatal(err)
	}
	return frozen
}

// The FrozenValueStore adapter over both cas ContentStores (RUN-WIR-4): the
// digest is the SHA-256 of the stored bytes, so Put and Get need no index; a
// body under a name it does not digest to is refused; and, for the file
// store, a second instance over the same root reads what the first wrote --
// the path a restarted process takes to replay an interrupted ModelStep.
func TestFrozenValuesOverContentStores(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		open func(t *testing.T, root string) run.FrozenValueStore
		file bool
	}{
		{"memory", func(*testing.T, string) run.FrozenValueStore {
			return FrozenValuesInMemory(artifact.NewMemoryBindingStore())
		}, false},
		{"file", fileFrozen, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			fv := tc.open(t, root)
			digest, raw := frozenBody(t, "hello")
			if err := fv.Put(ctx, digest, raw); err != nil {
				t.Fatalf("put: %v", err)
			}
			if err := fv.Put(ctx, digest, raw); err != nil {
				t.Fatalf("idempotent re-put: %v", err)
			}
			other, otherRaw := frozenBody(t, "other")
			if err := fv.Put(ctx, digest, otherRaw); err == nil {
				t.Fatal("a body under a digest it does not hash to must be refused")
			}
			got, ok, err := fv.Get(ctx, digest)
			if err != nil || !ok || string(got) != string(raw) {
				t.Fatalf("get = %q %v %v", got, ok, err)
			}
			if req, err := run.DecodeFrozenRequest(got, digest); err != nil || req.Model != "m-1" {
				t.Fatalf("decode = %+v %v", req, err)
			}
			if _, ok, err := fv.Get(ctx, other); err != nil || ok {
				t.Fatalf("missing digest = %v %v, want (false, nil)", ok, err)
			}
			if _, _, err := fv.Get(ctx, "not-a-digest"); err == nil {
				t.Fatal("a malformed digest must be an error, not a miss")
			}
			if tc.file {
				got2, ok, err := tc.open(t, root).Get(ctx, digest)
				if err != nil || !ok || string(got2) != string(raw) {
					t.Fatalf("second instance get = %q %v %v", got2, ok, err)
				}
			}
		})
	}
}

// A store serving another Authority answers with unauthorized: the adapter
// surfaces it as an error rather than a miss.
func TestFrozenValuesRejectsForeignAuthority(t *testing.T) {
	ctx := context.Background()
	store, err := artifact.NewMemoryContentStore("someone-else", artifact.MemoryContentStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	fv, err := FrozenValues(store, artifact.NewMemoryBindingStore())
	if err != nil {
		t.Fatal(err)
	}
	digest, raw := frozenBody(t, "hello")
	if err := fv.Put(ctx, digest, raw); err != nil {
		t.Fatalf("the store accepts the bytes; authority is checked on read: %v", err)
	}
	_, _, err = fv.Get(ctx, digest)
	var aerr *artifact.Error
	if !errors.As(err, &aerr) || aerr.Code != artifact.ErrUnauthorized {
		t.Fatalf("get from a foreign authority = %v, want unauthorized", err)
	}
}
