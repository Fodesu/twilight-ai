package filestore

import (
	"context"
	"testing"

	"github.com/memohai/twilight/agent/run"
)

var _ run.FrozenValueStore = (*FrozenValues)(nil)

// TestFrozenValues covers the store contract: round trip, idempotent re-put,
// conflicting re-put, missing digest, delete, and two instances over one root
// (the process-restart path RecoverModelExecution depends on).
func TestFrozenValues(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	fv, err := NewFrozenValues(root)
	if err != nil {
		t.Fatal(err)
	}
	const digest = run.Digest("sha256:0123abcd")
	body := []byte(`{"model":"m-1"}`)

	puts := []struct {
		name    string
		digest  run.Digest
		value   []byte
		wantErr bool
	}{
		{"first put", digest, body, false},
		{"idempotent re-put", digest, body, false},
		{"conflicting re-put", digest, []byte(`{"model":"other"}`), true},
		{"empty digest", "", body, true},
	}
	for _, tc := range puts {
		if err := fv.Put(ctx, tc.digest, tc.value); (err != nil) != tc.wantErr {
			t.Fatalf("%s: err = %v, wantErr %v", tc.name, err, tc.wantErr)
		}
	}

	got, ok, err := fv.Get(ctx, digest)
	if err != nil || !ok || string(got) != string(body) {
		t.Fatalf("get = %q %v %v", got, ok, err)
	}
	if _, ok, err := fv.Get(ctx, "sha256:missing"); err != nil || ok {
		t.Fatalf("missing digest = %v %v, want (false, nil)", ok, err)
	}

	// A second instance over the same root is the restarted process.
	fv2, err := NewFrozenValues(root)
	if err != nil {
		t.Fatal(err)
	}
	got2, ok, err := fv2.Get(ctx, digest)
	if err != nil || !ok || string(got2) != string(body) {
		t.Fatalf("second instance get = %q %v %v", got2, ok, err)
	}

	fv2.Delete(digest)
	if _, ok, _ := fv.Get(ctx, digest); ok {
		t.Fatal("deleted digest still readable through the first instance")
	}
	fv2.Delete(digest) // deleting a missing body is a no-op
}
