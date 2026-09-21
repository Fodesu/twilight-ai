package owner

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/felinics/twilight/agent/artifact"
	"github.com/felinics/twilight/agent/run/effect"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/filestore"
	runmod "github.com/felinics/twilight/agent/session/run"
)

type nopPort struct{ effect.ExecutionPort }

// OWN-PRT-3: durability is one declared bundle. A durable Session Store
// with a memory content store, or with explicitly passed memory binding and
// ledger stores, is refused until Artifacts.Ephemeral accepts it.
func TestNewRefusesMixedDurability(t *testing.T) {
	root := t.TempDir()
	store, err := filestore.New(root)
	if err != nil {
		t.Fatal(err)
	}
	content, err := filestore.NewContentStore(filepath.Join(root, "content"), runmod.FrozenAuthority, filestore.ContentStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	memoryBindings := artifact.NewMemoryBindingStore()
	cases := []struct {
		name string
		p    Ports
		ok   bool
	}{
		{"durable store, no content", Ports{Store: store, Executor: nopPort{}}, false},
		{"durable store and content, explicit memory bindings and ledger",
			Ports{Store: store, Content: content, Artifacts: Artifacts{Bindings: memoryBindings, Ledger: artifact.NewMemoryLedger(artifact.SetBuilder{Resolver: memoryBindings})}, Executor: nopPort{}}, false},
		{"durable content over a memory store", Ports{Store: session.NewMemoryStore(), Content: content, Executor: nopPort{}}, false},
		{"ephemeral opt-in", Ports{Store: store, Content: content, Artifacts: Artifacts{Ephemeral: true}, Executor: nopPort{}}, true},
		{"all memory", Ports{Store: session.NewMemoryStore(), Executor: nopPort{}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(tc.p)
			if tc.ok && err != nil {
				t.Fatalf("New = %v", err)
			}
			if !tc.ok && !errors.Is(err, ErrEphemeralArtifacts) {
				t.Fatalf("New = %v, want ErrEphemeralArtifacts", err)
			}
		})
	}
}
