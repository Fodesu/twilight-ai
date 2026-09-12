package artifact_test

import (
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/artifact"
	"github.com/felinics/twilight/agent/artifact/artifacttest"
)

func TestMemoryConformance(t *testing.T) {
	artifacttest.Run(t, func(t *testing.T) artifacttest.Fixture {
		var mu sync.Mutex
		now := time.Unix(1_000_000, 0)
		clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
		bindings := artifact.NewMemoryBindingStore()
		return artifacttest.Fixture{
			Bindings: bindings,
			Ledger:   artifact.NewMemoryLedger(artifact.SetBuilder{Resolver: bindings}),
			NewContent: func(t *testing.T, authority artifact.Authority) artifact.ContentStore {
				store, err := artifact.NewMemoryContentStore(authority, artifact.MemoryContentStoreOptions{Now: clock, EphemeralTTL: time.Hour})
				if err != nil {
					t.Fatal(err)
				}
				return store
			},
			Advance: func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() },
		}
	})
}
