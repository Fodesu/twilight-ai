package ref_test

import (
	"context"
	"sync"
	"testing"

	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/ref"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
)

// countingCache wraps the in-memory cache to count writes, so a test can see
// whether the assembly's interval reached the Writer (REF-MEM-2, EXT-PRJ-7).
type countingCache struct {
	inner *extension.MemoryProjectionCache
	mu    sync.Mutex
	saves int
}

func newCountingCache() *countingCache {
	return &countingCache{inner: extension.NewMemoryProjectionCache()}
}

func (c *countingCache) Load(ctx context.Context, sid session.SessionID, id extension.ProjectionID, v extension.ProjectionVersion) (jsonstable.Value, session.Head, bool, error) {
	return c.inner.Load(ctx, sid, id, v)
}

func (c *countingCache) Save(ctx context.Context, sid session.SessionID, id extension.ProjectionID, v extension.ProjectionVersion, state jsonstable.Value, through session.Head) error {
	c.mu.Lock()
	c.saves++
	c.mu.Unlock()
	return c.inner.Save(ctx, sid, id, v, state, through)
}

func (c *countingCache) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.saves
}

// TestAssemblyCacheEveryIsConfigurable pins the deployment's knob all the way
// to the Writer: a small interval writes entries as the log grows, a large one
// leaves the log uncached until Close, and Close writes regardless.
func TestAssemblyCacheEveryIsConfigurable(t *testing.T) {
	ctx := context.Background()
	for name, tc := range map[string]struct {
		every      session.Seq
		wantBefore bool
	}{
		"an interval of one row writes after the first commit": {every: 1, wantBefore: true},
		"a large interval defers to Close":                     {every: 1 << 40, wantBefore: false},
	} {
		t.Run(name, func(t *testing.T) {
			cache := newCountingCache()
			m, err := ref.New(ref.Options{ProjectionCache: cache, CacheEvery: tc.every})
			if err != nil {
				t.Fatal(err)
			}
			const sid session.SessionID = "s-interval"
			if err := m.EnsureSession(ctx, sid); err != nil {
				t.Fatal(err)
			}
			if _, err := m.SubmitInput(ctx, sid, "in-1", "hello"); err != nil {
				t.Fatal(err)
			}
			if got := cache.count() > 0; got != tc.wantBefore {
				t.Fatalf("wrote entries after one commit = %v, want %v (every=%d)", got, tc.wantBefore, tc.every)
			}
			// Close is always a refresh point, whatever the interval.
			if err := m.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if cache.count() == 0 {
				t.Error("Close wrote nothing: the final refresh is not policy-gated on the interval")
			}
		})
	}
}

// TestAssemblyNeverCachesTheMachineProjection is the other half of REF-MEM-2:
// the Writer leaves the machine projection to the Runtime's SnapshotPolicy, so
// no entry appears for it however small the interval is.
func TestAssemblyNeverCachesTheMachineProjection(t *testing.T) {
	ctx := context.Background()
	cache := newCountingCache()
	m, err := ref.New(ref.Options{ProjectionCache: cache, CacheEvery: 1})
	if err != nil {
		t.Fatal(err)
	}
	const sid session.SessionID = "s-machine"
	if err := m.EnsureSession(ctx, sid); err != nil {
		t.Fatal(err)
	}
	if _, err := m.SubmitInput(ctx, sid, "in-1", "hello"); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if cache.count() == 0 {
		t.Fatal("no projection was cached at all, so the check below proves nothing")
	}
	machine := extension.ProjectionID("twilight/run/machine")
	if _, _, ok, err := cache.Load(ctx, sid, machine, 1); err != nil || ok {
		t.Errorf("machine projection entry: ok=%v err=%v, want absent", ok, err)
	}
}
