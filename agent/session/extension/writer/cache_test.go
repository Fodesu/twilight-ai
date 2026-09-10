package writer

import (
	"context"
	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
	"sync"
	"testing"
)

// This file covers EXT-PRJ-3: a projection's folded state living in the
// extension.ProjectionCache, and a reopening Writer resuming from it instead of folding
// the log again.

const (
	alphaID = extension.ProjectionID("twilight/k/alpha")
	betaID  = extension.ProjectionID("twilight/k/beta")
)

// applyCounter records how many events each projection folded, which is how a
// test tells a fold that started from a cache entry from a full one.
type applyCounter struct {
	mu    sync.Mutex
	calls map[extension.ProjectionID]int
}

func newApplyCounter() *applyCounter { return &applyCounter{calls: map[extension.ProjectionID]int{}} }

func (c *applyCounter) inc(id extension.ProjectionID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls[id]++
}

func (c *applyCounter) get(id extension.ProjectionID) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[id]
}

func (c *applyCounter) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = map[extension.ProjectionID]int{}
}

// cacheModule declares two projections over one event type. alpha stands for a
// projection the Writer refreshes; beta for one whose owning component does.
func cacheModule(c *applyCounter) extension.ModuleDescriptor {
	typ := tpfx("k") + "row"
	mk := func(id extension.ProjectionID) extension.ProjectionDefinition {
		return extension.ProjectionDefinition{
			ID: id, Version: 1, Consumes: []session.EventType{typ},
			Initial: func() (any, error) { return noteState{}, nil },
			Apply: func(state any, e extension.DecodedEvent) (any, error) {
				c.inc(id)
				s := state.(noteState)
				s.Notes = append(append([]string(nil), s.Notes...), e.Value.(notePayload).Text)
				return s, nil
			},
			StateCodec: extension.JSONStateCodec[noteState]{},
		}
	}
	return extension.ModuleDescriptor{Source: extension.SourceTwilight, ID: "k",
		Events:      []extension.EventDefinition{{Type: typ, Current: 1, Codecs: map[extension.PayloadVersion]extension.PayloadCodec{1: extension.JSONCodec[notePayload]{}}}},
		Projections: []extension.ProjectionDefinition{mk(alphaID), mk(betaID)}}
}

type cacheFixture struct {
	store    *session.MemoryStore
	registry *extension.Registry
	cache    *extension.MemoryProjectionCache
	counter  *applyCounter
}

func newCacheFixture(t testing.TB) *cacheFixture {
	t.Helper()
	f := &cacheFixture{store: session.NewMemoryStore(), cache: extension.NewMemoryProjectionCache(), counter: newApplyCounter()}
	registry, err := extension.BuildRegistry(session.ProtocolVersion1, cacheModule(f.counter))
	if err != nil {
		t.Fatal(err)
	}
	f.registry = registry
	if _, err := f.store.Create(context.Background(), session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: "s"}); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *cacheFixture) open(t testing.TB, cfg WritersConfig) Writer {
	t.Helper()
	w, err := openWriter(context.Background(), f.store, f.registry, Admission{}, "s", session.OpenOptions{}, cfg)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	return w
}

// commit appends one group; each text becomes one event.
func (f *cacheFixture) commit(t testing.TB, w Writer, id string, texts ...string) {
	t.Helper()
	res, err := w.Commit(context.Background(), func(View) (*SemanticGroup, error) {
		g := &SemanticGroup{CommitID: session.CommitID(id)}
		for _, tx := range texts {
			g.Events = append(g.Events, TypedEvent{Type: tpfx("k") + "row", Value: notePayload{Text: tx}})
		}
		return g, nil
	})
	if err != nil {
		t.Fatalf("commit %s: %v", id, err)
	}
	if res.Outcome != CommitApplied {
		t.Fatalf("commit %s: outcome %s (%s)", id, res.Outcome, res.Detail)
	}
}

func (f *cacheFixture) notes(t testing.TB, w Writer, id extension.ProjectionID) []string {
	t.Helper()
	state, _, err := w.Projections().Load(context.Background(), "s", id, 1)
	if err != nil {
		t.Fatalf("load %s: %v", id, err)
	}
	return state.(noteState).Notes
}

// rows reads the committed log, which is where the digests a cache entry must
// record come from.
func (f *cacheFixture) rows(t *testing.T) []session.SessionEvent {
	t.Helper()
	page, err := f.store.Read(context.Background(), session.ReadRequest{SessionID: "s"})
	if err != nil {
		t.Fatal(err)
	}
	return page.Events
}

// encodeState builds the cached form of a projection state.
func (f *cacheFixture) encodeState(t *testing.T, notes ...string) jsonstable.Value {
	t.Helper()
	v, err := extension.JSONStateCodec[noteState]{}.Encode(noteState{Notes: notes})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func sameNotes(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestWriterCachesAtCloseAndResumesEverything: a clean Close leaves an entry at
// the end of the log, so reopening folds nothing.
func TestWriterCachesAtCloseAndResumesEverything(t *testing.T) {
	ctx := context.Background()
	f := newCacheFixture(t)
	w := f.open(t, WritersConfig{Cache: f.cache})
	f.commit(t, w, "c1", "n1")
	f.commit(t, w, "c2", "n2")
	f.commit(t, w, "c3", "n3")
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []extension.ProjectionID{alphaID, betaID} {
		_, through, ok, err := f.cache.Load(ctx, "s", id, 1)
		if err != nil || !ok {
			t.Fatalf("%s: entry after Close: ok=%v err=%v", id, ok, err)
		}
		if through.Next != 3 {
			t.Fatalf("%s: Close left the entry at %d, want 3", id, through.Next)
		}
	}

	f.counter.reset()
	reopened := f.open(t, WritersConfig{Cache: f.cache})
	for _, id := range []extension.ProjectionID{alphaID, betaID} {
		if n := f.counter.get(id); n != 0 {
			t.Errorf("%s folded %d events, want 0 when the entry covers the whole log", id, n)
		}
		if got := f.notes(t, reopened, id); !sameNotes(got, []string{"n1", "n2", "n3"}) {
			t.Errorf("%s notes = %v, want [n1 n2 n3]", id, got)
		}
	}
}

// TestWriterResumesOnlyTheUncoveredTail is the point of the mechanism: an entry
// behind the head means only the remainder is folded.
func TestWriterResumesOnlyTheUncoveredTail(t *testing.T) {
	ctx := context.Background()
	f := newCacheFixture(t)
	w := f.open(t, WritersConfig{Cache: f.cache})
	f.commit(t, w, "c1", "n1")
	f.commit(t, w, "c2", "n2")
	f.commit(t, w, "c3", "n3")
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	rows := f.rows(t)
	if len(rows) != 3 {
		t.Fatalf("log has %d rows, want 3", len(rows))
	}
	// Rewind alpha's entry to cover the first two rows only.
	if err := f.cache.Save(ctx, "s", alphaID, 1, f.encodeState(t, "n1", "n2"), session.Head{Next: 2, Digest: rows[1].Digest}); err != nil {
		t.Fatal(err)
	}

	f.counter.reset()
	reopened := f.open(t, WritersConfig{Cache: f.cache})
	if n := f.counter.get(alphaID); n != 1 {
		t.Errorf("alpha folded %d events, want 1 (only the covered tail)", n)
	}
	if n := f.counter.get(betaID); n != 0 {
		t.Errorf("beta folded %d events, want 0", n)
	}
	if got := f.notes(t, reopened, alphaID); !sameNotes(got, []string{"n1", "n2", "n3"}) {
		t.Errorf("alpha notes = %v, want [n1 n2 n3]", got)
	}
}

// TestWriterRejectsUnusableCacheEntries: every entry that cannot be trusted
// falls back to folding the whole log, and never fails the open.
func TestWriterRejectsUnusableCacheEntries(t *testing.T) {
	ctx := context.Background()
	// A log whose first group holds two events, so a row inside a group exists.
	f := newCacheFixture(t)
	w := f.open(t, WritersConfig{Cache: f.cache})
	f.commit(t, w, "c1", "n1", "n2")
	f.commit(t, w, "c2", "n3")
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	rows := f.rows(t)
	if len(rows) != 3 || rows[0].Last || !rows[1].Last {
		t.Fatalf("fixture log is not shaped as expected: %+v", rows)
	}
	garbage, err := jsonstable.FromValue(map[string]any{"notes": 7})
	if err != nil {
		t.Fatal(err)
	}

	cases := map[string]struct {
		through session.Head
		state   jsonstable.Value
	}{
		"ahead of the log":  {through: session.Head{Next: 9, Digest: rows[2].Digest}},
		"empty head":        {through: session.Head{}},
		"unknown digest":    {through: session.Head{Next: 3, Digest: "sha256:0000"}},
		"inside a group":    {through: session.Head{Next: 1, Digest: rows[0].Digest}},
		"undecodable state": {through: session.Head{Next: 3, Digest: rows[2].Digest}, state: garbage},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			state := tc.state
			if state.IsZero() {
				state = f.encodeState(t, "n1", "n2", "n3")
			}
			if err := f.cache.Save(ctx, "s", alphaID, 1, state, tc.through); err != nil {
				t.Fatal(err)
			}
			f.counter.reset()
			reopened, err := openWriter(ctx, f.store, f.registry, Admission{}, "s", session.OpenOptions{Takeover: true}, WritersConfig{Cache: f.cache})
			if err != nil {
				t.Fatalf("open with an unusable entry: %v", err)
			}
			// A rejected entry means the whole log is folded again: three events.
			if n := f.counter.get(alphaID); n != 3 {
				t.Errorf("alpha folded %d events, want 3 (a full fold)", n)
			}
			if got := f.notes(t, reopened, alphaID); !sameNotes(got, []string{"n1", "n2", "n3"}) {
				t.Errorf("alpha notes = %v, want [n1 n2 n3]", got)
			}
			if err := reopened.Close(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestWriterCachePolicyGovernsWritingButNotReading separates the two halves of
// the contract: a policy only decides who writes an entry, while a Writer
// always starts from an entry it finds.
func TestWriterCachePolicyGovernsWritingButNotReading(t *testing.T) {
	ctx := context.Background()
	f := newCacheFixture(t)
	// A policy that declines alpha and defers for beta.
	policy := extension.CacheEvery(1).Exclude(alphaID)

	w := f.open(t, WritersConfig{Cache: f.cache, CachePolicy: policy})
	f.commit(t, w, "c1", "n1")
	f.commit(t, w, "c2", "n2")
	if _, _, ok, _ := f.cache.Load(ctx, "s", alphaID, 1); ok {
		t.Error("alpha has an entry although the policy declines it")
	}
	_, through, ok, err := f.cache.Load(ctx, "s", betaID, 1)
	if err != nil || !ok || through.Next != 2 {
		t.Fatalf("beta entry: ok=%v through=%d err=%v, want an entry at 2", ok, through.Next, err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, _ := f.cache.Load(ctx, "s", alphaID, 1); ok {
		t.Error("alpha has an entry after Close although the policy declines it")
	}

	// An entry alpha's owner wrote is still used, policy or not.
	rows := f.rows(t)
	if err := f.cache.Save(ctx, "s", alphaID, 1, f.encodeState(t, "n1", "n2"), session.Head{Next: 2, Digest: rows[1].Digest}); err != nil {
		t.Fatal(err)
	}
	f.counter.reset()
	reopened := f.open(t, WritersConfig{Cache: f.cache, CachePolicy: policy})
	if n := f.counter.get(alphaID); n != 0 {
		t.Errorf("alpha folded %d events, want 0: a declined projection is still started from", n)
	}
	if got := f.notes(t, reopened, alphaID); !sameNotes(got, []string{"n1", "n2"}) {
		t.Errorf("alpha notes = %v, want [n1 n2]", got)
	}
}

// TestCacheEveryBoundsHowFarBehindAnEntryFalls pins the invariant a deployment
// relies on: between refreshes a projection's entry is at most n rows behind.
func TestCacheEveryBoundsHowFarBehindAnEntryFalls(t *testing.T) {
	ctx := context.Background()
	f := newCacheFixture(t)
	w := f.open(t, WritersConfig{Cache: f.cache, CachePolicy: extension.CacheEvery(3)})
	// The first two rows are inside the interval: nothing is written yet.
	f.commit(t, w, "c1", "n1")
	f.commit(t, w, "c2", "n2")
	if _, _, ok, _ := f.cache.Load(ctx, "s", alphaID, 1); ok {
		t.Error("an entry was written before the interval elapsed")
	}
	f.commit(t, w, "c3", "n3")
	_, through, ok, err := f.cache.Load(ctx, "s", alphaID, 1)
	if err != nil || !ok || through.Next != 3 {
		t.Fatalf("entry after three rows: ok=%v through=%d err=%v, want an entry at 3", ok, through.Next, err)
	}
	// Close refreshes regardless of the interval.
	f.commit(t, w, "c4", "n4")
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	_, through, _, err = f.cache.Load(ctx, "s", alphaID, 1)
	if err != nil || through.Next != 4 {
		t.Fatalf("entry after Close: through=%d err=%v, want 4", through.Next, err)
	}
}

// TestWriterWithoutCacheFoldsEverything is the unchanged deployment: no cache
// configured means no entry is written and none is started from.
func TestWriterWithoutCacheFoldsEverything(t *testing.T) {
	ctx := context.Background()
	f := newCacheFixture(t)
	w := f.open(t, WritersConfig{})
	f.commit(t, w, "c1", "n1")
	f.commit(t, w, "c2", "n2")
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, _ := f.cache.Load(ctx, "s", alphaID, 1); ok {
		t.Error("an entry was written although no cache was configured")
	}
	f.counter.reset()
	reopened := f.open(t, WritersConfig{})
	if n := f.counter.get(alphaID); n != 2 {
		t.Errorf("alpha folded %d events, want 2", n)
	}
	if got := f.notes(t, reopened, alphaID); !sameNotes(got, []string{"n1", "n2"}) {
		t.Errorf("alpha notes = %v, want [n1 n2]", got)
	}
}

// TestCoversGroupBoundary pins the validation that keeps a half-applied group
// from being started from (EXT-PRJ-1).
func TestCoversGroupBoundary(t *testing.T) {
	rows := []session.SessionEvent{
		{Seq: 0, Digest: "d0", Last: false},
		{Seq: 1, Digest: "d1", Last: true},
		{Seq: 2, Digest: "d2", Last: true},
	}
	cases := map[string]struct {
		through session.Head
		want    bool
	}{
		"end of the first group":  {session.Head{Next: 2, Digest: "d1"}, true},
		"end of the log":          {session.Head{Next: 3, Digest: "d2"}, true},
		"inside a group":          {session.Head{Next: 1, Digest: "d0"}, false},
		"empty":                   {session.Head{}, false},
		"past the log":            {session.Head{Next: 4, Digest: "d2"}, false},
		"digest does not match":   {session.Head{Next: 2, Digest: "nope"}, false},
		"seq does not match head": {session.Head{Next: 99, Digest: "d2"}, false},
	}
	for name, tc := range cases {
		if got := coversGroupBoundary(rows, tc.through); got != tc.want {
			t.Errorf("%s: coversGroupBoundary = %v, want %v", name, got, tc.want)
		}
	}
}
