package writer

import (
	"context"
	"sync"
	"testing"

	"github.com/felinics/twilight/agent/session"
)

// recordingObserver keeps every notification in the order it arrived.
type recordingObserver struct {
	mu     sync.Mutex
	groups [][]session.SessionEvent
}

func (o *recordingObserver) Committed(_ context.Context, _ session.SessionID, rows []session.SessionEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.groups = append(o.groups, append([]session.SessionEvent(nil), rows...))
}

type panickingObserver struct{}

func (panickingObserver) Committed(context.Context, session.SessionID, []session.SessionEvent) {
	panic("observer failed")
}

// EXT-WRT-7: every applied group reaches the observers once, in commit order,
// with the sealed rows; rejected and replayed commits notify nothing; a
// panicking observer neither fails the Commit nor starves the next observer.
func TestCommitObserversSeeAppliedGroupsInOrder(t *testing.T) {
	f := newCacheFixture(t)
	rec := &recordingObserver{}
	w := f.open(t, WritersConfig{Observers: []CommitObserver{panickingObserver{}, rec}})

	f.commit(t, w, "c1", "one", "two")
	f.commit(t, w, "c2", "three")
	// Replay: already applied, no notification.
	res, err := w.Commit(context.Background(), func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "c1", Events: []TypedEvent{{Type: tpfx("k") + "row", Value: notePayload{Text: "one"}}, {Type: tpfx("k") + "row", Value: notePayload{Text: "two"}}}}, nil
	})
	if err != nil || res.Outcome != CommitAlreadyApplied {
		t.Fatalf("replay = %v %v", res.Outcome, err)
	}
	// Rejected: unknown event type, no notification.
	res, err = w.Commit(context.Background(), func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "c3", Events: []TypedEvent{{Type: "twilight/nope/x", Value: notePayload{Text: "x"}}}}, nil
	})
	if err != nil || res.Outcome != CommitInvalid {
		t.Fatalf("invalid = %v %v", res.Outcome, err)
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.groups) != 2 {
		t.Fatalf("notifications = %d, want 2", len(rec.groups))
	}
	rows := f.rows(t)
	var seq session.Seq
	for i, g := range rec.groups {
		for _, r := range g {
			if r.Seq != seq || r.Digest != rows[seq].Digest || r.CommitID != rows[seq].CommitID {
				t.Fatalf("notification %d row %d = %+v, want log row %+v", i, r.Seq, r, rows[seq])
			}
			seq++
		}
	}
	if seq != session.Seq(len(rows)) {
		t.Fatalf("observed %d rows, log has %d", seq, len(rows))
	}
}

// Concurrent committers are notified in the order their groups landed: the
// observer sees a strictly increasing Seq sequence.
func TestCommitObserverOrderUnderConcurrency(t *testing.T) {
	f := newCacheFixture(t)
	rec := &recordingObserver{}
	w := f.open(t, WritersConfig{Observers: []CommitObserver{rec}})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			f.commit(t, w, "p"+string(rune('a'+i)), "n")
		}(i)
	}
	wg.Wait()
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.groups) != 16 {
		t.Fatalf("notifications = %d, want 16", len(rec.groups))
	}
	for i := 1; i < len(rec.groups); i++ {
		if rec.groups[i][0].Seq <= rec.groups[i-1][0].Seq {
			t.Fatalf("notification %d (seq %d) arrived after seq %d", i, rec.groups[i][0].Seq, rec.groups[i-1][0].Seq)
		}
	}
}
