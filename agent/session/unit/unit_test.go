package unit_test

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agent/artifact"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/extension"
	runmod "github.com/felinics/twilight/agent/session/run"
	"github.com/felinics/twilight/agent/session/unit"
	"github.com/felinics/twilight/agent/session/writer"
)

func newWriter(t *testing.T) writer.Writer {
	t.Helper()
	ctx := context.Background()
	registry, err := extension.BuildRegistry(session.ProtocolVersion1, chatlog.Module, runmod.Module)
	if err != nil {
		t.Fatal(err)
	}
	store := session.NewMemoryStore()
	if _, err := store.Create(ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: "s"}); err != nil {
		t.Fatal(err)
	}
	bindings := artifact.NewMemoryBindingStore()
	w, err := writer.OpenWriter(ctx, store, registry, writer.Admission{Bindings: bindings, Ledger: artifact.NewMemoryLedger(artifact.SetBuilder{Resolver: bindings})}, "s", session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func submitted(id chatlog.InputID) unit.Part {
	return unit.PartFunc(func(_ context.Context, _ writer.View, now int64) ([]writer.TypedBatch, error) {
		return []writer.TypedBatch{{Stream: chatlog.Stream, Events: []writer.TypedEvent{{
			Type: chatlog.TypeInputSubmitted, RecordedAtUnixMilli: now,
			Value: chatlog.InputSubmittedPayload{InputID: id, Content: run.MustParseCanonicalJSON(`{"text":"x"}`), SubmittedAtUnixMilli: now}}}}}, nil
	})
}

// Parts writing the same stream are merged into one batch in Part order,
// streams keep the order of their first Part, a Part error refuses the unit
// with nothing written, and a replay of the CommitID is already applied
// without preparing any Part.
func TestCommitMergesRefusesAndReplays(t *testing.T) {
	ctx := context.Background()
	w := newWriter(t)
	newRun, err := run.BuildNewRun("r1", "")
	if err != nil {
		t.Fatal(err)
	}
	prepared := 0
	counting := unit.PartFunc(func(ctx context.Context, v writer.View, now int64) ([]writer.TypedBatch, error) {
		prepared++
		return submitted("b").Prepare(ctx, v, now)
	})
	intent, _ := unit.Intent("u1")
	work := unit.Work{CommitID: "u1", Intent: intent, Parts: []unit.Part{submitted("a"), runmod.CreateRun(newRun, nil), counting}}
	res, err := unit.Commit(ctx, w, 7, work)
	if err != nil || res.Outcome != writer.CommitApplied {
		t.Fatalf("commit = %+v %v", res, err)
	}
	if len(res.Commit.Batches) != 2 || res.Commit.Batches[0].Stream != chatlog.Stream || len(res.Commit.Batches[0].Events) != 2 ||
		res.Commit.Batches[1].Stream != runmod.Stream("r1") {
		t.Fatalf("batches = %+v", res.Commit.Batches)
	}
	if res.Commit.Batches[0].Events[0].RecordedAtUnixMilli != 7 {
		t.Fatal("now not stamped")
	}
	again, err := unit.Commit(ctx, w, 8, work)
	if err != nil || again.Outcome != writer.CommitAlreadyApplied || again.Commit.Seq != res.Commit.Seq || prepared != 1 {
		t.Fatalf("replay = %+v %v prepared=%d", again, err, prepared)
	}
	boom := errors.New("boom")
	refused := unit.Work{CommitID: "u2", Intent: intent, Parts: []unit.Part{submitted("c"), unit.PartFunc(func(context.Context, writer.View, int64) ([]writer.TypedBatch, error) { return nil, boom })}}
	if _, err := unit.Commit(ctx, w, 9, refused); !errors.Is(err, boom) {
		t.Fatalf("refused unit = %v", err)
	}
	_, err = w.Commit(ctx, func(v writer.View) (*writer.SemanticGroup, error) {
		if v.Committed("u2") {
			t.Fatal("refused unit reached the ledger")
		}
		if _, ok := v.StreamHead(runmod.Stream("r1")); !ok {
			t.Fatal("run stream not indexed")
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unit.Commit(ctx, w, 1, unit.Work{CommitID: "u3", Intent: intent}); err == nil {
		t.Fatal("unit without events accepted")
	}
	if _, err := unit.Commit(ctx, w, 1, unit.Work{CommitID: "u4", Parts: []unit.Part{submitted("d")}}); err == nil {
		t.Fatal("unit without intent accepted")
	}
}

// A unit's Intent is sealed into the commit; a later unit with the same
// CommitID is judged by it without preparing a Part: same intent is already
// applied, a different intent is a conflict (EXT-WRT-2), and a unit that
// declares no intent can only be a replay.
func TestCommitJudgesReplayByIntent(t *testing.T) {
	ctx := context.Background()
	w := newWriter(t)
	intentA, _ := unit.Intent(map[string]string{"op": "a"})
	intentB, _ := unit.Intent(map[string]string{"op": "b"})
	res, err := unit.Commit(ctx, w, 1, unit.Work{CommitID: "u1", Intent: intentA, Parts: []unit.Part{submitted("a")}})
	if err != nil || res.Outcome != writer.CommitApplied || res.Commit.Intent != intentA {
		t.Fatalf("commit = %+v %v", res, err)
	}
	boom := unit.PartFunc(func(context.Context, writer.View, int64) ([]writer.TypedBatch, error) {
		t.Fatal("replay prepared a part")
		return nil, nil
	})
	if again, err := unit.Commit(ctx, w, 2, unit.Work{CommitID: "u1", Intent: intentA, Parts: []unit.Part{boom}}); err != nil || again.Outcome != writer.CommitAlreadyApplied {
		t.Fatalf("same intent = %+v %v", again, err)
	}
	if other, err := unit.Commit(ctx, w, 2, unit.Work{CommitID: "u1", Intent: intentB, Parts: []unit.Part{boom}}); err != nil || other.Outcome != writer.CommitConflict {
		t.Fatalf("different intent = %+v %v, want conflict", other, err)
	}
	// A CommitID sealed without an intent (a plain writer.Commit) cannot be
	// verified by a unit: conflict, not a silent replay.
	if _, err := w.Commit(ctx, func(writer.View) (*writer.SemanticGroup, error) {
		return &writer.SemanticGroup{CommitID: "plain", Batches: []writer.TypedBatch{{Stream: chatlog.Stream, Events: []writer.TypedEvent{{
			Type: chatlog.TypeInputSubmitted, Value: chatlog.InputSubmittedPayload{InputID: "p", Content: run.MustParseCanonicalJSON(`{"text":"x"}`)}}}}}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if none, err := unit.Commit(ctx, w, 2, unit.Work{CommitID: "plain", Intent: intentA, Parts: []unit.Part{boom}}); err != nil || none.Outcome != writer.CommitConflict {
		t.Fatalf("replay of an intent-less commit = %+v %v, want conflict", none, err)
	}
}
