package filestore_test

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/filestore"
)

var update = flag.Bool("update", false, "rewrite the golden testdata files from the current wire")

// TestLogFileGolden freezes the exact on-disk bytes of a stream: header.json
// and log.jsonl produced from fixed inputs. It is the end-of-line fixture for
// the kernel wire — a diff here is a wire change. Regenerate deliberately with
// `go test -run TestLogFileGolden -update ./agent/session/filestore` and record
// the change in agent-session.md.
func TestLogFileGolden(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := filestore.New(root)
	if err != nil {
		t.Fatal(err)
	}
	const sid session.SessionID = "golden"
	if _, err := store.Create(ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: sid, CreatedAtUnixMilli: 1}); err != nil {
		t.Fatal(err)
	}
	w, err := store.Open(ctx, sid, session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ev := func(typ, payload string, at int64) session.UncommittedEvent {
		return session.UncommittedEvent{Type: session.EventType(typ), Payload: jsonstable.MustParse(payload), RecordedAtUnixMilli: at}
	}
	if _, err := w.Append(ctx, session.Group{CommitID: "c1", Events: []session.UncommittedEvent{
		ev("twilight/x/a", `{"a":1}`, 1),
		ev("twilight/x/b", `{"b":[1,2]}`, 2),
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(ctx, session.Group{CommitID: "c2", Events: []session.UncommittedEvent{
		{Type: "twilight/x/c", Payload: jsonstable.MustParse(`{}`), RecordedAtUnixMilli: 3, SourceSeqs: []session.Seq{0}, Ignorable: true},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}

	compare(t, filepath.Join(filepath.Dir(store.LogPath(sid)), "header.json"), "golden_header.json")
	compare(t, store.LogPath(sid), "golden_log.jsonl")
}

func compare(t *testing.T, gotPath, goldenName string) {
	t.Helper()
	got, err := os.ReadFile(gotPath)
	if err != nil {
		t.Fatal(err)
	}
	goldenPath := filepath.Join("testdata", goldenName)
	if *update {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenPath, got, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("missing golden file (regenerate with -update after a deliberate wire change): %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("%s drifted from the on-disk golden — an intentional wire change must be regenerated with -update and recorded in agent-session.md:\n got:\n%s\nwant:\n%s", goldenName, got, want)
	}
}
