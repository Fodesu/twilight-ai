package filestore

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/session"
)

// A missing endpoint file must not delete a segment a child edge still
// names. Collect rebuilds the endpoints from the live path and then
// truncates to that path's right endpoint.
func TestCollectRepairsMissingCovers(t *testing.T) {
	ctx := context.Background()
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	header, err := store.Create(ctx, session.CreateRequest{SessionID: "p", CreatedAtUnixMilli: 1})
	if err != nil {
		t.Fatal(err)
	}
	w, err := store.Open(ctx, "p", session.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	appendOne := func(id string) {
		t.Helper()
		if _, err := w.Append(ctx, session.Proposal{CommitID: session.CommitID(id), Batches: []session.StreamBatch{{
			Stream: session.StreamRef{Domain: "chat"},
			Events: []session.Event{{Type: "twilight/x/n", Payload: jsonstable.MustParse(`{"n":1}`)}},
		}}}); err != nil {
			t.Fatal(err)
		}
	}
	appendOne("c0")
	appendOne("c1")
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(ctx, session.CreateRequest{SessionID: "c", CreatedAtUnixMilli: 2, Fork: &session.ForkOrigin{Session: "p", Seq: 0}}); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(filepath.Dir(store.LogPath("p")), coversFile)); err != nil {
		t.Fatal(err)
	}
	report, err := store.Delete(ctx, "p")
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Removed) != 0 || len(report.Truncated) != 0 {
		t.Fatalf("delete with missing covers = %+v, want the child edge to keep the segment", report)
	}
	commits, _, _, err := store.ReadSegment(ctx, header.ID, 0, 0)
	if err != nil || len(commits) != 2 {
		t.Fatalf("segment before repair = %d commits, %v", len(commits), err)
	}
	report, err = store.Collect(ctx)
	if err != nil || report.Truncated[header.ID] != 1 || len(report.Dropped[header.ID]) != 1 || report.Dropped[header.ID][0] != "c1" {
		t.Fatalf("collect repair = %+v %v", report, err)
	}
	commits, _, _, err = store.ReadSegment(ctx, header.ID, 0, 0)
	if err != nil || len(commits) != 1 || commits[0].CommitID != "c0" {
		t.Fatalf("segment after repair = %+v %v", commits, err)
	}
	page, err := store.ReadCommits(ctx, session.CommitReadRequest{SessionID: "c"})
	if err != nil || len(page.Commits) != 1 || page.Commits[0].CommitID != "c0" {
		t.Fatalf("child after repair = %+v %v", page.Commits, err)
	}
}
