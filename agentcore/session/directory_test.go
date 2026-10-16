package session_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
)

func directoryCreate(t *testing.T, store session.Store, ids ...session.SessionID) {
	t.Helper()
	for i, id := range ids {
		if _, err := store.Create(context.Background(), session.CreateRequest{
			SessionID: id, CreatedAtUnixMilli: int64(i + 10),
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func directoryIDs(records []session.SessionRecord) []session.SessionID {
	var ids []session.SessionID
	for _, record := range records {
		ids = append(ids, record.ID)
	}
	return ids
}

func TestDirectoryPaginationBoundaries(t *testing.T) {
	ctx := context.Background()
	store := filestoretest.Store(t)
	var directory session.Directory = store
	page, err := directory.ListSessions(ctx, "", 2)
	if err != nil || len(page.Items) != 0 || page.Next != "" || page.More {
		t.Fatalf("empty directory = %+v, %v", page, err)
	}
	directoryCreate(t, store, "s-c", "s-a", "s-e", "s-b", "s-d")
	tests := []struct {
		name  string
		after session.SessionID
		limit int
		ids   []session.SessionID
		next  session.SessionID
		more  bool
	}{
		{"first", "", 2, []session.SessionID{"s-a", "s-b"}, "s-b", true},
		{"exclusive_cursor", "s-b", 2, []session.SessionID{"s-c", "s-d"}, "s-d", true},
		{"partial_last", "s-d", 2, []session.SessionID{"s-e"}, "s-e", false},
		{"exact_last", "s-c", 2, []session.SessionID{"s-d", "s-e"}, "s-e", false},
		{"past_last", "s-e", 2, nil, "", false},
		{"absent_cursor_between_ids", "s-bb", 3, []session.SessionID{"s-c", "s-d", "s-e"}, "s-e", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			page, err := directory.ListSessions(ctx, tc.after, tc.limit)
			if err != nil || !reflect.DeepEqual(directoryIDs(page.Items), tc.ids) || page.Next != tc.next || page.More != tc.more {
				t.Fatalf("ListSessions(%q, %d) = %+v, %v; want ids=%v next=%q more=%v", tc.after, tc.limit, page, err, tc.ids, tc.next, tc.more)
			}
			for _, record := range page.Items {
				stored, err := store.Record(ctx, record.ID)
				if err != nil || !reflect.DeepEqual(record, stored) {
					t.Fatalf("directory did not return complete SessionRecord: got %+v; stored %+v, %v", record, stored, err)
				}
				if lease, held, err := store.LeaseOf(ctx, record.ID); err != nil || held {
					t.Fatalf("enumeration acquired write ownership: %+v, %v, %v", lease, held, err)
				}
			}
		})
	}
	// Enumeration is independent of current write ownership, too.
	h, err := store.Open(ctx, "s-c", session.OpenOptions{Owner: "another-owner"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(ctx)
	page, err = directory.ListSessions(ctx, "s-b", 1)
	if err != nil || !reflect.DeepEqual(directoryIDs(page.Items), []session.SessionID{"s-c"}) {
		t.Fatalf("owned Session disappeared from directory: %+v, %v", page, err)
	}
}

func TestDirectoryRejectsZeroNegativeLimitsAndCancelledContexts(t *testing.T) {
	store := filestoretest.Store(t)
	directoryCreate(t, store, "s")
	for _, limit := range []int{0, -1} {
		page, err := store.ListSessions(context.Background(), "", limit)
		if err == nil || len(page.Items) != 0 || page.Next != "" || page.More {
			t.Fatalf("limit %d = %+v, %v; want error and empty page", limit, page, err)
		}
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	page, err := store.ListSessions(cancelled, "", 1)
	if !errors.Is(err, context.Canceled) || len(page.Items) != 0 || page.Next != "" || page.More {
		t.Fatalf("cancelled directory read = %+v, %v", page, err)
	}
}

func TestDirectoryPagesReflectCreationBetweenReads(t *testing.T) {
	ctx := context.Background()
	store := filestoretest.Store(t)
	directoryCreate(t, store, "s-b", "s-d", "s-f")
	page, err := store.ListSessions(ctx, "", 1)
	if err != nil || page.Next != "s-b" || !page.More {
		t.Fatalf("first page = %+v, %v", page, err)
	}
	// Pages are not a snapshot. New IDs after the cursor are visible on
	// continuation; an earlier ID requires starting from the beginning.
	directoryCreate(t, store, "s-a", "s-c", "s-z")
	seen := directoryIDs(page.Items)
	for page.More {
		after := page.Next
		page, err = store.ListSessions(ctx, after, 2)
		if err != nil || page.Next <= after {
			t.Fatalf("continuation after %q = %+v, %v", after, page, err)
		}
		seen = append(seen, directoryIDs(page.Items)...)
	}
	if want := []session.SessionID{"s-b", "s-c", "s-d", "s-f", "s-z"}; !reflect.DeepEqual(seen, want) {
		t.Fatalf("continuation = %v; want %v", seen, want)
	}
	page, err = store.ListSessions(ctx, "", 10)
	want := []session.SessionID{"s-a", "s-b", "s-c", "s-d", "s-f", "s-z"}
	if err != nil || page.More || !reflect.DeepEqual(directoryIDs(page.Items), want) {
		t.Fatalf("fresh enumeration = %+v, %v; want %v", page, err, want)
	}
}
