package filestore

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/felinics/twilight/agent/session"
)

// A session directory whose header names another Session, or whose ownership
// record cannot be read, is corrupt to every entry point: the Store must not
// serve it under the requested SessionID nor treat it as unowned.
func TestSessionDirectoryIntegrity(t *testing.T) {
	ctx := context.Background()
	create := func(t *testing.T, s *Store, sid session.SessionID) {
		t.Helper()
		if _, err := s.Create(ctx, session.CreateRequest{ProtocolVersion: session.ProtocolVersion1, SessionID: sid, CreatedAtUnixMilli: 1}); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct {
		name          string
		damage        func(t *testing.T, s *Store)
		headerCorrupt bool
	}{
		{"header of another session", func(t *testing.T, s *Store) {
			create(t, s, "b")
			raw, err := os.ReadFile(filepath.Join(s.dir("b"), headerFile))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(s.dir("a"), headerFile), raw, 0o644); err != nil {
				t.Fatal(err)
			}
		}, true},
		{"unreadable owner record", func(t *testing.T, s *Store) {
			if err := os.WriteFile(filepath.Join(s.dir("a"), ownerFile), []byte("{not json"), 0o644); err != nil {
				t.Fatal(err)
			}
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			create(t, s, "a")
			tc.damage(t, s)
			for _, takeover := range []bool{false, true} {
				if _, err := s.Open(ctx, "a", session.OpenOptions{Takeover: takeover}); !session.IsCode(err, session.ErrCorrupt) {
					t.Fatalf("Open(takeover=%v) = %v, want corrupt", takeover, err)
				}
			}
			_, err = s.Header(ctx, "a")
			if got := session.IsCode(err, session.ErrCorrupt); got != tc.headerCorrupt {
				t.Fatalf("Header = %v, corrupt=%v want %v", err, got, tc.headerCorrupt)
			}
		})
	}
}
