package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/felinics/twilight/agent/store/sqlite"
)

func TestPing(t *testing.T) {
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "ping.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Ping(context.Background()); err != nil {
		t.Fatalf("Ping on open database: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := db.Ping(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Ping with canceled context = %v, want context.Canceled", err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(context.Background()); err == nil {
		t.Fatal("Ping on closed database succeeded")
	}
}
