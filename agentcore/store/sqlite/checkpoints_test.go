package sqlite_test

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/checkpoint"
	"github.com/felinics/twilight/agentcore/store/sqlite/sqlitetest"
)

func TestCheckpointStore(t *testing.T) {
	ctx := context.Background()
	cp := sqlitetest.Open(t).Checkpoints()
	if next, ok, err := cp.Load(ctx, "relay", "session/s1"); err != nil || ok || next != 0 {
		t.Fatalf("unsaved checkpoint = %d %v %v, want 0 false", next, ok, err)
	}
	steps := []struct {
		name    string
		next    uint64
		wantErr error
		wantAt  uint64
	}{
		{"first save", 3, nil, 3},
		{"forward", 7, nil, 7},
		{"same position is idempotent", 7, nil, 7},
		{"backwards is refused", 5, checkpoint.ErrRewind, 7},
	}
	for _, s := range steps {
		t.Run(s.name, func(t *testing.T) {
			if err := cp.Save(ctx, "relay", "session/s1", s.next); !errors.Is(err, s.wantErr) {
				t.Fatalf("save = %v, want %v", err, s.wantErr)
			}
			if next, ok, err := cp.Load(ctx, "relay", "session/s1"); err != nil || !ok || next != s.wantAt {
				t.Fatalf("load = %d %v %v, want %d", next, ok, err, s.wantAt)
			}
		})
	}
	// Consumers and ledgers are independent keys.
	if err := cp.Save(ctx, "projection", "session/s1", 1); err != nil {
		t.Fatal(err)
	}
	if err := cp.Save(ctx, "relay", "session/s2", 1); err != nil {
		t.Fatal(err)
	}
	if next, _, _ := cp.Load(ctx, "relay", "session/s1"); next != 7 {
		t.Fatalf("relay/s1 moved to %d", next)
	}
}
