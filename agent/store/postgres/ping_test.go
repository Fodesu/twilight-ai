package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/store/postgres/postgrestest"
)

func TestPingReflectsPoolAvailability(t *testing.T) {
	db := postgrestest.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := db.Ping(ctx); err != nil {
		t.Fatalf("open database: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(ctx); err == nil {
		t.Fatal("closed pool reported ready")
	}
}
