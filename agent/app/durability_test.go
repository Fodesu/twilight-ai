package app_test

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agent/owner"
	executionstore "github.com/felinics/twilight/agent/executor/store"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/filestore"
)

// Build has no memory fallback for the Session Store or the Worker's record
// store, and the record store belongs to the durability bundle
// (OWN-PRT-3, RUN-EXE-8): a durable Store over a memory record store is
// refused until Artifacts.Ephemeral accepts it.
func TestBuildRequiresDeclaredStores(t *testing.T) {
	durable, err := filestore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	local := app.ExecutorConfig{Models: nil}
	cases := []struct {
		name string
		cfg  app.Config
		want error // nil: Build succeeds; app.ErrEphemeralExecutions: that error; otherwise any error
	}{
		{"no store", app.Config{Executions: executionstore.NewMemoryStore(), Executor: local}, errors.New("required")},
		{"no record store for the local worker", app.Config{Store: session.NewMemoryStore(), Executor: local}, errors.New("required")},
		{"durable store over memory records", app.Config{Store: durable, Executions: executionstore.NewMemoryStore(), Executor: local}, app.ErrEphemeralExecutions},
		{"durable store over memory records, ephemeral opt-in", app.Config{Store: durable, Executions: executionstore.NewMemoryStore(), Executor: local, Artifacts: owner.Artifacts{Ephemeral: true}}, nil},
		{"all memory", app.Config{Store: session.NewMemoryStore(), Executions: executionstore.NewMemoryStore(), Executor: local}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, err := app.Build(tc.cfg)
			switch {
			case tc.want == nil && err != nil:
				t.Fatalf("Build = %v", err)
			case tc.want == nil:
				_ = a.Close(context.Background())
			case errors.Is(tc.want, app.ErrEphemeralExecutions) && !errors.Is(err, app.ErrEphemeralExecutions):
				t.Fatalf("Build = %v, want ErrEphemeralExecutions", err)
			case err == nil:
				t.Fatal("Build succeeded without a required store")
			}
		})
	}
}
