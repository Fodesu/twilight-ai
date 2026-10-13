package cloudtest_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agent/component/ownerservice"
	"github.com/felinics/twilight/agent/component/toolbackend"
	"github.com/felinics/twilight/agent/component/worker"
	"github.com/felinics/twilight/agent/config"
	"github.com/felinics/twilight/agent/environment/mock"
	"github.com/felinics/twilight/agent/workspace/workspacetest"
	"github.com/felinics/twilight/agentcore/executor/store/storetest"
)

type readyComponent interface {
	Ready(context.Context) error
	Close(context.Context) error
}

func TestComposedComponentsReady(t *testing.T) {
	ctx := context.Background()
	// Unreachable remote backends must not make local DB readiness fail.
	const unavailable = "http://127.0.0.1:1"
	for name, compose := range map[string]func(*testing.T) (readyComponent, error){
		"owner": func(t *testing.T) (readyComponent, error) {
			root := t.TempDir()
			return ownerservice.Compose(ctx, ownerservice.Config{
				Identity: config.Identity{Name: "owner"},
				Stores:   config.Store{SQLite: filepath.Join(root, "owner.db")},
				Sessions: ownerservice.Filestore{Root: filepath.Join(root, "sessions")},
				Content:  ownerservice.Filestore{Root: filepath.Join(root, "content")},
				Executor: unavailable, ToolBackend: unavailable,
				Presets: []ownerservice.Preset{{ID: "preset", Model: "model"}},
			})
		},
		"worker": func(t *testing.T) (readyComponent, error) {
			return worker.Compose(ctx, worker.Config{
				Identity:   config.Identity{Name: "worker"},
				Executions: config.Store{SQLite: filepath.Join(t.TempDir(), "worker.db")},
				Backends:   worker.Backends{Model: unavailable, Tool: unavailable},
			})
		},
		"tool backend": func(t *testing.T) (readyComponent, error) {
			return toolbackend.Compose(ctx, toolbackend.Config{
				Workspaces:   config.Store{SQLite: filepath.Join(t.TempDir(), "tools.db")},
				Environments: toolbackend.Environments{Mock: &toolbackend.MockProvider{Root: t.TempDir()}},
			})
		},
	} {
		t.Run(name, func(t *testing.T) {
			c, err := compose(t)
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			t.Cleanup(func() {
				if !closed {
					_ = c.Close(ctx)
				}
			})
			if err := c.Ready(ctx); err != nil {
				t.Fatalf("Ready with open DB: %v", err)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if err := c.Ready(canceled); !errors.Is(err, context.Canceled) {
				t.Fatalf("Ready with canceled context = %v, want context.Canceled", err)
			}
			if err := c.Close(ctx); err != nil {
				t.Fatal(err)
			}
			closed = true
			if err := c.Ready(ctx); err == nil {
				t.Fatal("Ready succeeded after closing DB")
			}
		})
	}
}

func TestNewComponentsReadyWithoutDBChecker(t *testing.T) {
	ctx := context.Background()
	w, err := worker.New(ctx, worker.Options{
		ID: "worker", Executions: &storetest.Map{},
		Routes: worker.Routes(worker.Backends{Model: "http://127.0.0.1:1"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close(ctx) })
	provider, err := mock.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tb, err := toolbackend.New(toolbackend.Options{
		Workspaces: &workspacetest.Map{}, Provider: provider, Backend: mock.Backend,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tb.Close(ctx) })
	// Ready needs no Application access when New supplies no DB checker.
	owner := ownerservice.New(nil, app.SessionOptions{})
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	for name, c := range map[string]interface{ Ready(context.Context) error }{
		"owner": owner, "worker": w, "tool backend": tb,
	} {
		t.Run(name, func(t *testing.T) {
			for _, checkCtx := range []context.Context{ctx, canceled} {
				if err := c.Ready(checkCtx); err != nil {
					t.Fatalf("Ready without a DB checker: %v", err)
				}
			}
		})
	}
}
