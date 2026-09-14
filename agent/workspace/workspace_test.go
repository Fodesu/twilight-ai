package workspace_test

import (
	"context"
	"errors"
	"testing"

	runtimepkg "github.com/felinics/twilight/agent/runtime"
	"github.com/felinics/twilight/agent/workspace"
)

func TestWorkspaceTargetIsLogical(t *testing.T) {
	w := workspace.Workspace{ID: "ws-123", Project: "repo/foo", Base: "abc123"}
	target := w.Target()
	if target.Kind != "workspace" || target.ID != "ws-123" {
		t.Fatalf("target = %+v", target)
	}
}

type checkpointProvider struct {
	state    runtimepkg.StateRef
	restored runtimepkg.RestoreSpec
}

var _ runtimepkg.Provider = (*checkpointProvider)(nil)

type restoredEnvironment struct{ ref runtimepkg.EnvironmentRef }

func (e restoredEnvironment) Ref() runtimepkg.EnvironmentRef { return e.ref }
func (restoredEnvironment) Close(context.Context) error      { return nil }

func (*checkpointProvider) Create(context.Context, runtimepkg.Spec) (runtimepkg.Environment, error) {
	return nil, errors.New("test provider: create unavailable")
}

func (*checkpointProvider) Attach(context.Context, runtimepkg.EnvironmentRef) (runtimepkg.Environment, error) {
	return nil, errors.New("test provider: source environment removed")
}

func (p *checkpointProvider) Restore(_ context.Context, spec runtimepkg.RestoreSpec) (runtimepkg.Environment, error) {
	if spec.State != p.state {
		return nil, errors.New("test provider: checkpoint unavailable")
	}
	p.restored = spec
	return restoredEnvironment{ref: "restored-environment"}, nil
}

// The contract carries durable state and destination independently of an old environment.
func TestCheckpointRestoresIntoAnotherWorkspace(t *testing.T) {
	checkpoint := workspace.Checkpoint{Ref: "checkpoint", Workspace: "source", Backend: "test", StateRef: "durable-state"}
	provider := &checkpointProvider{state: checkpoint.StateRef}
	const old runtimepkg.EnvironmentRef = "deleted-environment"
	if _, err := provider.Attach(context.Background(), old); err == nil {
		t.Fatal("source environment still exists")
	}
	spec := runtimepkg.RestoreSpec{State: checkpoint.StateRef, Destination: runtimepkg.Spec{Subject: "fork", Base: "base-revision"}}
	env, err := provider.Restore(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if env.Ref() == old || provider.restored != spec {
		t.Fatalf("restore = %q %+v", env.Ref(), provider.restored)
	}
}
