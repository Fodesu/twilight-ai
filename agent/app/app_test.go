package app_test

import (
	"context"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
)

func TestBuildRemoteApplication(t *testing.T) {
	p, err := app.NewPresetFromDefinitions("model", nil)
	if err != nil {
		t.Fatal(err)
	}
	a, err := app.Build(durablePorts(t, app.Config{
		Executor: app.ExecutorConfig{Mode: app.ExecutorRemote, Endpoint: "http://executor"},
		Presets:  []app.Preset{{ID: "default", Value: p}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	ref, err := a.RegisterPreset("second", p)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Digest != mustDigest(p) {
		t.Fatalf("preset digest = %q, want %q", ref.Digest, mustDigest(p))
	}
	if p.Model != run.ModelRef("model") {
		t.Fatalf("model = %q", p.Model)
	}
}

func mustDigest(p preset.AgentPreset) run.Digest {
	d, err := preset.DigestPreset(&p)
	if err != nil {
		panic(err)
	}
	return d
}

// A Build that fails after the Worker exists releases it: the Worker's
// settlement hub is closed, so a subscription ends instead of waiting.
func TestBuildRollsBackOnFailure(t *testing.T) {
	p, err := app.NewPresetFromDefinitions("model", nil)
	if err != nil {
		t.Fatal(err)
	}
	hub := executor.NewSettlementHub("build-test", 8)
	// The local executor composes a Worker over the hub.
	cfg := durablePorts(t, app.Config{Executor: app.ExecutorConfig{Mode: app.ExecutorLocal},
		Worker: executor.WorkerOptions{Settlements: hub}})
	cfg.Presets = []app.Preset{{ID: "", Value: p}}
	if _, err := app.Build(cfg); err == nil {
		t.Fatal("Build with a nameless preset succeeded")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := hub.Settlements(ctx, "", 0, func(effect.Settlement) bool { return true }); err != nil {
		t.Fatalf("the Worker of a failed Build is still running: %v", err)
	}
}
