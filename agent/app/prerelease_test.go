package app

import (
	"testing"

	"github.com/felinics/twilight/agent/workspace"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/turn"
)

// While module.Prerelease holds, every first-party module changes its wire
// shapes in place: no event type of the modules this application composes
// carries a codec history.
func TestPrereleaseModulesHaveNoHistory(t *testing.T) {
	if !module.Prerelease {
		t.Skip("released: event types may carry a codec history")
	}
	for _, m := range []module.ModuleDescriptor{chatlog.Module, sessionstore.Module, turn.Module, workspace.Module} {
		if err := module.NoHistory(&m); err != nil {
			t.Errorf("%s: %v", m.ID, err)
		}
	}
}
