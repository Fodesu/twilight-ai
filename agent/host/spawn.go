package host

import (
	"github.com/felinics/twilight/agent/orchestration"
	"github.com/felinics/twilight/agent/run/loop"
)

// SpawnOptions configures the subagent effect (Ports.Spawn, HST-SPN). The
// effect itself lives in orchestration.
type SpawnOptions = orchestration.SpawnOptions

// SpawnTool is the model-facing definition of the spawn tool for preset
// catalogs. Its Execute never runs: the spawn effect intercepts the tool's
// Assignments.
func SpawnTool(opts SpawnOptions) loop.ExecutableTool { return orchestration.SpawnTool(opts) }
