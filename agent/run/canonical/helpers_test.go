package canonical

import (
	"github.com/felinics/twilight/agent/run"
)

func cj(raw string) run.CanonicalJSON { return run.MustParseCanonicalJSON(raw) }
