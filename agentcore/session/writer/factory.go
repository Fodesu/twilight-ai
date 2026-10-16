package writer

import (
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
)

// Factory composes Writers over caller-owned session handles. It holds no
// session map and never opens, renews or releases a handle. Configuration must
// not be mutated while Bind is running. Bind one Writer per acquired handle.
type Factory struct {
	Store     session.Store
	Registry  *module.Registry
	Admission Admission
	Config    WritersConfig
}
