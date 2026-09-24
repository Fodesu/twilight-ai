package schema

import (
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/canonical"
	"github.com/felinics/twilight/agentcore/run/frozen"
	"github.com/felinics/twilight/agentcore/run/wire"
)

// The Run protocol's contracts. There is no version to select: every Run
// in every Session is decided, folded, named and digested by these.
var (
	// Identity derives every RunID-scoped identity: step, call, response,
	// effect and command ids (RUN-WIR-1). Its preimages are persisted
	// semantics and never change.
	Identity run.Identity = canonical.Identity{}
	// Canonical is the digest rules for the bodies facts name (RUN-WIR-4).
	Canonical run.Canonical = canonical.Digests{}
	// Machine is the state machine: Decide, Evolve and CreateGroup (RUN-MCH).
	Machine run.Machine = run.StateMachine{Canonical: Canonical, Identity: Identity}
	// Wire names and decodes the fact and command variants (RUN-WIR-2).
	Wire wire.Codec = wire.Facts{}
	// Snapshot encodes a MachineState for the projection cache.
	Snapshot wire.SnapshotCodec = wire.Snapshot{}
	// Bodies encodes the frozen bodies facts name by digest (RUN-WIR-4).
	Bodies frozen.Codec = frozen.Bodies{}
)
