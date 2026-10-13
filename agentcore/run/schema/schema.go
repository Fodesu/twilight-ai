package schema

import (
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/canonical"
	"github.com/felinics/twilight/agentcore/run/frozen"
	"github.com/felinics/twilight/agentcore/run/wire"
)

// The Run protocol's contracts, each a zero-size value that nothing can
// replace: every Run in every Session is decided, folded, named and
// digested by these. This package exists because the state
// machine (package run) cannot import the derivations that depend on it
// (package canonical); it is the one place the two are composed.

// Identity derives every RunID-scoped identity: step, call, response,
// effect and command ids. Its preimages are persisted semantics.
func Identity() canonical.Identity { return canonical.Identity{} }

// Canonical is the digest rules for the bodies facts name.
func Canonical() canonical.Digests { return canonical.Digests{} }

// Machine is the state machine: Decide, Evolve and CreateGroup,
// composed with the identity and digest rules above.
func Machine() run.StateMachine {
	return run.StateMachine{Canonical: canonical.Digests{}, Identity: canonical.Identity{}}
}

// Wire names and decodes the fact and command variants.
func Wire() wire.Facts { return wire.Facts{} }

// Snapshot encodes a MachineState for the projection cache.
func Snapshot() wire.Snapshot { return wire.Snapshot{} }

// Bodies encodes the frozen bodies facts name by digest.
func Bodies() frozen.Bodies { return frozen.Bodies{} }
