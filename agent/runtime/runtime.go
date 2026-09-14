// Package runtime defines the provider-neutral physical materialization
// boundary for logical workspaces. It deliberately does not depend on E2B,
// Docker, a VM SDK, or Agent Core effect types.
package runtime

import "context"

// Backend identifies an implementation of the runtime provider contract.
type Backend string

// EnvironmentRef identifies a provider environment. It is not an execution
// or tool-call identity.
type EnvironmentRef string

// StateRef identifies durable provider state that can materialize a new environment.
type StateRef string

// Binding is the durable association between a logical workspace and its
// current physical environment.
type Binding struct {
	Backend        Backend        `json:"backend"`
	EnvironmentRef EnvironmentRef `json:"environmentRef"`
	Generation     uint64         `json:"generation"`
}

// Spec is enough for a provider to materialize an environment without
// exposing provider-specific configuration to the workspace or Agent Core.
type Spec struct {
	Subject string
	Base    string
}

// RestoreSpec supplies a durable checkpoint and the destination environment spec.
type RestoreSpec struct {
	State       StateRef
	Destination Spec
}

// Environment is a provider-owned materialized runtime. Execution operations
// remain in the executor/provider adapter; this interface only models the
// lifecycle needed for attach, restore, and replacement.
type Environment interface {
	Ref() EnvironmentRef
	Close(context.Context) error
}

// Provider creates, restores, and adopts physical environments. Implementations
// may use a local process, container, VM, or a cloud sandbox.
type Provider interface {
	Create(context.Context, Spec) (Environment, error)
	Restore(context.Context, RestoreSpec) (Environment, error)
	Attach(context.Context, EnvironmentRef) (Environment, error)
}
