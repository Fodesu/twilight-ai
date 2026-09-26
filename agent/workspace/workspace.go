// Package workspace models a durable logical work world. It is an optional
// application capability: Agent Core does not import this package or interpret
// its revisions, snapshots, or runtime bindings.
package workspace

import (
	"context"

	"github.com/felinics/twilight/agent/environment"
	"github.com/felinics/twilight/agentcore/run"
)

// ID identifies one logical mutable work world. It remains stable when the
// physical runtime is replaced.
type ID string

// RevisionRef identifies the immutable base from which a workspace was
// materialized (for example, a git commit).
type RevisionRef string

// SnapshotRef identifies a durable workspace snapshot.
type SnapshotRef string

// BackendID identifies a runtime provider without exposing its API to the
// workspace domain.
type BackendID = environment.Backend

// EnvironmentRef identifies a provider environment. It is distinct from an
// execution/job ID: many executions may run in one environment.
type EnvironmentRef = environment.EnvironmentRef

// RuntimeBinding records the current physical materialization of a Workspace.
// Generation changes when the workspace is rebound to a new environment.
type RuntimeBinding = environment.Binding

// Snapshot is an immutable durable snapshot anchor produced by a provider.
// StateRef is provider-neutral at this boundary and is interpreted by the
// provider selected by Backend.
type Snapshot struct {
	Ref       SnapshotRef          `json:"ref"`
	Workspace ID                   `json:"workspace"`
	Backend   BackendID            `json:"backend"`
	StateRef  environment.StateRef `json:"stateRef"`
	Parent    *SnapshotRef         `json:"parent,omitempty"`
}

// Fork describes creation of a new logical workspace from an existing durable
// anchor. A fork receives a new identity and never aliases the source's mutable
// runtime binding.
type Fork struct {
	Source      ID          `json:"source"`
	Destination ID          `json:"destination"`
	Snapshot    SnapshotRef `json:"snapshot"`
	Base        RevisionRef `json:"base,omitempty"`
}

// Workspace is the durable logical work world operated on by an application.
// Filesystem contents and provider objects are not embedded here; only the
// anchors needed to materialize or restore them are retained.
type Workspace struct {
	ID       ID              `json:"id"`
	Project  string          `json:"project,omitempty"`
	Base     RevisionRef     `json:"base"`
	Snapshot *SnapshotRef    `json:"snapshot,omitempty"`
	Runtime  *RuntimeBinding `json:"runtime,omitempty"`
}

// Target returns the opaque Agent Core target for this workspace. The reverse
// dependency is intentional: this optional domain adapts to Core, never the
// other way around.
func (w Workspace) Target() run.TargetRef {
	return run.TargetRef{Kind: "workspace", ID: string(w.ID)}
}

// Store persists logical workspaces and their durable anchors. Implementations
// may use a database, object store, or another application-owned repository.
type Store interface {
	Create(context.Context, Workspace) error
	Get(context.Context, ID) (Workspace, error)
	Put(context.Context, Workspace) error
	PutSnapshot(context.Context, Snapshot) error
	GetSnapshot(context.Context, SnapshotRef) (Snapshot, error)
	Fork(context.Context, Fork) (Workspace, error)
}
