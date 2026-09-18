package run

import (
	"github.com/felinics/twilight/agent/es"
)

type RunID string

// OwnerID identifies the upper-level entity a Run serves (the Turn, in the
// reference agent). Run stores it and never interprets it.
type OwnerID string
type StepID string
type CallID string
type CommandID string
type ResponseID string
type InputID string
type ToolRef string
type ModelRef string

// TargetRef is an opaque resource identity an Effect may operate on. Agent
// Core preserves it for execution routing but never interprets its Kind or
// lifecycle; optional domains such as Workspace define those semantics.
type TargetRef struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// Digest is "sha256:<64 lowercase hex>" over canonical protocol bytes.
// It remains an alias while Run protocol types live in this package.
type Digest = es.Digest

// PromptToken is opaque to agent; the application uses it to identify the
// context revision from which a Prompt was built.
type PromptToken string

// ExecutionClaim is an opaque identity chosen by the execution loop for one
// start command. It lets a caller replay the same start request without
// accidentally acquiring a second execution grant.
type ExecutionClaim string

// Scope is the opaque identity of the store one Run lives in: a Session in
// Twilight. Run never interprets it. It only scopes what has to stay distinct
// across stores -- execution keys handed to a shared executor, the takeover
// claim of a new owner, the prompt builder's read of the surrounding
// conversation -- and the adapter that realizes RunStore converts it to and
// from its own identity type.
type Scope string

// RunPosition is the index of a Run's last fact in the Run's own stream.
// Only the Run's own facts move it; whatever else the surrounding store
// appends leaves it untouched, which is what makes Prepare's hard CAS
// insensitive to concurrent writes of other modules (RUN-CMT-4).
type RunPosition uint64
