// Package effect defines the process-independent protocol between the Run
// Loop and the component that performs model/tool effects. It contains no
// transport binding and no persistence implementation.
package effect

import (
	"context"
	"errors"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/sdk"
)

// AssignmentKind names the effect requested by an Assignment.
type AssignmentKind string

const (
	AssignmentModel AssignmentKind = "model"
	AssignmentTool  AssignmentKind = "tool"
)

// AssignmentKey identifies one execution attempt of one target. Session is
// part of the identity so a shared executor cannot collide two Sessions that
// happen to use the same Run/Step/Call identifiers.
type AssignmentKey struct {
	Session session.SessionID
	RunID   run.RunID
	StepID  run.StepID
	CallID  run.CallID
	Claim   run.ExecutionClaim
}

type ModelAssignment struct {
	Model         run.ModelRef      `json:"model"`
	Request       *run.ModelRequest `json:"request,omitempty"`
	RequestDigest run.Digest        `json:"requestDigest"`
}

type ToolAssignment struct {
	ToolRef          run.ToolRef
	DefinitionDigest run.Digest
	Arguments        run.CanonicalJSON
	Policy           run.ResponsePolicy
	Workspace        run.WorkspaceRef
}

// Assignment is the complete immutable description of one external effect.
type Assignment struct {
	Session session.SessionID
	RunID   run.RunID
	StepID  run.StepID
	CallID  run.CallID
	Claim   run.ExecutionClaim
	Schema  uint16
	Kind    AssignmentKind
	Model   *ModelAssignment
	Tool    *ToolAssignment
}

func (a Assignment) Key() AssignmentKey {
	return AssignmentKey{Session: a.Session, RunID: a.RunID, StepID: a.StepID, CallID: a.CallID, Claim: a.Claim}
}

func (a Assignment) Digest() (run.Digest, error) {
	return es.DigestCanonical(a)
}

// Outcome is the process-independent result after an Assignment has been
// accepted. The wire protocol encodes Err and the sealed Tool value separately.
type Outcome struct {
	Key       AssignmentKey
	Model     *sdk.ModelResult
	Tool      ToolExecutionOutcome
	Err       error
	Cancelled bool
	// Unknown means the assignment crossed the effect boundary but the
	// executor could not establish a terminal provider outcome. It must not
	// be interpreted as a dispatch rejection or an ordinary provider failure.
	Unknown bool
}

// ToolExecutionOutcome is sealed: succeeded, failed-known, or unknown.
type ToolExecutionOutcome interface{ toolExecutionOutcome() }

type ToolExecutionSucceeded struct{ Result run.ToolExecutionResult }

func (ToolExecutionSucceeded) toolExecutionOutcome() {}

type ToolExecutionFailed struct{ Failure run.ToolFailure }

func (ToolExecutionFailed) toolExecutionOutcome() {}

type ToolExecutionUnknown struct{ Failure run.ToolFailure }

func (ToolExecutionUnknown) toolExecutionOutcome() {}

// ExecutionStatus is the lifecycle state of an accepted effect, independent
// of the Run state machine.
type ExecutionStatus string

const (
	ExecutionNotFound        ExecutionStatus = "not_found"
	ExecutionAccepted        ExecutionStatus = "accepted"
	ExecutionDispatching     ExecutionStatus = "dispatching"
	ExecutionRunning         ExecutionStatus = "running"
	ExecutionCancelRequested ExecutionStatus = "cancel_requested"
	ExecutionCompleted       ExecutionStatus = "completed"
	ExecutionFailed          ExecutionStatus = "failed"
	ExecutionCancelled       ExecutionStatus = "cancelled"
	ExecutionUnknown         ExecutionStatus = "unknown"
)

var (
	ErrExecutionNotFound = errors.New("agent: effect: execution not found")
	ErrOutcomeNotReady   = errors.New("agent: effect: outcome not ready")
	// ErrDispatchUnknown means the dispatch response was lost after the
	// request may have crossed the effect boundary. It must not trigger a
	// compensating re-dispatch or a RecoverModelExecution automatically.
	ErrDispatchUnknown = errors.New("agent: effect: dispatch outcome unknown")
)

// AttachmentState describes what an executor found for an AssignmentKey.
type AttachmentState string

const (
	AttachmentMissing  AttachmentState = "missing"
	AttachmentActive   AttachmentState = "active"
	AttachmentOrphaned AttachmentState = "orphaned"
	AttachmentTerminal AttachmentState = "terminal"
)

// Attachment is the result of an attach/inspection request. Orphaned means a
// durable execution record exists, but this Worker does not currently own a
// live backend execution. The control plane must decide whether to reconcile,
// take over, or dispose it; it must not treat it as proof of no effect.
type Attachment struct {
	State               AttachmentState `json:"state"`
	Execution           ExecutionStatus `json:"execution"`
	Owner               string          `json:"owner,omitempty"`
	FencingEpoch        uint64          `json:"fencingEpoch,omitempty"`
	LeaseUntilUnixMilli int64           `json:"leaseUntilUnixMilli,omitempty"`
	BackendAttached     bool            `json:"backendAttached,omitempty"`
}

// BackendBinding identifies a provider-side execution that belongs to one
// Assignment. ExecutionRef is opaque to Agent Core; Workspace is the logical
// environment selected by the preset. Providers must make PrepareBinding
// idempotent by AssignmentKey because a crash can happen before the binding
// is returned to the Worker.
type BackendBinding struct {
	Provider     string           `json:"provider,omitempty"`
	Workspace    run.WorkspaceRef `json:"workspace,omitempty"`
	ExecutionRef string           `json:"executionRef"`
}

// BindingPort is an optional backend capability for durable provider jobs.
// The Worker persists the returned binding before dispatching. DispatchBound
// and AttachBound must address the same provider execution; they must not
// silently create a new execution for the same binding.
type BindingPort interface {
	PrepareBinding(context.Context, Assignment) (BackendBinding, error)
	DispatchBound(context.Context, Assignment, BackendBinding) error
	AttachBound(context.Context, AssignmentKey, BackendBinding) (Attachment, error)
}

// Port is the Agent Core effect port. It is intentionally message-shaped:
// none of its methods accepts a process-local callback.
type Port interface {
	Validate(context.Context, Assignment) (*run.ToolFailure, error)
	Dispatch(context.Context, Assignment) error
	Attach(context.Context, AssignmentKey) (Attachment, error)
	GetStatus(context.Context, AssignmentKey) (ExecutionStatus, error)
	GetOutcome(context.Context, AssignmentKey) (Outcome, error)
	Cancel(context.Context, AssignmentKey) error
}
