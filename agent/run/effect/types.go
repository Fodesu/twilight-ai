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
)

// Port is the Agent Core effect port. It is intentionally message-shaped:
// none of its methods accepts a process-local callback.
type Port interface {
	Validate(context.Context, Assignment) (*run.ToolFailure, error)
	Dispatch(context.Context, Assignment) error
	Attach(context.Context, AssignmentKey) (bool, error)
	GetStatus(context.Context, AssignmentKey) (ExecutionStatus, error)
	GetOutcome(context.Context, AssignmentKey) (Outcome, error)
	Cancel(context.Context, AssignmentKey) error
}
