// Package effect defines the process-independent protocol between the Run
// Loop and the component that performs model/tool effects. It contains no
// transport binding and no persistence implementation.
package effect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/model"
	"github.com/felinics/twilight/sdk"
)

// AssignmentKind names the effect requested by an Assignment.
type AssignmentKind string

const (
	AssignmentModel AssignmentKind = "model"
	AssignmentTool  AssignmentKind = "tool"
)

// AssignmentKey identifies one execution attempt of one target. Session is
// the Run's Scope (its Session, in Twilight) and part of the identity, so a
// shared executor cannot collide two stores that happen to use the same
// Run/Step/Call identifiers.
type AssignmentKey struct {
	Session run.Scope
	RunID   run.RunID
	StepID  run.StepID
	CallID  run.CallID
	Claim   run.ExecutionClaim
}

// AssignmentBody is the sealed effect an Assignment asks for: a model call
// or a tool call. Kind is derived from the variant, never stored beside it,
// so an Assignment cannot claim one kind and carry another.
type AssignmentBody interface {
	Kind() AssignmentKind
	assignmentBody()
}

type ModelAssignment struct {
	Model         run.ModelRef        `json:"model"`
	Request       *model.ModelRequest `json:"request,omitempty"`
	RequestDigest run.Digest          `json:"requestDigest"`
}

func (ModelAssignment) Kind() AssignmentKind { return AssignmentModel }
func (ModelAssignment) assignmentBody()      {}

type ToolAssignment struct {
	ToolRef          run.ToolRef
	DefinitionDigest run.Digest
	Arguments        run.CanonicalJSON
	Policy           run.ResponsePolicy
}

func (ToolAssignment) Kind() AssignmentKind { return AssignmentTool }
func (ToolAssignment) assignmentBody()      {}

// Assignment is the complete immutable description of one external effect.
type Assignment struct {
	Session run.Scope
	RunID   run.RunID
	StepID  run.StepID
	CallID  run.CallID
	Claim   run.ExecutionClaim
	Target  *run.TargetRef
	Schema  uint16
	// Body is the effect: exactly one of ModelAssignment or ToolAssignment.
	Body AssignmentBody
}

func (a Assignment) Key() AssignmentKey {
	return AssignmentKey{Session: a.Session, RunID: a.RunID, StepID: a.StepID, CallID: a.CallID, Claim: a.Claim}
}

// Kind is the body's kind; empty for an Assignment without a body.
func (a Assignment) Kind() AssignmentKind {
	if a.Body == nil {
		return ""
	}
	return a.Body.Kind()
}

// Model returns the model body, if the Assignment is a model call.
func (a Assignment) Model() (ModelAssignment, bool) {
	m, ok := a.Body.(ModelAssignment)
	return m, ok
}

// Tool returns the tool body, if the Assignment is a tool call.
func (a Assignment) Tool() (ToolAssignment, bool) {
	t, ok := a.Body.(ToolAssignment)
	return t, ok
}

func (a Assignment) Digest() (run.Digest, error) {
	return es.DigestCanonical(a)
}

// assignmentWire is the JSON shape: the kind discriminator with one body
// object. It is what the execution store persists and the HTTP protocol
// carries; decoding refuses a shape that names one kind and carries another.
type assignmentWire struct {
	Session run.Scope
	RunID   run.RunID
	StepID  run.StepID
	CallID  run.CallID
	Claim   run.ExecutionClaim
	Target  *run.TargetRef
	Schema  uint16
	Kind    AssignmentKind
	Model   *ModelAssignment
	Tool    *ToolAssignment
}

func (a Assignment) MarshalJSON() ([]byte, error) {
	w := assignmentWire{Session: a.Session, RunID: a.RunID, StepID: a.StepID, CallID: a.CallID, Claim: a.Claim, Target: a.Target, Schema: a.Schema, Kind: a.Kind()}
	switch b := a.Body.(type) {
	case ModelAssignment:
		w.Model = &b
	case ToolAssignment:
		w.Tool = &b
	case nil:
	default:
		return nil, fmt.Errorf("agent: effect: unknown assignment body %T", a.Body)
	}
	return json.Marshal(w)
}

func (a *Assignment) UnmarshalJSON(raw []byte) error {
	var w assignmentWire
	if err := json.Unmarshal(raw, &w); err != nil {
		return err
	}
	out := Assignment{Session: w.Session, RunID: w.RunID, StepID: w.StepID, CallID: w.CallID, Claim: w.Claim, Target: w.Target, Schema: w.Schema}
	switch {
	case w.Kind == AssignmentModel && w.Model != nil && w.Tool == nil:
		out.Body = *w.Model
	case w.Kind == AssignmentTool && w.Tool != nil && w.Model == nil:
		out.Body = *w.Tool
	case w.Kind == "" && w.Model == nil && w.Tool == nil:
	default:
		return fmt.Errorf("agent: effect: assignment kind %q does not match its body", w.Kind)
	}
	*a = out
	return nil
}

// OutcomeResult is the sealed result of an accepted Assignment: exactly one
// of the variants below. Illegal combinations (a result and an error, a
// cancellation that is also unknown) cannot be expressed.
type OutcomeResult interface{ outcomeResult() }

// FailureCode classifies a ModelFailed; it is wire-stable (protocol).
type FailureCode string

const (
	// FailureExecutor: the provider or executor failed with no more specific
	// classification.
	FailureExecutor FailureCode = "executor_error"
	// FailureFrozenValueMissing: the executor could not read the frozen
	// body the Assignment named (frozen.ErrMissing).
	FailureFrozenValueMissing FailureCode = "frozen_value_missing"
	// FailureMalformedRequest: the frozen request decoded but could not be
	// materialized into a provider request.
	FailureMalformedRequest FailureCode = "malformed_frozen_request"
	// FailureDeadline: the effect's own deadline elapsed.
	FailureDeadline FailureCode = "deadline_exceeded"
)

// ModelSucceeded carries the provider's complete result.
type ModelSucceeded struct{ Result sdk.ModelResult }

// ModelFailed is a provider or executor failure with a wire-stable code.
type ModelFailed struct {
	Code    FailureCode
	Message string
}

// ToolExecutionOutcome is the sealed result a tool implementation returns:
// succeeded, failed-known, or unknown. Each is also an OutcomeResult.
type ToolExecutionOutcome interface {
	OutcomeResult
	toolExecutionOutcome()
}

type ToolExecutionSucceeded struct{ Result run.ToolExecutionResult }

type ToolExecutionFailed struct{ Failure run.ToolFailure }

type ToolExecutionUnknown struct{ Failure run.ToolFailure }

// Cancelled: the executor stopped the effect as requested; Message says why.
type Cancelled struct{ Message string }

// Unknown: the assignment crossed the effect boundary but the executor
// closed its recovery without a terminal provider outcome. It must not be
// read as a dispatch rejection or an ordinary provider failure.
type Unknown struct{ Message string }

func (ModelSucceeded) outcomeResult()         {}
func (ModelFailed) outcomeResult()            {}
func (ToolExecutionSucceeded) outcomeResult() {}
func (ToolExecutionFailed) outcomeResult()    {}
func (ToolExecutionUnknown) outcomeResult()   {}
func (Cancelled) outcomeResult()              {}
func (Unknown) outcomeResult()                {}

func (ToolExecutionSucceeded) toolExecutionOutcome() {}
func (ToolExecutionFailed) toolExecutionOutcome()    {}
func (ToolExecutionUnknown) toolExecutionOutcome()   {}

// Outcome is the process-independent result after an Assignment has been
// accepted. The wire protocol (agent/run/protocol) encodes Result as a tagged
// envelope; there is no Go error in it.
type Outcome struct {
	Key    AssignmentKey
	Result OutcomeResult
}

// ModelResult returns the provider result of a ModelSucceeded outcome.
func (o Outcome) ModelResult() (sdk.ModelResult, bool) {
	r, ok := o.Result.(ModelSucceeded)
	return r.Result, ok
}

// Status is the ExecutionStatus a terminal Outcome corresponds to.
func (o Outcome) Status() ExecutionStatus {
	switch o.Result.(type) {
	case ModelSucceeded, ToolExecutionSucceeded:
		return ExecutionCompleted
	case ModelFailed, ToolExecutionFailed:
		return ExecutionFailed
	case Cancelled:
		return ExecutionCancelled
	case Unknown, ToolExecutionUnknown:
		return ExecutionUnknown
	default:
		return ExecutionUnknown
	}
}

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

// Terminal reports whether the provider execution has a final outcome.
func (s ExecutionStatus) Terminal() bool {
	switch s {
	case ExecutionCompleted, ExecutionFailed, ExecutionCancelled, ExecutionUnknown:
		return true
	default:
		return false
	}
}

var (
	ErrExecutionNotFound = errors.New("agent: effect: execution not found")
	ErrOutcomeNotReady   = errors.New("agent: effect: outcome not ready")
	// ErrOutcomeUnavailable means the executor holds a record for the key but
	// will never produce a readable Outcome for it (a provider this process
	// has no backend for, a record it cannot decode): a definitive answer, as
	// opposed to a transport failure that a later read may not see.
	ErrOutcomeUnavailable = errors.New("agent: effect: outcome unavailable")
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

// Valid reports whether the executor returned a defined attachment state.
func (s AttachmentState) Valid() bool {
	switch s {
	case AttachmentMissing, AttachmentActive, AttachmentOrphaned, AttachmentTerminal:
		return true
	default:
		return false
	}
}

// Terminal reports whether the executor has a durable terminal observation.
// This describes the attachment observation, not the provider lifecycle; use
// ExecutionStatus.Terminal for the latter.
func (s AttachmentState) Terminal() bool { return s == AttachmentTerminal }

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

// ExecutionPort is the Agent Core effect port: the process-independent
// contract through which the Loop hands effects to whatever executes them.
// It is intentionally message-shaped: none of its methods accepts a
// process-local callback.
type ExecutionPort interface {
	Validate(context.Context, Assignment) (*run.ToolFailure, error)
	Dispatch(context.Context, Assignment) error
	Attach(context.Context, AssignmentKey) (Attachment, error)
	GetStatus(context.Context, AssignmentKey) (ExecutionStatus, error)
	// A returned error describes the read operation. The execution remains
	// unsettled until a successful read returns its explicit Outcome.
	GetOutcome(context.Context, AssignmentKey) (Outcome, error)
	Cancel(context.Context, AssignmentKey) error
}
