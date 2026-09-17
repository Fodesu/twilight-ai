package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	executionstore "github.com/felinics/twilight/agent/executor/store"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/effect"
)

// ExecutionRef is the Executor's physical binding of one attempt: the
// provider (backend) it was handed to and that backend's opaque handle
// (RUN-EXE-9). It lives in the Execution Record and in the Backend contract;
// Agent Core addresses executions by AssignmentKey only.
type ExecutionRef = executionstore.ExecutionRef

// ErrUnknownProvider reports a record whose ExecutionRef names a provider
// this Worker has no Backend for (RUN-EXE-10).
var ErrUnknownProvider = errors.New("executor: unknown execution provider")

// ExecutionBackend performs one provider's effects. It is addressed by Ref:
// the Worker persists the Ref Prepare returns before Start, and every later
// lifecycle operation names that Ref. A backend keeps no table keyed by
// AssignmentKey.
type ExecutionBackend interface {
	// Validate checks that this Backend can serve the Assignment; it produces
	// no external effect (RUN-EXE-5).
	Validate(ctx context.Context, a effect.Assignment) (*run.ToolFailure, error)
	// Prepare allocates or derives the Ref of the Assignment without starting
	// the effect. It is idempotent by AssignmentKey: a repeated Prepare of the
	// same key returns the same Ref, so a process that dies between Prepare
	// and the record write recovers the same physical binding.
	Prepare(ctx context.Context, a effect.Assignment) (ref string, err error)
	// Start begins the execution Ref names. A definite rejection is an
	// ordinary error; a request that may have crossed the effect boundary is
	// effect.ErrDispatchUnknown. Start of a Ref that already started is a
	// no-op.
	Start(ctx context.Context, ref string, a effect.Assignment) error
	// Restart allocates the Ref of a new physical execution of the same
	// Assignment after the Backend reported the previous one missing: the
	// next generation of the attempt (RUN-EXE-9). Unlike Prepare it is not
	// required to return the same Ref; a Backend whose physical execution is
	// the durable object itself (a child Session) returns the same Ref.
	Restart(ctx context.Context, previous string, a effect.Assignment) (ref string, err error)
	// Attach reports what the Backend finds for Ref: missing, active,
	// orphaned or terminal.
	Attach(ctx context.Context, ref string) (effect.Attachment, error)
	Status(ctx context.Context, ref string) (effect.ExecutionStatus, error)
	// Outcome blocks until Ref has a terminal Outcome or ctx ends; its error
	// describes the read only.
	Outcome(ctx context.Context, ref string) (effect.Outcome, error)
	Cancel(ctx context.Context, ref string) error
}

// Route hands the Assignments Match accepts to one provider's Backend. The
// Worker evaluates routes once, at Dispatch, and persists the chosen
// provider in the record (RUN-EXE-10); a nil Match accepts every Assignment.
type Route struct {
	Provider string
	Match    func(effect.Assignment) bool
	Backend  ExecutionBackend
}

// Default is the Route that accepts every Assignment: the last route of a
// Worker's table.
func Default(provider string, b ExecutionBackend) Route { return Route{Provider: provider, Backend: b} }

// PortBackend adapts an effect.ExecutionPort -- an executor addressed by
// AssignmentKey, such as the remote HTTP client -- to the ExecutionBackend
// contract, so a Worker can route between it and colocated backends. The Ref
// is the key itself, encoded, so Prepare is deterministic and the adapter
// holds no state. New backends implement ExecutionBackend directly.
func PortBackend(p effect.ExecutionPort) ExecutionBackend { return portBackend{p} }

type portBackend struct{ port effect.ExecutionPort }

func (b portBackend) Validate(ctx context.Context, a effect.Assignment) (*run.ToolFailure, error) {
	return b.port.Validate(ctx, a)
}

func (b portBackend) Prepare(_ context.Context, a effect.Assignment) (string, error) {
	raw, err := json.Marshal(a.Key())
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func (b portBackend) key(ref string) (effect.AssignmentKey, error) {
	var key effect.AssignmentKey
	if err := json.Unmarshal([]byte(ref), &key); err != nil {
		return effect.AssignmentKey{}, fmt.Errorf("executor: ref is not an assignment key: %w", err)
	}
	return key, nil
}

func (b portBackend) Start(ctx context.Context, _ string, a effect.Assignment) error {
	return b.port.Dispatch(ctx, a)
}

// Restart of a Port-shaped executor re-dispatches the same key: the Ref is
// the key, so it does not change.
func (b portBackend) Restart(_ context.Context, previous string, _ effect.Assignment) (string, error) {
	return previous, nil
}

func (b portBackend) Attach(ctx context.Context, ref string) (effect.Attachment, error) {
	key, err := b.key(ref)
	if err != nil {
		return effect.Attachment{State: effect.AttachmentMissing, Execution: effect.ExecutionNotFound}, nil
	}
	return b.port.Attach(ctx, key)
}

func (b portBackend) Status(ctx context.Context, ref string) (effect.ExecutionStatus, error) {
	key, err := b.key(ref)
	if err != nil {
		return effect.ExecutionNotFound, err
	}
	return b.port.GetStatus(ctx, key)
}

func (b portBackend) Outcome(ctx context.Context, ref string) (effect.Outcome, error) {
	key, err := b.key(ref)
	if err != nil {
		return effect.Outcome{}, err
	}
	return b.port.GetOutcome(ctx, key)
}

func (b portBackend) Cancel(ctx context.Context, ref string) error {
	key, err := b.key(ref)
	if err != nil {
		return err
	}
	return b.port.Cancel(ctx, key)
}
