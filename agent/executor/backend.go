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

// ExecutionRef is the Executor's physical binding of the current attempt for
// one effect: the provider (backend) it was handed to and that backend's
// opaque handle (RUN-EXE-9). It lives in the Execution Record and in the
// Backend contract; Agent Core addresses executions by AssignmentKey only.
type ExecutionRef = executionstore.ExecutionRef

// ErrUnknownProvider reports a record whose ExecutionRef names a provider
// this Worker has no Backend for (RUN-EXE-10). It is a definitive answer for
// Outcome reads (effect.ErrOutcomeUnavailable): this process cannot reach the
// execution and retrying will not change that.
var ErrUnknownProvider = fmt.Errorf("executor: unknown execution provider: %w", effect.ErrOutcomeUnavailable)

// ErrOrphanDisposed wraps the Warn a Worker emits when Reconcile disposes a
// record no Worker could adopt within WorkerOptions.DisposeAfter (RUN-EXE-6).
var ErrOrphanDisposed = errors.New("executor: orphaned execution disposed after DisposeAfter")

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
	// next attempt for the effect (RUN-EXE-9). Unlike Prepare it is not
	// required to return the same Ref; a Backend whose physical execution is
	// the durable object itself (a child Session) returns the same Ref.
	// Whether a tool may be restarted at all is decided before the call,
	// by the Worker, from the Assignment's Replay policy (RUN-EXE-9).
	Restart(ctx context.Context, previous string, a effect.Assignment) (ref string, err error)
	// Attach reports what the Backend finds for Ref. Missing is a proof: the
	// execution Ref names does not exist and will not start, so the Worker
	// may replay or settle it. A Backend that cannot confirm either way
	// answers orphaned, never missing; the Worker then keeps the lease and
	// asks again (RUN-EXE-3). Active and terminal are observations of a
	// live or finished execution. A Ref the Backend cannot interpret is an
	// error, not an observation.
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

// Progress relays the frames of the port behind this backend when it offers
// them (RUN-EXE-12); a port without progress ends the stream at once.
func (b portBackend) Progress(ctx context.Context, key effect.AssignmentKey, after uint64, fn func(effect.ProgressFrame) bool) error {
	if p, ok := b.port.(effect.ProgressPort); ok {
		return p.Progress(ctx, key, after, fn)
	}
	return nil
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

// Attach forwards to the Port by key. A Ref that is not a key is an error:
// the adapter cannot prove anything about an execution it cannot name.
func (b portBackend) Attach(ctx context.Context, ref string) (effect.Attachment, error) {
	key, err := b.key(ref)
	if err != nil {
		return effect.Attachment{}, err
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
