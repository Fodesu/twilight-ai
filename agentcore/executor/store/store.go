package store

import (
	"context"
	"errors"
	"time"

	"github.com/felinics/twilight/agentcore/executor/protocol"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
)

var (
	ErrAssignmentConflict = errors.New("executor/store: assignment conflict")
	ErrLeaseLost          = errors.New("executor/store: execution lease lost")
	ErrStateConflict      = errors.New("executor/store: state conflict")
)

// LegalTransition is the record state machine every Store enforces in
// TransitionOwned (RUN-EXE-3).
func LegalTransition(from, to effect.ExecutionStatus) bool {
	if to == effect.ExecutionCancelRequested {
		return from == effect.ExecutionAccepted || from == effect.ExecutionDispatching || from == effect.ExecutionRunning
	}
	switch from {
	case effect.ExecutionAccepted:
		return to == effect.ExecutionDispatching
	case effect.ExecutionDispatching:
		return to == effect.ExecutionRunning
	case effect.ExecutionRunning:
		// This transition is only used by explicit control-plane retry after
		// backend reconciliation found no attachable execution.
		return to == effect.ExecutionDispatching
	default:
		return false
	}
}

// ExecutionRef is the Executor's physical binding of the current attempt it
// makes for one effect (RUN-EXE-9): the provider (backend) the record was
// handed to and that backend's opaque handle. It is persisted before the
// execution starts and never leaves the Executor: Agent Core addresses
// executions by AssignmentKey, the effect under its Session and Run, and the
// Run itself records the EffectID alone.
type ExecutionRef struct {
	Provider string `json:"provider"`
	Ref      string `json:"ref"`
}

// Record is the worker's durable record of the attempts made for one effect:
// the execution plane's side of the effect, which the Run never reads.
// Assignment is immutable and carries the execution payload; ExecutionRef is
// set before Start; State and Outcome move monotonically to a terminal state.
// Owner and FencingEpoch protect takeover.
type Record struct {
	Assignment       effect.Assignment `json:"assignment"`
	AssignmentDigest run.Digest        `json:"assignmentDigest"`
	ExecutionRef     ExecutionRef      `json:"executionRef"`
	// Superseded lists the ExecutionRefs of the earlier attempts made for
	// this effect, oldest first: a takeover that found the physical execution
	// missing restarted it under a new Ref (RUN-EXE-9). The audit trail
	// keeps every Ref the effect was ever bound to.
	Superseded          []ExecutionRef            `json:"superseded,omitempty"`
	State               effect.ExecutionStatus    `json:"state"`
	Owner               string                    `json:"owner,omitempty"`
	FencingEpoch        uint64                    `json:"fencingEpoch,omitempty"`
	LeaseUntilUnixMilli int64                     `json:"leaseUntilUnixMilli,omitempty"`
	Outcome             *protocol.OutcomeEnvelope `json:"outcome,omitempty"`
	// SettledAtUnixMilli is when the record became terminal.
	SettledAtUnixMilli int64 `json:"settledAtUnixMilli,omitempty"`
	// AcknowledgedAtUnixMilli is when the Owner reported the settlement of
	// this Outcome as a Session fact (effect.Acknowledger); zero until then.
	AcknowledgedAtUnixMilli int64 `json:"acknowledgedAtUnixMilli,omitempty"`
	// Collected marks a terminal record whose payload and Outcome were
	// collected on acknowledgement (RUN-EXE-13): the key, digest, state and
	// ExecutionRef remain, so the acceptance of the key is never forgotten
	// while the record exists.
	Collected bool `json:"collected,omitempty"`
}

type Store interface {
	Create(context.Context, Record) (Record, bool, error)
	Get(context.Context, effect.AssignmentKey) (Record, bool, error)
	Put(context.Context, Record) error
	PutOwned(context.Context, Record, string, uint64) error
	TransitionOwned(context.Context, effect.AssignmentKey, string, uint64, effect.ExecutionStatus, effect.ExecutionStatus) error
	Acquire(context.Context, effect.AssignmentKey, string, time.Duration) (Record, bool, error)
	Renew(context.Context, effect.AssignmentKey, string, uint64, time.Duration) error
	LeaseOwned(context.Context, effect.AssignmentKey, string, uint64) (bool, error)
	// ListOwned returns the records whose Owner is the given Worker id: what
	// a restarted incarnation resumes. No other listing is part of the data
	// plane; which orphaned records to recover is decided by whoever observes
	// them (RUN-EXE-6).
	ListOwned(context.Context, string) ([]Record, error)
}
