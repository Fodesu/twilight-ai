// Package process is the effect process: the third event-sourced authority
// of the agent core, between the Session (which decides an effect is
// requested and settles its result) and the Executor (which runs it). Its
// scope is one effect, its facts are the relay's own decisions about that
// effect — requested, dispatched, failed to dispatch, delivered,
// acknowledged, given up — and its state is their fold. Neither the Run nor
// the Executor records these decisions: the Run records only that the
// effect was requested and how it settled, the Executor only what it
// accepted. The process ledger is where "was this dispatched, how many
// times, and why did we stop" has an answer (RUN-CMT-7, RUN-EXE-15).
//
// The process manager, in the sense of the CQRS literature, is the Relay:
// it reads the two authorities' ledgers from checkpoints, translates facts
// into commands for the other side, and records each decision here before
// acting on it, so a decision made and a command sent cannot drift apart
// across a crash. It holds no business logic: what an effect is and how a
// Run settles stay in the Run; how an execution is recovered stays in the
// Executor.
package process

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
)

// The commit vocabulary is the kernel's (agentcore/ledger).
type (
	CommitSeq = ledger.CommitSeq
	CommitID  = ledger.CommitID
	Epoch     = ledger.Epoch
	EventType = ledger.EventType
	Event     = ledger.Event
	Commit    = ledger.Commit
	Head      = ledger.Head
)

const (
	// EventDispatchRequested: the Session recorded the effect as started;
	// the relay owes the Executor a Dispatch. Always the first event.
	EventDispatchRequested EventType = "dispatch_requested"
	// EventDispatched: the Executor holds the effect (Attach is not missing).
	EventDispatched EventType = "dispatched"
	// EventDispatchFailed: a Dispatch attempt was refused before the effect
	// started; the relay may try again.
	EventDispatchFailed EventType = "dispatch_failed"
	// EventOutcomeDelivered: the Outcome was settled into the Session.
	EventOutcomeDelivered EventType = "outcome_delivered"
	// EventAcknowledged: the Executor was told the settlement is a Session
	// fact (RUN-EXE-13).
	EventAcknowledged EventType = "acknowledged"
	// EventGivenUp: the relay stopped trying; the reason is recorded.
	EventGivenUp EventType = "given_up"
)

// Kind is what the effect asks for.
type Kind string

const (
	KindModel Kind = "model"
	KindTool  Kind = "tool"
)

// Phase is where the process stands.
type Phase string

const (
	PhaseRequested    Phase = "requested"
	PhaseDispatched   Phase = "dispatched"
	PhaseDelivered    Phase = "delivered"
	PhaseAcknowledged Phase = "acknowledged"
	PhaseGivenUp      Phase = "given_up"
)

// Terminal reports a phase the process will not leave.
func (p Phase) Terminal() bool { return p == PhaseAcknowledged || p == PhaseGivenUp }

// --- payloads ---

// Requested is the payload of dispatch_requested: the effect's coordinates
// in the Run, from the start fact that requested it.
type Requested struct {
	RunID  run.RunID    `json:"runId"`
	StepID run.StepID   `json:"stepId"`
	CallID run.CallID   `json:"callId,omitempty"`
	Effect run.EffectID `json:"effect"`
	Kind   Kind         `json:"kind"`
}

// Failed is the payload of dispatch_failed.
type Failed struct {
	Attempt int    `json:"attempt"`
	Reason  string `json:"reason"`
}

// GivenUp is the payload of given_up.
type GivenUp struct {
	Reason string `json:"reason"`
}

// --- identity ---

// Command identities: once per process for request, dispatched, delivered,
// acknowledged and given_up; a dispatch failure is named by its attempt.
func RequestCommitID(key effect.AssignmentKey) CommitID {
	return ledger.DeriveCommitID(key, "process/request", "")
}
func DispatchedCommitID(key effect.AssignmentKey) CommitID {
	return ledger.DeriveCommitID(key, "process/dispatched", "")
}
func DeliveredCommitID(key effect.AssignmentKey) CommitID {
	return ledger.DeriveCommitID(key, "process/delivered", "")
}
func AcknowledgedCommitID(key effect.AssignmentKey) CommitID {
	return ledger.DeriveCommitID(key, "process/acknowledged", "")
}
func GivenUpCommitID(key effect.AssignmentKey) CommitID {
	return ledger.DeriveCommitID(key, "process/given_up", "")
}
func FailedCommitID(key effect.AssignmentKey, attempt int) CommitID {
	return ledger.DeriveCommitID(key, "process/dispatch_failed", fmt.Sprint(attempt))
}

// --- fold ---

// State is the fold of one process ledger.
type State struct {
	Key         effect.AssignmentKey `json:"key"`
	Requested   Requested            `json:"requested"`
	Phase       Phase                `json:"phase"`
	Attempts    int                  `json:"attempts"`
	LastFailure string               `json:"lastFailure,omitempty"`
	Reason      string               `json:"reason,omitempty"`
}

// Fold applies one commit; an event not legal from the state is
// ledger.ErrStateConflict.
func Fold(state State, c *Commit) (State, error) { //nolint:gocritic // hugeParam: a fold takes and returns the state by value
	for i := range c.Events {
		e := &c.Events[i]
		var err error
		state, err = apply(state, e)
		if err != nil {
			return State{}, fmt.Errorf("%w: commit %d event %d (%s): %w", ledger.ErrStateConflict, c.Seq, i, e.Type, err)
		}
	}
	return state, nil
}

func apply(s State, e *Event) (State, error) { //nolint:gocritic // hugeParam: value fold
	switch e.Type {
	case EventDispatchRequested:
		if s.Phase != "" {
			return s, errors.New("requested twice")
		}
		var p Requested
		if err := e.Decode(&p); err != nil {
			return s, err
		}
		s.Requested, s.Phase = p, PhaseRequested
	case EventDispatched:
		if s.Phase != PhaseRequested {
			return s, fmt.Errorf("dispatched from %s", s.Phase)
		}
		s.Phase = PhaseDispatched
	case EventDispatchFailed:
		if s.Phase != PhaseRequested {
			return s, fmt.Errorf("dispatch failed from %s", s.Phase)
		}
		var p Failed
		if err := e.Decode(&p); err != nil {
			return s, err
		}
		if p.Attempt != s.Attempts+1 {
			return s, fmt.Errorf("attempt %d after %d", p.Attempt, s.Attempts)
		}
		s.Attempts, s.LastFailure = p.Attempt, p.Reason
	case EventOutcomeDelivered:
		if s.Phase != PhaseRequested && s.Phase != PhaseDispatched {
			return s, fmt.Errorf("delivered from %s", s.Phase)
		}
		s.Phase = PhaseDelivered
	case EventAcknowledged:
		if s.Phase != PhaseDelivered {
			return s, fmt.Errorf("acknowledged from %s", s.Phase)
		}
		s.Phase = PhaseAcknowledged
	case EventGivenUp:
		if s.Phase == "" || s.Phase.Terminal() {
			return s, fmt.Errorf("given up from %s", s.Phase)
		}
		var p GivenUp
		if err := e.Decode(&p); err != nil {
			return s, err
		}
		s.Phase, s.Reason = PhaseGivenUp, p.Reason
	default:
		return s, fmt.Errorf("unknown event type %q", e.Type)
	}
	return s, nil
}

// Store is the process ledger authority, keyed by AssignmentKey.
type Store interface {
	// Load folds the key's ledger; ok is false for a key with no ledger.
	Load(ctx context.Context, key effect.AssignmentKey) (State, Head, bool, error)
	// Read returns the key's commits from Seq from, in order, and the Head.
	Read(ctx context.Context, key effect.AssignmentKey, from CommitSeq) ([]Commit, Head, error)
	// Append commits c under the kernel's rules (agentcore/ledger): Seq is
	// Head.Next or ErrConflict; a known CommitID is ErrAlreadyApplied and
	// nothing is written; the commit is folded before it is
	// written (ErrStateConflict). epoch is the Session Epoch of the owner
	// whose relay writes: one below the highest the ledger has seen is
	// ErrFenced, so a superseded owner's relay writes nothing.
	Append(ctx context.Context, epoch Epoch, key effect.AssignmentKey, c Commit) error
	// Open returns the keys of the processes of a Session that are not
	// terminal: what a relay resumes after a takeover.
	Open(ctx context.Context, scope run.Scope) ([]effect.AssignmentKey, error)
}
