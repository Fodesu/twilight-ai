// Package process is the dispatch ledger of one effect: the process
// manager's own decisions between the Session (which records that an effect
// was requested and how it settled) and the Executor (which records what it
// accepted and ran). Whether an effect is outstanding is the Run's state,
// whether the Executor holds an attempt is Attach's answer; neither is
// repeated here. What only the process manager knows, and what must survive
// its crash, is how many times it handed the effect to the Executor again
// and whether it gave up (RUN-EXE-15). The reconciler (agentcore/run/reconcile)
// is the process manager: it reads the three sources and writes here before
// it acts, so a decision made and a Dispatch sent cannot drift apart.
package process

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/ledger"
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
	// EventDispatchAttempted: the process manager is about to hand the
	// effect to the Executor again; Attempt numbers the redispatches from 1.
	EventDispatchAttempted EventType = "dispatch_attempted"
	// EventGivenUp: the process manager stopped redispatching; the reason is
	// recorded and the Run disposes the effect (RUN-CMT-7).
	EventGivenUp EventType = "given_up"
)

// Attempted is the payload of dispatch_attempted.
type Attempted struct {
	Attempt int `json:"attempt"`
}

// GivenUp is the payload of given_up.
type GivenUp struct {
	Reason string `json:"reason"`
}

// --- identity ---

// deriveCommitID names a command on one key; the naming rule is this
// domain's, the ledger only enforces uniqueness.
func deriveCommitID(key effect.AssignmentKey, command, discriminator string) CommitID {
	d, err := es.DigestCanonical(struct {
		Key           effect.AssignmentKey `json:"scope"`
		Command       string               `json:"command"`
		Discriminator string               `json:"discriminator,omitempty"`
	}{key, command, discriminator})
	if err != nil {
		panic(err) // AssignmentKey is three strings; canonical encoding cannot fail
	}
	return CommitID(d)
}

// AttemptCommitID names the n-th redispatch of the effect.
func AttemptCommitID(key effect.AssignmentKey, attempt int) CommitID {
	return deriveCommitID(key, "process/dispatch_attempted", fmt.Sprint(attempt))
}

// GivenUpCommitID names the one decision to stop.
func GivenUpCommitID(key effect.AssignmentKey) CommitID {
	return deriveCommitID(key, "process/given_up", "")
}

// --- fold ---

// State is the fold of one effect's dispatch ledger.
type State struct {
	Key      effect.AssignmentKey `json:"key"`
	Attempts int                  `json:"attempts"`
	GivenUp  bool                 `json:"givenUp,omitempty"`
	Reason   string               `json:"reason,omitempty"`
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
	if s.GivenUp {
		return s, errors.New("decision after given_up")
	}
	switch e.Type {
	case EventDispatchAttempted:
		var p Attempted
		if err := e.Decode(&p); err != nil {
			return s, err
		}
		if p.Attempt != s.Attempts+1 {
			return s, fmt.Errorf("attempt %d after %d", p.Attempt, s.Attempts)
		}
		s.Attempts = p.Attempt
	case EventGivenUp:
		var p GivenUp
		if err := e.Decode(&p); err != nil {
			return s, err
		}
		s.GivenUp, s.Reason = true, p.Reason
	default:
		return s, fmt.Errorf("unknown event type %q", e.Type)
	}
	return s, nil
}

// --- store ---

// Store is the dispatch ledger authority, keyed by AssignmentKey.
type Store interface {
	// Load folds the key's ledger; ok is false for a key with no ledger.
	Load(ctx context.Context, key effect.AssignmentKey) (State, Head, bool, error)
	// Read returns the key's commits from Seq from, in order, and the Head.
	Read(ctx context.Context, key effect.AssignmentKey, from CommitSeq) ([]Commit, Head, error)
	// Append commits c under the kernel's rules (agentcore/ledger): Seq is
	// Head.Next or ErrConflict; a known CommitID is ErrAlreadyApplied and
	// nothing is written; the commit is folded before it is written
	// (ErrStateConflict). epoch is the Session Epoch of the owner whose
	// reconciler writes: one below the highest the ledger has seen is
	// ErrFenced, so a superseded owner's reconciler writes nothing.
	Append(ctx context.Context, epoch Epoch, key effect.AssignmentKey, c Commit) error
}

// Attempt records the next redispatch of key under epoch and returns its
// number. It is written before the Dispatch it announces (RUN-EXE-15): a
// crash between the two costs one attempt of the budget, never a Dispatch
// the ledger does not know about.
func Attempt(ctx context.Context, s Store, epoch Epoch, key effect.AssignmentKey, now int64) (int, error) {
	state, head, _, err := s.Load(ctx, key)
	if err != nil {
		return 0, err
	}
	n := state.Attempts + 1
	if err := append1(ctx, s, epoch, key, head, AttemptCommitID(key, n), EventDispatchAttempted, Attempted{Attempt: n}, now); err != nil {
		return 0, err
	}
	return n, nil
}

// GiveUp records that the process manager stops redispatching key.
func GiveUp(ctx context.Context, s Store, epoch Epoch, key effect.AssignmentKey, reason string, now int64) error {
	state, head, _, err := s.Load(ctx, key)
	if err != nil {
		return err
	}
	if state.GivenUp {
		return nil
	}
	return append1(ctx, s, epoch, key, head, GivenUpCommitID(key), EventGivenUp, GivenUp{Reason: reason}, now)
}

func append1(ctx context.Context, s Store, epoch Epoch, key effect.AssignmentKey, head Head, id CommitID, typ EventType, payload any, now int64) error {
	ev, err := ledger.NewEvent(typ, now, payload)
	if err != nil {
		return err
	}
	err = s.Append(ctx, epoch, key, Commit{Seq: head.Next, CommitID: id, Events: []Event{ev}})
	if err == nil || errors.Is(err, ledger.ErrAlreadyApplied) {
		return nil
	}
	return err
}
