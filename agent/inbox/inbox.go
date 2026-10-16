// Package inbox is the agent's durable application-command delivery port.
// A gateway or other caller leaves commands for a Session's next consumer;
// these are application requests, not Core domain facts. Enqueue proves
// delivery to the inbox, not acceptance or commitment by the Session.
//
// Consumers process pending commands in Seq order. Before Resolve they must
// commit (or find) the corresponding durable command receipt in the Session
// under its fenced write authority. That receipt proves the effect or rejection;
// Resolve only acknowledges delivery. Resolve has no Session fencing and must
// never be used as the authoritative commit or as proof of exactly-once effects.
// A crash after the receipt but before Resolve is recovered by finding the
// receipt and acknowledging the entry, without applying the command again.
//
// Routing to the current consumer is only a latency optimization: the inbox
// survives callers, consumers and periods with no Session owner.
package inbox

import (
	"context"
	"errors"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
)

// CommandID is the caller-chosen identity of one command; a retried Enqueue
// with the same ID is the same command.
type CommandID string

// Kind names what the owner is asked to do; the application defines the set
// and each Kind's Payload.
type Kind string

// Command is what a caller enqueues.
type Command struct {
	ID      CommandID         `json:"id"`
	Kind    Kind              `json:"kind"`
	Payload run.CanonicalJSON `json:"payload,omitempty"`
}

// Status is a resolved command's disposition, backed by a Session receipt.
type Status string

const (
	// StatusApplied: the owner committed the command's effect, or found it
	// already committed.
	StatusApplied Status = "applied"
	// StatusRejected: the Session's committed state does not admit the
	// command (a Stop with no active Turn, a Withdraw of a delivered input);
	// the command is closed, retrying it would answer the same.
	StatusRejected Status = "rejected"
)

// Result is the delivery acknowledgement of a command's durable Session receipt.
// It is not itself that receipt or authoritative evidence of a Session commit.
type Result struct {
	Status              Status `json:"status"`
	Reason              string `json:"reason,omitempty"`
	ResolvedAtUnixMilli int64  `json:"resolvedAtUnixMilli"`
}

// Entry is a stored command: its Session-local position, when it was
// enqueued, and its Result once resolved (nil while pending).
type Entry struct {
	Seq                 uint64  `json:"seq"`
	Command             Command `json:"command"`
	EnqueuedAtUnixMilli int64   `json:"enqueuedAtUnixMilli"`
	Result              *Result `json:"result,omitempty"`
}

// Pending reports whether the entry awaits the owner.
func (e *Entry) Pending() bool { return e.Result == nil }

var (
	// ErrCommandConflict: the CommandID is already stored with a different
	// Kind or Payload.
	ErrCommandConflict = errors.New("inbox: command id reused for a different command")
	// ErrNotPending: Resolve named an entry that does not exist or is
	// already resolved.
	ErrNotPending = errors.New("inbox: entry is not pending")
)

// Store is the durable application inbox port, independent of Session write
// ownership. Every write is one transaction. Adapters stamp times with their
// own clock; this package deliberately grants no Core write authority.
type Store interface {
	// Enqueue stores c for sid and returns its Entry. A command whose ID is
	// already stored for sid is returned as stored, whatever its state, and
	// nothing is written; the same ID with a different Kind or Payload is
	// ErrCommandConflict. Seq is assigned per Session, increasing in
	// enqueue order.
	Enqueue(ctx context.Context, sid session.SessionID, c Command) (Entry, error)
	// Lookup returns sid's entry with CommandID id; ok is false when none.
	Lookup(ctx context.Context, sid session.SessionID, id CommandID) (Entry, bool, error)
	// Pending returns sid's unresolved entries in Seq order.
	Pending(ctx context.Context, sid session.SessionID) ([]Entry, error)
	// Resolve records r on the pending entry (sid, seq); an entry that is
	// missing or already resolved is ErrNotPending and nothing is written.
	// The caller must first prove the disposition with a durable Session
	// receipt. This unfenced acknowledgement cannot establish that proof.
	Resolve(ctx context.Context, sid session.SessionID, seq uint64, r Result) error
	// Sessions returns Sessions with at least one pending entry, in
	// SessionID order, at most limit of them (0 means every one): the
	// discovery index for agent activation. A page is a sample of
	// what needs an owner, not a cursor over all of it: a Session left out
	// appears once earlier ones are drained.
	Sessions(ctx context.Context, limit int) ([]session.SessionID, error)
}
