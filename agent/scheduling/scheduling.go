// Package scheduling defines the agent's durable, versioned activation queue.
// It schedules attempts to consume Session work; a job claim is not Session
// write authority, and Session ownership neither requires nor grants a claim.
// Adapters must persist signals, source high-watermarks and claims atomically
// across crashes and enforce token fencing across all their handles/processes.
// No production in-memory adapter or background discovery loop is provided.
package scheduling

import (
	"context"
	"errors"
	"time"

	"github.com/felinics/twilight/agentcore/session"
)

// Signal reports progress of one source within a Session. Source is a stable
// revision namespace (e.g. "ledger", "execution/<id>", "inbox"), not a unique
// work ID. Revision must increase within that namespace; its initial value may
// be zero. NotBeforeUnixMilli is an absolute due time; zero means Store's now.
// Negative times and empty Session/Source are invalid.
type Signal struct {
	Session            session.SessionID
	Source             string
	Revision           uint64
	NotBeforeUnixMilli int64
}

// Claim is a fenced activation attempt. Generation snapshots all signals
// accepted when claimed; it is not a Session epoch. Token is opaque, globally
// fresh for every acquisition (even by the same owner). UntilUnixMilli is an
// informational snapshot: Renew does not require callers to update this value.
type Claim struct {
	Session        session.SessionID
	Token          string
	Generation     uint64
	UntilUnixMilli int64
}

var (
	// ErrInvalid reports empty keys/owner, a zero claim generation, negative
	// due times, durations below one millisecond (the clock precision), or
	// exhausted generation/expiry ranges.
	ErrInvalid = errors.New("scheduling: invalid argument")
	// ErrClaimLost reports an unknown, expired, finished or superseded claim,
	// including a token or generation mismatch. Finish is not idempotent:
	// a repeated Finish with the same token also returns ErrClaimLost.
	ErrClaimLost = errors.New("scheduling: claim lost")
)

// Store is a durable activation scheduling port. Each method is atomic and
// checks context cancellation before changing state. All due/expiry decisions
// use the adapter's clock, not a worker's clock. Valid durations are at least
// one millisecond and are rounded up to whole milliseconds. Invalid arguments
// must not change state. A zero Claim/false/nil means no eligible work.
//
// Generation starts at one and increases once per accepted Signal, across all
// sources in a Session. It and the source high-watermarks survive draining:
// rediscovering the same revision must not manufacture work. Adapters must not
// silently wrap generations or reuse tokens.
type Store interface {
	// Signal accepts only revisions strictly above the source high-watermark
	// (the first revision, including zero, is accepted). Duplicates/older
	// revisions are successful no-ops: they do not change generation, due,
	// claims or backoff. Accepted progress coalesces at the earliest due time
	// of unclaimed signals/backoff. During a live claim, new signals maintain
	// a separate uncovered due: they do not inherit the claim's covered due,
	// which is retained only for expiry recovery. New progress may wake a
	// Session whose previous attempt is in backoff.
	Signal(ctx context.Context, signal Signal) error

	// Claim takes one due Session with no live claim, ordered by due time
	// then SessionID. It does not acquire Session write ownership. An expired
	// claim's covered work becomes eligible again, without another Signal;
	// a takeover covers both that work and any signals since acquisition.
	Claim(ctx context.Context, owner string, duration time.Duration) (Claim, bool, error)

	// Renew extends a live matching token/generation to at least now+duration,
	// never shortening its current expiry. It neither consumes new signals
	// nor changes the claim's generation. Expired claims cannot be revived.
	Renew(ctx context.Context, claim Claim, duration time.Duration) error

	// Finish releases a live matching claim. Zero means its covered work is
	// drained; positive nextAtUnixMilli reschedules it (continuation/backoff).
	// Newer signals cannot be cleared or delayed. A positive continuation
	// merges at the earlier of nextAt and their due time: future probe signals
	// must not postpone immediately runnable work left by this claim.
	// In particular, downstream Session ErrOwned is not a drained attempt:
	// the consumer must Finish with a retry time, not swallow the job.
	Finish(ctx context.Context, claim Claim, nextAtUnixMilli int64) error
}
