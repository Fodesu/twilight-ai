// Package ledger is the commit vocabulary of an append-only event ledger:
// commits, events and the module extension slots a commit-carrying record
// holds.
package ledger

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/jsonstable"
)

var (
	// ErrConflict: the commit's Seq is not the ledger's Head.Next.
	ErrConflict = errors.New("ledger: commit sequence conflict")
	// ErrAlreadyApplied: a commit with the same CommitID exists.
	ErrAlreadyApplied = errors.New("ledger: commit already applied")
	// ErrStateConflict: the commit's events are not legal from the fold.
	ErrStateConflict = errors.New("ledger: state conflict")
	// ErrFenced: the writer's epoch is behind the ledger's; it was superseded.
	ErrFenced = errors.New("ledger: writer fenced by a later epoch")
)

// CommitSeq is the position of a commit in one ledger.
type CommitSeq uint64

// CommitID names the operation a commit records. How operations are named is
// the writing domain's rule; the ledger enforces that one CommitID lands once.
type CommitID string

// CausationID is an opaque cross-domain lineage identifier. Its namespace and
// meaning are owned by the domain or application that records it, never by
// this package.
type CausationID string

// Epoch is a writer's fencing epoch, judged at Append.
type Epoch uint64

// EventType names a fact.
type EventType string

// Event is one fact committed to a ledger.
type Event struct {
	Type                EventType        `json:"type"`
	RecordedAtUnixMilli int64            `json:"recordedAtUnixMilli"`
	Payload             jsonstable.Value `json:"payload"`
}

// NewEvent renders payload as the event's canonical JSON; nil is {}.
func NewEvent(typ EventType, recordedAtUnixMilli int64, payload any) (Event, error) {
	if payload == nil {
		payload = struct{}{}
	}
	v, err := jsonstable.FromValue(payload)
	if err != nil {
		return Event{}, fmt.Errorf("ledger: %s payload: %w", typ, err)
	}
	return Event{Type: typ, RecordedAtUnixMilli: recordedAtUnixMilli, Payload: v}, nil
}

// Decode decodes the payload into dst.
func (e *Event) Decode(dst any) error { return e.Payload.Decode(dst) }

// Head is a ledger's tip: the next Seq to assign.
type Head struct {
	Next CommitSeq `json:"next"`
}
