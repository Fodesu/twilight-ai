// Package session is the commit-ledger kernel of a Twilight Session
// (docs/design/agent-session.md). It owns the header, the Commit as the
// atomic unit of append, logical streams within commits, Session-level
// writer ownership with epoch fencing, and ordered reads over an append-only
// store. Payloads are opaque canonical JSON that Session modules encode and
// interpret.
package session

type (
	SessionID string
	CommitID  string
	EventType string
	// Epoch is the writer ownership generation of a stream, from 1.
	Epoch uint64
)
