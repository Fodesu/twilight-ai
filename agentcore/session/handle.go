package session

import (
	"context"
	"sync"
)

// ledgerHandle is the ownership handle over one root. It holds no copy of
// the tip's index: membership and stream heads are read from the backend's
// indexes on demand (SES-REP-3), so its memory and its Open cost do not
// grow with the segment. Every such read is bounded by the handle's head,
// which only its own Appends advance, so what it knows is exactly what it
// read at Open or wrote under its lease: a superseded handle never learns
// of a successor's commits and reaches the Epoch fence at Append.
type ledgerHandle struct {
	mu       sync.Mutex
	l        *Ledger
	root     SessionRecord
	ancestry *Ancestry
	lease    Lease
	opts     OpenOptions
	head     Head
	// streams caches the tip's head of each stream the handle was asked
	// about, read from the backend once below the handle's head and
	// advanced by this handle's own Appends (SES-REP-3).
	streams map[StreamRef]StreamSeq
	// failed is set once an Append's durable outcome is unknown (SES-APP-1):
	// the handle then answers nothing about the ledger, because what reached
	// storage is exactly what it cannot know. The caller reopens.
	failed error
}

func (w *ledgerHandle) SessionID() SessionID { return w.root.ID }
func (w *ledgerHandle) Epoch() Epoch         { return w.lease.Epoch }

func (w *ledgerHandle) Lease() Lease {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lease
}

// Renew is SES-OWN-1: the adapter moves the expiry only for the current
// lease, so a superseded handle learns of the supersession here as well as
// at Append.
func (w *ledgerHandle) Renew(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failed != nil {
		return w.failed
	}
	until, err := w.l.be.Renew(ctx, w.lease, w.opts.LeaseDuration)
	if err != nil {
		return err
	}
	w.lease.UntilUnixMilli = until
	return nil
}

func (w *ledgerHandle) Head() Head {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.head
}

// Committed answers from the segments' indexes (SES-REP-3), bounded by what
// this handle knows: the tip's commits below its head (read at Open or
// written by it) and the immutable inherited prefix. A successor's commits
// lie at or past the head, so a superseded handle does not learn of them
// here and reaches the Epoch fence at Append, exactly as when the index
// lived in its memory.
func (w *ledgerHandle) Committed(id CommitID) bool {
	w.mu.Lock()
	failed, head := w.failed, w.head
	w.mu.Unlock()
	if failed != nil {
		return false
	}
	ctx := context.Background()
	if seq, ok, err := w.l.be.Locate(ctx, w.root.Tip, id); err != nil {
		return false
	} else if ok && seq < head.Next {
		return true
	}
	inherited, err := w.ancestry.ContainsInherited(ctx, w.l.be, id)
	return err == nil && inherited
}

// countStreams advances the cached head of each stream the commit wrote and
// the handle has been asked about; w.mu is held.
func (w *ledgerHandle) countStreams(c *Commit) {
	for i := range c.Batches {
		stream := c.Batches[i].Stream
		if _, known := w.streams[stream]; known {
			w.streams[stream] += StreamSeq(len(c.Batches[i].Events))
		}
	}
}

// StreamHead reads the stream's head below the handle's head from the tip's
// index the first time it is asked about a stream, and advances the cached
// value with each Append (SES-REP-3, SES-FRK-5); the bound keeps a
// superseded handle from seeing its successor's streams.
func (w *ledgerHandle) StreamHead(stream StreamRef) (StreamSeq, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failed != nil {
		return 0, false
	}
	n, known := w.streams[stream]
	if !known {
		var err error
		n, err = w.l.be.StreamHead(context.Background(), w.root.Tip, stream, w.head.Next)
		if err != nil {
			return 0, false
		}
		w.streams[stream] = n
	}
	return n, n > 0
}

// LookupCommit is SES-REP-4, under the same bound as Committed: a tip commit
// below the head, else an inherited one.
func (w *ledgerHandle) LookupCommit(id CommitID) (Commit, bool, error) {
	w.mu.Lock()
	failed, head := w.failed, w.head
	w.mu.Unlock()
	if failed != nil {
		return Commit{}, false, failed
	}
	ctx := context.Background()
	if c, ok, err := w.l.be.LookupCommit(ctx, w.root.Tip, id); err != nil {
		return Commit{}, false, err
	} else if ok && c.Seq < head.Next {
		return c, true, nil
	}
	return w.ancestry.LookupInherited(ctx, w.l.be, id)
}

func (w *ledgerHandle) Append(ctx context.Context, p Proposal) (Commit, error) {
	if err := ctx.Err(); err != nil {
		return Commit{}, err
	}
	sid := w.root.ID
	c := Commit{CommitID: p.CommitID, Batches: cloneBatches(p.Batches)}
	if err := ValidateCommit(&c); err != nil {
		return Commit{}, newError(ErrInvalid, "append", sid, err.Error())
	}
	if w.Committed(p.CommitID) {
		return Commit{}, &Error{Code: ErrConflict, Operation: "append", SessionID: sid, CommitID: p.CommitID, Detail: "CommitID already in the ledger"}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failed != nil {
		return Commit{}, w.failed
	}
	c.Seq = w.head.Next
	if err := w.l.be.Append(ctx, w.lease, w.root.Tip, c); err != nil {
		if IsCode(err, ErrHandleFailed) {
			w.failed = err
		}
		return Commit{}, err
	}
	w.head = Head{Next: c.Seq + 1}
	w.countStreams(&c)
	return cloneCommit(c), nil
}

// Close releases the lease.
func (w *ledgerHandle) Close(ctx context.Context) error {
	return w.l.be.Release(ctx, w.lease)
}
