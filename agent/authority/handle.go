package authority

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/driver"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/writer"
)

// ResumeAlreadyDriving is the Drive disposition when another local driver of
// the same Run carries the Turn forward (DRV-1).
const ResumeAlreadyDriving = driver.ResumeAlreadyDriving

// ErrSessionOpen reports an Open of a Session this authority already holds
// open: one generation of ownership at a time (AUTH-OWN-1).
var ErrSessionOpen = errors.New("authority: session is already open")

// openSession is one generation of ownership over a Session: the Writer
// taken at Open and the recovery lifetime installed with it. Handles point
// at a generation, so a Close releases only the generation it belongs to.
type openSession struct {
	w writer.Writer
}

// Handle is this authority's current execution capability over one Session
// (AUTH-OWN-1, DRV-3): its Writer is open under this process's epoch, the
// takeover disposition has run, and the recovery lifetime is installed. A
// SessionID is a durable identity; a Handle says this process owns it now.
//
// The Handle carries no operations of its own. Every command of the core --
// turn.Commands, chatlog.Commands, driver.Drive, run.Runtime.Commit -- takes
// its Writer, so all authoritative writes of the Session land on one Writer,
// one epoch and one projection view, and the Writer fences a stale owner.
// Reads take a SessionID and need no Handle.
type Handle struct {
	// Recovered is the takeover disposition count from opening (RUN-CMT-7).
	Recovered int

	gen *openSession
	a   *Authority
}

// Open takes ownership of the Session -- its Writer and its recovery
// listeners -- and runs the takeover disposition (DRV-3). The stream must
// exist. A Session this authority already holds open is ErrSessionOpen:
// close the Handle first. A failed takeover disposition releases the Writer
// again, so a failed Open leaves nothing owned.
func (a *Authority) Open(ctx context.Context, sid session.SessionID) (*Handle, error) {
	a.mu.Lock()
	if _, open := a.open[sid]; open {
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrSessionOpen, sid)
	}
	gen := &openSession{}
	a.open[sid] = gen
	a.mu.Unlock()
	w, err := a.Writers.Writer(ctx, sid)
	if err != nil {
		a.forget(sid, gen)
		return nil, err
	}
	gen.w = w
	n, err := a.Driver.Open(ctx, w)
	if err != nil {
		a.forget(sid, gen)
		_ = writer.CloseWriter(context.WithoutCancel(ctx), a.Writers, sid)
		return nil, err
	}
	return &Handle{Recovered: n, gen: gen, a: a}, nil
}

// forget drops gen from the open table when it is still the current one.
func (a *Authority) forget(sid session.SessionID, gen *openSession) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.open[sid] != gen {
		return false
	}
	delete(a.open, sid)
	return true
}

// ID is the Session this Handle owns.
func (h *Handle) ID() session.SessionID { return h.gen.w.SessionID() }

// Writer is the write capability itself: the Session's Writer under this
// process's epoch. Core commands take it; a Writer that has lost ownership
// refuses to commit, which is what makes the Handle a capability rather
// than a name.
func (h *Handle) Writer() writer.Writer { return h.gen.w }

// Close releases this Handle's generation of ownership: it stops the
// Session's recovery listeners and releases its Writer. A Handle whose
// generation was already released (by Close, DeleteSession or a later Open
// after its Close) does nothing. A Writer that failed (EXT-WRT-4) does not
// block the close; the next Open reopens from the log.
func (h *Handle) Close(ctx context.Context) error {
	sid := h.ID()
	if !h.a.forget(sid, h.gen) {
		return nil
	}
	h.a.Driver.Stop(sid)
	return writer.CloseWriter(ctx, h.a.Writers, sid)
}
