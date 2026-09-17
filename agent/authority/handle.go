package authority

import (
	"context"

	"github.com/felinics/twilight/agent/driver"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/writer"
)

// ResumeAlreadyDriving is the Drive disposition when another local driver of
// the same Run carries the Turn forward (HST-DRV-1).
const ResumeAlreadyDriving = driver.ResumeAlreadyDriving

// Handle is this authority's current execution capability over one Session
// (HST-SES-1, HST-DRV-5): the Writer is open under this process's epoch,
// the takeover disposition has run, and the recovery lifetime is installed.
// A SessionID is a durable identity; a Handle says this process owns it now.
// It carries no operations of its own: the core services -- Coordinator,
// Driver, chatlog.Service -- take the SessionID, and the Writer's epoch
// fencing keeps a stale owner from writing.
type Handle struct {
	// Recovered is the takeover disposition count from opening (RUN-CMT-7).
	Recovered int

	id session.SessionID
	a  *Authority
}

// Open takes ownership of the Session -- its Writer and its recovery
// listeners -- and runs the takeover disposition (HST-DRV-5). The stream
// must exist. Opening a Session again replaces its recovery listeners.
func (a *Authority) Open(ctx context.Context, sid session.SessionID) (*Handle, error) {
	if _, err := a.Writers.Writer(ctx, sid); err != nil {
		return nil, err
	}
	n, err := a.Driver.Open(ctx, sid)
	if err != nil {
		return nil, err
	}
	return &Handle{Recovered: n, id: sid, a: a}, nil
}

// ID is the Session this Handle owns.
func (h *Handle) ID() session.SessionID { return h.id }

// Close stops the Session's recovery listeners and releases its Writer. A
// Writer that failed (EXT-WRT-4) does not block the close; the next Open
// reopens from the log.
func (h *Handle) Close(ctx context.Context) error {
	h.a.Driver.Stop(h.id)
	return writer.CloseWriter(ctx, h.a.Writers, h.id)
}
