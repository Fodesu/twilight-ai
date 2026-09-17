package writer

import (
	"context"

	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
)

// Projections reads projections through each Session's Writer: the owner
// process's ProjectionReader (EXT-PRJ-3). Every layer above the Writer that
// needs a Session's state -- prompt builders, the turn Router, the Driver,
// application code -- holds this one reader and calls the domain's typed
// read functions on it.
func Projections(ws Writers) extension.ProjectionReader { return writersReader{ws} }

type writersReader struct{ writers Writers }

func (r writersReader) Load(ctx context.Context, sid session.SessionID, id extension.ProjectionID, v extension.ProjectionVersion) (any, session.Head, error) {
	w, err := r.writers.Writer(ctx, sid)
	if err != nil {
		return nil, session.Head{}, err
	}
	return w.Projections().Load(ctx, sid, id, v)
}
