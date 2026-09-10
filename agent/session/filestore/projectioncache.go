package filestore

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"

	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
)

// projectionsDir holds cached projection states beside the session's authoritative
// files. Everything under it is derived data: losing it, or finding it corrupt
// or stale, only costs a reader a longer fold (EXT-PRJ-3).
const projectionsDir = "projections"

// ProjectionCache returns the durable extension.ProjectionCache of this Store,
// so a process that reopens a Session resumes its projections from the last
// cached entry instead of refolding the whole log.
func (s *Store) ProjectionCache() extension.ProjectionCache { return projectionCache{s} }

// projectionRecord is the on-disk form of one cache entry. Through is the stream
// head the state was folded to; a reader revalidates it against the log before
// trusting State, so a record that is stale or ahead is simply ignored.
type projectionRecord struct {
	Through session.Head    `json:"through"`
	State   json.RawMessage `json:"state"`
}

type projectionCache struct{ store *Store }

func (c projectionCache) Load(_ context.Context, sid session.SessionID, id extension.ProjectionID, v extension.ProjectionVersion) (jsonstable.Value, session.Head, bool, error) {
	raw, err := os.ReadFile(projectionPath(c.store, sid, id, v))
	if err != nil {
		// A missing or unreadable entry is a cache miss, not a failure: the
		// reader falls back to folding from the beginning.
		return jsonstable.Value{}, session.Head{}, false, nil
	}
	var rec projectionRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return jsonstable.Value{}, session.Head{}, false, nil
	}
	state, err := jsonstable.Parse(rec.State)
	if err != nil {
		return jsonstable.Value{}, session.Head{}, false, nil
	}
	return state, rec.Through, true, nil
}

func (c projectionCache) Save(_ context.Context, sid session.SessionID, id extension.ProjectionID, v extension.ProjectionVersion, state jsonstable.Value, through session.Head) error {
	rec, err := json.Marshal(projectionRecord{Through: through, State: state.Bytes()})
	if err != nil {
		return err
	}
	path := projectionPath(c.store, sid, id, v)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return writeAtomic(path, rec)
}

// projectionPath is <root>/<sid>/projections/<id>/<version>.json. A projection
// ID contains slashes, so it is percent-encoded exactly like a Session ID.
func projectionPath(s *Store, sid session.SessionID, id extension.ProjectionID, v extension.ProjectionVersion) string {
	return filepath.Join(s.dir(sid), projectionsDir, encodeID(string(id)),
		strconv.FormatUint(uint64(v), 10)+".json")
}

var _ extension.ProjectionCache = projectionCache{}
