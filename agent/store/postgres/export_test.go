package postgres

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"

	"github.com/felinics/twilight/agentcore/session"
)

// SeedSegmentCommits bulk-loads commits into a segment for benchmarks: the
// rows Append would write, without the per-commit transaction.
func (d *DB) SeedSegmentCommits(ctx context.Context, segment session.SegmentID, commits []session.Commit) error {
	rows := make([][]any, 0, len(commits))
	for i := range commits {
		c := &commits[i]
		body, err := json.Marshal(c)
		if err != nil {
			return err
		}
		streams, err := json.Marshal(session.IndexEntryOf(c).Streams)
		if err != nil {
			return err
		}
		rows = append(rows, []any{string(segment), int64(c.Seq), string(c.CommitID), string(body), string(streams)}) //nolint:gosec // G115: seq values fit int64
	}
	_, err := d.pool.CopyFrom(ctx, pgx.Identifier{"session_commits"}, []string{"segment", "seq", "commit_id", "body", "streams"}, pgx.CopyFromRows(rows))
	return err
}

// IndexOf is the backend's Index, exposed for benchmarks.
func (s *SessionStore) IndexOf(ctx context.Context, segment session.SegmentID) (session.CommitIndex, session.Head, error) {
	return s.backend.Index(ctx, segment)
}
