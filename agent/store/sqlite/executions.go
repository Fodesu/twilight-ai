package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/executor/protocol"
	executionstore "github.com/felinics/twilight/agent/executor/store"
	"github.com/felinics/twilight/agent/run/effect"
)

// ExecutionStore is executor/store.Store over the execution_records table.
// A record is one JSON document keyed by the digest of its AssignmentKey;
// every lease operation reads, judges and writes it in one immediate
// transaction, so the store is the fencing authority for any number of
// Workers over the same file (RUN-EXE-6). Nothing is ever evicted: a
// collected record keeps answering for its key (RUN-EXE-13).
type ExecutionStore struct {
	db  *sql.DB
	now func() time.Time
}

var _ executionstore.Store = (*ExecutionStore)(nil)

func recordKey(key effect.AssignmentKey) (string, error) {
	d, err := es.DigestCanonical(key)
	if err != nil {
		return "", err
	}
	return string(d), nil
}

type querier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func getRecord(ctx context.Context, q querier, key effect.AssignmentKey) (executionstore.Record, bool, error) {
	k, err := recordKey(key)
	if err != nil {
		return executionstore.Record{}, false, err
	}
	var raw string
	err = q.QueryRowContext(ctx, `SELECT record FROM execution_records WHERE key = ?`, k).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return executionstore.Record{}, false, nil
	}
	if err != nil {
		return executionstore.Record{}, false, err
	}
	var r executionstore.Record
	if err := json.Unmarshal([]byte(raw), &r); err != nil {
		return executionstore.Record{}, false, err
	}
	return r, true, nil
}

func putRecord(ctx context.Context, t *sql.Tx, record *executionstore.Record) error {
	k, err := recordKey(record.Assignment.Key())
	if err != nil {
		return err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	_, err = t.ExecContext(ctx, `INSERT INTO execution_records (key, record) VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET record = excluded.record`, k, string(raw))
	return err
}

func (s *ExecutionStore) Create(ctx context.Context, record executionstore.Record) (executionstore.Record, bool, error) { //nolint:gocritic // hugeParam: the Store contract takes the record by value; it is persisted, never shared
	var out executionstore.Record
	created := false
	err := tx(ctx, s.db, func(t *sql.Tx) error {
		old, ok, err := getRecord(ctx, t, record.Assignment.Key())
		if err != nil {
			return err
		}
		if ok {
			out = old
			if old.AssignmentDigest != record.AssignmentDigest {
				return executionstore.ErrAssignmentConflict
			}
			return nil
		}
		out, created = record, true
		return putRecord(ctx, t, &record)
	})
	if errors.Is(err, executionstore.ErrAssignmentConflict) {
		return out, false, err
	}
	if err != nil {
		return executionstore.Record{}, false, err
	}
	return out, created, nil
}

func (s *ExecutionStore) Get(ctx context.Context, key effect.AssignmentKey) (executionstore.Record, bool, error) {
	return getRecord(ctx, s.db, key)
}

func (s *ExecutionStore) Put(ctx context.Context, record executionstore.Record) error { //nolint:gocritic // hugeParam: the Store contract takes the record by value; it is persisted, never shared
	return tx(ctx, s.db, func(t *sql.Tx) error {
		old, ok, err := getRecord(ctx, t, record.Assignment.Key())
		if err != nil {
			return err
		}
		if ok && old.AssignmentDigest != record.AssignmentDigest {
			return executionstore.ErrAssignmentConflict
		}
		return putRecord(ctx, t, &record)
	})
}

func (s *ExecutionStore) PutOwned(ctx context.Context, record executionstore.Record, owner string, epoch uint64) error { //nolint:gocritic // hugeParam: the Store contract takes the record by value; it is persisted, never shared
	return tx(ctx, s.db, func(t *sql.Tx) error {
		old, ok, err := getRecord(ctx, t, record.Assignment.Key())
		if err != nil {
			return err
		}
		if !ok {
			return effect.ErrExecutionNotFound
		}
		if old.AssignmentDigest != record.AssignmentDigest {
			return executionstore.ErrAssignmentConflict
		}
		if old.Owner != owner || old.FencingEpoch != epoch || old.LeaseUntilUnixMilli <= s.now().UnixMilli() {
			return executionstore.ErrLeaseLost
		}
		return putRecord(ctx, t, &record)
	})
}

func (s *ExecutionStore) TransitionOwned(ctx context.Context, key effect.AssignmentKey, owner string, epoch uint64, from, to effect.ExecutionStatus) error {
	return tx(ctx, s.db, func(t *sql.Tx) error {
		r, ok, err := getRecord(ctx, t, key)
		if err != nil {
			return err
		}
		if !ok {
			return effect.ErrExecutionNotFound
		}
		if r.Owner != owner || r.FencingEpoch != epoch || r.LeaseUntilUnixMilli <= s.now().UnixMilli() {
			return executionstore.ErrLeaseLost
		}
		if r.State != from || !executionstore.LegalTransition(from, to) {
			return executionstore.ErrStateConflict
		}
		r.State = to
		return putRecord(ctx, t, &r)
	})
}

func (s *ExecutionStore) Acquire(ctx context.Context, key effect.AssignmentKey, owner string, ttl time.Duration) (executionstore.Record, bool, error) {
	now := s.now()
	var out executionstore.Record
	acquired := false
	err := tx(ctx, s.db, func(t *sql.Tx) error {
		r, ok, err := getRecord(ctx, t, key)
		if err != nil {
			return err
		}
		if !ok {
			return effect.ErrExecutionNotFound
		}
		out = r
		if protocol.StatusTerminal(r.State) {
			return nil
		}
		expired := r.LeaseUntilUnixMilli <= now.UnixMilli()
		if r.Owner != "" && r.Owner != owner && !expired {
			return nil
		}
		if r.Owner != owner || r.FencingEpoch == 0 || expired {
			r.FencingEpoch++
		}
		r.Owner = owner
		r.LeaseUntilUnixMilli = now.Add(ttl).UnixMilli()
		out, acquired = r, true
		return putRecord(ctx, t, &r)
	})
	if err != nil {
		return executionstore.Record{}, false, err
	}
	return out, acquired, nil
}

func (s *ExecutionStore) Renew(ctx context.Context, key effect.AssignmentKey, owner string, epoch uint64, ttl time.Duration) error {
	now := s.now()
	return tx(ctx, s.db, func(t *sql.Tx) error {
		r, ok, err := getRecord(ctx, t, key)
		if err != nil {
			return err
		}
		if !ok {
			return effect.ErrExecutionNotFound
		}
		if protocol.StatusTerminal(r.State) || r.Owner != owner || r.FencingEpoch != epoch {
			return executionstore.ErrLeaseLost
		}
		r.LeaseUntilUnixMilli = now.Add(ttl).UnixMilli()
		return putRecord(ctx, t, &r)
	})
}

func (s *ExecutionStore) LeaseOwned(ctx context.Context, key effect.AssignmentKey, owner string, epoch uint64) (bool, error) {
	r, ok, err := getRecord(ctx, s.db, key)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, effect.ErrExecutionNotFound
	}
	return !protocol.StatusTerminal(r.State) && r.Owner == owner && r.FencingEpoch == epoch && r.LeaseUntilUnixMilli > s.now().UnixMilli(), nil
}

// ListOwned returns the records owned by the given Worker id (executionstore.Store).
func (s *ExecutionStore) ListOwned(ctx context.Context, owner string) ([]executionstore.Record, error) {
	all, err := s.List(ctx)
	if err != nil {
		return nil, err
	}
	var out []executionstore.Record
	for i := range all {
		if all[i].Owner == owner {
			out = append(out, all[i])
		}
	}
	return out, nil
}

// List returns every record, for operators and tests; it is not part of
// executionstore.Store. A row that no longer decodes is skipped, so one
// corrupt record does not hide the others.
func (s *ExecutionStore) List(ctx context.Context) ([]executionstore.Record, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT record FROM execution_records ORDER BY key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []executionstore.Record
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var r executionstore.Record
		if err := json.Unmarshal([]byte(raw), &r); err != nil {
			continue
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
