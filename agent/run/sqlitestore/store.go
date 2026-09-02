// Package sqlitestore is the SQLite Store adapter for agent/run.
//
// It persists RunHeader, the append-only TransitionRecord log, the derived
// MachineState snapshot, and execution leases. LoadHead reads the snapshot
// directly; the log is read only by LoadLog, which Runtime.Record and Rebuild
// use for verification. Runtime.Commit remains the write path; this package
// does not call Decide or Evolve.
package sqlitestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/memohai/twilight/agent/run"

	_ "modernc.org/sqlite" // SQLite driver
)

// Store is a file-backed run.Store. Runtime does not Close it; the host does.
type Store struct {
	db     *sql.DB
	closed sync.Once
}

var _ run.Store = (*Store)(nil)

const dsnParams = "?_txlock=immediate&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)"

// Open creates or opens a SQLite database at path and applies the schema.
func Open(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("agent: sqlite store: empty path")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("agent: sqlite store: resolve path: %w", err)
	}
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(abs)+dsnParams)
	if err != nil {
		return nil, fmt.Errorf("agent: sqlite store: open: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("agent: sqlite store: ping: %w", err)
	}
	s := &Store{db: db}
	if err := s.init(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) init(ctx context.Context) error {
	for _, stmt := range schema {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("agent: sqlite store: schema: %w", err)
		}
	}
	return nil
}

var schema = []string{
	`CREATE TABLE IF NOT EXISTS runs (
		run_id TEXT PRIMARY KEY NOT NULL,
		schema_version INTEGER NOT NULL,
		header TEXT NOT NULL,
		revision INTEGER NOT NULL DEFAULT 0,
		snapshot TEXT NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS transitions (
		run_id TEXT NOT NULL,
		revision INTEGER NOT NULL,
		command_id TEXT NOT NULL,
		record TEXT NOT NULL,
		PRIMARY KEY (run_id, revision),
		UNIQUE (run_id, command_id),
		FOREIGN KEY (run_id) REFERENCES runs(run_id)
	)`,
	`CREATE TABLE IF NOT EXISTS leases (
		run_id TEXT NOT NULL,
		lease_key TEXT NOT NULL,
		claim TEXT NOT NULL,
		grant TEXT NOT NULL,
		start_command_id TEXT NOT NULL,
		deadline_unix_nano INTEGER,
		PRIMARY KEY (run_id, lease_key),
		FOREIGN KEY (run_id) REFERENCES runs(run_id)
	)`,
	`CREATE INDEX IF NOT EXISTS leases_deadline ON leases (deadline_unix_nano) WHERE deadline_unix_nano IS NOT NULL`,
}

// Close releases the database. It is safe to call more than once.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	var err error
	s.closed.Do(func() {
		if s.db != nil {
			err = s.db.Close()
			s.db = nil
		}
	})
	return err
}

func (s *Store) ready(ctx context.Context) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if s == nil || s.db == nil {
		return errors.New("agent: sqlite store: nil store")
	}
	return nil
}

//nolint:gocritic // hugeParam: Store.Create takes RunHeader by value as the persisted creation record.
func (s *Store) Create(ctx context.Context, header run.RunHeader) (bool, run.RunHeader, error) {
	if err := s.ready(ctx); err != nil {
		return false, run.RunHeader{}, err
	}
	proto, err := run.ProtocolFor(header.SchemaVersion)
	if err != nil {
		return false, run.RunHeader{}, err
	}
	rawHeader, err := json.Marshal(header)
	if err != nil {
		return false, run.RunHeader{}, fmt.Errorf("agent: sqlite store: marshal header: %w", err)
	}
	rawSnapshot, err := proto.EncodeMachineState(&header.InitialState)
	if err != nil {
		return false, run.RunHeader{}, fmt.Errorf("agent: sqlite store: encode snapshot: %w", err)
	}
	var created bool
	var existing run.RunHeader
	err = s.withTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO runs (run_id, schema_version, header, revision, snapshot) VALUES (?, ?, ?, 0, ?)`,
			string(header.RunID), header.SchemaVersion, string(rawHeader), string(rawSnapshot))
		if err != nil {
			return fmt.Errorf("agent: sqlite store: insert run: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("agent: sqlite store: insert run: %w", err)
		}
		if n == 1 {
			created = true
			existing = header
			existing.InitialState.Current = run.Open{}
			return nil
		}
		loaded, err := loadHeader(ctx, tx, header.RunID)
		if err != nil {
			return err
		}
		existing = loaded
		return nil
	})
	return created, existing, err
}

func (s *Store) LoadHead(ctx context.Context, id run.RunID) (run.RunHead, error) {
	if err := s.ready(ctx); err != nil {
		return run.RunHead{}, err
	}
	var head run.RunHead
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var err error
		head, err = loadHead(ctx, tx, id)
		return err
	})
	return head, err
}

func (s *Store) LoadLog(ctx context.Context, id run.RunID, from uint64) ([]run.TransitionRecord, error) {
	if err := s.ready(ctx); err != nil {
		return nil, err
	}
	var log []run.TransitionRecord
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		if err := requireRun(ctx, tx, id); err != nil {
			return err
		}
		var err error
		log, err = loadLog(ctx, tx, id, from)
		return err
	})
	return log, err
}

// LoadRecord reads head and log inside one transaction, so the returned log
// ends exactly at head.Revision.
func (s *Store) LoadRecord(ctx context.Context, id run.RunID) (run.RunHead, []run.TransitionRecord, error) {
	if err := s.ready(ctx); err != nil {
		return run.RunHead{}, nil, err
	}
	var head run.RunHead
	var log []run.TransitionRecord
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var err error
		if head, err = loadHead(ctx, tx, id); err != nil {
			return err
		}
		log, err = loadLog(ctx, tx, id, 1)
		return err
	})
	return head, log, err
}

func loadLog(ctx context.Context, tx *sql.Tx, id run.RunID, from uint64) ([]run.TransitionRecord, error) {
	rows, err := tx.QueryContext(ctx, `SELECT record FROM transitions WHERE run_id = ? AND revision >= ? ORDER BY revision ASC`, string(id), from)
	if err != nil {
		return nil, fmt.Errorf("agent: sqlite store: load log: %w", err)
	}
	defer rows.Close()
	var log []run.TransitionRecord
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("agent: sqlite store: load log: %w", err)
		}
		rec, err := run.DecodeTransitionRecord([]byte(raw))
		if err != nil {
			return nil, fmt.Errorf("agent: sqlite store: decode transition: %w", err)
		}
		log = append(log, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("agent: sqlite store: load log: %w", err)
	}
	return log, nil
}

type sqliteTx struct {
	ctx  context.Context
	tx   *sql.Tx
	id   run.RunID
	head run.RunHead
}

func (t *sqliteTx) Head() run.RunHead { return t.head }

func (t *sqliteTx) LookupTransition(command run.CommandID) (run.TransitionRecord, bool, error) {
	var raw string
	err := t.tx.QueryRowContext(t.ctx, `SELECT record FROM transitions WHERE run_id = ? AND command_id = ?`, string(t.id), string(command)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return run.TransitionRecord{}, false, nil
	}
	if err != nil {
		return run.TransitionRecord{}, false, fmt.Errorf("agent: sqlite store: lookup transition: %w", err)
	}
	record, err := run.DecodeTransitionRecord([]byte(raw))
	if err != nil {
		return run.TransitionRecord{}, false, fmt.Errorf("agent: sqlite store: decode transition: %w", err)
	}
	return record, true, nil
}

// Commit is the Run's critical section: the connection pool is size one and
// the transaction is opened with an immediate write lock, so no other writer
// can observe or advance this Run until the transaction ends.
func (s *Store) Commit(ctx context.Context, id run.RunID, fn func(run.RunTx) (*run.Append, error)) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	if fn == nil {
		return errors.New("agent: sqlite store: nil commit fn")
	}
	return s.withTx(ctx, func(tx *sql.Tx) error {
		head, err := loadHead(ctx, tx, id)
		if err != nil {
			return err
		}
		a, err := fn(&sqliteTx{ctx: ctx, tx: tx, id: id, head: head})
		if err != nil || a == nil {
			return err
		}
		return appendTransition(ctx, tx, id, &head, a)
	})
}

func appendTransition(ctx context.Context, tx *sql.Tx, id run.RunID, head *run.RunHead, a *run.Append) error {
	if err := run.ValidateAppend(id, head, a); err != nil {
		return err
	}
	proto, err := run.ProtocolFor(head.Header.SchemaVersion)
	if err != nil {
		return err
	}
	rawSnapshot, err := proto.EncodeMachineState(&a.State)
	if err != nil {
		return fmt.Errorf("agent: sqlite store: encode snapshot: %w", err)
	}
	rawRecord, err := json.Marshal(a.Transition)
	if err != nil {
		return fmt.Errorf("agent: sqlite store: marshal transition: %w", err)
	}
	res, err := tx.ExecContext(ctx, `UPDATE runs SET revision = ?, snapshot = ? WHERE run_id = ? AND revision = ?`,
		a.Transition.Revision, string(rawSnapshot), string(id), a.ExpectedRevision)
	if err != nil {
		return fmt.Errorf("agent: sqlite store: advance run: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("agent: sqlite store: advance run: %w", err)
	} else if n != 1 {
		return run.ErrAppendConflict
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO transitions (run_id, revision, command_id, record) VALUES (?, ?, ?, ?)`,
		string(id), a.Transition.Revision, string(a.Transition.CommandID), string(rawRecord)); err != nil {
		return fmt.Errorf("%w: %w", run.ErrCommandConflict, err)
	}
	return applyLeaseOps(ctx, tx, id, a.Leases)
}

func (s *Store) RenewLease(ctx context.Context, id run.RunID, key string, grant run.ExecutionGrant, deadline time.Time) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	if grant == "" {
		return run.ErrStaleRuntime
	}
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if err := requireRun(ctx, tx, id); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `UPDATE leases SET deadline_unix_nano = ? WHERE run_id = ? AND lease_key = ? AND grant = ?`,
			deadlineValue(deadline), string(id), key, string(grant))
		if err != nil {
			return fmt.Errorf("agent: sqlite store: renew lease: %w", err)
		}
		if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("agent: sqlite store: renew lease: %w", err)
		} else if n != 1 {
			return run.ErrStaleRuntime
		}
		return nil
	})
}

func (s *Store) ExpiredLeases(ctx context.Context, before time.Time) ([]run.ExpiredLease, error) {
	if err := s.ready(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT run_id, lease_key, claim, grant, start_command_id, deadline_unix_nano
		FROM leases WHERE deadline_unix_nano IS NOT NULL AND deadline_unix_nano <= ? ORDER BY run_id, lease_key`, before.UnixNano())
	if err != nil {
		return nil, fmt.Errorf("agent: sqlite store: expired leases: %w", err)
	}
	defer rows.Close()
	var out []run.ExpiredLease
	for rows.Next() {
		var runID, key string
		lease, err := scanLease(rows, &runID, &key)
		if err != nil {
			return nil, err
		}
		out = append(out, run.ExpiredLease{RunID: run.RunID(runID), Key: key, Lease: lease})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("agent: sqlite store: expired leases: %w", err)
	}
	return out, nil
}

func (s *Store) ReplaceSnapshot(ctx context.Context, id run.RunID, revision uint64, state *run.MachineState) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	if state == nil || state.RunID != id {
		return errors.New("agent: sqlite store: snapshot RunID mismatch")
	}
	return s.withTx(ctx, func(tx *sql.Tx) error {
		schemaVersion, err := loadSchemaVersion(ctx, tx, id)
		if err != nil {
			return err
		}
		proto, err := run.ProtocolFor(schemaVersion)
		if err != nil {
			return err
		}
		raw, err := proto.EncodeMachineState(state)
		if err != nil {
			return fmt.Errorf("agent: sqlite store: encode snapshot: %w", err)
		}
		res, err := tx.ExecContext(ctx, `UPDATE runs SET snapshot = ? WHERE run_id = ? AND revision = ?`, string(raw), string(id), revision)
		if err != nil {
			return fmt.Errorf("agent: sqlite store: replace snapshot: %w", err)
		}
		if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("agent: sqlite store: replace snapshot: %w", err)
		} else if n != 1 {
			return run.ErrAppendConflict
		}
		return nil
	})
}

func (s *Store) withTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("agent: sqlite store: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("agent: sqlite store: commit: %w", err)
	}
	return nil
}

func requireRun(ctx context.Context, tx *sql.Tx, id run.RunID) error {
	_, err := loadSchemaVersion(ctx, tx, id)
	return err
}

func loadSchemaVersion(ctx context.Context, tx *sql.Tx, id run.RunID) (uint16, error) {
	var schemaVersion uint16
	err := tx.QueryRowContext(ctx, `SELECT schema_version FROM runs WHERE run_id = ?`, string(id)).Scan(&schemaVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, run.ErrRunNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("agent: sqlite store: load run: %w", err)
	}
	return schemaVersion, nil
}

func loadHead(ctx context.Context, tx *sql.Tx, id run.RunID) (run.RunHead, error) {
	var headerJSON, snapshotJSON string
	var rev int64
	err := tx.QueryRowContext(ctx, `SELECT header, revision, snapshot FROM runs WHERE run_id = ?`, string(id)).
		Scan(&headerJSON, &rev, &snapshotJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return run.RunHead{}, run.ErrRunNotFound
	}
	if err != nil {
		return run.RunHead{}, fmt.Errorf("agent: sqlite store: load run: %w", err)
	}
	if rev < 0 {
		return run.RunHead{}, fmt.Errorf("agent: sqlite store: negative revision %d", rev)
	}
	header, err := decodeHeader([]byte(headerJSON))
	if err != nil {
		return run.RunHead{}, err
	}
	proto, err := run.ProtocolFor(header.SchemaVersion)
	if err != nil {
		return run.RunHead{}, err
	}
	state, err := proto.DecodeMachineState([]byte(snapshotJSON))
	if err != nil {
		return run.RunHead{}, fmt.Errorf("agent: sqlite store: decode snapshot: %w", err)
	}
	leases, err := loadLeases(ctx, tx, id)
	if err != nil {
		return run.RunHead{}, err
	}
	return run.RunHead{Header: header, State: state, Revision: uint64(rev), Leases: leases}, nil
}

func loadHeader(ctx context.Context, tx *sql.Tx, id run.RunID) (run.RunHeader, error) {
	var headerJSON string
	err := tx.QueryRowContext(ctx, `SELECT header FROM runs WHERE run_id = ?`, string(id)).Scan(&headerJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return run.RunHeader{}, run.ErrRunNotFound
	}
	if err != nil {
		return run.RunHeader{}, fmt.Errorf("agent: sqlite store: load header: %w", err)
	}
	return decodeHeader([]byte(headerJSON))
}

type leaseScanner interface {
	Scan(dest ...any) error
}

func scanLease(row leaseScanner, runID, key *string) (run.ExecutionLease, error) {
	var claim, grant, startID string
	var deadline sql.NullInt64
	if err := row.Scan(runID, key, &claim, &grant, &startID, &deadline); err != nil {
		return run.ExecutionLease{}, fmt.Errorf("agent: sqlite store: scan lease: %w", err)
	}
	lease := run.ExecutionLease{
		Claim:          run.ExecutionClaim(claim),
		Grant:          run.ExecutionGrant(grant),
		StartCommandID: run.CommandID(startID),
	}
	if deadline.Valid {
		lease.Deadline = time.Unix(0, deadline.Int64)
	}
	return lease, nil
}

func loadLeases(ctx context.Context, tx *sql.Tx, id run.RunID) (map[string]run.ExecutionLease, error) {
	rows, err := tx.QueryContext(ctx, `SELECT run_id, lease_key, claim, grant, start_command_id, deadline_unix_nano FROM leases WHERE run_id = ?`, string(id))
	if err != nil {
		return nil, fmt.Errorf("agent: sqlite store: load leases: %w", err)
	}
	defer rows.Close()
	leases := make(map[string]run.ExecutionLease)
	for rows.Next() {
		var runID, key string
		lease, err := scanLease(rows, &runID, &key)
		if err != nil {
			return nil, err
		}
		leases[key] = lease
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("agent: sqlite store: load leases: %w", err)
	}
	return leases, nil
}

func deadlineValue(deadline time.Time) any {
	if deadline.IsZero() {
		return nil
	}
	return deadline.UnixNano()
}

func applyLeaseOps(ctx context.Context, tx *sql.Tx, id run.RunID, ops run.LeaseOps) error {
	if ops.Clear {
		if _, err := tx.ExecContext(ctx, `DELETE FROM leases WHERE run_id = ?`, string(id)); err != nil {
			return fmt.Errorf("agent: sqlite store: clear leases: %w", err)
		}
	}
	for _, key := range ops.Delete {
		if _, err := tx.ExecContext(ctx, `DELETE FROM leases WHERE run_id = ? AND lease_key = ?`, string(id), key); err != nil {
			return fmt.Errorf("agent: sqlite store: delete lease: %w", err)
		}
	}
	for key, lease := range ops.Put {
		if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO leases (run_id, lease_key, claim, grant, start_command_id, deadline_unix_nano) VALUES (?, ?, ?, ?, ?, ?)`,
			string(id), key, string(lease.Claim), string(lease.Grant), string(lease.StartCommandID), deadlineValue(lease.Deadline)); err != nil {
			return fmt.Errorf("agent: sqlite store: put lease: %w", err)
		}
	}
	return nil
}

func decodeHeader(raw []byte) (run.RunHeader, error) {
	var header run.RunHeader
	if err := json.Unmarshal(raw, &header); err != nil {
		return run.RunHeader{}, fmt.Errorf("agent: sqlite store: decode header: %w", err)
	}
	// MachineState.Current is omitted from the header's JSON encoding. A valid
	// header is always Open (RUN-NEW-1); ValidateRunHeader checks the digest.
	header.InitialState.Current = run.Open{}
	if err := run.ValidateRunHeader(&header); err != nil {
		return run.RunHeader{}, fmt.Errorf("agent: sqlite store: header: %w", err)
	}
	return header, nil
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return errors.New("agent: sqlite store: nil context")
	}
	return ctx.Err()
}
