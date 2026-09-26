// Package postgres is the shared store of a multi-replica deployment
// (CLD-STO-1): one Postgres database holds every durable fact the components
// share -- execution records and leases, the dispatch ledger, checkpoints,
// the command inbox, artifact bindings and claims, workspace records, the
// Session Backend and the cas content store -- so any owner, worker or
// backend replica sees the same ledgers and leases and a Session's Turns
// may run on different machines.
//
// SQL lives in queries/*.sql and is compiled by sqlc into internal/db: typed
// calls, parameter and row structs, nothing else. This package owns what
// sqlc cannot: the transaction boundary, the advisory lock per logical key
// that makes each read-fold-judge-write the only one for that key, the
// fencing and CAS rules of every contract, and the idempotency of replays.
// Primary keys and unique constraints stand behind that judgement as the
// last guard. The schema is migrations/*.sql, applied in order by Migrate.
package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/felinics/twilight/agent/store/postgres/internal/db"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// Options tune a DB.
type Options struct {
	// Now is the clock leases and expiries are judged by; nil selects
	// time.Now. It must agree with the Worker's clock (WorkerOptions.Clock).
	Now func() time.Time
}

// DB is one connection pool to the shared database and the stores over it.
type DB struct {
	pool *pgxpool.Pool
	q    *db.Queries
	now  func() time.Time
}

// Open connects to dsn and brings the schema up to date (Migrate).
func Open(ctx context.Context, dsn string, options ...Options) (*DB, error) {
	if dsn == "" {
		return nil, errors.New("postgres: empty dsn")
	}
	var opts Options
	if len(options) > 0 {
		opts = options[0]
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: open: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	d := &DB{pool: pool, q: db.New(pool), now: now}
	if err := d.Migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return d, nil
}

// Close closes the pool. Stores obtained from it stop working.
func (d *DB) Close() error {
	d.pool.Close()
	return nil
}

// tx runs fn in one transaction that holds the advisory lock of key for its
// duration: the read-fold-judge-write inside is the only one for that key.
func (d *DB) tx(ctx context.Context, key string, fn func(q *db.Queries) error) error {
	return pgx.BeginFunc(ctx, d.pool, func(t pgx.Tx) error {
		q := d.q.WithTx(t)
		if err := q.AdvisoryLock(ctx, key); err != nil {
			return err
		}
		return fn(q)
	})
}

// Migrate applies the migrations the database has not seen, in order, under
// one advisory lock, recording each version; every replica may run it at
// start and exactly one applies each step.
func (d *DB) Migrate(ctx context.Context) error {
	if _, err := d.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS twilight_schema (version BIGINT PRIMARY KEY, applied_at BIGINT NOT NULL)`); err != nil {
		return fmt.Errorf("postgres: schema table: %w", err)
	}
	names, err := fs.Glob(migrationFiles, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	return pgx.BeginFunc(ctx, d.pool, func(t pgx.Tx) error {
		q := d.q.WithTx(t)
		if err := q.AdvisoryLock(ctx, "twilight_schema"); err != nil {
			return err
		}
		current, err := q.SchemaVersion(ctx)
		if err != nil {
			return err
		}
		for i, name := range names {
			version := int64(i) + 1
			if version <= current {
				continue
			}
			sqlText, err := migrationFiles.ReadFile(name)
			if err != nil {
				return err
			}
			if _, err := t.Exec(ctx, string(sqlText)); err != nil {
				return fmt.Errorf("postgres: %s: %w", name, err)
			}
			if err := q.RecordSchemaVersion(ctx, db.RecordSchemaVersionParams{Version: version, AppliedAt: d.now().UnixMilli()}); err != nil {
				return err
			}
		}
		return nil
	})
}

// isUniqueViolation reports a unique or primary key violation.
func isUniqueViolation(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23505"
}

// noRows reports a :one query that found nothing.
func noRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }
