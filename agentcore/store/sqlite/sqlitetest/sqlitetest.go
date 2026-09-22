// Package sqlitetest opens throwaway SQLite databases for tests.
package sqlitetest

import (
	"path/filepath"
	"testing"

	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/store/sqlite"
)

// Open opens a fresh database under t.TempDir() and closes it when the test
// ends. Every store in tests is durable: there is no memory store to fall
// back to.
func Open(t testing.TB, options ...sqlite.Options) *sqlite.DB {
	t.Helper()
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "twilight.db"), options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// Artifacts opens a fresh database and returns its binding store and the
// retention ledger that verifies sets against it.
func Artifacts(t testing.TB) (artifact.BindingStore, artifact.RetentionLedger) {
	t.Helper()
	db := Open(t)
	bindings := db.Bindings()
	return bindings, db.Ledger(artifact.SetBuilder{Resolver: bindings})
}
