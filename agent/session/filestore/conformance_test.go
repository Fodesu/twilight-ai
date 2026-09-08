package filestore_test

import (
	"testing"

	"github.com/memohai/twilight/agent/session/filestore"
	"github.com/memohai/twilight/agent/session/run/runtimetest"
	"github.com/memohai/twilight/agent/session/sessiontest"
)

func newStore(t testing.TB) *filestore.Store {
	t.Helper()
	store, err := filestore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestKernelConformance(t *testing.T) {
	sessiontest.Run(t, func(t *testing.T) sessiontest.Fixture {
		return sessiontest.Fixture{Store: newStore(t)}
	})
}

func TestRuntimeConformance(t *testing.T) {
	runtimetest.Run(t, func(t testing.TB) runtimetest.Fixture {
		return runtimetest.Fixture{Store: newStore(t)}
	})
}
