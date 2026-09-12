package turntest

import (
	"testing"

	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/filestore"
)

func TestMemoryStoreConformance(t *testing.T) {
	Run(t, func(testing.TB) Fixture { return Fixture{Store: session.NewMemoryStore()} })
}

func TestFileStoreConformance(t *testing.T) {
	Run(t, func(t testing.TB) Fixture {
		store, err := filestore.New(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return Fixture{Store: store}
	})
}
