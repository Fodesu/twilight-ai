package turntest

import (
	"testing"

	"github.com/felinics/twilight/agent/session/filestore"
	"github.com/felinics/twilight/agent/session/filestore/filestoretest"
)

func TestMemoryStoreConformance(t *testing.T) {
	Run(t, func(testing.TB) Fixture { return Fixture{Store: filestoretest.Store(t)} })
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
