package runtimetest

import (
	"testing"

	"github.com/memohai/twilight/agent/session"
)

func TestMemoryStoreConformance(t *testing.T) {
	Run(t, func(testing.TB) Fixture {
		return Fixture{Store: session.NewMemoryStore()}
	})
}
