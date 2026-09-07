package runtimetest

import (
	"testing"

	"github.com/memohai/twilight/agent/session"
)

func TestMemoryStoreConformance(t *testing.T) {
	Run(t, func(testing.TB) session.Store { return session.NewMemoryStore() })
}
