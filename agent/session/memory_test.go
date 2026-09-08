package session_test

import (
	"testing"

	"github.com/memohai/twilight/agent/session"
	"github.com/memohai/twilight/agent/session/sessiontest"
)

func TestMemoryStoreConformance(t *testing.T) {
	sessiontest.Run(t, func(t *testing.T) sessiontest.Fixture {
		return sessiontest.Fixture{Store: session.NewMemoryStore()}
	})
}
