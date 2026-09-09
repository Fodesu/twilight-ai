package session_test

import (
	"testing"

	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/sessiontest"
)

func TestMemoryStoreConformance(t *testing.T) {
	sessiontest.Run(t, func(t *testing.T) sessiontest.Fixture {
		return sessiontest.Fixture{Store: session.NewMemoryStore()}
	})
}
