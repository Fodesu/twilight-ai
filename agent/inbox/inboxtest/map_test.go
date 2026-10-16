package inboxtest

import (
	"testing"

	"github.com/felinics/twilight/agent/inbox"
)

func TestMapConformance(t *testing.T) {
	Run(t, func(*testing.T) inbox.Store { return &Map{} })
}
