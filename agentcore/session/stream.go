package session

import (
	"errors"

	"github.com/felinics/twilight/agentcore/ledger"
)

// The stream vocabulary is the kernel's (agentcore/ledger): a commit's
// events are grouped by Domain, and reads track positions in CommitSeq
// order. session aliases it. Domains belong to Session modules: which
// domains exist, whether a domain is keyed and how its streams cross a
// segment edge are the owning module's declarations (EXT-STR-1). The kernel
// fixes the shape, the order and atomicity of commits across streams, and
// offers both lineage read modes; it names no domain of its own.
type (
	Domain        = ledger.Domain
	EventBatch    = ledger.EventBatch
	Position      = ledger.Position
	StreamSeq     = ledger.StreamSeq
	StreamLineage = ledger.StreamLineage
)

const (
	LineageSession = ledger.LineageSession
	LineageSegment = ledger.LineageSegment
)

// ValidateStreamLineage checks that a read names one of the two modes.
func ValidateStreamLineage(l StreamLineage) error { return ledger.ValidateStreamLineage(l) }

// ValidateStreamRef checks the shape of a batch's stream attribution as a
// Session stream: a ledger-valid domain that is not empty. Which domains a
// module owns, whether a domain is keyed and which ID an event binds to are
// Module Framework checks (EXT-STR-1), not the kernel's.
func ValidateStreamRef(r Domain) error {
	if r.Name == "" {
		return errors.New("stream domain is empty")
	}
	return ledger.ValidateDomain(r)
}

// HasTypePrefix reports whether typ matches one of the prefixes (empty list
// matches everything).
func HasTypePrefix(typ EventType, prefixes []EventType) bool {
	if len(prefixes) == 0 {
		return true
	}
	for _, p := range prefixes {
		if len(typ) >= len(p) && typ[:len(p)] == p {
			return true
		}
	}
	return false
}
