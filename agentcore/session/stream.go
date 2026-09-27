package session

import (
	"errors"
	"fmt"
	"strings"
)

// StreamRef names the logical stream one batch belongs to: a Domain and,
// for a keyed stream, the ID of the aggregate within it (a singleton stream
// has an empty ID). Domains belong to Session modules: which domains exist,
// whether a domain is keyed and how its streams cross a segment edge are the
// owning module's declarations (EXT-STR-1). The kernel fixes the shape here,
// the order and atomicity of commits across streams, and offers both lineage
// read modes (StreamLineage); it names no domain of its own.
type StreamRef struct {
	Domain string `json:"domain"`
	ID     string `json:"id,omitempty"`
}

// String renders the stream for diagnostics and indexes.
func (r StreamRef) String() string {
	if r.ID == "" {
		return r.Domain
	}
	return r.Domain + "/" + r.ID
}

// StreamSeq is the position of an event inside its logical stream: the first
// event a stream ever receives is 0. It is derived from the ledger by
// counting a stream's events in CommitSeq order and is a read optimization
// only; CommitSeq is the canonical order.
type StreamSeq uint64

// StreamLineage is how a stream read crosses segment edges (SES-FRK-5). A
// fork inherits the commits of its
// ancestry; a stream's owning module declares which of the two histories its
// streams are, and a read names that mode. The kernel applies the mode it is
// given and does not know which one a domain declared.
type StreamLineage string

const (
	// LineageSession reads the stream as the Session's semantic history: the
	// inherited prefix stitched before the tip segment's own commits, so a
	// fork or a new tip continues the stream where its ancestry left it.
	LineageSession StreamLineage = "session"
	// LineageSegment reads the stream as execution history of the segment
	// that wrote it: the tip segment's own commits only, so a fork or a new
	// tip starts the stream empty.
	LineageSegment StreamLineage = "segment"
)

// ValidateStreamLineage checks that a read names one of the two modes.
func ValidateStreamLineage(l StreamLineage) error {
	switch l {
	case LineageSession, LineageSegment:
		return nil
	case "":
		return errors.New("stream lineage is empty")
	default:
		return fmt.Errorf("unknown stream lineage %q", l)
	}
}

// ValidateStreamRef checks the shape of a batch's stream attribution: a
// non-empty domain without the separator String uses, and an ID that is
// empty or a valid identity. Which domains a module owns, whether a domain
// is keyed and which ID an event binds to are Module Framework checks
// (EXT-STR-1), not the kernel's.
func ValidateStreamRef(r StreamRef) error {
	if err := validIdentity("stream domain", r.Domain); err != nil {
		return err
	}
	if strings.Contains(r.Domain, "/") {
		return fmt.Errorf("stream domain %q contains %q", r.Domain, "/")
	}
	if r.ID == "" {
		return nil
	}
	return validIdentity("stream ID", r.ID)
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
