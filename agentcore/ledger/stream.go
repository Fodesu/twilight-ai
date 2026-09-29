package ledger

import (
	"errors"
	"fmt"
	"strings"
)

// Domain names the logical group one batch of a commit belongs to: a Name
// and, for a keyed domain, the ID of the aggregate within it (a singleton
// group has an empty ID). Which domains exist, whether a domain is keyed and
// what its IDs mean are the writing domain's declarations; the ledger fixes
// the shape here and names no domain of its own. The zero Domain is the
// unpartitioned ledger's single group.
type Domain struct {
	Name string `json:"name"`
	Id   string `json:"id,omitempty"`
}

// String renders the group for diagnostics and indexes.
func (d Domain) String() string {
	if d.Id == "" {
		return d.Name
	}
	return d.Name + "/" + d.Id
}

// ValidateDomain checks the shape of a batch's domain attribution: a name
// without the separator String uses, and an ID that is empty or a valid
// identity. The empty domain is valid: an unpartitioned ledger writes every
// commit to it. Which domains a writer owns and which ID an event binds to
// are the writing domain's checks, not the ledger's.
func ValidateDomain(d Domain) error {
	if d.Name == "" {
		if d.Id != "" {
			return errors.New("domain ID without a name")
		}
		return nil
	}
	if err := validIdentity("domain name", d.Name); err != nil {
		return err
	}
	if strings.Contains(d.Name, "/") {
		return fmt.Errorf("domain name %q contains %q", d.Name, "/")
	}
	if d.Id == "" {
		return nil
	}
	return validIdentity("domain ID", d.Id)
}

// EventBatch is the ordered slice of one commit that belongs to one domain.
type EventBatch struct {
	Domain Domain  `json:"domain"`
	Events []Event `json:"events"`
}

// Position is the ledger position of one event: the commit it landed in and
// its index among that commit's events in batch order. Positions order every
// event of a ledger totally, so a projection that needs to order what it
// derives records the position of the event that produced it instead of
// keeping a counter of its own.
type Position struct {
	Commit CommitSeq `json:"commit"`
	Index  uint32    `json:"index"`
}

// Less reports whether p precedes q in the ledger.
func (p Position) Less(q Position) bool {
	if p.Commit != q.Commit {
		return p.Commit < q.Commit
	}
	return p.Index < q.Index
}

// StreamSeq is the position of an event inside its logical stream: the first
// event a stream ever receives is 0. It is derived from the ledger by
// counting a stream's events in CommitSeq order and is a read optimization
// only; CommitSeq is the canonical order.
type StreamSeq uint64

// StreamLineage is how a stream read crosses a fork. A fork's path includes
// a prefix of the parent's commits. A stream's owning writer declares which
// of the two histories its streams are, and a read names that mode. The
// ledger applies the mode it is given and does not know which one a domain
// declared.
type StreamLineage string

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

// ValidateStreamRef checks a domain as a stream attribution: ledger-valid
// and not empty. The empty domain is the unpartitioned ledger's group, not
// a stream; which domains a writer owns and which ID an event binds to are
// the module framework's checks (EXT-STR-1).
func ValidateStreamRef(r Domain) error {
	if r.Name == "" {
		return errors.New("stream domain is empty")
	}
	return ValidateDomain(r)
}

const (
	// LineageSession reads the stream as the host's semantic history: the
	// inherited prefix stitched before the tip's own commits, so a fork
	// continues the stream at the next event after that prefix.
	LineageSession StreamLineage = "session"
	// LineageSegment reads the stream as execution history of the part that
	// wrote it: the tip's own commits only, so a fork or a new tip starts
	// the stream empty.
	LineageSegment StreamLineage = "segment"
)
