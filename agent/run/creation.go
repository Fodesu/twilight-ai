package run

import (
	"context"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/felinics/twilight/agent/es"
)

// NewRun is the immutable, versioned creation data for a Run. RunID is
// caller-supplied so retries retain a stable identity. Owner and Attempt name
// the upper-level entity this Run serves and its ordinal under it; Run stores
// them and never interprets them.
type NewRun struct {
	SchemaVersion uint16         `json:"schemaVersion"`
	RunID         RunID          `json:"runId"`
	Owner         OwnerID        `json:"owner,omitempty"`
	Attempt       uint32         `json:"attempt,omitempty"`
	CausationID   es.CausationID `json:"causationId,omitempty"`
}

// BuildNewRun constructs a current-version Run creation value with no owner.
func BuildNewRun(runID RunID, causationID es.CausationID) (NewRun, error) {
	return BuildNewRunFor(runID, "", 0, causationID)
}

// BuildNewRunFor constructs a current-version Run creation value for one
// attempt under owner.
func BuildNewRunFor(runID RunID, owner OwnerID, attempt uint32, causationID es.CausationID) (NewRun, error) {
	run := NewRun{SchemaVersion: SchemaVersion1, RunID: runID, Owner: owner, Attempt: attempt, CausationID: causationID}
	if err := ValidateNewRun(run); err != nil {
		return NewRun{}, err
	}
	return run, nil
}

// ValidateNewRun verifies version support and textual identity encoding.
func ValidateNewRun(run NewRun) error {
	if run.RunID == "" {
		return errors.New("agent: new run: empty RunID")
	}
	if !utf8.ValidString(string(run.RunID)) {
		return errors.New("agent: new run: RunID is not valid UTF-8")
	}
	if !utf8.ValidString(string(run.Owner)) {
		return errors.New("agent: new run: Owner is not valid UTF-8")
	}
	if !utf8.ValidString(string(run.CausationID)) {
		return errors.New("agent: new run: CausationID is not valid UTF-8")
	}
	if run.SchemaVersion != SchemaVersion1 {
		return fmt.Errorf("agent: new run: unsupported schema version %d", run.SchemaVersion)
	}
	return nil
}

var (
	// ErrRunNotFound reports an operation addressed a RunID not in the Session.
	ErrRunNotFound = errors.New("agent: run not found")
)

// CheckContext avoids locking when cancellation already makes an operation
// inapplicable. Context is intentionally not retained by the Runtime.
func CheckContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("agent: runtime: nil context")
	}
	return ctx.Err()
}

func checkContext(ctx context.Context) error { return CheckContext(ctx) }
