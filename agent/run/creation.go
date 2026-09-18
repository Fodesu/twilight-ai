package run

import (
	"errors"
	"unicode/utf8"

	"github.com/felinics/twilight/agent/es"
)

// NewRun is the immutable creation data for a Run. RunID is
// caller-supplied so retries retain a stable identity. Owner and Attempt name
// the upper-level entity this Run serves and its ordinal under it; Run stores
// them and never interprets them.
// A Run carries no schema of its own: it is interpreted under the
// SchemaVersion of the Session segment it is created on (RUN-NEW-1).
type NewRun struct {
	RunID       RunID          `json:"runId"`
	Owner       OwnerID        `json:"owner,omitempty"`
	Attempt     uint32         `json:"attempt,omitempty"`
	CausationID es.CausationID `json:"causationId,omitempty"`
}

// BuildNewRun constructs a Run creation value with no owner.
func BuildNewRun(runID RunID, causationID es.CausationID) (NewRun, error) {
	return BuildNewRunFor(runID, "", 0, causationID)
}

// BuildNewRunFor constructs a Run creation value for one attempt under
// owner.
func BuildNewRunFor(runID RunID, owner OwnerID, attempt uint32, causationID es.CausationID) (NewRun, error) {
	run := NewRun{RunID: runID, Owner: owner, Attempt: attempt, CausationID: causationID}
	if err := ValidateNewRun(run); err != nil {
		return NewRun{}, err
	}
	return run, nil
}

// ValidateNewRun verifies textual identity encoding.
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
	return nil
}
