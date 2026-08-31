package run

import (
	"errors"
	"fmt"

	"github.com/memohai/twilight/agent/es"
)

// TransitionRecord is the atomic authority record for one accepted
// Runtime.Commit. A transition owns the complete ordered AgentEvent group for
// one Revision; runtimes should persist it as the unit of log completeness and
// expose its Events as the public committed event stream.
type TransitionRecord struct {
	SchemaVersion    uint16       `json:"schemaVersion"`
	RunID            RunID        `json:"runId"`
	Revision         uint64       `json:"revision"`
	CommandID        CommandID    `json:"commandId"`
	CommandDigest    Digest       `json:"commandDigest"`
	Events           []AgentEvent `json:"events"`
	TransitionDigest Digest       `json:"transitionDigest"`
}

type transitionRecordDigestBody struct {
	SchemaVersion uint16       `json:"schemaVersion"`
	RunID         RunID        `json:"runId"`
	Revision      uint64       `json:"revision"`
	CommandID     CommandID    `json:"commandId"`
	CommandDigest Digest       `json:"commandDigest"`
	Events        []AgentEvent `json:"events"`
}

func transitionRecordBody(record *TransitionRecord) transitionRecordDigestBody {
	return transitionRecordDigestBody{
		SchemaVersion: record.SchemaVersion,
		RunID:         record.RunID,
		Revision:      record.Revision,
		CommandID:     record.CommandID,
		CommandDigest: record.CommandDigest,
		Events:        record.Events,
	}
}

func transitionRecordView(record *TransitionRecord) es.RecordView[AgentEvent] {
	return es.RecordView[AgentEvent]{
		SchemaVersion: record.SchemaVersion,
		StreamID:      es.StreamID(record.RunID),
		Revision:      es.Revision(record.Revision),
		Events:        record.Events,
	}
}

//nolint:gocritic // EventInspector is value-based so generic records do not retain mutable event pointers.
func inspectTransitionEvent(event AgentEvent) (es.EventMetadata, error) {
	typ := factType(event.Fact)
	if typ == "" || event.Type != typ {
		return es.EventMetadata{}, fmt.Errorf("event type %q does not match fact variant %T", event.Type, event.Fact)
	}
	wantDigest, err := DigestFact(event.SchemaVersion, event.Type, event.Fact)
	if err != nil {
		return es.EventMetadata{}, err
	}
	if event.Digest != wantDigest {
		return es.EventMetadata{}, fmt.Errorf("fact digest mismatch at revision %d index %d", event.Revision, event.Index)
	}
	return es.EventMetadata{
		SchemaVersion: event.SchemaVersion,
		StreamID:      es.StreamID(event.RunID),
		Revision:      es.Revision(event.Revision),
		Index:         es.Index(event.Index),
	}, nil
}

func supportsRunSchema(version uint16) bool { return isSupportedSchemaVersion(version) }

// DigestTransitionRecord computes the digest for one transition aggregate. The
// digest binds the transition identity and the complete ordered event group;
// TransitionDigest itself is excluded from the digest input.
func DigestTransitionRecord(record *TransitionRecord) (Digest, error) {
	if record == nil {
		return "", errors.New("agent: transition: nil record")
	}
	body, err := encodeEnvelopeBody(record.SchemaVersion, "transition_record", transitionRecordBody(record))
	if err != nil {
		return "", err
	}
	return es.DigestBytes(body), nil
}

// BuildTransitionRecord freezes and validates the complete event group of one
// accepted command as an atomic transition record.
func BuildTransitionRecord(events []AgentEvent) (TransitionRecord, error) {
	if len(events) == 0 {
		return TransitionRecord{}, errors.New("agent: transition: empty event group")
	}
	if len(events) > int(^uint16(0))+1 {
		return TransitionRecord{}, fmt.Errorf("agent: transition: event group too large: %d", len(events))
	}
	frozen := cloneEvents(events)
	first := frozen[0]
	record := TransitionRecord{
		SchemaVersion: first.SchemaVersion,
		RunID:         first.RunID,
		Revision:      first.Revision,
		CommandID:     first.CommandID,
		CommandDigest: first.CommandDigest,
		Events:        frozen,
	}
	digest, err := DigestTransitionRecord(&record)
	if err != nil {
		return TransitionRecord{}, err
	}
	record.TransitionDigest = digest
	if err := ValidateTransitionRecord(&record); err != nil {
		return TransitionRecord{}, err
	}
	return record, nil
}

// ValidateTransitionRecord verifies that a transition is internally complete:
// every nested event belongs to the transition, indexes are contiguous, fact
// digests match, and the transition digest binds the whole aggregate.
func ValidateTransitionRecord(record *TransitionRecord) error {
	if record == nil {
		return errors.New("agent: transition: nil record")
	}
	if record.RunID == "" || record.Revision == 0 || record.CommandID == "" || record.CommandDigest == "" {
		return errors.New("agent: transition: missing identity")
	}
	if err := es.ValidateRecordView(transitionRecordView(record), supportsRunSchema, inspectTransitionEvent); err != nil {
		return fmt.Errorf("agent: transition: %w", err)
	}
	for i := range record.Events {
		e := record.Events[i]
		if e.CommandID != record.CommandID || e.CommandDigest != record.CommandDigest {
			return fmt.Errorf("agent: transition: revision %d command identity changed within transition", record.Revision)
		}
		if _, terminal := e.Fact.(RunEnded); terminal && i != len(record.Events)-1 {
			return fmt.Errorf("agent: transition: terminal fact must be the final event")
		}
	}
	if record.TransitionDigest == "" {
		return errors.New("agent: transition: missing digest")
	}
	wantDigest, err := DigestTransitionRecord(record)
	if err != nil {
		return err
	}
	if record.TransitionDigest != wantDigest {
		return fmt.Errorf("agent: transition: digest mismatch at revision %d", record.Revision)
	}
	return nil
}
