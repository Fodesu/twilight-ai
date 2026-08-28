package es

import (
	"errors"
	"fmt"
)

// StreamID identifies one append-only event stream.
type StreamID string

// Revision identifies one accepted atomic record in a stream. Revision zero
// is reserved for the domain's immutable initial state/header.
type Revision uint64

// Index is an event's zero-based position within one atomic record.
type Index uint16

// Event is the standard domain-neutral event envelope. Domains that require
// additional fields may use their own envelope and expose it through
// EventMetadata instead.
type Event[P any] struct {
	SchemaVersion uint16      `json:"schemaVersion"`
	StreamID      StreamID    `json:"streamId"`
	Revision      Revision    `json:"revision"`
	Index         Index       `json:"index"`
	Type          string      `json:"type"`
	CausationID   CausationID `json:"causationId,omitempty"`
	Payload       P           `json:"payload"`
	PayloadDigest Digest      `json:"payloadDigest"`
}

// BuildEvent constructs the standard event envelope and binds the payload's
// canonical digest. The caller owns event Type and schema selection.
func BuildEvent[P any](schemaVersion uint16, streamID StreamID, revision Revision, index Index, typ string, causationID CausationID, payload P) (Event[P], error) {
	digest, err := DigestCanonical(payload)
	if err != nil {
		return Event[P]{}, err
	}
	return Event[P]{
		SchemaVersion: schemaVersion,
		StreamID:      streamID,
		Revision:      revision,
		Index:         index,
		Type:          typ,
		CausationID:   causationID,
		Payload:       payload,
		PayloadDigest: digest,
	}, nil
}

// ValidateEvent validates a standard event envelope and payload digest.
func ValidateEvent[P any](event Event[P], supported SchemaSupported) error {
	if event.StreamID == "" || event.Revision == 0 || event.Type == "" {
		return errors.New("es: event: missing identity")
	}
	if supported == nil || !supported(event.SchemaVersion) {
		return fmt.Errorf("es: event: unsupported schema version %d", event.SchemaVersion)
	}
	if event.PayloadDigest == "" {
		return errors.New("es: event: missing payload digest")
	}
	want, err := DigestCanonical(event.Payload)
	if err != nil {
		return err
	}
	if event.PayloadDigest != want {
		return fmt.Errorf("es: event: payload digest mismatch at revision %d index %d", event.Revision, event.Index)
	}
	return nil
}

// InspectEvent is the EventInspector adapter for the standard Event envelope.
func InspectEvent[P any](event Event[P]) (EventMetadata, error) {
	return EventMetadata{
		SchemaVersion: event.SchemaVersion,
		StreamID:      event.StreamID,
		Revision:      event.Revision,
		Index:         event.Index,
	}, nil
}

// StandardEventInspector validates the standard Event payload before exposing
// its generic record metadata.
func StandardEventInspector[P any](supported SchemaSupported) EventInspector[Event[P]] {
	return func(event Event[P]) (EventMetadata, error) {
		if err := ValidateEvent(event, supported); err != nil {
			return EventMetadata{}, err
		}
		return InspectEvent(event)
	}
}

// Record is the standard complete atomic event group. Domains with additional
// record metadata (for example a Run command identity) retain their own
// record type and adapt it to RecordView for validation/folding.
type Record[E any] struct {
	SchemaVersion uint16   `json:"schemaVersion"`
	StreamID      StreamID `json:"streamId"`
	Revision      Revision `json:"revision"`
	Events        []E      `json:"events"`
	RecordDigest  Digest   `json:"recordDigest"`
}

type recordDigestBody[E any] struct {
	SchemaVersion uint16   `json:"schemaVersion"`
	StreamID      StreamID `json:"streamId"`
	Revision      Revision `json:"revision"`
	Events        []E      `json:"events"`
}

// DigestRecord computes the standard Record aggregate digest. Domains with
// additional record metadata should digest their own complete body with
// DigestCanonical, then use RecordView for the shared completeness checks.
func DigestRecord[E any](record *Record[E]) (Digest, error) {
	if record == nil {
		return "", errors.New("es: record: nil record")
	}
	return DigestCanonical(recordDigestBody[E]{
		SchemaVersion: record.SchemaVersion,
		StreamID:      record.StreamID,
		Revision:      record.Revision,
		Events:        record.Events,
	})
}

// EventMetadata is the identity portion es needs to establish complete event
// groups. Payload/schema validation remains a domain responsibility.
type EventMetadata struct {
	SchemaVersion uint16
	StreamID      StreamID
	Revision      Revision
	Index         Index
}

// RecordView adapts a domain record to es mechanics without requiring that the
// domain adopt es's on-wire field names or give up domain-specific metadata.
type RecordView[E any] struct {
	SchemaVersion uint16
	StreamID      StreamID
	Revision      Revision
	Events        []E
}

// EventInspector extracts generic event metadata and performs any domain
// payload validation needed before an event is folded.
type EventInspector[E any] func(E) (EventMetadata, error)

// SchemaSupported reports whether a record/event schema can be folded.
type SchemaSupported func(uint16) bool

// ValidateRecord validates a standard Record, including its aggregate digest.
func ValidateRecord[E any](record *Record[E], supported SchemaSupported, inspect EventInspector[E]) error {
	if record == nil {
		return errors.New("es: record: nil record")
	}
	if err := ValidateRecordView(RecordView[E]{
		SchemaVersion: record.SchemaVersion,
		StreamID:      record.StreamID,
		Revision:      record.Revision,
		Events:        record.Events,
	}, supported, inspect); err != nil {
		return err
	}
	if record.RecordDigest == "" {
		return errors.New("es: record: missing digest")
	}
	want, err := DigestRecord(record)
	if err != nil {
		return err
	}
	if record.RecordDigest != want {
		return fmt.Errorf("es: record: digest mismatch at revision %d", record.Revision)
	}
	return nil
}

// ValidateRecordView proves that a record contains a complete, ordered event
// group for exactly one stream revision. It intentionally does not interpret
// event Type, payload, command identity, or record digest; those are domain
// protocol fields validated by the supplied inspector and domain adapter.
func ValidateRecordView[E any](record RecordView[E], supported SchemaSupported, inspect EventInspector[E]) error {
	if record.StreamID == "" || record.Revision == 0 {
		return errors.New("es: record: missing stream identity")
	}
	if supported == nil || !supported(record.SchemaVersion) {
		return fmt.Errorf("es: record: unsupported schema version %d", record.SchemaVersion)
	}
	if inspect == nil {
		return errors.New("es: record: nil event inspector")
	}
	if len(record.Events) == 0 {
		return fmt.Errorf("es: record: revision %d has no events", record.Revision)
	}
	if len(record.Events) > int(^uint16(0))+1 {
		return fmt.Errorf("es: record: revision %d event group too large: %d", record.Revision, len(record.Events))
	}
	for i, event := range record.Events {
		meta, err := inspect(event)
		if err != nil {
			return fmt.Errorf("es: record: revision %d event %d: %w", record.Revision, i, err)
		}
		if meta.SchemaVersion != record.SchemaVersion {
			return fmt.Errorf("es: record: event %d schema version %d does not match record %d", i, meta.SchemaVersion, record.SchemaVersion)
		}
		if meta.StreamID != record.StreamID {
			return fmt.Errorf("es: record: event %d stream %q does not match record stream %q", i, meta.StreamID, record.StreamID)
		}
		if meta.Revision != record.Revision {
			return fmt.Errorf("es: record: event %d revision %d does not match record revision %d", i, meta.Revision, record.Revision)
		}
		if meta.Index != Index(i) {
			return fmt.Errorf("es: record: revision %d index %d has event index %d", record.Revision, i, meta.Index)
		}
	}
	return nil
}
