package session

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"unicode/utf8"

	"github.com/memohai/twilight/agent/es"
	"github.com/memohai/twilight/agent/jsonstable"
)

// ProtocolProfile freezes the kernel wire for one ProtocolVersion
// (SES-WIR-2): encodings, digest preimages and the append fingerprint.
type ProtocolProfile interface {
	Version() uint16
	HeaderDigest(SessionHeader) (es.Digest, error)
	EventDigest(SessionID, es.Revision, SessionEvent) (es.Digest, error)
	CommitDigest(SessionCommit) (es.Digest, error)
	SnapshotDigest(Snapshot) (es.Digest, error)
	FingerprintAppend(AppendRequest) (es.Digest, error)
	ValidateHeader(SessionHeader) error
	ValidateCommit(SessionCommit) error
	ValidateSnapshot(Snapshot) error
}

// ProfileV1 returns the ProtocolVersion1 profile.
func ProfileV1() ProtocolProfile { return profileV1{} }

// ProfileFor returns the profile bound to version.
func ProfileFor(version uint16) (ProtocolProfile, error) {
	if version == ProtocolVersion1 {
		return profileV1{}, nil
	}
	return nil, &Error{Code: ErrUnsupportedProfile, Operation: "profile", Detail: fmt.Sprintf("protocol version %d", version)}
}

type profileV1 struct{}

func (profileV1) Version() uint16 { return ProtocolVersion1 }

type headerDigestBody struct {
	ProtocolVersion uint16           `json:"protocolVersion"`
	SessionID       SessionID        `json:"sessionId"`
	CausationID     es.CausationID   `json:"causationId,omitempty"`
	Metadata        jsonstable.Value `json:"metadata,omitempty"`
}

func (profileV1) HeaderDigest(h SessionHeader) (es.Digest, error) {
	if h.ParentFork != nil {
		return "", &Error{Code: ErrUnsupported, Operation: "header", SessionID: h.SessionID, Detail: "fork is not in v1"}
	}
	return digestDomain("twilight/session/header", headerDigestBody{h.ProtocolVersion, h.SessionID, h.CausationID, h.Metadata})
}

type eventDigestBody struct {
	SessionID           SessionID        `json:"sessionId"`
	Revision            es.Revision      `json:"revision"`
	Index               uint16           `json:"index"`
	EventID             EventID          `json:"eventId"`
	Type                EventType        `json:"type"`
	RecordedAtUnixMilli int64            `json:"recordedAtUnixMilli"`
	SourceEvents        []EventID        `json:"sourceEvents,omitempty"`
	Payload             jsonstable.Value `json:"payload"`
}

func (profileV1) EventDigest(sid SessionID, rev es.Revision, e SessionEvent) (es.Digest, error) {
	return digestDomain("twilight/session/event", eventDigestBody{sid, rev, e.Index, e.EventID, e.Type, e.RecordedAtUnixMilli, e.SourceEvents, e.Payload})
}

type commitDigestBody struct {
	ProtocolVersion uint16         `json:"protocolVersion"`
	SessionID       SessionID      `json:"sessionId"`
	Revision        es.Revision    `json:"revision"`
	PreviousDigest  es.Digest      `json:"previousDigest"`
	CommitID        CommitID       `json:"commitId"`
	CausationID     es.CausationID `json:"causationId,omitempty"`
	CorrelationID   string         `json:"correlationId,omitempty"`
	Events          []es.Digest    `json:"events"`
}

func (profileV1) CommitDigest(c SessionCommit) (es.Digest, error) {
	digests := make([]es.Digest, len(c.Events))
	for i := range c.Events {
		digests[i] = c.Events[i].EventDigest
	}
	return digestDomain("twilight/session/commit", commitDigestBody{c.ProtocolVersion, c.SessionID, c.Revision, c.PreviousDigest, c.CommitID, c.CausationID, c.CorrelationID, digests})
}

type snapshotDigestBody struct {
	ProtocolVersion   uint16           `json:"protocolVersion"`
	SessionID         SessionID        `json:"sessionId"`
	ProjectionKey     ProjectionKey    `json:"projectionKey"`
	ProjectionVersion uint16           `json:"projectionVersion"`
	Through           Head             `json:"through"`
	State             jsonstable.Value `json:"state"`
}

func (profileV1) SnapshotDigest(s Snapshot) (es.Digest, error) {
	return digestDomain("twilight/session/snapshot", snapshotDigestBody{s.ProtocolVersion, s.SessionID, s.ProjectionKey, s.ProjectionVersion, s.Through, s.State})
}

type fingerprintEvent struct {
	EventID      EventID          `json:"eventId"`
	Type         EventType        `json:"type"`
	SourceEvents []EventID        `json:"sourceEvents,omitempty"`
	Payload      jsonstable.Value `json:"payload"`
}

type fingerprintBody struct {
	SessionID     SessionID          `json:"sessionId"`
	CommitID      CommitID           `json:"commitId"`
	CausationID   es.CausationID     `json:"causationId,omitempty"`
	CorrelationID string             `json:"correlationId,omitempty"`
	Events        []fingerprintEvent `json:"events"`
}

// FingerprintAppend covers everything that makes a retry "the same append":
// not ExpectedHead and not RecordedAtUnixMilli (SES-APP-1).
func (profileV1) FingerprintAppend(r AppendRequest) (es.Digest, error) {
	events := make([]fingerprintEvent, len(r.Events))
	for i, e := range r.Events {
		events[i] = fingerprintEvent{e.EventID, e.Type, e.SourceEvents, e.Payload}
	}
	return digestDomain("twilight/session/append", fingerprintBody{r.SessionID, r.CommitID, r.CausationID, r.CorrelationID, events})
}

func fingerprintCommit(p ProtocolProfile, c *SessionCommit) (es.Digest, error) {
	events := make([]UncommittedEvent, len(c.Events))
	for i, e := range c.Events {
		events[i] = UncommittedEvent{EventID: e.EventID, Type: e.Type, SourceEvents: e.SourceEvents, Payload: e.Payload}
	}
	return p.FingerprintAppend(AppendRequest{SessionID: c.SessionID, CommitID: c.CommitID, CausationID: c.CausationID, CorrelationID: c.CorrelationID, Events: events})
}

func (p profileV1) ValidateHeader(h SessionHeader) error {
	if h.ProtocolVersion != ProtocolVersion1 {
		return &Error{Code: ErrUnsupportedProfile, Operation: "header", SessionID: h.SessionID}
	}
	if err := validIdentity("SessionID", string(h.SessionID)); err != nil {
		return newError(ErrInvalid, "header", h.SessionID, err.Error())
	}
	if h.ParentFork != nil {
		return &Error{Code: ErrUnsupported, Operation: "header", SessionID: h.SessionID, Detail: "fork is not in v1"}
	}
	want, err := p.HeaderDigest(h)
	if err != nil {
		return err
	}
	if h.HeaderDigest != want {
		return newError(ErrCorrupt, "header", h.SessionID, "header digest mismatch")
	}
	return nil
}

func (p profileV1) ValidateCommit(c SessionCommit) error {
	if c.ProtocolVersion != ProtocolVersion1 {
		return &Error{Code: ErrUnsupportedProfile, Operation: "commit", SessionID: c.SessionID, CommitID: c.CommitID}
	}
	if c.Revision == 0 || len(c.Events) == 0 {
		return &Error{Code: ErrCorrupt, Operation: "commit", SessionID: c.SessionID, CommitID: c.CommitID, Detail: "empty commit or zero revision"}
	}
	for i := range c.Events {
		e := &c.Events[i]
		if int(e.Index) != i {
			return &Error{Code: ErrCorrupt, Operation: "commit", SessionID: c.SessionID, CommitID: c.CommitID, Detail: "event index gap"}
		}
		want, err := p.EventDigest(c.SessionID, c.Revision, *e)
		if err != nil {
			return err
		}
		if e.EventDigest != want {
			return &Error{Code: ErrCorrupt, Operation: "commit", SessionID: c.SessionID, CommitID: c.CommitID, Detail: fmt.Sprintf("event %d digest mismatch", i)}
		}
	}
	want, err := p.CommitDigest(c)
	if err != nil {
		return err
	}
	if c.CommitDigest != want {
		return &Error{Code: ErrCorrupt, Operation: "commit", SessionID: c.SessionID, CommitID: c.CommitID, Detail: "commit digest mismatch"}
	}
	return nil
}

func (p profileV1) ValidateSnapshot(s Snapshot) error {
	if s.ProtocolVersion != ProtocolVersion1 {
		return &Error{Code: ErrUnsupportedProfile, Operation: "snapshot", SessionID: s.SessionID}
	}
	want, err := p.SnapshotDigest(s)
	if err != nil {
		return err
	}
	if s.SnapshotDigest != want {
		return newError(ErrCorrupt, "snapshot", s.SessionID, "snapshot digest mismatch")
	}
	return nil
}

// validateUncommitted checks one event of an append group before it is
// sealed: identities, canonical payload and sorted-unique SourceEvents.
func validateUncommitted(e *UncommittedEvent) error {
	if err := validIdentity("EventID", string(e.EventID)); err != nil {
		return err
	}
	if err := validIdentity("EventType", string(e.Type)); err != nil {
		return err
	}
	if e.Payload.IsZero() {
		return errors.New("empty payload")
	}
	canon, err := jsonstable.Canonicalize(e.Payload.Bytes())
	if err != nil {
		return fmt.Errorf("payload: %w", err)
	}
	if !bytes.Equal(canon, e.Payload.Bytes()) {
		return errors.New("payload is not canonical")
	}
	if !bytes.HasPrefix(bytes.TrimSpace(e.Payload.Bytes()), []byte("{")) {
		return errors.New("payload is not an object")
	}
	for i := range e.SourceEvents {
		if err := validIdentity("SourceEvent", string(e.SourceEvents[i])); err != nil {
			return err
		}
		if i > 0 && e.SourceEvents[i] <= e.SourceEvents[i-1] {
			return errors.New("SourceEvents must be sorted and unique")
		}
	}
	return nil
}

func validIdentity(name, v string) error {
	if v == "" {
		return fmt.Errorf("%s is empty", name)
	}
	if !utf8.ValidString(v) {
		return fmt.Errorf("%s is not valid UTF-8", name)
	}
	return nil
}

func digestDomain(domain string, body any) (es.Digest, error) {
	raw, err := es.EncodeTypedPayload(ProtocolVersion1, domain, body)
	if err != nil {
		return "", err
	}
	return es.DigestBytes(raw), nil
}

// SortedUniqueEventIDs canonicalizes a SourceEvents set.
func SortedUniqueEventIDs(ids []EventID) []EventID {
	if len(ids) == 0 {
		return nil
	}
	out := append([]EventID(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	n := 0
	for i := range out {
		if n == 0 || out[i] != out[n-1] {
			out[n] = out[i]
			n++
		}
	}
	return out[:n]
}
