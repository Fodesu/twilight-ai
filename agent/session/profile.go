package session

import (
	"bytes"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/memohai/twilight/agent/es"
	"github.com/memohai/twilight/agent/jsonstable"
)

// ProtocolProfile freezes the kernel wire for one ProtocolVersion
// (SES-WIR-2): the header digest and the per-row chained digest.
type ProtocolProfile interface {
	Version() uint16
	HeaderDigest(SessionHeader) (es.Digest, error)
	// EventDigest computes a row's Digest given the previous row's Digest
	// (HeaderDigest for Seq 0). The row's own Digest field is ignored.
	EventDigest(prev es.Digest, sid SessionID, row SessionEvent) (es.Digest, error)
	ValidateHeader(SessionHeader) error
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
	ProtocolVersion    uint16           `json:"protocolVersion"`
	SessionID          SessionID        `json:"sessionId"`
	CreatedAtUnixMilli int64            `json:"createdAtUnixMilli"`
	CausationID        es.CausationID   `json:"causationId,omitempty"`
	Metadata           jsonstable.Value `json:"metadata,omitempty"`
}

func (profileV1) HeaderDigest(h SessionHeader) (es.Digest, error) {
	if h.ParentFork != nil {
		return "", &Error{Code: ErrUnsupported, Operation: "header", SessionID: h.SessionID, Detail: "fork is not in v1"}
	}
	return digestDomain("twilight/session/header", headerDigestBody{h.ProtocolVersion, h.SessionID, h.CreatedAtUnixMilli, h.CausationID, h.Metadata})
}

type eventDigestBody struct {
	Prev                es.Digest        `json:"prev"`
	SessionID           SessionID        `json:"sessionId"`
	Seq                 Seq              `json:"seq"`
	CommitID            CommitID         `json:"commitId"`
	Index               uint16           `json:"index"`
	Last                bool             `json:"last"`
	Type                EventType        `json:"type"`
	RecordedAtUnixMilli int64            `json:"recordedAtUnixMilli"`
	SourceSeqs          []Seq            `json:"sourceSeqs,omitempty"`
	Ignorable           bool             `json:"ignorable,omitempty"`
	Payload             jsonstable.Value `json:"payload"`
}

func (profileV1) EventDigest(prev es.Digest, sid SessionID, e SessionEvent) (es.Digest, error) {
	return digestDomain("twilight/session/event", eventDigestBody{prev, sid, e.Seq, e.CommitID, e.Index, e.Last, e.Type, e.RecordedAtUnixMilli, e.SourceSeqs, e.Ignorable, e.Payload})
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

// ValidateChain recomputes every row digest from the header and reports the
// first corrupt row (SES-REP-1). rows must start at Seq 0.
func ValidateChain(p ProtocolProfile, header SessionHeader, rows []SessionEvent) error {
	prev := header.HeaderDigest
	for i := range rows {
		r := &rows[i]
		if r.Seq != Seq(i) {
			return &Error{Code: ErrCorrupt, Operation: "read", SessionID: header.SessionID, Detail: fmt.Sprintf("seq gap at %d", i)}
		}
		want, err := p.EventDigest(prev, header.SessionID, *r)
		if err != nil {
			return err
		}
		if r.Digest != want {
			return &Error{Code: ErrCorrupt, Operation: "read", SessionID: header.SessionID, CommitID: r.CommitID, Detail: fmt.Sprintf("digest mismatch at seq %d", i)}
		}
		prev = r.Digest
	}
	return nil
}

// ValidateUncommitted checks one event of a group before it is sealed:
// identities and a canonical JSON object payload (SES-WIR-1).
func ValidateUncommitted(e *UncommittedEvent) error {
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
