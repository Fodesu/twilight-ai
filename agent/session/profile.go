package session

import (
	"bytes"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/jsonstable"
)

// headerDigestBody is the preimage of the SegmentHeader digest: every header
// field except the digest itself. No Session identity enters it: the segment
// is a canonical object of the ledger DAG, named by roots but not by any one
// of them (SES-WIR-2).
type headerDigestBody struct {
	ProtocolVersion uint16
	// Parent is omitted when nil, so a root segment's preimage has no edge
	// slot (SES-WIR-2).
	Parent      *LedgerRef `json:",omitempty"`
	Nonce       string
	CausationID es.CausationID
	Metadata    jsonstable.Value
}

// validateEventShape checks the event invariants every protocol version
// seals: a non-empty valid-UTF-8 type and a canonical JSON object payload.
func validateEventShape(typ EventType, payload jsonstable.Value) error {
	if err := validIdentity("EventType", string(typ)); err != nil {
		return err
	}
	if payload.IsZero() {
		return errors.New("empty payload")
	}
	canon, err := jsonstable.Canonicalize(payload.Bytes())
	if err != nil {
		return fmt.Errorf("payload: %w", err)
	}
	if !bytes.Equal(canon, payload.Bytes()) {
		return errors.New("payload is not canonical")
	}
	if !bytes.HasPrefix(bytes.TrimSpace(payload.Bytes()), []byte("{")) {
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

// digestDomain renders the versioned domain separator. The version is the
// profile's, never a constant: it is the only version signal a batch or
// commit digest carries (SES-VER-2).
func digestDomain(version uint16, domain string, body any) (es.Digest, error) {
	raw, err := es.EncodeTypedPayload(version, domain, body)
	if err != nil {
		return "", err
	}
	return es.DigestBytes(raw), nil
}
