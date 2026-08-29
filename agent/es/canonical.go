// Package es provides domain-neutral event-sourcing protocol mechanisms.
//
// It deliberately does not know Run, Session, Queue, command, fact, or
// Runtime semantics. Domains supply their own event payload codecs and use
// this package for canonical identity, complete-record validation, and fold
// ordering.
package es

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/memohai/twilight/agent/jsonstable"
)

// Digest is a SHA-256 digest over canonical protocol bytes.
type Digest string

// CausationID is an opaque cross-domain lineage identifier. Its namespace and
// meaning are owned by the domain or application, never by this package.
type CausationID string

// Canonicalize validates and canonicalizes external JSON protocol bytes.
func Canonicalize(raw []byte) ([]byte, error) {
	return jsonstable.Canonicalize(raw)
}

// MarshalCanonical encodes a Go protocol value into canonical JSON bytes.
func MarshalCanonical(v any) ([]byte, error) {
	return jsonstable.MarshalCanonical(v)
}

// DigestBytes computes the stable SHA-256 identity of canonical bytes. The
// caller is responsible for canonicalizing structured input first.
func DigestBytes(data []byte) Digest {
	sum := sha256.Sum256(data)
	return Digest("sha256:" + hex.EncodeToString(sum[:]))
}

// DigestCanonical canonicalizes v and computes its digest.
func DigestCanonical(v any) (Digest, error) {
	body, err := MarshalCanonical(v)
	if err != nil {
		return "", err
	}
	return DigestBytes(body), nil
}

// EncodeTypedPayload renders the common stable digest input for a versioned,
// type-discriminated domain payload. It is intentionally agnostic about which
// schema versions and type names a domain supports.
func EncodeTypedPayload(schemaVersion uint16, typ string, payload any) ([]byte, error) {
	canonical, err := MarshalCanonical(payload)
	if err != nil {
		return nil, err
	}
	prefix := fmt.Sprintf("v%d:%d:%s:", schemaVersion, len(typ), typ)
	return append([]byte(prefix), canonical...), nil
}
