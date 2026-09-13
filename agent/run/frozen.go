package run

import (
	"bytes"
	"context"
	"errors"
	"fmt"
)

// FrozenValueStore is the run layer's port to the content-addressed side
// store for frozen bodies that facts name by digest only (RUN-WIR-4): today
// the ModelRequest of a Prepared step. The digest is the SHA-256 of the stored
// bytes, so a body is one cas entry whose key is its digest; the adapter that
// realizes this port over a cas ContentStore lives in agent/session/run.
// Put is idempotent; a body may be dropped once the step that named it has
// settled, so readers must treat a missing body as a distinct condition.
type FrozenValueStore interface {
	Put(ctx context.Context, digest Digest, value []byte) error
	Get(ctx context.Context, digest Digest) ([]byte, bool, error)
}

// FrozenAuthority is the cas Authority under which frozen bodies are stored.
// It is a string, not an artifact type, because run does not depend on
// artifact; the adapter converts it.
const FrozenAuthority = "twilight/run/frozen"

// ErrFrozenValueMissing reports that a body named by a fact is no longer in
// the FrozenValueStore. Recovery cannot resend the request; the caller decides
// whether to retry the attempt with a fresh plan.
var ErrFrozenValueMissing = errors.New("agent: frozen value missing")

const frozenRequestType = "model_request"

// EncodeFrozenRequest renders the bytes stored for a request: the same
// versioned envelope body the RequestDigest is computed from, so that
// sha256(bytes) == want and the body is addressable by its own digest.
func EncodeFrozenRequest(req *ModelRequest, want Digest) ([]byte, error) {
	body, err := encodeEnvelopeBody(SchemaVersion1, frozenRequestType, *req)
	if err != nil {
		return nil, err
	}
	if got := sha256Digest(body); got != want {
		return nil, fmt.Errorf("agent: frozen request: body digest %s does not match %s", got, want)
	}
	return body, nil
}

// DecodeFrozenRequest restores a request body and checks it still digests to
// the name it was stored under.
func DecodeFrozenRequest(raw []byte, want Digest) (ModelRequest, error) {
	if got := sha256Digest(raw); got != want {
		return ModelRequest{}, fmt.Errorf("agent: frozen request: stored body digest %s does not match %s", got, want)
	}
	prefix := envelopePrefix(SchemaVersion1, frozenRequestType)
	if !bytes.HasPrefix(raw, prefix) {
		return ModelRequest{}, errors.New("agent: frozen request: stored body is not a model_request envelope")
	}
	var req ModelRequest
	if err := decodeStrictJSON(raw[len(prefix):], &req); err != nil {
		return ModelRequest{}, fmt.Errorf("agent: frozen request: %w", err)
	}
	return req, nil
}

// envelopePrefix is the domain prefix EncodeTypedPayload puts before the
// canonical body (agent/es).
func envelopePrefix(schemaVersion uint16, typ string) []byte {
	return []byte(fmt.Sprintf("v%d:%d:%s:", schemaVersion, len(typ), typ))
}
