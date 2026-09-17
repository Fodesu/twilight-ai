package run

import (
	"bytes"
	"context"
	"errors"
	"fmt"
)

// FrozenValueStore is the run layer's port to the content-addressed side
// store for the bodies that facts name by digest only (RUN-WIR-4): the
// ModelRequest of a Prepared step, the ModelResult of a completed step, a
// tool output and an external tool response. The digest is the SHA-256 of
// the stored bytes, so a body is one cas entry whose key is its digest; the
// adapter that realizes this port over a cas ContentStore lives in
// agent/session/run. Put is idempotent. The fact that names a body is its
// retention root: the body stays readable as long as the Session ledger
// holds the fact.
type FrozenValueStore interface {
	Put(ctx context.Context, digest Digest, value []byte) error
	Get(ctx context.Context, digest Digest) ([]byte, bool, error)
}

// FrozenAuthority is the cas Authority under which frozen bodies are stored.
// It is a string, not an artifact type, because run does not depend on
// artifact; the adapter converts it.
const FrozenAuthority = "twilight/run/frozen"

// ErrFrozenValueMissing reports that a body named by a fact is not in the
// FrozenValueStore. For a request, recovery cannot resend it and the caller
// decides whether to retry the attempt with a fresh plan; for a result or an
// output, the structural projection stays valid and only materialization
// fails.
var ErrFrozenValueMissing = errors.New("agent: frozen value missing")

// Frozen body envelope types. Each body is stored as the versioned envelope
// its digest is computed from (protocol_v1.go), so sha256(bytes) == digest.
const (
	frozenRequestType      = "model_request"
	frozenModelResultType  = "model_result"
	frozenToolOutputType   = "tool_output"
	frozenToolResponseType = "tool_response_payload"
)

// EncodeFrozenRequest renders the bytes stored for a request: the same
// versioned envelope body the RequestDigest is computed from, so that
// sha256(bytes) == want and the body is addressable by its own digest.
func EncodeFrozenRequest(req *ModelRequest, want Digest) ([]byte, error) {
	return encodeFrozen(frozenRequestType, *req, want)
}

// DecodeFrozenRequest restores a request body and checks it still digests to
// the name it was stored under.
func DecodeFrozenRequest(raw []byte, want Digest) (ModelRequest, error) {
	return decodeFrozen[ModelRequest](raw, frozenRequestType, want)
}

// EncodeFrozenModelResult renders the bytes stored for a model result under
// ModelStepCompleted.ResultDigest.
func EncodeFrozenModelResult(result *ModelResult, want Digest) ([]byte, error) {
	return encodeFrozen(frozenModelResultType, *result, want)
}

// DecodeFrozenModelResult restores a model result named by ResultDigest.
func DecodeFrozenModelResult(raw []byte, want Digest) (ModelResult, error) {
	return decodeFrozen[ModelResult](raw, frozenModelResultType, want)
}

// EncodeFrozenToolOutput renders the bytes stored for a tool output under
// ToolCallCompleted.OutputDigest.
func EncodeFrozenToolOutput(output CanonicalJSON, want Digest) ([]byte, error) {
	if output.IsZero() {
		return nil, errors.New("agent: frozen tool output: empty output")
	}
	return encodeFrozen(frozenToolOutputType, toolOutputDigestBody{Output: output}, want)
}

// DecodeFrozenToolOutput restores a tool output named by OutputDigest.
func DecodeFrozenToolOutput(raw []byte, want Digest) (CanonicalJSON, error) {
	body, err := decodeFrozen[toolOutputDigestBody](raw, frozenToolOutputType, want)
	if err != nil {
		return CanonicalJSON{}, err
	}
	return body.Output, nil
}

// EncodeFrozenToolResponse renders the bytes stored for an external tool
// response under ToolCallAnswered.ResponseDigest.
func EncodeFrozenToolResponse(payload CanonicalJSON, want Digest) ([]byte, error) {
	return encodeFrozen(frozenToolResponseType, toolResponsePayloadDigestBody{Payload: payload}, want)
}

// DecodeFrozenToolResponse restores an external tool response named by
// ResponseDigest.
func DecodeFrozenToolResponse(raw []byte, want Digest) (CanonicalJSON, error) {
	body, err := decodeFrozen[toolResponsePayloadDigestBody](raw, frozenToolResponseType, want)
	if err != nil {
		return CanonicalJSON{}, err
	}
	return body.Payload, nil
}

func encodeFrozen(typ string, body any, want Digest) ([]byte, error) {
	raw, err := encodeEnvelopeBody(SchemaVersion1, typ, body)
	if err != nil {
		return nil, err
	}
	if got := sha256Digest(raw); got != want {
		return nil, fmt.Errorf("agent: frozen %s: body digest %s does not match %s", typ, got, want)
	}
	return raw, nil
}

func decodeFrozen[T any](raw []byte, typ string, want Digest) (T, error) {
	var zero T
	if got := sha256Digest(raw); got != want {
		return zero, fmt.Errorf("agent: frozen %s: stored body digest %s does not match %s", typ, got, want)
	}
	prefix := envelopePrefix(SchemaVersion1, typ)
	if !bytes.HasPrefix(raw, prefix) {
		return zero, fmt.Errorf("agent: frozen %s: stored body is not a %s envelope", typ, typ)
	}
	var out T
	if err := decodeStrictJSON(raw[len(prefix):], &out); err != nil {
		return zero, fmt.Errorf("agent: frozen %s: %w", typ, err)
	}
	return out, nil
}

// envelopePrefix is the domain prefix EncodeTypedPayload puts before the
// canonical body (agent/es).
func envelopePrefix(schemaVersion uint16, typ string) []byte {
	return []byte(fmt.Sprintf("v%d:%d:%s:", schemaVersion, len(typ), typ))
}
