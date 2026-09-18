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
// its digest is computed from (canonical_v1.go), so sha256(bytes) == digest.
const (
	frozenRequestType      = "model_request"
	frozenModelResultType  = "model_result"
	frozenToolOutputType   = "tool_output"
	frozenToolResponseType = "tool_response_payload"
)

// bodiesV1 is the SchemaVersion1 frozen-body codec.
type bodiesV1 struct{}

func (bodiesV1) EncodeRequest(req *ModelRequest, want Digest) ([]byte, error) {
	return encodeFrozen(SchemaVersion1, frozenRequestType, *req, want)
}

func (bodiesV1) EncodeModelResult(result *ModelResult, want Digest) ([]byte, error) {
	return encodeFrozen(SchemaVersion1, frozenModelResultType, *result, want)
}

func (bodiesV1) EncodeToolOutput(output CanonicalJSON, want Digest) ([]byte, error) {
	if output.IsZero() {
		return nil, errors.New("agent: frozen tool output: empty output")
	}
	return encodeFrozen(SchemaVersion1, frozenToolOutputType, toolOutputDigestBody{Output: output}, want)
}

func (bodiesV1) EncodeToolResponse(payload CanonicalJSON, want Digest) ([]byte, error) {
	return encodeFrozen(SchemaVersion1, frozenToolResponseType, toolResponsePayloadDigestBody{Payload: payload}, want)
}

func (bodiesV1) DecodeRequest(raw []byte, want Digest) (ModelRequest, error) {
	return decodeFrozen[ModelRequest](raw, SchemaVersion1, frozenRequestType, want)
}

func (bodiesV1) DecodeModelResult(raw []byte, want Digest) (ModelResult, error) {
	return decodeFrozen[ModelResult](raw, SchemaVersion1, frozenModelResultType, want)
}

func (bodiesV1) DecodeToolOutput(raw []byte, want Digest) (CanonicalJSON, error) {
	body, err := decodeFrozen[toolOutputDigestBody](raw, SchemaVersion1, frozenToolOutputType, want)
	if err != nil {
		return CanonicalJSON{}, err
	}
	return body.Output, nil
}

func (bodiesV1) DecodeToolResponse(raw []byte, want Digest) (CanonicalJSON, error) {
	body, err := decodeFrozen[toolResponsePayloadDigestBody](raw, SchemaVersion1, frozenToolResponseType, want)
	if err != nil {
		return CanonicalJSON{}, err
	}
	return body.Payload, nil
}

// frozenSchema reads the schema version a stored body was encoded under from
// its envelope prefix and binds that version's codec, so a store holding
// bodies of several versions decodes each with the schema that wrote it.
func frozenSchema(raw []byte) (Schema, error) {
	var version uint16
	if _, err := fmt.Sscanf(string(raw[:min(len(raw), 8)]), "v%d:", &version); err != nil {
		return Schema{}, errors.New("agent: frozen body: not a typed envelope")
	}
	return SchemaFor(version)
}

// DecodeFrozenRequest restores a request body and checks it still digests to
// the name it was stored under. The version comes from the body itself.
func DecodeFrozenRequest(raw []byte, want Digest) (ModelRequest, error) {
	schema, err := frozenSchema(raw)
	if err != nil {
		return ModelRequest{}, err
	}
	return schema.Bodies.DecodeRequest(raw, want)
}

// DecodeFrozenModelResult restores a model result named by ResultDigest.
func DecodeFrozenModelResult(raw []byte, want Digest) (ModelResult, error) {
	schema, err := frozenSchema(raw)
	if err != nil {
		return ModelResult{}, err
	}
	return schema.Bodies.DecodeModelResult(raw, want)
}

// DecodeFrozenToolOutput restores a tool output named by OutputDigest.
func DecodeFrozenToolOutput(raw []byte, want Digest) (CanonicalJSON, error) {
	schema, err := frozenSchema(raw)
	if err != nil {
		return CanonicalJSON{}, err
	}
	return schema.Bodies.DecodeToolOutput(raw, want)
}

// DecodeFrozenToolResponse restores an external tool response named by
// ResponseDigest.
func DecodeFrozenToolResponse(raw []byte, want Digest) (CanonicalJSON, error) {
	schema, err := frozenSchema(raw)
	if err != nil {
		return CanonicalJSON{}, err
	}
	return schema.Bodies.DecodeToolResponse(raw, want)
}

func encodeFrozen(version uint16, typ string, body any, want Digest) ([]byte, error) { //nolint:unparam // version is the caller schema's; only v1 exists today.
	raw, err := encodeEnvelopeBody(version, typ, body)
	if err != nil {
		return nil, err
	}
	if got := sha256Digest(raw); got != want {
		return nil, fmt.Errorf("agent: frozen %s: body digest %s does not match %s", typ, got, want)
	}
	return raw, nil
}

func decodeFrozen[T any](raw []byte, version uint16, typ string, want Digest) (T, error) {
	var zero T
	if got := sha256Digest(raw); got != want {
		return zero, fmt.Errorf("agent: frozen %s: stored body digest %s does not match %s", typ, got, want)
	}
	prefix := envelopePrefix(version, typ)
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
