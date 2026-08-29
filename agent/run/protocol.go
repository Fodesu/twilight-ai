package run

import (
	"errors"
	"fmt"

	"github.com/memohai/twilight/agent/es"
)

// SchemaVersion1 is the first published wire schema. Canonical encoding and
// Evolve folding semantics stay stable within a published version.
const SchemaVersion1 uint16 = 1

// currentSchemaVersion is what new commands and facts are written with.
const currentSchemaVersion = SchemaVersion1

// CommandEnvelope carries one command with its persisted protocol identity.
type CommandEnvelope struct {
	SchemaVersion uint16       `json:"schemaVersion"`
	Type          string       `json:"type"`
	RunID         RunID        `json:"runId"`
	ID            CommandID    `json:"id"`
	Digest        Digest       `json:"digest"`
	Command       AgentCommand `json:"command"`
}

// AgentEvent carries one fact produced by an accepted command. All events of
// one transition share Revision, CommandID and CommandDigest; Index orders
// them within the transition. Identity is assigned by the authority.
type AgentEvent struct {
	SchemaVersion uint16    `json:"schemaVersion"`
	Type          string    `json:"type"`
	RunID         RunID     `json:"runId"`
	Revision      uint64    `json:"revision"`
	Index         uint16    `json:"index"`
	CommandID     CommandID `json:"commandId"`
	CommandDigest Digest    `json:"commandDigest"`
	Digest        Digest    `json:"digest"` // canonical digest of the fact
	Fact          Fact      `json:"fact"`
}

// encodeEnvelopeBody is the digest input for a command: schema version, type
// discriminator and canonical command bytes. The Digest field itself, base
// revisions and grants never enter the digest (spec §5.5).
func encodeEnvelopeBody(schemaVersion uint16, typ string, body any) ([]byte, error) {
	return es.EncodeTypedPayload(schemaVersion, typ, body)
}

// EncodeCommand renders the canonical bytes of a command envelope, excluding
// the Digest field.
//
//nolint:gocritic // hugeParam: command envelope encoding is value-based and does not retain caller aliases.
func EncodeCommand(env CommandEnvelope) ([]byte, error) {
	if env.Type == "" || env.Type != commandType(env.Command) {
		return nil, fmt.Errorf("agent: encode: type %q does not match command variant", env.Type)
	}
	body, err := encodeEnvelopeBody(env.SchemaVersion, env.Type, env.Command)
	if err != nil {
		return nil, err
	}
	header := fmt.Sprintf("run:%d:%s:cmd:%d:%s:", len(env.RunID), env.RunID, len(env.ID), env.ID)
	return append([]byte(header), body...), nil
}

// DigestCommand computes the canonical digest of one command.
func DigestCommand(schemaVersion uint16, typ string, command AgentCommand) (Digest, error) {
	if typ == "" || typ != commandType(command) {
		return "", fmt.Errorf("agent: digest: type %q does not match command variant", typ)
	}
	body, err := encodeEnvelopeBody(schemaVersion, typ, command)
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

// EncodeFact renders the canonical bytes of one fact.
func EncodeFact(schemaVersion uint16, typ string, fact Fact) ([]byte, error) {
	if typ == "" || typ != factType(fact) {
		return nil, fmt.Errorf("agent: encode: type %q does not match fact variant", typ)
	}
	return encodeEnvelopeBody(schemaVersion, typ, fact)
}

// DigestFact computes the canonical digest of one fact.
func DigestFact(schemaVersion uint16, typ string, fact Fact) (Digest, error) {
	body, err := EncodeFact(schemaVersion, typ, fact)
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

// EncodeRunSeed renders the canonical bytes of an admission seed so admission
// records can reuse the protocol's identity rules.
func EncodeRunSeed(seed RunSeed) ([]byte, error) {
	return encodeEnvelopeBody(currentSchemaVersion, "run_seed", seed)
}

// DigestRunSeed computes the canonical digest of an admission seed.
func DigestRunSeed(schemaVersion uint16, seed RunSeed) (Digest, error) {
	body, err := encodeEnvelopeBody(schemaVersion, "run_seed", seed)
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

type toolResponseDecisionDigestBody struct {
	Kind     ResponseKind     `json:"kind"`
	Decision ResponseDecision `json:"decision"`
	Reason   string           `json:"reason,omitempty"`
}

// DigestToolResponseDecision computes the content digest for approval and
// rejection ingress. The ResponseID remains the routing/idempotency key; this
// digest binds the actual decision payload recorded in the command/fact.
func DigestToolResponseDecision(kind ResponseKind, decision ResponseDecision, reason string) (Digest, error) {
	if kind != ResponseApproval && kind != ResponseExternal {
		return "", fmt.Errorf("agent: response decision: unsupported kind %q", kind)
	}
	if decision != ResponseDecisionApproved && decision != ResponseDecisionRejected {
		return "", fmt.Errorf("agent: response decision: unsupported decision %q", decision)
	}
	body, err := encodeEnvelopeBody(currentSchemaVersion, "tool_response_decision", toolResponseDecisionDigestBody{
		Kind: kind, Decision: decision, Reason: reason,
	})
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

type toolResponsePayloadDigestBody struct {
	Payload CanonicalJSON `json:"payload"`
}

// DigestToolResponsePayload computes the content digest for an ExternalResponse
// answer payload.
func DigestToolResponsePayload(payload CanonicalJSON) (Digest, error) {
	body, err := encodeEnvelopeBody(currentSchemaVersion, "tool_response_payload", toolResponsePayloadDigestBody{Payload: payload})
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

// DigestRequest covers every field of a frozen ModelRequest with no exclusions
// (spec §2.1 rule 7).
//
//nolint:gocritic // hugeParam: digest covers the complete immutable ModelRequest value.
func DigestRequest(req ModelRequest) (Digest, error) {
	body, err := encodeEnvelopeBody(currentSchemaVersion, "model_request", req)
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

// DigestToolDefinition covers one provider-neutral tool definition.
func DigestToolDefinition(def ToolDefinition) (Digest, error) {
	body, err := encodeEnvelopeBody(currentSchemaVersion, "tool_definition", def)
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

// DigestToolSpec covers one agent ToolSpec (ref, definition, digest, policy).
//
//nolint:gocritic // hugeParam: digest covers the complete immutable ToolSpec value.
func DigestToolSpec(spec ToolSpec) (Digest, error) {
	body, err := encodeEnvelopeBody(currentSchemaVersion, "tool_spec", spec)
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

// DigestToolSpecs covers an ordered ToolSpec list: ref, schema, order and
// policy all participate (spec §3.7.1 rule 1).
func DigestToolSpecs(specs []ToolSpec) (Digest, error) {
	body, err := encodeEnvelopeBody(currentSchemaVersion, "tool_specs", specs)
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

// DigestModelStepBinding combines model, request digest and tools digest into
// the immutable ModelStep binding digest.
func DigestModelStepBinding(model ModelRef, requestDigest, toolsDigest Digest) (Digest, error) {
	if model == "" || requestDigest == "" || toolsDigest == "" {
		return "", errors.New("agent: model step binding requires model, request digest and tools digest")
	}
	return sha256Digest([]byte(namespacedHash("twilight/model-step-binding",
		string(model), string(requestDigest), string(toolsDigest)))), nil
}

// digestBindingSet covers the full ordered pre-Response call set of one
// ToolStep; it feeds DeriveToolStepID and is carried inside ToolStepOpened.
// It is pinned to SchemaVersion1: the value is persisted in v1 facts, so a
// future schema bump must not change how replayed v1 state folds.
func digestBindingSet(bindings []ToolCallBinding) (Digest, error) {
	body, err := encodeEnvelopeBody(SchemaVersion1, "tool_call_bindings", bindings)
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

// DigestToolCallBinding covers one binding: definition, policy and canonical
// arguments plus the CallID (spec §4.2). Runtime conformance suites use this
// helper to construct the same frozen binding identities as the Loop.
func DigestToolCallBinding(callID CallID, definitionDigest Digest, policy ResponsePolicy, arguments CanonicalJSON) (Digest, error) {
	return sha256Digest([]byte(namespacedHash("twilight/tool-call-binding",
		string(callID), string(definitionDigest), fmt.Sprintf("%d", policy), arguments.String()))), nil
}

func digestToolCallBinding(callID CallID, definitionDigest Digest, policy ResponsePolicy, arguments CanonicalJSON) (Digest, error) {
	return DigestToolCallBinding(callID, definitionDigest, policy, arguments)
}
