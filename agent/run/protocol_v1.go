package run

import (
	"errors"
	"fmt"
)

// protocolV1 is the SchemaVersion1 implementation. Replay of a v1 Run must
// call these methods even after currentSchemaVersion moves forward.
type protocolV1 struct{}

func (protocolV1) Version() uint16 { return SchemaVersion1 }

func (protocolV1) DigestRequest(req ModelRequest) (Digest, error) {
	return digestRequestV1(req)
}

func (protocolV1) DigestToolDefinition(def ToolDefinition) (Digest, error) {
	return digestToolDefinitionV1(def)
}

func (protocolV1) DigestToolSpec(spec ToolSpec) (Digest, error) {
	return digestToolSpecV1(spec)
}

func (protocolV1) DigestToolSpecs(specs []ToolSpec) (Digest, error) {
	return digestToolSpecsV1(specs)
}

func (protocolV1) DigestModelStepBinding(model ModelRef, requestDigest, toolsDigest Digest) (Digest, error) {
	return digestModelStepBindingV1(model, requestDigest, toolsDigest)
}

func (protocolV1) DigestToolResponseDecision(kind ResponseKind, decision ResponseDecision, reason string) (Digest, error) {
	return digestToolResponseDecisionV1(kind, decision, reason)
}

func (protocolV1) DigestToolResponsePayload(payload CanonicalJSON) (Digest, error) {
	return digestToolResponsePayloadV1(payload)
}

func (protocolV1) DigestCommand(typ string, command AgentCommand) (Digest, error) {
	if typ == "" || typ != commandType(command) {
		return "", fmt.Errorf("agent: digest: type %q does not match command variant", typ)
	}
	body, err := encodeEnvelopeBody(SchemaVersion1, typ, command)
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

func (protocolV1) EncodeFact(typ string, fact Fact) ([]byte, error) {
	if typ == "" || typ != factType(fact) {
		return nil, fmt.Errorf("agent: encode: type %q does not match fact variant", typ)
	}
	return encodeEnvelopeBody(SchemaVersion1, typ, fact)
}

func (p protocolV1) DigestFact(typ string, fact Fact) (Digest, error) {
	body, err := p.EncodeFact(typ, fact)
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

func (protocolV1) DecodeCommand(typ string, raw []byte) (AgentCommand, error) {
	return decodeCommandVariantV1(typ, raw)
}

func (protocolV1) DecodeFact(typ string, raw []byte) (Fact, error) {
	return decodeFactVariantV1(typ, raw)
}

func (protocolV1) Decide(s MachineState, c AgentCommand) ([]Fact, error) {
	return decideV1(s, c)
}

func (protocolV1) Evolve(s MachineState, f Fact) (MachineState, error) {
	return evolveV1(s, f)
}

func (p protocolV1) BuildEnvelope(run RunID, id CommandID, cmd AgentCommand) (CommandEnvelope, error) {
	typ := commandType(cmd)
	if typ == "" {
		return CommandEnvelope{}, fmt.Errorf("agent: envelope: unknown command variant %T", cmd)
	}
	d, err := p.DigestCommand(typ, cmd)
	if err != nil {
		return CommandEnvelope{}, err
	}
	return CommandEnvelope{
		SchemaVersion: SchemaVersion1,
		Type:          typ,
		RunID:         run,
		ID:            id,
		Digest:        d,
		Command:       cmd,
	}, nil
}

func (protocolV1) EncodeMachineState(s *MachineState) ([]byte, error) {
	return encodeMachineStateV1(s)
}

func (protocolV1) ValidateHeader(h *RunHeader) error {
	return validateHeaderV1(h)
}

func digestRequestV1(req ModelRequest) (Digest, error) {
	body, err := encodeEnvelopeBody(SchemaVersion1, "model_request", req)
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

func digestToolDefinitionV1(def ToolDefinition) (Digest, error) {
	body, err := encodeEnvelopeBody(SchemaVersion1, "tool_definition", def)
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

func digestToolSpecV1(spec ToolSpec) (Digest, error) {
	body, err := encodeEnvelopeBody(SchemaVersion1, "tool_spec", spec)
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

func digestToolSpecsV1(specs []ToolSpec) (Digest, error) {
	body, err := encodeEnvelopeBody(SchemaVersion1, "tool_specs", specs)
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

func digestToolResponseDecisionV1(kind ResponseKind, decision ResponseDecision, reason string) (Digest, error) {
	if kind != ResponseApproval && kind != ResponseExternal {
		return "", fmt.Errorf("agent: response decision: unsupported kind %q", kind)
	}
	if decision != ResponseDecisionApproved && decision != ResponseDecisionRejected {
		return "", fmt.Errorf("agent: response decision: unsupported decision %q", decision)
	}
	body, err := encodeEnvelopeBody(SchemaVersion1, "tool_response_decision", toolResponseDecisionDigestBody{
		Kind: kind, Decision: decision, Reason: reason,
	})
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

func digestToolResponsePayloadV1(payload CanonicalJSON) (Digest, error) {
	body, err := encodeEnvelopeBody(SchemaVersion1, "tool_response_payload", toolResponsePayloadDigestBody{Payload: payload})
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

func encodeMachineStateV1(s *MachineState) ([]byte, error) {
	return marshalCanonical(stateComparable(s))
}

func initialStateVersionV1() uint16 { return 1 }

func validateHeaderV1(h *RunHeader) error {
	if h.InitialStateVersion != initialStateVersionV1() {
		return fmt.Errorf("agent: run header: unsupported initial state version %d for schema %d", h.InitialStateVersion, SchemaVersion1)
	}
	return nil
}

func unsupportedSchemaVersion(schemaVersion uint16) error {
	return fmt.Errorf("agent: unsupported schema version %d", schemaVersion)
}

func digestModelStepBindingV1(model ModelRef, requestDigest, toolsDigest Digest) (Digest, error) {
	if model == "" || requestDigest == "" || toolsDigest == "" {
		return "", errors.New("agent: model step binding requires model, request digest and tools digest")
	}
	return sha256Digest([]byte(namespacedHash("twilight/model-step-binding",
		string(model), string(requestDigest), string(toolsDigest)))), nil
}
