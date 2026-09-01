package run

import (
	"errors"
	"fmt"
)

// SchemaVersion1 digest, decode, Decide, and Evolve helpers. ProtocolV1 binds
// these once; replay of a v1 Run must keep using them after later versions exist.

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
