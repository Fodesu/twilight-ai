package run

import (
	"fmt"

	"github.com/memohai/twilight/agent/es"
	"github.com/memohai/twilight/agent/session"
)

// SchemaVersion1 is the current pre-release wire schema. Its canonical
// encoding and Evolve folding semantics may still change before publication.
// Once a schema is published, its encoding and folding semantics are frozen.
const SchemaVersion1 uint16 = 1

// CommandEnvelope carries one command with its protocol identity. Commands
// are not persisted: ID is the CommitID of the SessionCommit the command
// produces, and replay is told from conflict by the Writer's row fingerprint
// (RUN-WIR-2, EXT-WRT-2).
type CommandEnvelope struct {
	SchemaVersion uint16            `json:"schemaVersion"`
	Type          string            `json:"type"`
	SessionID     session.SessionID `json:"sessionId"`
	RunID         RunID             `json:"runId"`
	ID            CommandID         `json:"id"`
	Command       AgentCommand      `json:"command"`
}

// encodeEnvelopeBody is the digest input for a command: schema version, type
// discriminator and canonical command bytes.
func encodeEnvelopeBody(schemaVersion uint16, typ string, body any) ([]byte, error) {
	return es.EncodeTypedPayload(schemaVersion, typ, body)
}

// Protocol is the digest, decode, Decide, Evolve, and envelope operations for
// one SchemaVersion. ProtocolFor binds the functions once from the Run header;
// subsequent calls do not take a version argument (RUN-CMT-7).
type Protocol struct {
	version uint16

	digestRequest              func(ModelRequest) (Digest, error)
	digestToolDefinition       func(ToolDefinition) (Digest, error)
	digestToolSpec             func(ToolSpec) (Digest, error)
	digestToolSpecs            func([]ToolSpec) (Digest, error)
	digestModelStepBinding     func(ModelRef, Digest, Digest) (Digest, error)
	digestToolResponseDecision func(ResponseKind, ResponseDecision, string) (Digest, error)
	digestToolResponsePayload  func(CanonicalJSON) (Digest, error)
	digestModelResult          func(ModelResult) (Digest, error)
	digestToolOutput           func(CanonicalJSON) (Digest, error)
	buildCreateGroup           func(NewRun, []AgentInput) ([]Fact, error)
	decodeCommand              func(string, []byte) (AgentCommand, error)
	decodeFact                 func(string, []byte) (Fact, error)
	decide                     func(MachineState, AgentCommand) ([]Fact, error)
	evolve                     func(MachineState, Fact) (MachineState, error)
	encodeMachineState         func(*MachineState) ([]byte, error)
	decodeMachineState         func([]byte) (MachineState, error)
}

// ProtocolV1 is the SchemaVersion1 binding. New Runs are created with this
// protocol; every later operation on a Run binds through
// ProtocolFor(header.SchemaVersion) or RuntimeSnapshot.Protocol() (RUN-CMT-7).
// There are no package-level functions that implicitly select a version.
func ProtocolV1() Protocol { return protocolV1 }

var protocolV1 = Protocol{
	version:                    SchemaVersion1,
	digestRequest:              digestRequestV1,
	digestToolDefinition:       digestToolDefinitionV1,
	digestToolSpec:             digestToolSpecV1,
	digestToolSpecs:            digestToolSpecsV1,
	digestModelStepBinding:     digestModelStepBindingV1,
	digestToolResponseDecision: digestToolResponseDecisionV1,
	digestToolResponsePayload:  digestToolResponsePayloadV1,
	digestModelResult:          digestModelResultV1,
	digestToolOutput:           digestToolOutputV1,
	buildCreateGroup:           buildCreateGroupV1,
	decodeCommand:              decodeCommandVariantV1,
	decodeFact:                 decodeFactVariantV1,
	decide:                     decideV1,
	evolve:                     evolveV1,
	encodeMachineState:         encodeMachineStateV1,
	decodeMachineState:         decodeMachineStateV1,
}

// ProtocolFor binds the protocol functions for a persisted schema version.
// Call it at the Run header, envelope, or event boundary; do not thread the
// version number through digest, Decide, or Evolve.
func ProtocolFor(schemaVersion uint16) (Protocol, error) {
	switch schemaVersion {
	case SchemaVersion1:
		return protocolV1, nil
	default:
		return Protocol{}, unsupportedSchemaVersion(schemaVersion)
	}
}

func (p Protocol) ready() error {
	if p.version == 0 {
		return fmt.Errorf("agent: uninitialized protocol")
	}
	return nil
}

func (p Protocol) Version() uint16 { return p.version }

func (p Protocol) DigestRequest(req ModelRequest) (Digest, error) { //nolint:gocritic // hugeParam: digest covers the complete immutable ModelRequest value.
	if err := p.ready(); err != nil {
		return "", err
	}
	return p.digestRequest(req)
}

func (p Protocol) DigestToolDefinition(def ToolDefinition) (Digest, error) {
	if err := p.ready(); err != nil {
		return "", err
	}
	return p.digestToolDefinition(def)
}

func (p Protocol) DigestToolSpec(spec ToolSpec) (Digest, error) { //nolint:gocritic // hugeParam: digest covers the complete immutable ToolSpec value.
	if err := p.ready(); err != nil {
		return "", err
	}
	return p.digestToolSpec(spec)
}

func (p Protocol) DigestToolSpecs(specs []ToolSpec) (Digest, error) {
	if err := p.ready(); err != nil {
		return "", err
	}
	return p.digestToolSpecs(specs)
}

func (p Protocol) DigestModelStepBinding(model ModelRef, requestDigest, toolsDigest Digest) (Digest, error) {
	if err := p.ready(); err != nil {
		return "", err
	}
	return p.digestModelStepBinding(model, requestDigest, toolsDigest)
}

func (p Protocol) DigestToolResponseDecision(kind ResponseKind, decision ResponseDecision, reason string) (Digest, error) {
	if err := p.ready(); err != nil {
		return "", err
	}
	return p.digestToolResponseDecision(kind, decision, reason)
}

func (p Protocol) DigestToolResponsePayload(payload CanonicalJSON) (Digest, error) {
	if err := p.ready(); err != nil {
		return "", err
	}
	return p.digestToolResponsePayload(payload)
}

// DigestModelResult names a frozen model result (ModelStepCompleted.ResultDigest).
func (p Protocol) DigestModelResult(result ModelResult) (Digest, error) { //nolint:gocritic // hugeParam: digest covers the complete immutable ModelResult value.
	if err := p.ready(); err != nil {
		return "", err
	}
	return p.digestModelResult(result)
}

// DigestToolOutput names one tool output (ToolCallCompleted.OutputDigest).
func (p Protocol) DigestToolOutput(output CanonicalJSON) (Digest, error) {
	if err := p.ready(); err != nil {
		return "", err
	}
	return p.digestToolOutput(output)
}

// BuildCreateGroup returns the RunCreated and InputAccepted facts that
// establish a Run (RUN-NEW-1). Encoding them as Session events is the module
// implementation's job.
func (p Protocol) BuildCreateGroup(run NewRun, inputs []AgentInput) ([]Fact, error) {
	if err := p.ready(); err != nil {
		return nil, err
	}
	if run.SchemaVersion != p.version {
		return nil, fmt.Errorf("agent: create group: run schema %d does not match protocol %d", run.SchemaVersion, p.version)
	}
	return p.buildCreateGroup(run, inputs)
}

func (p Protocol) EncodeFact(typ string, fact Fact) ([]byte, error) {
	if err := p.ready(); err != nil {
		return nil, err
	}
	if typ == "" || typ != factType(fact) {
		return nil, fmt.Errorf("agent: encode: type %q does not match fact variant", typ)
	}
	return encodeEnvelopeBody(p.version, typ, fact)
}

func (p Protocol) DigestFact(typ string, fact Fact) (Digest, error) {
	body, err := p.EncodeFact(typ, fact)
	if err != nil {
		return "", err
	}
	return sha256Digest(body), nil
}

func (p Protocol) DecodeCommand(typ string, raw []byte) (AgentCommand, error) {
	if err := p.ready(); err != nil {
		return nil, err
	}
	return p.decodeCommand(typ, raw)
}

func (p Protocol) DecodeFact(typ string, raw []byte) (Fact, error) {
	if err := p.ready(); err != nil {
		return nil, err
	}
	return p.decodeFact(typ, raw)
}

func (p Protocol) Decide(s MachineState, c AgentCommand) ([]Fact, error) { //nolint:gocritic // hugeParam: protocol methods stay value-based.
	if err := p.ready(); err != nil {
		return nil, err
	}
	return p.decide(s, c)
}

func (p Protocol) Evolve(s MachineState, f Fact) (MachineState, error) { //nolint:gocritic // hugeParam: protocol methods stay value-based.
	if err := p.ready(); err != nil {
		return s, err
	}
	return p.evolve(s, f)
}

// BuildEnvelope is the sanctioned envelope constructor (RUN-WIR-3).
func (p Protocol) BuildEnvelope(sid session.SessionID, run RunID, id CommandID, cmd AgentCommand) (CommandEnvelope, error) {
	typ := commandType(cmd)
	if typ == "" {
		return CommandEnvelope{}, fmt.Errorf("agent: envelope: unknown command variant %T", cmd)
	}
	return CommandEnvelope{
		SchemaVersion: p.version,
		Type:          typ,
		SessionID:     sid,
		RunID:         run,
		ID:            id,
		Command:       cmd,
	}, nil
}

// FactType returns the local event name of a fact (the part of the EventType
// after twilight/run/).
func FactType(f Fact) string { return factType(f) }

// EncodeMachineState renders the persisted snapshot bytes of a MachineState
// under this schema. The bytes are canonical: statesEquivalent, the
// InitialStateDigest preimage, and durable snapshot storage all use them.
func (p Protocol) EncodeMachineState(s *MachineState) ([]byte, error) {
	if err := p.ready(); err != nil {
		return nil, err
	}
	return p.encodeMachineState(s)
}

// DecodeMachineState restores a MachineState from bytes produced by
// EncodeMachineState of the same schema, including the Current step.
func (p Protocol) DecodeMachineState(raw []byte) (MachineState, error) {
	if err := p.ready(); err != nil {
		return MachineState{}, err
	}
	return p.decodeMachineState(raw)
}

type toolResponseDecisionDigestBody struct {
	Kind     ResponseKind     `json:"kind"`
	Decision ResponseDecision `json:"decision"`
	Reason   string           `json:"reason,omitempty"`
}

type toolResponsePayloadDigestBody struct {
	Payload CanonicalJSON `json:"payload"`
}

type toolOutputDigestBody struct {
	Output CanonicalJSON `json:"output"`
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
// arguments plus the CallID (RUN-MCH-2). Runtime conformance suites use this
// helper to construct the same frozen binding identities as the Loop.
func DigestToolCallBinding(callID CallID, definitionDigest Digest, policy ResponsePolicy, arguments CanonicalJSON) (Digest, error) {
	return sha256Digest([]byte(namespacedHash("twilight/tool-call-binding",
		string(callID), string(definitionDigest), fmt.Sprintf("%d", policy), arguments.String()))), nil
}

func digestToolCallBinding(callID CallID, definitionDigest Digest, policy ResponsePolicy, arguments CanonicalJSON) (Digest, error) {
	return DigestToolCallBinding(callID, definitionDigest, policy, arguments)
}
