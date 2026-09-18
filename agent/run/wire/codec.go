package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/run"
)

// CommandEnvelope carries one command with its protocol identity. Commands
// are not persisted: ID is the CommitID of the commit the command produces,
// and replay is told from conflict by the store's command index (RUN-WIR-2,
// EXT-WRT-2). The envelope names no store: which store a command reaches is
// the bound RunStore's.
type CommandEnvelope struct {
	SchemaVersion uint16           `json:"schemaVersion"`
	Type          string           `json:"type"`
	RunID         run.RunID        `json:"runId"`
	ID            run.CommandID    `json:"id"`
	Command       run.AgentCommand `json:"command"`
}

type commandEnvelopeWire struct {
	SchemaVersion uint16          `json:"schemaVersion"`
	Type          string          `json:"type"`
	RunID         run.RunID       `json:"runId"`
	ID            run.CommandID   `json:"id"`
	Command       json.RawMessage `json:"command"`
}

type commandEnvelopeMarshal struct {
	SchemaVersion uint16           `json:"schemaVersion"`
	Type          string           `json:"type"`
	RunID         run.RunID        `json:"runId"`
	ID            run.CommandID    `json:"id"`
	Command       run.AgentCommand `json:"command"`
}

// DecodeCommandEnvelope decodes the command wire shape and restores the
// sealed command variant from Type; malformed or unsupported wire data is
// rejected before it can enter a RunStore.
func DecodeCommandEnvelope(raw []byte) (CommandEnvelope, error) {
	var env CommandEnvelope
	if err := es.DecodeStrict(raw, &env); err != nil {
		return CommandEnvelope{}, err
	}
	return env, nil
}

//nolint:gocritic // hugeParam: value receiver keeps json.Marshaler active for non-pointer CommandEnvelope values.
func (e CommandEnvelope) MarshalJSON() ([]byte, error) {
	if e.Command == nil {
		return nil, errors.New("agent: codec: command envelope has nil command")
	}
	codec, err := codecFor(e.SchemaVersion)
	if err != nil {
		return nil, err
	}
	typ := codec.CommandType(e.Command)
	if typ == "" {
		return nil, fmt.Errorf("agent: codec: unknown command variant %T", e.Command)
	}
	if e.Type != "" && e.Type != typ {
		return nil, fmt.Errorf("agent: codec: command type %q does not match variant %q", e.Type, typ)
	}
	return json.Marshal(commandEnvelopeMarshal{SchemaVersion: e.SchemaVersion, Type: typ, RunID: e.RunID, ID: e.ID, Command: e.Command})
}

func (e *CommandEnvelope) UnmarshalJSON(raw []byte) error {
	var wire commandEnvelopeWire
	if err := es.DecodeStrict(raw, &wire); err != nil {
		return err
	}
	codec, err := codecFor(wire.SchemaVersion)
	if err != nil {
		return err
	}
	cmd, err := codec.DecodeCommand(wire.Type, wire.Command)
	if err != nil {
		return err
	}
	if err := requireCanonicalEquivalent(raw, commandEnvelopeMarshal{
		SchemaVersion: wire.SchemaVersion, Type: wire.Type, RunID: wire.RunID, ID: wire.ID, Command: cmd,
	}); err != nil {
		return err
	}
	*e = CommandEnvelope{SchemaVersion: wire.SchemaVersion, Type: wire.Type, RunID: wire.RunID, ID: wire.ID, Command: cmd}
	return nil
}

// codecFor binds the wire codec of one persisted version. The envelope
// codec keeps its own table so the wire layer does not depend on the Schema
// binding that composes it.
func codecFor(schemaVersion uint16) (Codec, error) {
	switch schemaVersion {
	case run.SchemaVersion1:
		return V1{}, nil
	default:
		return nil, run.UnsupportedSchemaVersion(schemaVersion)
	}
}

func requireCanonicalEquivalent(raw []byte, canonicalShape any) error {
	rawCanonical, err := es.Canonicalize(raw)
	if err != nil {
		return err
	}
	shapeCanonical, err := es.MarshalCanonical(canonicalShape)
	if err != nil {
		return err
	}
	if !bytes.Equal(rawCanonical, shapeCanonical) {
		return errors.New("agent: codec: JSON shape does not match canonical protocol fields")
	}
	return nil
}

// Codec names and (de)codes the sealed fact and command variants of one
// schema version. Encoding is canonical JSON of the variant under the typed
// payload envelope (schema version, type discriminator, body).
type Codec interface {
	// FactType is the wire discriminator of a fact variant, "" if unknown.
	FactType(run.Fact) string
	// CommandType is the wire discriminator of a command variant, "" if unknown.
	CommandType(run.AgentCommand) string
	// DecodeFact restores the sealed fact variant named by typ.
	DecodeFact(typ string, raw []byte) (run.Fact, error)
	// DecodeCommand restores the sealed command variant named by typ.
	DecodeCommand(typ string, raw []byte) (run.AgentCommand, error)
	// EncodeFact renders the typed payload bytes of a fact; typ must be the
	// fact's own discriminator.
	EncodeFact(typ string, fact run.Fact) ([]byte, error)
	// Envelope is the sanctioned envelope constructor (RUN-WIR-3).
	Envelope(runID run.RunID, id run.CommandID, cmd run.AgentCommand) (CommandEnvelope, error)
}

// --- v1 wire ------------------------------------------------------------------------

// V1 speaks through variantsV1, its own frozen variant table.
type V1 struct{}

func (V1) FactType(f run.Fact) string            { return variantsV1.factType(f) }
func (V1) CommandType(c run.AgentCommand) string { return variantsV1.commandType(c) }
func (V1) DecodeFact(typ string, raw []byte) (run.Fact, error) {
	return variantsV1.decodeFact(typ, raw)
}

func (V1) DecodeCommand(typ string, raw []byte) (run.AgentCommand, error) {
	return variantsV1.decodeCommand(typ, raw)
}

func (V1) EncodeFact(typ string, fact run.Fact) ([]byte, error) {
	if typ == "" || typ != variantsV1.factType(fact) {
		return nil, fmt.Errorf("agent: encode: type %q does not match fact variant", typ)
	}
	return es.EncodeTypedPayload(run.SchemaVersion1, typ, fact)
}

func (V1) Envelope(runID run.RunID, id run.CommandID, cmd run.AgentCommand) (CommandEnvelope, error) {
	typ := variantsV1.commandType(cmd)
	if typ == "" {
		return CommandEnvelope{}, fmt.Errorf("agent: envelope: unknown command variant %T", cmd)
	}
	return CommandEnvelope{SchemaVersion: run.SchemaVersion1, Type: typ, RunID: runID, ID: id, Command: cmd}, nil
}

func (SnapshotV1) Encode(s *run.MachineState) ([]byte, error)  { return encodeMachineStateV1(s) }
func (SnapshotV1) Decode(raw []byte) (run.MachineState, error) { return decodeMachineStateV1(raw) }
