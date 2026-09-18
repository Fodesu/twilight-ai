package run

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

type commandEnvelopeWire struct {
	SchemaVersion uint16          `json:"schemaVersion"`
	Type          string          `json:"type"`
	RunID         RunID           `json:"runId"`
	ID            CommandID       `json:"id"`
	Command       json.RawMessage `json:"command"`
}

type commandEnvelopeMarshal struct {
	SchemaVersion uint16       `json:"schemaVersion"`
	Type          string       `json:"type"`
	RunID         RunID        `json:"runId"`
	ID            CommandID    `json:"id"`
	Command       AgentCommand `json:"command"`
}

// DecodeCommandEnvelope decodes the command wire shape and restores the
// sealed command variant from Type; malformed or unsupported wire data is
// rejected before it can enter a RunStore.
func DecodeCommandEnvelope(raw []byte) (CommandEnvelope, error) {
	var env CommandEnvelope
	if err := decodeStrictJSON(raw, &env); err != nil {
		return CommandEnvelope{}, err
	}
	return env, nil
}

//nolint:gocritic // hugeParam: value receiver keeps json.Marshaler active for non-pointer CommandEnvelope values.
func (e CommandEnvelope) MarshalJSON() ([]byte, error) {
	if e.Command == nil {
		return nil, errors.New("agent: codec: command envelope has nil command")
	}
	schema, err := SchemaFor(e.SchemaVersion)
	if err != nil {
		return nil, err
	}
	typ := schema.Wire.CommandType(e.Command)
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
	if err := decodeStrictJSON(raw, &wire); err != nil {
		return err
	}
	schema, err := SchemaFor(wire.SchemaVersion)
	if err != nil {
		return err
	}
	cmd, err := schema.Wire.DecodeCommand(wire.Type, wire.Command)
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

func requireCanonicalEquivalent(raw []byte, canonicalShape any) error {
	rawCanonical, err := canonicalJSON(raw)
	if err != nil {
		return err
	}
	shapeCanonical, err := marshalCanonical(canonicalShape)
	if err != nil {
		return err
	}
	if !bytes.Equal(rawCanonical, shapeCanonical) {
		return errors.New("agent: codec: JSON shape does not match canonical protocol fields")
	}
	return nil
}

func decodeStrictJSON(raw []byte, dst any) error {
	canonical, err := canonicalJSON(raw)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(canonical))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return errors.New("agent: codec: trailing data after JSON value")
		}
		return err
	}
	return nil
}
