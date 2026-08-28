package agent

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
	Digest        Digest          `json:"digest"`
	Command       json.RawMessage `json:"command"`
}

type commandEnvelopeMarshal struct {
	SchemaVersion uint16       `json:"schemaVersion"`
	Type          string       `json:"type"`
	RunID         RunID        `json:"runId"`
	ID            CommandID    `json:"id"`
	Digest        Digest       `json:"digest"`
	Command       AgentCommand `json:"command"`
}

type agentEventWire struct {
	SchemaVersion uint16          `json:"schemaVersion"`
	Type          string          `json:"type"`
	RunID         RunID           `json:"runId"`
	Revision      uint64          `json:"revision"`
	Index         uint16          `json:"index"`
	CommandID     CommandID       `json:"commandId"`
	CommandDigest Digest          `json:"commandDigest"`
	Digest        Digest          `json:"digest"`
	Fact          json.RawMessage `json:"fact"`
}

type agentEventMarshal struct {
	SchemaVersion uint16    `json:"schemaVersion"`
	Type          string    `json:"type"`
	RunID         RunID     `json:"runId"`
	Revision      uint64    `json:"revision"`
	Index         uint16    `json:"index"`
	CommandID     CommandID `json:"commandId"`
	CommandDigest Digest    `json:"commandDigest"`
	Digest        Digest    `json:"digest"`
	Fact          Fact      `json:"fact"`
}

// DecodeCommandEnvelope decodes the persisted command wire shape and restores
// the sealed command variant from Type. The digest is verified during decode;
// malformed or unsupported wire data is rejected before it can enter Runtime.
func DecodeCommandEnvelope(raw []byte) (CommandEnvelope, error) {
	var env CommandEnvelope
	if err := decodeStrictJSON(raw, &env); err != nil {
		return CommandEnvelope{}, err
	}
	return env, nil
}

// DecodeAgentEvent decodes the persisted event wire shape and restores the
// sealed fact variant from Type. The fact digest is verified during decode.
func DecodeAgentEvent(raw []byte) (AgentEvent, error) {
	var event AgentEvent
	if err := decodeStrictJSON(raw, &event); err != nil {
		return AgentEvent{}, err
	}
	return event, nil
}

func (e CommandEnvelope) MarshalJSON() ([]byte, error) {
	if e.Command == nil {
		return nil, errors.New("agent: codec: command envelope has nil command")
	}
	typ := commandType(e.Command)
	if typ == "" {
		return nil, fmt.Errorf("agent: codec: unknown command variant %T", e.Command)
	}
	if e.Type != "" && e.Type != typ {
		return nil, fmt.Errorf("agent: codec: command type %q does not match variant %q", e.Type, typ)
	}
	return json.Marshal(commandEnvelopeMarshal{
		SchemaVersion: e.SchemaVersion,
		Type:          typ,
		RunID:         e.RunID,
		ID:            e.ID,
		Digest:        e.Digest,
		Command:       e.Command,
	})
}

func (e *CommandEnvelope) UnmarshalJSON(raw []byte) error {
	var wire commandEnvelopeWire
	if err := decodeStrictJSON(raw, &wire); err != nil {
		return err
	}
	if !isSupportedSchemaVersion(wire.SchemaVersion) {
		return fmt.Errorf("agent: codec: unsupported schema version %d", wire.SchemaVersion)
	}
	cmd, err := decodeCommandVariant(wire.Type, wire.Command)
	if err != nil {
		return err
	}
	want, err := DigestCommand(wire.SchemaVersion, wire.Type, cmd)
	if err != nil {
		return err
	}
	if wire.Digest == "" {
		return errors.New("agent: codec: command envelope missing digest")
	}
	if wire.Digest != want {
		return fmt.Errorf("agent: codec: command digest mismatch: got %s want %s", wire.Digest, want)
	}
	if err := requireCanonicalEquivalent(raw, commandEnvelopeMarshal{
		SchemaVersion: wire.SchemaVersion,
		Type:          wire.Type,
		RunID:         wire.RunID,
		ID:            wire.ID,
		Digest:        wire.Digest,
		Command:       cmd,
	}); err != nil {
		return err
	}
	*e = CommandEnvelope{
		SchemaVersion: wire.SchemaVersion,
		Type:          wire.Type,
		RunID:         wire.RunID,
		ID:            wire.ID,
		Digest:        wire.Digest,
		Command:       cmd,
	}
	return nil
}

func (e AgentEvent) MarshalJSON() ([]byte, error) {
	if e.Fact == nil {
		return nil, errors.New("agent: codec: event has nil fact")
	}
	typ := factType(e.Fact)
	if typ == "" {
		return nil, fmt.Errorf("agent: codec: unknown fact variant %T", e.Fact)
	}
	if e.Type != "" && e.Type != typ {
		return nil, fmt.Errorf("agent: codec: event type %q does not match variant %q", e.Type, typ)
	}
	return json.Marshal(agentEventMarshal{
		SchemaVersion: e.SchemaVersion,
		Type:          typ,
		RunID:         e.RunID,
		Revision:      e.Revision,
		Index:         e.Index,
		CommandID:     e.CommandID,
		CommandDigest: e.CommandDigest,
		Digest:        e.Digest,
		Fact:          e.Fact,
	})
}

func (e *AgentEvent) UnmarshalJSON(raw []byte) error {
	var wire agentEventWire
	if err := decodeStrictJSON(raw, &wire); err != nil {
		return err
	}
	if !isSupportedSchemaVersion(wire.SchemaVersion) {
		return fmt.Errorf("agent: codec: unsupported schema version %d", wire.SchemaVersion)
	}
	fact, err := decodeFactVariant(wire.Type, wire.Fact)
	if err != nil {
		return err
	}
	want, err := DigestFact(wire.SchemaVersion, wire.Type, fact)
	if err != nil {
		return err
	}
	if wire.Digest == "" {
		return errors.New("agent: codec: event missing fact digest")
	}
	if wire.Digest != want {
		return fmt.Errorf("agent: codec: fact digest mismatch: got %s want %s", wire.Digest, want)
	}
	if err := requireCanonicalEquivalent(raw, agentEventMarshal{
		SchemaVersion: wire.SchemaVersion,
		Type:          wire.Type,
		RunID:         wire.RunID,
		Revision:      wire.Revision,
		Index:         wire.Index,
		CommandID:     wire.CommandID,
		CommandDigest: wire.CommandDigest,
		Digest:        wire.Digest,
		Fact:          fact,
	}); err != nil {
		return err
	}
	*e = AgentEvent{
		SchemaVersion: wire.SchemaVersion,
		Type:          wire.Type,
		RunID:         wire.RunID,
		Revision:      wire.Revision,
		Index:         wire.Index,
		CommandID:     wire.CommandID,
		CommandDigest: wire.CommandDigest,
		Digest:        wire.Digest,
		Fact:          fact,
	}
	return nil
}

func isSupportedSchemaVersion(v uint16) bool {
	return v == SchemaVersion1
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

func decodeCommandVariant(typ string, raw json.RawMessage) (AgentCommand, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, fmt.Errorf("agent: codec: command %q has empty body", typ)
	}
	switch typ {
	case "prepare_model_request":
		var c PrepareModelRequest
		return c, decodeStrictJSON(raw, &c)
	case "start_model_execution":
		var c StartModelExecution
		return c, decodeStrictJSON(raw, &c)
	case "recover_model_execution":
		var c RecoverModelExecution
		return c, decodeStrictJSON(raw, &c)
	case "submit_model_result":
		var c SubmitModelResult
		return c, decodeStrictJSON(raw, &c)
	case "submit_model_failure":
		var c SubmitModelFailure
		return c, decodeStrictJSON(raw, &c)
	case "reject_model_result":
		var c RejectModelResult
		return c, decodeStrictJSON(raw, &c)
	case "start_tool_call":
		var c StartToolCall
		return c, decodeStrictJSON(raw, &c)
	case "submit_tool_result":
		var c SubmitToolResult
		return c, decodeStrictJSON(raw, &c)
	case "submit_tool_failure":
		var c SubmitToolFailure
		return c, decodeStrictJSON(raw, &c)
	case "approve_tool_call":
		var c ApproveToolCall
		return c, decodeStrictJSON(raw, &c)
	case "reject_tool_call":
		var c RejectToolCall
		return c, decodeStrictJSON(raw, &c)
	case "submit_tool_response":
		var c SubmitToolResponse
		return c, decodeStrictJSON(raw, &c)
	case "cancel_run":
		var c CancelRun
		return c, decodeStrictJSON(raw, &c)
	case "accept_input":
		var c AcceptInput
		return c, decodeStrictJSON(raw, &c)
	default:
		return nil, fmt.Errorf("agent: codec: unknown command type %q", typ)
	}
}

func decodeFactVariant(typ string, raw json.RawMessage) (Fact, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, fmt.Errorf("agent: codec: fact %q has empty body", typ)
	}
	switch typ {
	case "model_step_prepared":
		var f ModelStepPrepared
		return f, decodeStrictJSON(raw, &f)
	case "model_step_started":
		var f ModelStepStarted
		return f, decodeStrictJSON(raw, &f)
	case "model_step_recovered":
		var f ModelStepRecovered
		return f, decodeStrictJSON(raw, &f)
	case "model_step_rejected":
		var f ModelStepRejected
		return f, decodeStrictJSON(raw, &f)
	case "model_step_completed":
		var f ModelStepCompleted
		return f, decodeStrictJSON(raw, &f)
	case "tool_step_opened":
		var f ToolStepOpened
		return f, decodeStrictJSON(raw, &f)
	case "tool_call_started":
		var f ToolCallStarted
		return f, decodeStrictJSON(raw, &f)
	case "tool_call_approved":
		var f ToolCallApproved
		return f, decodeStrictJSON(raw, &f)
	case "tool_call_completed":
		var f ToolCallCompleted
		return f, decodeStrictJSON(raw, &f)
	case "tool_call_answered":
		var f ToolCallAnswered
		return f, decodeStrictJSON(raw, &f)
	case "tool_call_failed":
		var f ToolCallFailed
		return f, decodeStrictJSON(raw, &f)
	case "tool_step_closed":
		var f ToolStepClosed
		return f, decodeStrictJSON(raw, &f)
	case "input_accepted":
		var f InputAccepted
		return f, decodeStrictJSON(raw, &f)
	case "run_ended":
		var f RunEnded
		return f, decodeStrictJSON(raw, &f)
	default:
		return nil, fmt.Errorf("agent: codec: unknown fact type %q", typ)
	}
}
