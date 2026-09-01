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

type transitionRecordWire struct {
	SchemaVersion    uint16       `json:"schemaVersion"`
	RunID            RunID        `json:"runId"`
	Revision         uint64       `json:"revision"`
	CommandID        CommandID    `json:"commandId"`
	CommandDigest    Digest       `json:"commandDigest"`
	Events           []AgentEvent `json:"events"`
	TransitionDigest Digest       `json:"transitionDigest"`
}

type transitionRecordMarshal = transitionRecordWire

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

// DecodeTransitionRecord decodes the persisted transition aggregate and
// verifies that the complete event group is internally consistent.
func DecodeTransitionRecord(raw []byte) (TransitionRecord, error) {
	var record TransitionRecord
	if err := decodeStrictJSON(raw, &record); err != nil {
		return TransitionRecord{}, err
	}
	return record, nil
}

//nolint:gocritic // hugeParam: value receiver keeps json.Marshaler active for non-pointer CommandEnvelope values.
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
	proto, err := ProtocolFor(wire.SchemaVersion)
	if err != nil {
		return err
	}
	cmd, err := proto.DecodeCommand(wire.Type, wire.Command)
	if err != nil {
		return err
	}
	want, err := proto.DigestCommand(wire.Type, cmd)
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

//nolint:gocritic // hugeParam: value receiver keeps json.Marshaler active for non-pointer AgentEvent values.
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

//nolint:gocritic // hugeParam: value receiver keeps json.Marshaler active for non-pointer TransitionRecord values.
func (r TransitionRecord) MarshalJSON() ([]byte, error) {
	if err := ValidateTransitionRecord(&r); err != nil {
		return nil, err
	}
	return json.Marshal(transitionRecordMarshal(r))
}

func (r *TransitionRecord) UnmarshalJSON(raw []byte) error {
	var wire transitionRecordWire
	if err := decodeStrictJSON(raw, &wire); err != nil {
		return err
	}
	record := TransitionRecord(wire)
	if err := ValidateTransitionRecord(&record); err != nil {
		return err
	}
	if err := requireCanonicalEquivalent(raw, transitionRecordMarshal(record)); err != nil {
		return err
	}
	*r = record
	return nil
}

func (e *AgentEvent) UnmarshalJSON(raw []byte) error {
	var wire agentEventWire
	if err := decodeStrictJSON(raw, &wire); err != nil {
		return err
	}
	proto, err := ProtocolFor(wire.SchemaVersion)
	if err != nil {
		return err
	}
	fact, err := proto.DecodeFact(wire.Type, wire.Fact)
	if err != nil {
		return err
	}
	want, err := proto.DigestFact(wire.Type, fact)
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

func decodeCommandAs[T AgentCommand](raw []byte) (AgentCommand, error) {
	var c T
	err := decodeStrictJSON(raw, &c)
	return c, err
}

func decodeFactAs[T Fact](raw []byte) (Fact, error) {
	var f T
	err := decodeStrictJSON(raw, &f)
	return f, err
}

func decodeCommandVariantV1(typ string, raw []byte) (AgentCommand, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, fmt.Errorf("agent: codec: command %q has empty body", typ)
	}
	switch typ {
	case "prepare_model_request":
		return decodeCommandAs[PrepareModelRequest](raw)
	case "start_model_execution":
		return decodeCommandAs[StartModelExecution](raw)
	case "recover_model_execution":
		return decodeCommandAs[RecoverModelExecution](raw)
	case "submit_model_result":
		return decodeCommandAs[SubmitModelResult](raw)
	case "submit_model_failure":
		return decodeCommandAs[SubmitModelFailure](raw)
	case "reject_model_result":
		return decodeCommandAs[RejectModelResult](raw)
	case "start_tool_call":
		return decodeCommandAs[StartToolCall](raw)
	case "submit_tool_result":
		return decodeCommandAs[SubmitToolResult](raw)
	case "submit_tool_failure":
		return decodeCommandAs[SubmitToolFailure](raw)
	case "approve_tool_call":
		return decodeCommandAs[ApproveToolCall](raw)
	case "reject_tool_call":
		return decodeCommandAs[RejectToolCall](raw)
	case "submit_tool_response":
		return decodeCommandAs[SubmitToolResponse](raw)
	case "cancel_run":
		return decodeCommandAs[CancelRun](raw)
	case "accept_input":
		return decodeCommandAs[AcceptInput](raw)
	default:
		return nil, fmt.Errorf("agent: codec: unknown command type %q", typ)
	}
}

func decodeFactVariantV1(typ string, raw []byte) (Fact, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, fmt.Errorf("agent: codec: fact %q has empty body", typ)
	}
	switch typ {
	case "model_step_prepared":
		return decodeFactAs[ModelStepPrepared](raw)
	case "model_step_started":
		return decodeFactAs[ModelStepStarted](raw)
	case "model_step_recovered":
		return decodeFactAs[ModelStepRecovered](raw)
	case "model_step_rejected":
		return decodeFactAs[ModelStepRejected](raw)
	case "model_step_completed":
		return decodeFactAs[ModelStepCompleted](raw)
	case "tool_step_opened":
		return decodeFactAs[ToolStepOpened](raw)
	case "tool_call_started":
		return decodeFactAs[ToolCallStarted](raw)
	case "tool_call_approved":
		return decodeFactAs[ToolCallApproved](raw)
	case "tool_call_completed":
		return decodeFactAs[ToolCallCompleted](raw)
	case "tool_call_answered":
		return decodeFactAs[ToolCallAnswered](raw)
	case "tool_call_failed":
		return decodeFactAs[ToolCallFailed](raw)
	case "input_accepted":
		return decodeFactAs[InputAccepted](raw)
	case "run_ended":
		return decodeFactAs[RunEnded](raw)
	default:
		return nil, fmt.Errorf("agent: codec: unknown fact type %q", typ)
	}
}
