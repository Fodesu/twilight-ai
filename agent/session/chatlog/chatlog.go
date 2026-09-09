// Package chatlog is the first-party conversation-content module
// (docs/design/agent-session-chatlog.md): Input, assistant, tool_result and
// summary entries, their canonical codec, and the Surface and Context
// projections. Turn lifecycle belongs to agent/turn; execution facts to
// agent/run.
package chatlog

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/artifact"
	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
)

const ModuleID extension.ModuleID = "chatlog"

type (
	TurnID       string
	InputID      string
	AssistantID  string
	ToolResultID string
	SummaryID    string
	CallID       string
	CheckpointID string
)

// EventTypes (CHT-EVT-1). v1 companion and coordinator write the first six;
// checkpoints are written by the host's compaction (CHT-EVT-3).
const (
	TypeInputSubmitted        session.EventType = "twilight/chatlog/input_submitted"
	TypeInputDelivered        session.EventType = "twilight/chatlog/input_delivered"
	TypeInputWithdrawn        session.EventType = "twilight/chatlog/input_withdrawn"
	TypeInputRejected         session.EventType = "twilight/chatlog/input_rejected"
	TypeAssistant             session.EventType = "twilight/chatlog/assistant"
	TypeToolResult            session.EventType = "twilight/chatlog/tool_result"
	TypeToolResultSuperseded  session.EventType = "twilight/chatlog/tool_result_superseded"
	TypeSummary               session.EventType = "twilight/chatlog/summary"
	TypeCheckpointCreated     session.EventType = "twilight/chatlog/checkpoint_created"
	TypeCheckpointInvalidated session.EventType = "twilight/chatlog/checkpoint_invalidated"
)

// --- parts --------------------------------------------------------------------

type PartKind string

const (
	PartText      PartKind = "twilight/chatlog/text"
	PartReasoning PartKind = "twilight/chatlog/reasoning"
	PartToolCall  PartKind = "twilight/chatlog/tool_call"
	PartReference PartKind = "twilight/chatlog/reference"
)

type Part interface{ PartKind() PartKind }

type TextPart struct{ Text string }
type ReasoningPart struct{ Text string }
type ToolCallPart struct {
	CallID         CallID
	ProviderCallID string
	Name           string
	Input          jsonstable.Value
}
type ReferencePart struct {
	BindingID artifact.BindingID
	Name      string
}

func (TextPart) PartKind() PartKind      { return PartText }
func (ReasoningPart) PartKind() PartKind { return PartReasoning }
func (ToolCallPart) PartKind() PartKind  { return PartToolCall }
func (ReferencePart) PartKind() PartKind { return PartReference }

// Parts is the ordered part list with its discriminated-union wire.
type Parts []Part

type partWire struct {
	Kind           PartKind          `json:"kind"`
	Text           string            `json:"text,omitempty"`
	CallID         CallID            `json:"callId,omitempty"`
	ProviderCallID string            `json:"providerCallId,omitempty"`
	Name           string            `json:"name,omitempty"`
	Input          *jsonstable.Value `json:"input,omitempty"`
	BindingID      string            `json:"bindingId,omitempty"`
}

func (ps Parts) MarshalJSON() ([]byte, error) {
	wires := make([]partWire, 0, len(ps))
	for i, p := range ps {
		w, err := encodePart(p)
		if err != nil {
			return nil, fmt.Errorf("part %d: %w", i, err)
		}
		wires = append(wires, w)
	}
	return json.Marshal(wires)
}

func (ps *Parts) UnmarshalJSON(raw []byte) error {
	value, err := jsonstable.Parse(raw)
	if err != nil {
		return err
	}
	var wires []partWire
	if err := extension.StrictDecode(value, &wires); err != nil {
		return err
	}
	out := make(Parts, 0, len(wires))
	for i := range wires {
		p, err := decodePart(&wires[i])
		if err != nil {
			return fmt.Errorf("part %d: %w", i, err)
		}
		out = append(out, p)
	}
	*ps = out
	return nil
}

func encodePart(p Part) (partWire, error) {
	switch v := p.(type) {
	case TextPart:
		return partWire{Kind: PartText, Text: v.Text}, nil
	case ReasoningPart:
		return partWire{Kind: PartReasoning, Text: v.Text}, nil
	case ToolCallPart:
		if v.CallID == "" || v.Name == "" || v.Input.IsZero() {
			return partWire{}, errors.New("tool_call part requires callId, name and input")
		}
		input := v.Input
		return partWire{Kind: PartToolCall, CallID: v.CallID, ProviderCallID: v.ProviderCallID, Name: v.Name, Input: &input}, nil
	case ReferencePart:
		if v.BindingID == "" {
			return partWire{}, errors.New("reference part requires bindingId")
		}
		return partWire{Kind: PartReference, BindingID: string(v.BindingID), Name: v.Name}, nil
	default:
		return partWire{}, fmt.Errorf("unknown part %T", p)
	}
}

func decodePart(w *partWire) (Part, error) {
	switch w.Kind {
	case PartText:
		if w.CallID != "" || w.BindingID != "" || w.Input != nil {
			return nil, errors.New("text part carries foreign fields")
		}
		return TextPart{Text: w.Text}, nil
	case PartReasoning:
		if w.CallID != "" || w.BindingID != "" || w.Input != nil {
			return nil, errors.New("reasoning part carries foreign fields")
		}
		return ReasoningPart{Text: w.Text}, nil
	case PartToolCall:
		if w.CallID == "" || w.Name == "" || w.Input == nil || w.Input.IsZero() || w.Text != "" || w.BindingID != "" {
			return nil, errors.New("malformed tool_call part")
		}
		return ToolCallPart{CallID: w.CallID, ProviderCallID: w.ProviderCallID, Name: w.Name, Input: *w.Input}, nil
	case PartReference:
		if w.BindingID == "" || w.Text != "" || w.CallID != "" {
			return nil, errors.New("malformed reference part")
		}
		return ReferencePart{BindingID: artifact.BindingID(w.BindingID), Name: w.Name}, nil
	default:
		return nil, fmt.Errorf("unknown part kind %q", w.Kind)
	}
}

// --- entries ------------------------------------------------------------------

type ToolResultStatus string

const (
	ToolSuccess ToolResultStatus = "success"
	ToolError   ToolResultStatus = "error"
	ToolUnknown ToolResultStatus = "unknown"
)

type Input struct {
	ID      InputID          `json:"id"`
	TurnID  TurnID           `json:"turnId,omitempty"`
	Content jsonstable.Value `json:"content"`
	Digest  es.Digest        `json:"digest"`
}

type Assistant struct {
	ID           AssistantID `json:"id"`
	TurnID       TurnID      `json:"turnId"`
	Parts        Parts       `json:"parts"`
	SourceDigest es.Digest   `json:"sourceDigest,omitempty"`
	Digest       es.Digest   `json:"digest"`
}

type ToolResult struct {
	ID           ToolResultID     `json:"id"`
	TurnID       TurnID           `json:"turnId"`
	CallID       CallID           `json:"callId"`
	Status       ToolResultStatus `json:"status"`
	Parts        Parts            `json:"parts"`
	SourceDigest es.Digest        `json:"sourceDigest,omitempty"`
	Digest       es.Digest        `json:"digest"`
}

type Summary struct {
	ID     SummaryID `json:"id"`
	Parts  Parts     `json:"parts"`
	Digest es.Digest `json:"digest"`
}

// Digests (CHT-COD-3): domain equals the EventType; `v` is not covered.

func DigestInput(id InputID, content jsonstable.Value) (es.Digest, error) {
	return digestDomain(TypeInputSubmitted, struct {
		ID      InputID          `json:"id"`
		Content jsonstable.Value `json:"content"`
	}{id, content})
}

func DigestAssistant(a *Assistant) (es.Digest, error) {
	return digestDomain(TypeAssistant, struct {
		ID           AssistantID `json:"id"`
		TurnID       TurnID      `json:"turnId"`
		Parts        Parts       `json:"parts"`
		SourceDigest es.Digest   `json:"sourceDigest,omitempty"`
	}{a.ID, a.TurnID, a.Parts, a.SourceDigest})
}

func DigestToolResult(r *ToolResult) (es.Digest, error) {
	return digestDomain(TypeToolResult, struct {
		ID           ToolResultID     `json:"id"`
		TurnID       TurnID           `json:"turnId"`
		CallID       CallID           `json:"callId"`
		Status       ToolResultStatus `json:"status"`
		Parts        Parts            `json:"parts"`
		SourceDigest es.Digest        `json:"sourceDigest,omitempty"`
	}{r.ID, r.TurnID, r.CallID, r.Status, r.Parts, r.SourceDigest})
}

func DigestSummary(s *Summary) (es.Digest, error) {
	return digestDomain(TypeSummary, struct {
		ID    SummaryID `json:"id"`
		Parts Parts     `json:"parts"`
	}{s.ID, s.Parts})
}

// EntryDigestPair names one active Context entry (CHT-EVT-3).
type EntryDigestPair struct {
	Kind   EntryKind `json:"kind"`
	ID     string    `json:"id"`
	Digest es.Digest `json:"digest"`
}

// DigestBaseContext covers the ordered active Context sequence a checkpoint
// replaces. An empty base digests as nil (empty and nil are one wire value).
func DigestBaseContext(pairs []EntryDigestPair) (es.Digest, error) {
	if len(pairs) == 0 {
		pairs = nil
	}
	return digestDomain(TypeCheckpointCreated, struct {
		Base []EntryDigestPair `json:"base"`
	}{pairs})
}

// DigestCheckpoint covers every checkpoint field except Digest itself.
func DigestCheckpoint(p *CheckpointCreatedPayload) (es.Digest, error) {
	retained := p.Retained
	if len(retained) == 0 {
		retained = nil
	}
	return digestDomain(TypeCheckpointCreated, struct {
		CheckpointID      CheckpointID      `json:"checkpointId"`
		CoveredThrough    session.Seq       `json:"coveredThrough"`
		BaseContextDigest es.Digest         `json:"baseContextDigest"`
		SummaryID         SummaryID         `json:"summaryId"`
		SummaryDigest     es.Digest         `json:"summaryDigest"`
		Retained          []EntryDigestPair `json:"retained,omitempty"`
	}{p.CheckpointID, p.CoveredThrough, p.BaseContextDigest, p.SummaryID, p.SummaryDigest, retained})
}

func digestDomain(typ session.EventType, body any) (es.Digest, error) {
	raw, err := es.EncodeTypedPayload(1, string(typ), body)
	if err != nil {
		return "", err
	}
	return es.DigestBytes(raw), nil
}

// --- payloads (CHT 5) -----------------------------------------------------------

type InputSubmittedPayload struct {
	InputID              InputID          `json:"inputId"`
	Content              jsonstable.Value `json:"content"`
	SubmittedAtUnixMilli int64            `json:"submittedAtUnixMilli"`
}
type InputDeliveredPayload struct {
	InputID InputID `json:"inputId"`
	TurnID  TurnID  `json:"turnId"`
}
type InputWithdrawnPayload struct {
	InputID InputID `json:"inputId"`
	Reason  string  `json:"reason,omitempty"`
}
type InputRejectedPayload struct {
	InputID InputID `json:"inputId"`
	Reason  string  `json:"reason,omitempty"`
}
type AssistantPayload struct {
	Assistant Assistant `json:"assistant"`
}

// SourceDigest lets the run Runtime verify the assistant names a digest that a
// fact of the same commit recorded (TRN-MAP-3).
func (p AssistantPayload) SourceDigest() es.Digest { return p.Assistant.SourceDigest }

type ToolResultPayload struct {
	ToolResult ToolResult `json:"toolResult"`
}

// SourceDigest is the fact-recorded digest of the tool output (TRN-MAP-3).
func (p ToolResultPayload) SourceDigest() es.Digest { return p.ToolResult.SourceDigest }

type ToolResultSupersededPayload struct {
	ToolResultID            ToolResultID `json:"toolResultId"`
	ReplacementToolResultID ToolResultID `json:"replacementToolResultId"`
}
type SummaryPayload struct {
	Summary Summary `json:"summary"`
}

// CheckpointCreatedPayload compacts the context (CHT-EVT-3): entries up to
// CoveredThrough are replaced by the summary plus the Retained subset.
type CheckpointCreatedPayload struct {
	CheckpointID      CheckpointID      `json:"checkpointId"`
	CoveredThrough    session.Seq       `json:"coveredThrough"`
	BaseContextDigest es.Digest         `json:"baseContextDigest"`
	SummaryID         SummaryID         `json:"summaryId"`
	SummaryDigest     es.Digest         `json:"summaryDigest"`
	Retained          []EntryDigestPair `json:"retained,omitempty"`
	Digest            es.Digest         `json:"digest"`
}

type CheckpointInvalidatedPayload struct {
	CheckpointID CheckpointID `json:"checkpointId"`
	Reason       string       `json:"reason,omitempty"`
}

func checkAssistant(p *AssistantPayload) error {
	a := &p.Assistant
	if a.ID == "" || a.TurnID == "" {
		return errors.New("assistant requires id and turnId")
	}
	want, err := DigestAssistant(a)
	if err != nil {
		return err
	}
	if a.Digest != want {
		return errors.New("assistant digest mismatch")
	}
	return nil
}

func checkToolResult(p *ToolResultPayload) error {
	r := &p.ToolResult
	if r.ID == "" || r.TurnID == "" || r.CallID == "" {
		return errors.New("tool_result requires id, turnId and callId")
	}
	switch r.Status {
	case ToolSuccess, ToolError, ToolUnknown:
	default:
		return fmt.Errorf("unknown tool_result status %q", r.Status)
	}
	for _, part := range r.Parts {
		switch part.(type) {
		case TextPart, ReferencePart:
		default:
			return errors.New("tool_result parts must be text or reference")
		}
	}
	want, err := DigestToolResult(r)
	if err != nil {
		return err
	}
	if r.Digest != want {
		return errors.New("tool_result digest mismatch")
	}
	return nil
}

func checkCheckpointCreated(p *CheckpointCreatedPayload) error {
	if p.CheckpointID == "" || p.SummaryID == "" || p.BaseContextDigest == "" || p.SummaryDigest == "" {
		return errors.New("checkpoint requires checkpointId, summaryId and both digests")
	}
	for _, pair := range p.Retained {
		switch pair.Kind {
		case EntryInput, EntryAssistant, EntryToolResult, EntrySummary:
		default:
			return fmt.Errorf("retained entry has unknown kind %q", pair.Kind)
		}
		if pair.ID == "" || pair.Digest == "" {
			return errors.New("retained entry requires id and digest")
		}
	}
	want, err := DigestCheckpoint(p)
	if err != nil {
		return err
	}
	if p.Digest != want {
		return errors.New("checkpoint digest mismatch")
	}
	return nil
}

func checkCheckpointInvalidated(p *CheckpointInvalidatedPayload) error {
	if p.CheckpointID == "" {
		return errors.New("checkpoint_invalidated requires checkpointId")
	}
	return nil
}

func checkSummary(p *SummaryPayload) error {
	if p.Summary.ID == "" {
		return errors.New("summary requires id")
	}
	want, err := DigestSummary(&p.Summary)
	if err != nil {
		return err
	}
	if p.Summary.Digest != want {
		return errors.New("summary digest mismatch")
	}
	return nil
}

// PartsExtractor returns the BindingIDs of ReferenceParts in appearance
// order (CHT-COD-2).
var PartsExtractor extension.BindingExtractor = extension.BindingExtractorFunc(func(value any) ([]artifact.BindingID, error) {
	var parts Parts
	switch v := value.(type) {
	case AssistantPayload:
		parts = v.Assistant.Parts
	case ToolResultPayload:
		parts = v.ToolResult.Parts
	case SummaryPayload:
		parts = v.Summary.Parts
	default:
		return nil, fmt.Errorf("parts extractor: unexpected %T", value)
	}
	var out []artifact.BindingID
	for _, p := range parts {
		if ref, ok := p.(ReferencePart); ok {
			out = append(out, ref.BindingID)
		}
	}
	return out, nil
})

var partsBinding = extension.BindingReferenceDefinition{
	Extractor:          PartsExtractor,
	Cardinality:        extension.Cardinality{Min: 0},
	RequiredDurability: artifact.EventBound,
}

func def[T any](typ session.EventType, check func(*T) error, bindings ...extension.BindingReferenceDefinition) extension.EventDefinition {
	return extension.EventDefinition{Type: typ, Current: 1,
		Codecs:   map[extension.PayloadVersion]extension.PayloadCodec{1: extension.JSONCodec[T]{Check: check}},
		Bindings: bindings}
}

// Module is the chatlog ModuleDescriptor (CHT-SCP-1: no Requires).
var Module = extension.ModuleDescriptor{
	Source: extension.SourceTwilight,
	ID:     ModuleID,
	Events: []extension.EventDefinition{
		def[InputSubmittedPayload](TypeInputSubmitted, func(p *InputSubmittedPayload) error {
			if p.InputID == "" || p.Content.IsZero() {
				return errors.New("input_submitted requires inputId and content")
			}
			return nil
		}),
		def[InputDeliveredPayload](TypeInputDelivered, func(p *InputDeliveredPayload) error {
			if p.InputID == "" || p.TurnID == "" {
				return errors.New("input_delivered requires inputId and turnId")
			}
			return nil
		}),
		def[InputWithdrawnPayload](TypeInputWithdrawn, nil),
		def[InputRejectedPayload](TypeInputRejected, nil),
		def[AssistantPayload](TypeAssistant, checkAssistant, partsBinding),
		def[ToolResultPayload](TypeToolResult, checkToolResult, partsBinding),
		def[ToolResultSupersededPayload](TypeToolResultSuperseded, nil),
		def[SummaryPayload](TypeSummary, checkSummary, partsBinding),
		def[CheckpointCreatedPayload](TypeCheckpointCreated, checkCheckpointCreated),
		def[CheckpointInvalidatedPayload](TypeCheckpointInvalidated, checkCheckpointInvalidated),
	},
	Projections: []extension.ProjectionDefinition{SurfaceProjection, ContextProjection},
}
