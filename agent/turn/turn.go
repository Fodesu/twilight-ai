// Package turn is the minimal Turn coordinator: one Turn owns one primary
// Run, delivers inputs into it, drives it, and materializes the committed
// Run record into chatlog events on an append-only session log.
//
// This is the vertical slice described in docs/design/agent-turn.md, not the
// full Session kernel: the Log has no fork, snapshot or import, event
// identity is a sequence number, and materialization coverage is the
// highest Run revision already mapped for the Turn. Those simplifications are
// what the slice exists to test the Run API against; the Session spec stays
// a draft until this shape has been driven from a real host.
package turn

import (
	"context"
	"errors"
	"sync"

	"github.com/memohai/twilight/agent/run"
)

type SessionID string
type TurnID string

// Ref addresses one Turn.
type Ref struct {
	Session SessionID
	Turn    TurnID
}

// Event types this package appends. Chatlog types follow
// agent-session-chatlog.md; turn types follow agent-turn.md.
const (
	EventTurnStarted    = "twilight/turn/started"
	EventTurnCompleted  = "twilight/turn/completed"
	EventTurnFailed     = "twilight/turn/failed"
	EventInputDelivered = "twilight/chatlog/input_delivered"
	EventAssistant      = "twilight/chatlog/assistant"
	EventToolResult     = "twilight/chatlog/tool_result"
)

// Event is one committed fact on a session log. Seq is assigned by the Log.
// Revision/Index are set on events materialized from a Run transition and
// carry the source AgentEvent position; they are the coverage watermark.
type Event struct {
	Seq      uint64
	Type     string
	Turn     TurnID
	Revision uint64
	Index    uint16
	Payload  run.CanonicalJSON
}

// Log is an append-only per-session event log. Append assigns contiguous
// Seq values and persists the whole group or nothing.
type Log interface {
	Append(ctx context.Context, session SessionID, events []Event) error
	Replay(ctx context.Context, session SessionID) ([]Event, error)
}

// MemoryLog is the in-process Log.
type MemoryLog struct {
	mu   sync.Mutex
	logs map[SessionID][]Event
}

func NewMemoryLog() *MemoryLog { return &MemoryLog{logs: make(map[SessionID][]Event)} }

func (m *MemoryLog) Append(ctx context.Context, session SessionID, events []Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(events) == 0 {
		return errors.New("agent: turn: empty append")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	log := m.logs[session]
	next := uint64(len(log)) + 1
	for i := range events {
		events[i].Seq = next
		next++
	}
	m.logs[session] = append(log, events...)
	return nil
}

func (m *MemoryLog) Replay(ctx context.Context, session SessionID) ([]Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Event(nil), m.logs[session]...), nil
}

// --- payloads -------------------------------------------------------------

type StartedPayload struct {
	TurnID   TurnID        `json:"turnId"`
	RunID    run.RunID     `json:"runId"`
	InputIDs []run.InputID `json:"inputIds"`
}

type InputDeliveredPayload struct {
	InputID run.InputID       `json:"inputId"`
	TurnID  TurnID            `json:"turnId"`
	Content run.CanonicalJSON `json:"content"`
}

type Settlement string

const (
	SettlementCompleted Settlement = "completed"
	SettlementFailed    Settlement = "failed"
	SettlementStopped   Settlement = "stopped"
)

type SettledPayload struct {
	TurnID       TurnID     `json:"turnId"`
	RunID        run.RunID  `json:"runId"`
	Settlement   Settlement `json:"settlement"`
	Revision     uint64     `json:"revision"`
	FailureClass string     `json:"failureClass,omitempty"`
	Reason       string     `json:"reason,omitempty"`
}

type ToolCallPayload struct {
	CallID         run.CallID        `json:"callId"`
	ProviderCallID string            `json:"providerCallId,omitempty"`
	Name           string            `json:"name"`
	Input          run.CanonicalJSON `json:"input"`
}

type AssistantPayload struct {
	TurnID    TurnID            `json:"turnId"`
	StepID    run.StepID        `json:"stepId"`
	Text      string            `json:"text,omitempty"`
	ToolCalls []ToolCallPayload `json:"toolCalls,omitempty"`
}

type ToolResultStatus string

const (
	ToolSuccess ToolResultStatus = "success"
	ToolError   ToolResultStatus = "error"
	ToolUnknown ToolResultStatus = "unknown"
)

type ToolResultPayload struct {
	TurnID         TurnID     `json:"turnId"`
	CallID         run.CallID `json:"callId"`
	ProviderCallID string     `json:"providerCallId,omitempty"`
	// Name is the tool name the model used for this call, echoed back with
	// the result for providers that pair on it.
	Name    string            `json:"name,omitempty"`
	Status  ToolResultStatus  `json:"status"`
	Output  run.CanonicalJSON `json:"output,omitzero"`
	Failure string            `json:"failure,omitempty"`
	Message string            `json:"message,omitempty"`
}
