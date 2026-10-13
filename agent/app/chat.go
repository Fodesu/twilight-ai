package app

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/turn"
)

const ChatVersion = 2

// ErrChatSnapshotBusy means no common projection watermark was available.
var ErrChatSnapshotBusy = errors.New("chat: snapshot busy")

type ChatCapabilities struct {
	Version   int  `json:"version"`
	Submit    bool `json:"submit"`
	Stop      bool `json:"stop"`
	Resume    bool `json:"resume"`
	Approvals bool `json:"approvals"`
}

func (app *Application) ChatCapabilities() ChatCapabilities {
	available := app.inbox != nil
	return ChatCapabilities{Version: ChatVersion, Submit: available, Stop: available, Resume: available}
}

// ChatSnapshot exposes both the raw transcript for inspection and the
// canonical presentation groups consumed by clients.
type ChatSnapshot struct {
	Version       int                `json:"version"`
	Session       session.SessionID  `json:"session"`
	NextCommit    ledger.CommitSeq   `json:"nextCommit"`
	Transcript    []ChatMessageV2    `json:"transcript"`
	Turns         []ChatTurn         `json:"turns"`
	Queued        []ChatMessageV2    `json:"queued,omitempty"`
	Unassigned    []ChatMessageV2    `json:"unassigned,omitempty"`
	ActiveEffects []ChatActiveEffect `json:"activeEffects"`
	Approvals     []ChatApproval     `json:"approvals"`
}

// ChatMessageV2 deliberately excludes Core's step identity.
type ChatMessageV2 struct {
	Kind         chatlog.EntryKind `json:"kind"`
	ID           string            `json:"id"`
	Position     ledger.Position   `json:"position"`
	Text         string            `json:"text"`
	Status       string            `json:"status"`
	TurnID       string            `json:"turnId"`
	RunID        string            `json:"runId"`
	CallID       string            `json:"callId"`
	SupersededBy string            `json:"supersededBy"`
	Calls        []ChatToolCall    `json:"calls"`
}

// Items is ordered by the committed history position within this Turn.
type ChatTurn struct {
	ID       string          `json:"id"`
	Status   turn.TurnStatus `json:"status"`
	Reason   string          `json:"reason"`
	RunID    string          `json:"runId"`
	InputIDs []string        `json:"inputIds"`
	Items    []ChatMessageV2 `json:"items,omitempty"`
}

type ChatEffectKey struct {
	RunID    string `json:"runId"`
	EffectID string `json:"effectId"`
}

type ChatActiveEffect struct {
	ChatEffectKey
	Kind  string `json:"kind"`
	Label string `json:"label"`
}

type ChatApproval struct {
	ID     string `json:"id"`
	RunID  string `json:"runId"`
	CallID string `json:"callId"`
	Label  string `json:"label"`
}

// ChatSnapshot returns history, turns and in-flight effects at one watermark.
// Separate lease-free reads can race a commit; bounded retries never manufacture
// a head by combining data from different commits.
func (app *Application) ChatSnapshot(ctx context.Context, sid session.SessionID) (ChatSnapshot, error) {
	return retryChatSnapshot(ctx, func() (ChatSnapshot, bool, error) {
		return app.readChatSnapshot(ctx, sid)
	})
}

func retryChatSnapshot(ctx context.Context, read func() (ChatSnapshot, bool, error)) (ChatSnapshot, error) {
	for attempt := 0; attempt < 8; attempt++ {
		if err := ctx.Err(); err != nil {
			return ChatSnapshot{}, err
		}
		snapshot, consistent, err := read()
		if err != nil || consistent {
			return snapshot, err
		}
		if attempt == 7 {
			break
		}
		if err := waitChatRetry(ctx); err != nil {
			return ChatSnapshot{}, err
		}
	}
	return ChatSnapshot{}, ErrChatSnapshotBusy
}

func waitChatRetry(ctx context.Context) error {
	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (app *Application) readChatSnapshot(ctx context.Context, sid session.SessionID) (ChatSnapshot, bool, error) {
	history, err := app.History(ctx, sid)
	if err != nil {
		return ChatSnapshot{}, false, err
	}
	turns, th, err := app.Projection(ctx, sid, turn.SurfaceProjectionID, turn.SurfaceProjection.Version)
	if err != nil {
		return ChatSnapshot{}, false, err
	}
	machine, mh, err := app.Projection(ctx, sid, sessionstore.MachineProjectionID, sessionstore.MachineProjection.Version)
	if err != nil {
		return ChatSnapshot{}, false, err
	}
	ts, ok := turns.(turn.TurnSurface)
	if !ok {
		return ChatSnapshot{}, false, fmt.Errorf("chat: turn projection is %T", turns)
	}
	ms, ok := machine.(sessionstore.Machine)
	if !ok {
		return ChatSnapshot{}, false, fmt.Errorf("chat: machine projection is %T", machine)
	}
	if history.Head.Next != th.Next || th.Next != mh.Next {
		return ChatSnapshot{}, false, nil
	}
	return materializeChatSnapshot(sid, history, ts, ms), true, nil
}

func materializeChatSnapshot(sid session.SessionID, history ChatHistory, ts turn.TurnSurface, ms sessionstore.Machine) ChatSnapshot {
	out := ChatSnapshot{Version: ChatVersion, Session: sid, NextCommit: history.Head.Next,
		Transcript: make([]ChatMessageV2, 0, len(history.Messages)), Turns: make([]ChatTurn, 0, len(ts.Order)),
		Queued: make([]ChatMessageV2, 0), Unassigned: make([]ChatMessageV2, 0),
		ActiveEffects: []ChatActiveEffect{}, Approvals: []ChatApproval{}}
	for i := range history.Messages {
		out.Transcript = append(out.Transcript, chatMessageV2(&history.Messages[i]))
	}
	assigned := make(map[string]struct{}, len(history.Messages))
	for _, id := range ts.Order {
		view := ts.Turns[id]
		chatTurnView := chatTurn(view)
		chatTurnView.Items = turnMessages(history.Messages, chatTurnView)
		for _, item := range chatTurnView.Items {
			assigned[item.ID] = struct{}{}
		}
		out.Turns = append(out.Turns, chatTurnView)
	}
	for i := range history.Messages {
		message := chatMessageV2(&history.Messages[i])
		if _, ok := assigned[message.ID]; ok {
			continue
		}
		if message.Kind == chatlog.EntryInput && message.TurnID == "" {
			out.Queued = append(out.Queued, message)
			continue
		}
		out.Unassigned = append(out.Unassigned, message)
	}
	for id := range ms.Active {
		state := ms.Active[id]
		out.ActiveEffects = append(out.ActiveEffects, chatMachineEffects(id, &state)...)
	}
	sort.Slice(out.ActiveEffects, func(i, j int) bool {
		a, b := out.ActiveEffects[i], out.ActiveEffects[j]
		if a.RunID != b.RunID {
			return a.RunID < b.RunID
		}
		return a.EffectID < b.EffectID
	})
	return out
}

func chatMessageV2(m *ChatMessage) ChatMessageV2 {
	return ChatMessageV2{Kind: m.Kind, ID: m.ID, Position: m.Position, Text: m.Text, Status: m.Status,
		TurnID: m.TurnID, RunID: m.RunID, CallID: m.CallID, SupersededBy: m.SupersededBy,
		Calls: append([]ChatToolCall{}, m.Calls...)}
}

func chatTurn(view turn.TurnView) ChatTurn {
	out := ChatTurn{ID: string(view.TurnID), Status: view.Status, Reason: view.Reason, RunID: string(view.RunID), InputIDs: []string{}}
	for _, id := range view.InputIDs {
		out.InputIDs = append(out.InputIDs, string(id))
	}
	return out
}

func turnMessages(messages []ChatMessage, chatTurn ChatTurn) []ChatMessageV2 {
	inputIDs := make(map[string]struct{}, len(chatTurn.InputIDs))
	for _, id := range chatTurn.InputIDs {
		inputIDs[id] = struct{}{}
	}
	items := make([]ChatMessageV2, 0)
	for i := range messages {
		message := &messages[i]
		if message.TurnID == chatTurn.ID ||
			(chatTurn.RunID != "" && message.RunID == chatTurn.RunID) {
			items = append(items, chatMessageV2(message))
			continue
		}
		if message.Kind != chatlog.EntryInput {
			continue
		}
		if _, ok := inputIDs[message.ID]; ok {
			items = append(items, chatMessageV2(message))
		}
	}
	return items
}

func chatMachineEffects(id run.RunID, state *run.MachineState) []ChatActiveEffect {
	switch step := state.Current.(type) {
	case run.ModelStep:
		if step.Status == run.ModelExecuting {
			return []ChatActiveEffect{chatEffect(id, step.Effect, "model", string(step.Model))}
		}
	case run.ToolStep:
		return chatToolEffects(id, step.Calls)
	}
	return nil
}

func chatToolEffects(id run.RunID, calls []run.ToolCallState) []ChatActiveEffect {
	out := []ChatActiveEffect{}
	for i := range calls {
		call := &calls[i]
		if call.Status == run.ToolExecuting {
			out = append(out, chatEffect(id, call.Effect, "tool", string(call.ToolRef)))
		}
	}
	return out
}

func chatEffect(id run.RunID, effect run.EffectID, kind, label string) ChatActiveEffect {
	return ChatActiveEffect{ChatEffectKey: ChatEffectKey{RunID: string(id), EffectID: string(effect)}, Kind: kind, Label: label}
}
