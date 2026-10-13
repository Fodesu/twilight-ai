package app

import (
	"context"
	"fmt"
	"sort"

	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/session"
)

// ChatHistory is the materialized UI projection, not the compacted model context.
// Head.Next is the FIRST commit outside this snapshot. Subscribe from that
// inclusive sequence to bridge snapshot and live events without a race.
type ChatHistory struct {
	ProtocolVersion int           `json:"protocolVersion"`
	Head            ledger.Head   `json:"head"`
	Messages        []ChatMessage `json:"messages"`
}

type ChatMessage struct {
	Kind         chatlog.EntryKind `json:"kind"`
	ID           string            `json:"id"`
	Position     ledger.Position   `json:"position"`
	Text         string            `json:"text"`
	Status       string            `json:"status,omitempty"`
	TurnID       string            `json:"turnId,omitempty"`
	RunID        string            `json:"runId,omitempty"`
	StepID       string            `json:"stepId,omitempty"`
	CallID       string            `json:"callId,omitempty"`
	SupersededBy string            `json:"supersededBy,omitempty"`
	Calls        []ChatToolCall    `json:"calls,omitempty"`
}

type ChatToolCall struct {
	CallID string `json:"callId"`
	Name   string `json:"name"`
	Input  any    `json:"input"`
}

// History reads one projection plus immutable CAS bodies, so the head and all
// messages describe the SAME snapshot even while a turn is being committed.
func (app *Application) History(ctx context.Context, sid session.SessionID) (ChatHistory, error) {
	state, head, err := app.Projection(ctx, sid, chatlog.SurfaceProjectionID, chatlog.SurfaceProjection.Version)
	if err != nil {
		return ChatHistory{}, err
	}
	surface, ok := state.(chatlog.Surface)
	if !ok {
		return ChatHistory{}, fmt.Errorf("chat history: projection is %T", state)
	}
	out := ChatHistory{ProtocolVersion: 1, Head: head, Messages: make([]ChatMessage, 0, len(surface.EntryOrder))}
	materializer := chatlog.NewMaterializer(app.Content())
	// Include pending/withdrawn/rejected inputs too: they are durable submissions,
	// although only delivered inputs have a position in EntryOrder.
	var inputErr error
	surface.Inputs.Range(func(id chatlog.InputID, view chatlog.InputView) bool {
		var body struct {
			Text string `json:"text"`
		}
		if err := view.Input.Content.Decode(&body); err != nil {
			inputErr = err
			return false
		}
		out.Messages = append(out.Messages, ChatMessage{Kind: chatlog.EntryInput, ID: string(id), Position: view.Position,
			Text: body.Text, Status: string(view.Status), TurnID: string(view.Input.TurnID)})
		return true
	})
	if inputErr != nil {
		return ChatHistory{}, inputErr
	}
	for _, ordered := range surface.EntryOrder {
		if ordered.Kind == chatlog.EntryInput {
			continue
		}
		entry, message, err := historyEntry(&surface, ordered)
		if err != nil {
			return ChatHistory{}, err
		}
		rendered, err := materializer.Entry(ctx, &entry)
		if err != nil {
			return ChatHistory{}, err
		}
		message.Text = rendered.Text()
		for _, call := range rendered.Calls {
			message.Calls = append(message.Calls, ChatToolCall{CallID: string(call.CallID), Name: call.Name, Input: call.Input})
		}
		out.Messages = append(out.Messages, message)
	}
	sort.SliceStable(out.Messages, func(i, j int) bool { return out.Messages[i].Position.Less(out.Messages[j].Position) })
	return out, nil
}

func historyEntry(surface *chatlog.Surface, ordered chatlog.SurfaceEntry) (chatlog.Entry, ChatMessage, error) {
	entry := chatlog.Entry{Kind: ordered.Kind, ID: ordered.ID, Position: ordered.Position}
	message := ChatMessage{Kind: ordered.Kind, ID: ordered.ID, Position: ordered.Position}
	switch ordered.Kind {
	case chatlog.EntryAssistant:
		value, exists := surface.Assistants.Get(chatlog.AssistantID(ordered.ID))
		if !exists {
			return entry, message, fmt.Errorf("chat history: missing assistant %s", ordered.ID)
		}
		entry.Assistant = &value
		message.TurnID, message.RunID, message.StepID = string(value.TurnID), string(value.RunID), string(value.StepID)
	case chatlog.EntryToolResult:
		value, exists := surface.ToolResults.Get(chatlog.ToolResultID(ordered.ID))
		if !exists {
			return entry, message, fmt.Errorf("chat history: missing tool result %s", ordered.ID)
		}
		entry.ToolResult = &value
		message.TurnID, message.RunID = string(value.TurnID), string(value.RunID)
		message.CallID, message.Status = string(value.CallID), string(value.Status)
		if replacement, exists := surface.Superseded.Get(value.ID); exists {
			message.SupersededBy = string(replacement)
		}
	case chatlog.EntrySummary:
		value, exists := surface.Summaries.Get(chatlog.SummaryID(ordered.ID))
		if !exists {
			return entry, message, fmt.Errorf("chat history: missing summary %s", ordered.ID)
		}
		entry.Summary = &value
	default:
		return entry, message, fmt.Errorf("chat history: unknown entry kind %s", ordered.Kind)
	}
	return entry, message, nil
}
