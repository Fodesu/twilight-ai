package app

import (
	"bytes"
	"encoding/json"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/observe"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/session"
)

// ChatServerEvent is the closed presentation union, never a Core fact.
type ChatServerEvent interface{ chatServerEvent() }

type ChatCommitted struct {
	Kind           string             `json:"kind"`
	Session        session.SessionID  `json:"session"`
	Cursor         ledger.Position    `json:"cursor"`
	EffectsStarted []ChatActiveEffect `json:"effectsStarted"`
	EffectsEnded   []ChatEffectKey    `json:"effectsEnded"`
	EndedRuns      []string           `json:"endedRuns"`
}

func (ChatCommitted) chatServerEvent() {}

type ChatProgress struct {
	Kind    string            `json:"kind"`
	Session session.SessionID `json:"session"`
	ChatEffectKey
	Generation int    `json:"generation"`
	Sequence   uint64 `json:"sequence"`
	Update     string `json:"update"`
	Text       string `json:"text"`
}

func (ChatProgress) chatServerEvent() {}

// NormalizeChatEvent emits an invalidation for every durable row, including
// unknown module versions. Unsupported transient observations are ignored.
// Errors are stream failures, not commits or wire payloads.
func NormalizeChatEvent(e Event) (ChatServerEvent, bool, error) {
	if e.Err != nil {
		return nil, false, e.Err
	}
	if e.Row.Type != "" {
		out := ChatCommitted{Kind: "committed", Session: e.Session, Cursor: e.Position,
			EffectsStarted: []ChatActiveEffect{}, EffectsEnded: []ChatEffectKey{}, EndedRuns: []string{}}
		if fact, ok := e.Value.(sessionstore.Event); ok {
			out.addFact(fact)
		}
		return out, true, nil
	}
	if e.Progress != nil {
		return normalizeChatProgress(e.Session, e.Progress)
	}
	return nil, false, nil
}

func (out *ChatCommitted) addFact(event sessionstore.Event) {
	id := event.RunID
	switch fact := event.Fact.(type) {
	case run.ModelStepStarted:
		out.EffectsStarted = append(out.EffectsStarted, chatEffect(id, fact.Effect, "model", "model"))
	case run.ToolCallStarted:
		out.EffectsStarted = append(out.EffectsStarted, chatEffect(id, fact.Effect, "tool", string(fact.CallID)))
	case run.RunEnded:
		out.EndedRuns = append(out.EndedRuns, string(id))
	default:
		if effect := chatSettledEffect(event.Fact); effect != "" {
			out.EffectsEnded = append(out.EffectsEnded, ChatEffectKey{RunID: string(id), EffectID: string(effect)})
		}
	}
}

func chatSettledEffect(fact run.Fact) run.EffectID {
	switch fact := fact.(type) {
	case run.ModelStepCompleted:
		return fact.Effect
	case run.ModelStepFailed:
		return fact.Effect
	case run.ModelStepRecovered:
		return fact.Effect
	case run.ModelStepRejected:
		return fact.Effect
	case run.ToolCallCompleted:
		return fact.Effect
	case run.ToolCallFailed:
		return fact.Effect
	}
	return ""
}

func normalizeChatProgress(sid session.SessionID, p *observe.Progress) (ChatServerEvent, bool, error) {
	update := chatProgressKind(p.Kind)
	if update == "" {
		return nil, false, nil
	}
	out := ChatProgress{Kind: "progress", Session: sid,
		ChatEffectKey: ChatEffectKey{RunID: string(p.RunID), EffectID: string(p.Effect)},
		Generation:    p.Generation, Sequence: p.Sequence, Update: update, Text: chatProgressText(p.Payload)}
	return out, true, nil
}

func chatProgressKind(kind string) string {
	switch loop.EventKind(kind) {
	case loop.EventModelTextDelta:
		return "text_delta"
	case loop.EventModelReasoningDelta:
		return "reasoning_delta"
	case loop.EventToolProgress:
		return "tool_progress"
	case loop.EventProgressReset:
		return "reset"
	default:
		// Model/tool lifecycle observations and other transient kinds are not
		// presentation progress. Only durable facts start and settle effects.
		return ""
	}
}

func chatProgressText(payload json.RawMessage) string {
	if len(payload) == 0 {
		return ""
	}
	trimmed := bytes.TrimSpace(payload)
	var text string
	if len(trimmed) != 0 && trimmed[0] == '"' && json.Unmarshal(payload, &text) == nil {
		return text
	}
	// RawMessage is already JSON, not a []byte to marshal (base64), and
	// structured tool progress must retain its original JSON faithfully.
	return string(payload)
}
