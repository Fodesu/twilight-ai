package turn

import (
	"fmt"

	"github.com/memohai/twilight/agent/run"
)

// MapTransition is the v1 FactMapper (TRN-MAP-1): it projects the AgentEvents
// of one committed transition into chatlog events for the Turn. Facts that
// carry no conversation content map to nothing. RunEnded is settlement, not
// content, and is handled by the coordinator.
func MapTransition(turnID TurnID, tr *run.TransitionRecord) ([]Event, error) {
	var out []Event
	for i := range tr.Events {
		ev := &tr.Events[i]
		var payload any
		var typ string
		switch f := ev.Fact.(type) {
		case run.ModelStepCompleted:
			calls := make([]ToolCallPayload, 0, len(f.Result.ToolCalls))
			for _, tc := range f.Result.ToolCalls {
				calls = append(calls, ToolCallPayload{CallID: run.CallID(tc.ToolCallID), Name: tc.ToolName, Input: tc.Input})
			}
			typ = EventAssistant
			payload = AssistantPayload{TurnID: turnID, StepID: f.StepID, Text: f.Result.Text, ToolCalls: calls}
		case run.ToolCallCompleted:
			typ = EventToolResult
			payload = ToolResultPayload{TurnID: turnID, CallID: f.CallID, Status: ToolSuccess, Output: f.Result.Output}
		case run.ToolCallAnswered:
			typ = EventToolResult
			payload = ToolResultPayload{TurnID: turnID, CallID: f.CallID, Status: ToolSuccess, Output: f.Payload}
		case run.ToolCallFailed:
			status := ToolError
			if f.Outcome == run.ToolOutcomeUnknown || f.Failure.Class == run.FailureEffectUnknown {
				status = ToolUnknown
			}
			typ = EventToolResult
			payload = ToolResultPayload{TurnID: turnID, CallID: f.CallID, Status: status, Failure: f.Failure.Class, Message: f.Failure.Message}
		default:
			continue
		}
		raw, err := run.CanonicalJSONFromValue(payload)
		if err != nil {
			return nil, fmt.Errorf("agent: turn: map %s: %w", typ, err)
		}
		out = append(out, Event{Type: typ, Turn: turnID, Revision: ev.Revision, Index: ev.Index, Payload: raw})
	}
	return out, nil
}
