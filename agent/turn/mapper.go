package turn

import (
	"fmt"

	"github.com/memohai/twilight/agent/run"
)

// MapTransition is the v1 FactMapper (TRN-MAP-1): it projects the AgentEvents
// of the last transition in prefix into chatlog events for the Turn. prefix is
// the complete record up to and including that transition (TRN-MAT-1), which
// is how a tool_result learns the ProviderCallID its ToolStepOpened carried.
// Facts with no conversation content map to nothing; RunEnded is settlement
// and is handled by the coordinator.
func MapTransition(turnID TurnID, prefix []run.TransitionRecord) ([]Event, error) {
	if len(prefix) == 0 {
		return nil, nil
	}
	type callInfo struct{ provider, name string }
	calls := make(map[run.CallID]callInfo)
	for i := range prefix {
		for j := range prefix[i].Events {
			if completed, ok := prefix[i].Events[j].Fact.(run.ModelStepCompleted); ok {
				for k, tc := range completed.Result.ToolCalls {
					calls[run.DeriveCallID(completed.StepID, k)] = callInfo{provider: tc.ToolCallID, name: tc.ToolName}
				}
			}
		}
	}
	tr := &prefix[len(prefix)-1]
	var out []Event
	for i := range tr.Events {
		ev := &tr.Events[i]
		var payload any
		var typ string
		switch f := ev.Fact.(type) {
		case run.ModelStepCompleted:
			calls := make([]ToolCallPayload, 0, len(f.Result.ToolCalls))
			for j, tc := range f.Result.ToolCalls {
				calls = append(calls, ToolCallPayload{CallID: run.DeriveCallID(f.StepID, j), ProviderCallID: tc.ToolCallID, Name: tc.ToolName, Input: tc.Input})
			}
			typ = EventAssistant
			payload = AssistantPayload{TurnID: turnID, StepID: f.StepID, Text: f.Result.Text, ToolCalls: calls}
		case run.ToolCallCompleted:
			typ = EventToolResult
			payload = ToolResultPayload{TurnID: turnID, CallID: f.CallID, ProviderCallID: calls[f.CallID].provider, Name: calls[f.CallID].name, Status: ToolSuccess, Output: f.Result.Output}
		case run.ToolCallAnswered:
			typ = EventToolResult
			payload = ToolResultPayload{TurnID: turnID, CallID: f.CallID, ProviderCallID: calls[f.CallID].provider, Name: calls[f.CallID].name, Status: ToolSuccess, Output: f.Payload}
		case run.ToolCallFailed:
			status := ToolError
			if f.Outcome == run.ToolOutcomeUnknown || f.Failure.Class == run.FailureEffectUnknown {
				status = ToolUnknown
			}
			typ = EventToolResult
			payload = ToolResultPayload{TurnID: turnID, CallID: f.CallID, ProviderCallID: calls[f.CallID].provider, Name: calls[f.CallID].name, Status: status, Failure: f.Failure.Class, Message: f.Failure.Message}
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
