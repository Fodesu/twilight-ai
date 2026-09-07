package chatlog

import (
	"errors"
	"fmt"

	"github.com/memohai/twilight/agent/es"
	"github.com/memohai/twilight/agent/session"
	"github.com/memohai/twilight/agent/session/extension"
)

const (
	SurfaceProjectionID extension.ProjectionID = "twilight/chatlog/surface"
	ContextProjectionID extension.ProjectionID = "twilight/chatlog/context"
)

type InputStatus string

const (
	InputSubmitted InputStatus = "submitted"
	InputDelivered InputStatus = "delivered"
	InputWithdrawn InputStatus = "withdrawn"
	InputRejected  InputStatus = "rejected"
)

type InputView struct {
	Input  Input       `json:"input"`
	Status InputStatus `json:"status"`
	// Seq orders inputs by submission within the stream.
	Seq uint64 `json:"seq"`
}

type EntryKind string

const (
	EntryInput      EntryKind = "input"
	EntryAssistant  EntryKind = "assistant"
	EntryToolResult EntryKind = "tool_result"
	EntrySummary    EntryKind = "summary"
)

type SurfaceEntry struct {
	Kind     EntryKind             `json:"kind"`
	ID       string                `json:"id"`
	Position session.EventPosition `json:"position"`
}

// Surface is the UI-facing read model (CHT-SUR-1).
type Surface struct {
	Inputs      map[InputID]InputView         `json:"inputs"`
	Assistants  map[AssistantID]Assistant     `json:"assistants"`
	ToolResults map[ToolResultID]ToolResult   `json:"toolResults"`
	Summaries   map[SummaryID]Summary         `json:"summaries"`
	EntryOrder  []SurfaceEntry                `json:"entryOrder"`
	Superseded  map[ToolResultID]ToolResultID `json:"superseded,omitempty"`
	nextSeq     uint64
}

// SubmittedInputs returns inputs still awaiting delivery, in submission order.
func (s *Surface) SubmittedInputs() []Input {
	var out []InputView
	for _, v := range s.Inputs {
		if v.Status == InputSubmitted {
			out = append(out, v)
		}
	}
	sortViews(out)
	inputs := make([]Input, len(out))
	for i := range out {
		inputs[i] = out[i].Input
	}
	return inputs
}

func sortViews(views []InputView) {
	for i := 1; i < len(views); i++ {
		for j := i; j > 0 && views[j].Seq < views[j-1].Seq; j-- {
			views[j], views[j-1] = views[j-1], views[j]
		}
	}
}

var chatlogConsumes = []session.EventType{TypeInputSubmitted, TypeInputDelivered, TypeInputWithdrawn, TypeInputRejected,
	TypeAssistant, TypeToolResult, TypeToolResultSuperseded, TypeSummary}

var SurfaceProjection = extension.ProjectionDefinition{
	ID: SurfaceProjectionID, Version: 1,
	Consumes:        chatlogConsumes,
	RequireComplete: []extension.ModuleID{ModuleID},
	Initial: func() (any, error) {
		return Surface{Inputs: map[InputID]InputView{}, Assistants: map[AssistantID]Assistant{}, ToolResults: map[ToolResultID]ToolResult{}, Summaries: map[SummaryID]Summary{}, Superseded: map[ToolResultID]ToolResultID{}}, nil
	},
	Apply:      applySurface,
	StateCodec: extension.JSONStateCodec[Surface]{},
}

func applySurface(state any, e extension.DecodedEvent) (any, error) {
	s := state.(Surface)
	s = cloneSurface(s)
	pos := session.EventPosition{Revision: e.Revision, Index: e.Event.Index, EventDigest: e.Event.EventDigest}
	switch p := e.Value.(type) {
	case InputSubmittedPayload:
		if _, dup := s.Inputs[p.InputID]; dup {
			return nil, fmt.Errorf("input %s submitted twice", p.InputID)
		}
		d, err := DigestInput(p.InputID, p.Content)
		if err != nil {
			return nil, err
		}
		s.nextSeq++
		s.Inputs[p.InputID] = InputView{Input: Input{ID: p.InputID, Content: p.Content, Digest: d}, Status: InputSubmitted, Seq: s.nextSeq}
	case InputDeliveredPayload:
		v, ok := s.Inputs[p.InputID]
		if !ok || v.Status != InputSubmitted {
			return nil, fmt.Errorf("input %s delivered while %s", p.InputID, v.Status)
		}
		v.Status = InputDelivered
		v.Input.TurnID = p.TurnID
		s.Inputs[p.InputID] = v
		s.EntryOrder = append(s.EntryOrder, SurfaceEntry{Kind: EntryInput, ID: string(p.InputID), Position: pos})
	case InputWithdrawnPayload:
		if err := terminateInput(&s, p.InputID, InputWithdrawn); err != nil {
			return nil, err
		}
	case InputRejectedPayload:
		if err := terminateInput(&s, p.InputID, InputRejected); err != nil {
			return nil, err
		}
	case AssistantPayload:
		if _, dup := s.Assistants[p.Assistant.ID]; dup {
			return nil, fmt.Errorf("assistant %s created twice", p.Assistant.ID)
		}
		s.Assistants[p.Assistant.ID] = p.Assistant
		s.EntryOrder = append(s.EntryOrder, SurfaceEntry{Kind: EntryAssistant, ID: string(p.Assistant.ID), Position: pos})
	case ToolResultPayload:
		if _, dup := s.ToolResults[p.ToolResult.ID]; dup {
			return nil, fmt.Errorf("tool_result %s created twice", p.ToolResult.ID)
		}
		s.ToolResults[p.ToolResult.ID] = p.ToolResult
		s.EntryOrder = append(s.EntryOrder, SurfaceEntry{Kind: EntryToolResult, ID: string(p.ToolResult.ID), Position: pos})
	case ToolResultSupersededPayload:
		if _, ok := s.ToolResults[p.ToolResultID]; !ok {
			return nil, fmt.Errorf("superseded tool_result %s unknown", p.ToolResultID)
		}
		if _, dup := s.Superseded[p.ToolResultID]; dup {
			return nil, fmt.Errorf("tool_result %s superseded twice", p.ToolResultID)
		}
		s.Superseded[p.ToolResultID] = p.ReplacementToolResultID
	case SummaryPayload:
		if _, dup := s.Summaries[p.Summary.ID]; dup {
			return nil, fmt.Errorf("summary %s created twice", p.Summary.ID)
		}
		s.Summaries[p.Summary.ID] = p.Summary
		s.EntryOrder = append(s.EntryOrder, SurfaceEntry{Kind: EntrySummary, ID: string(p.Summary.ID), Position: pos})
	default:
		return nil, fmt.Errorf("chatlog surface: unexpected %T", e.Value)
	}
	return s, nil
}

func terminateInput(s *Surface, id InputID, status InputStatus) error {
	v, ok := s.Inputs[id]
	if !ok || v.Status != InputSubmitted {
		return fmt.Errorf("input %s %s while %s", id, status, v.Status)
	}
	v.Status = status
	s.Inputs[id] = v
	return nil
}

func cloneSurface(s Surface) Surface {
	out := Surface{Inputs: make(map[InputID]InputView, len(s.Inputs)), Assistants: make(map[AssistantID]Assistant, len(s.Assistants)),
		ToolResults: make(map[ToolResultID]ToolResult, len(s.ToolResults)), Summaries: make(map[SummaryID]Summary, len(s.Summaries)),
		Superseded: make(map[ToolResultID]ToolResultID, len(s.Superseded)), EntryOrder: append([]SurfaceEntry(nil), s.EntryOrder...), nextSeq: s.nextSeq}
	for k, v := range s.Inputs {
		out.Inputs[k] = v
	}
	for k, v := range s.Assistants {
		out.Assistants[k] = v
	}
	for k, v := range s.ToolResults {
		out.ToolResults[k] = v
	}
	for k, v := range s.Summaries {
		out.Summaries[k] = v
	}
	for k, v := range s.Superseded {
		out.Superseded[k] = v
	}
	if out.nextSeq == 0 {
		// Restored from a snapshot: the counter is not persisted, but Seq only
		// has to be monotonic, so continue from the largest known value.
		for _, v := range out.Inputs {
			if v.Seq > out.nextSeq {
				out.nextSeq = v.Seq
			}
		}
	}
	return out
}

// --- context ------------------------------------------------------------------

// Entry is one element of the model-facing conversation (CHT-CTX-1).
type Entry struct {
	Kind       EntryKind   `json:"kind"`
	ID         string      `json:"id"`
	Digest     es.Digest   `json:"digest"`
	Input      *Input      `json:"input,omitempty"`
	Assistant  *Assistant  `json:"assistant,omitempty"`
	ToolResult *ToolResult `json:"toolResult,omitempty"`
	Summary    *Summary    `json:"summary,omitempty"`
}

// Context is the projection state: the ordered entries plus the bookkeeping
// ContextFold needs (submitted inputs awaiting delivery, superseded results).
type Context struct {
	Entries    []Entry                       `json:"entries"`
	Pending    map[InputID]Input             `json:"pending,omitempty"`
	Superseded map[ToolResultID]ToolResultID `json:"superseded,omitempty"`
}

var ContextProjection = extension.ProjectionDefinition{
	ID: ContextProjectionID, Version: 1,
	Consumes:        chatlogConsumes,
	RequireComplete: []extension.ModuleID{ModuleID},
	Initial: func() (any, error) {
		return Context{Pending: map[InputID]Input{}, Superseded: map[ToolResultID]ToolResultID{}}, nil
	},
	Apply:      applyContext,
	StateCodec: extension.JSONStateCodec[Context]{},
}

func applyContext(state any, e extension.DecodedEvent) (any, error) {
	c := state.(Context)
	c = Context{Entries: append([]Entry(nil), c.Entries...), Pending: copyInputs(c.Pending), Superseded: copyIDs(c.Superseded)}
	switch p := e.Value.(type) {
	case InputSubmittedPayload:
		d, err := DigestInput(p.InputID, p.Content)
		if err != nil {
			return nil, err
		}
		c.Pending[p.InputID] = Input{ID: p.InputID, Content: p.Content, Digest: d}
	case InputDeliveredPayload:
		in, ok := c.Pending[p.InputID]
		if !ok {
			return nil, fmt.Errorf("input %s delivered before submission", p.InputID)
		}
		delete(c.Pending, p.InputID)
		in.TurnID = p.TurnID
		c.Entries = append(c.Entries, Entry{Kind: EntryInput, ID: string(in.ID), Digest: in.Digest, Input: &in})
	case InputWithdrawnPayload:
		delete(c.Pending, p.InputID)
	case InputRejectedPayload:
		delete(c.Pending, p.InputID)
	case AssistantPayload:
		a := p.Assistant
		c.Entries = append(c.Entries, Entry{Kind: EntryAssistant, ID: string(a.ID), Digest: a.Digest, Assistant: &a})
	case ToolResultPayload:
		r := p.ToolResult
		c.Entries = append(c.Entries, Entry{Kind: EntryToolResult, ID: string(r.ID), Digest: r.Digest, ToolResult: &r})
	case ToolResultSupersededPayload:
		c.Superseded[p.ToolResultID] = p.ReplacementToolResultID
		kept := c.Entries[:0:0]
		for _, en := range c.Entries {
			if en.Kind == EntryToolResult && en.ID == string(p.ToolResultID) {
				continue
			}
			kept = append(kept, en)
		}
		c.Entries = kept
	case SummaryPayload:
		s := p.Summary
		c.Entries = append(c.Entries, Entry{Kind: EntrySummary, ID: string(s.ID), Digest: s.Digest, Summary: &s})
	default:
		return nil, fmt.Errorf("chatlog context: unexpected %T", e.Value)
	}
	return c, nil
}

func copyInputs(m map[InputID]Input) map[InputID]Input {
	out := make(map[InputID]Input, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func copyIDs(m map[ToolResultID]ToolResultID) map[ToolResultID]ToolResultID {
	out := make(map[ToolResultID]ToolResultID, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// ContextFold folds decoded chatlog events into entries (CHT-CTX-1).
func ContextFold(events []extension.DecodedEvent) ([]Entry, error) {
	state, _ := ContextProjection.Initial()
	for _, e := range events {
		if e.ModuleID != ModuleID || e.Unknown {
			return nil, errors.New("chatlog: context fold requires decoded chatlog events")
		}
		next, err := applyContext(state, e)
		if err != nil {
			return nil, err
		}
		state = next
	}
	return state.(Context).Entries, nil
}
