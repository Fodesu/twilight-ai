package chatlog

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/es"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
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
	// Seq orders inputs by submission; it comes from a projection-internal
	// counter, not the wire (v2 events carry no Seq).
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
	Kind EntryKind `json:"kind"`
	ID   string    `json:"id"`
	Seq  uint64    `json:"seq"`
}

type CheckpointStatus string

const (
	CheckpointActive      CheckpointStatus = "active"
	CheckpointInvalidated CheckpointStatus = "invalidated"
)

// CheckpointView records one checkpoint for readers; compaction never touches
// EntryOrder or the input queue (CHT-SUR-1).
type CheckpointView struct {
	Checkpoint CheckpointCreatedPayload `json:"checkpoint"`
	Status     CheckpointStatus         `json:"status"`
	Reason     string                   `json:"reason,omitempty"`
	Seq        uint64                   `json:"seq"`
}

// Surface is the UI-facing read model (CHT-SUR-1). Every table is persistent
// (Table): a fold shares them between states and pays O(sqrt(n)) per write.
type Surface struct {
	Inputs      Table[InputID, InputView]           `json:"inputs"`
	Assistants  Table[AssistantID, Assistant]       `json:"assistants"`
	ToolResults Table[ToolResultID, ToolResult]     `json:"toolResults"`
	Summaries   Table[SummaryID, Summary]           `json:"summaries"`
	EntryOrder  []SurfaceEntry                      `json:"entryOrder"`
	Superseded  Table[ToolResultID, ToolResultID]   `json:"superseded,omitzero"`
	Checkpoints Table[CheckpointID, CheckpointView] `json:"checkpoints,omitzero"`
	// nextPos assigns the next entry or input position; it is not persisted
	// and is reconstructed from the state when a snapshot is restored.
	nextPos uint64
}

// SubmittedInputs returns inputs still awaiting delivery, in submission order.
func (s *Surface) SubmittedInputs() []Input {
	var out []InputView
	s.Inputs.Range(func(_ InputID, v InputView) bool {
		if v.Status == InputSubmitted {
			out = append(out, v)
		}
		return true
	})
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
	TypeAssistant, TypeToolResult, TypeToolResultSuperseded, TypeSummary, TypeCheckpointCreated, TypeCheckpointInvalidated}

var SurfaceProjection = extension.ProjectionDefinition{
	ID: SurfaceProjectionID, Version: 1,
	Consumes: chatlogConsumes,
	Initial: func() (any, error) {
		return Surface{}, nil
	},
	Apply:      applySurface,
	StateCodec: extension.JSONStateCodec[Surface]{},
}

// applySurface is copy-on-write: the Surface value is copied, every table is
// shared with the previous state and Set returns a new one, and EntryOrder
// grows by append. Apply stays pure -- the previous state is never written.
// Positions come from nextPos: wire events carry no Seq, so the projection
// numbers its own entries; only monotonic order is required.
func applySurface(state any, e extension.DecodedEvent) (any, error) {
	s := state.(Surface)
	if s.nextPos == 0 {
		// Restored from a snapshot: the counter is not persisted, so continue
		// from the largest position already assigned.
		s.Inputs.Range(func(_ InputID, v InputView) bool {
			if v.Seq > s.nextPos {
				s.nextPos = v.Seq
			}
			return true
		})
		for _, en := range s.EntryOrder {
			if en.Seq > s.nextPos {
				s.nextPos = en.Seq
			}
		}
		s.Checkpoints.Range(func(_ CheckpointID, v CheckpointView) bool {
			if v.Seq > s.nextPos {
				s.nextPos = v.Seq
			}
			return true
		})
	}
	switch p := e.Value.(type) {
	case InputSubmittedPayload:
		if s.Inputs.Has(p.InputID) {
			return nil, fmt.Errorf("input %s submitted twice", p.InputID)
		}
		d, err := DigestInput(p.InputID, p.Content)
		if err != nil {
			return nil, err
		}
		s.nextPos++
		s.Inputs = s.Inputs.Set(p.InputID, InputView{Input: Input{ID: p.InputID, Content: p.Content, Digest: d}, Status: InputSubmitted, Seq: s.nextPos})
	case InputDeliveredPayload:
		v, ok := s.Inputs.Get(p.InputID)
		if !ok || v.Status != InputSubmitted {
			return nil, fmt.Errorf("input %s delivered while %s", p.InputID, v.Status)
		}
		v.Status = InputDelivered
		v.Input.TurnID = p.TurnID
		s.Inputs = s.Inputs.Set(p.InputID, v)
		s.nextPos++
		s.EntryOrder = append(s.EntryOrder, SurfaceEntry{Kind: EntryInput, ID: string(p.InputID), Seq: s.nextPos})
	case InputWithdrawnPayload:
		if err := terminateInput(&s, p.InputID, InputWithdrawn); err != nil {
			return nil, err
		}
	case InputRejectedPayload:
		if err := terminateInput(&s, p.InputID, InputRejected); err != nil {
			return nil, err
		}
	case AssistantPayload:
		if s.Assistants.Has(p.Assistant.ID) {
			return nil, fmt.Errorf("assistant %s created twice", p.Assistant.ID)
		}
		s.Assistants = s.Assistants.Set(p.Assistant.ID, p.Assistant)
		s.nextPos++
		s.EntryOrder = append(s.EntryOrder, SurfaceEntry{Kind: EntryAssistant, ID: string(p.Assistant.ID), Seq: s.nextPos})
	case ToolResultPayload:
		if s.ToolResults.Has(p.ToolResult.ID) {
			return nil, fmt.Errorf("tool_result %s created twice", p.ToolResult.ID)
		}
		s.ToolResults = s.ToolResults.Set(p.ToolResult.ID, p.ToolResult)
		s.nextPos++
		s.EntryOrder = append(s.EntryOrder, SurfaceEntry{Kind: EntryToolResult, ID: string(p.ToolResult.ID), Seq: s.nextPos})
	case ToolResultSupersededPayload:
		if !s.ToolResults.Has(p.ToolResultID) {
			return nil, fmt.Errorf("superseded tool_result %s unknown", p.ToolResultID)
		}
		if s.Superseded.Has(p.ToolResultID) {
			return nil, fmt.Errorf("tool_result %s superseded twice", p.ToolResultID)
		}
		s.Superseded = s.Superseded.Set(p.ToolResultID, p.ReplacementToolResultID)
	case SummaryPayload:
		if s.Summaries.Has(p.Summary.ID) {
			return nil, fmt.Errorf("summary %s created twice", p.Summary.ID)
		}
		s.Summaries = s.Summaries.Set(p.Summary.ID, p.Summary)
		s.nextPos++
		s.EntryOrder = append(s.EntryOrder, SurfaceEntry{Kind: EntrySummary, ID: string(p.Summary.ID), Seq: s.nextPos})
	case CheckpointCreatedPayload:
		if s.Checkpoints.Has(p.CheckpointID) {
			return nil, fmt.Errorf("checkpoint %s created twice", p.CheckpointID)
		}
		sum, ok := s.Summaries.Get(p.SummaryID)
		if !ok || sum.Digest != p.SummaryDigest {
			return nil, fmt.Errorf("checkpoint %s names summary %s which does not match", p.CheckpointID, p.SummaryID)
		}
		s.nextPos++
		s.Checkpoints = s.Checkpoints.Set(p.CheckpointID, CheckpointView{Checkpoint: p, Status: CheckpointActive, Seq: s.nextPos})
	case CheckpointInvalidatedPayload:
		v, ok := s.Checkpoints.Get(p.CheckpointID)
		if !ok || v.Status != CheckpointActive {
			return nil, fmt.Errorf("checkpoint %s invalidated while not active", p.CheckpointID)
		}
		v.Status = CheckpointInvalidated
		v.Reason = p.Reason
		s.Checkpoints = s.Checkpoints.Set(p.CheckpointID, v)
	default:
		return nil, fmt.Errorf("chatlog surface: unexpected %T", e.Value)
	}
	return s, nil
}

func terminateInput(s *Surface, id InputID, status InputStatus) error {
	v, ok := s.Inputs.Get(id)
	if !ok || v.Status != InputSubmitted {
		return fmt.Errorf("input %s %s while %s", id, status, v.Status)
	}
	v.Status = status
	s.Inputs = s.Inputs.Set(id, v)
	return nil
}

// cow returns a fresh copy of m for the one write that follows, so the
// previous state keeps its map untouched. Reads never copy.
func cow[K comparable, V any](m map[K]V) map[K]V {
	out := make(map[K]V, len(m)+1)
	for k, v := range m {
		out[k] = v
	}
	return out
}

// clip returns s with its capacity cut to its length, so an append by a later
// state cannot overwrite an element a holder of this state can still see.
// Plain appends never need it: a previous state only reads within its own
// length, so growth past it is invisible to it.
func clip[T any](s []T) []T { return s[:len(s):len(s)] }

// --- context ------------------------------------------------------------------

// Entry is one element of the model-facing conversation (CHT-CTX-1). Seq is
// the projection-internal position of the entry; checkpoints split base from
// gap by it (CHT-EVT-3).
type Entry struct {
	Kind       EntryKind   `json:"kind"`
	ID         string      `json:"id"`
	Digest     es.Digest   `json:"digest"`
	Seq        uint64      `json:"seq"`
	Input      *Input      `json:"input,omitempty"`
	Assistant  *Assistant  `json:"assistant,omitempty"`
	ToolResult *ToolResult `json:"toolResult,omitempty"`
	Summary    *Summary    `json:"summary,omitempty"`
}

// Pair names the entry for checkpoint base and retained sets.
func (e *Entry) Pair() EntryDigestPair {
	return EntryDigestPair{Kind: e.Kind, ID: e.ID, Digest: e.Digest}
}

// AppliedCheckpoint archives what a checkpoint replaced so an explicit
// invalidation restores it (CHT-EVT-3). Base excludes the checkpoint's own
// summary entry: invalidation drops the summary from the active context.
type AppliedCheckpoint struct {
	ID CheckpointID `json:"id"`
	// Base is the active context the checkpoint covered, in order.
	Base []Entry `json:"base"`
	// PrefixLen is what the checkpoint contributed to Entries: the summary
	// plus the retained entries.
	PrefixLen int `json:"prefixLen"`
}

// Context is the projection state: the ordered entries plus the bookkeeping
// ContextFold needs (submitted inputs awaiting delivery, superseded results,
// applied checkpoints).
type Context struct {
	Entries     []Entry                       `json:"entries"`
	Pending     map[InputID]Input             `json:"pending,omitempty"`
	Superseded  map[ToolResultID]ToolResultID `json:"superseded,omitempty"`
	Checkpoints []AppliedCheckpoint           `json:"checkpoints,omitempty"`
	// nextPos assigns entry positions like Surface.nextPos; it is not
	// persisted and is reconstructed on restore, including from the entries
	// archived in checkpoint bases.
	nextPos uint64
}

var ContextProjection = extension.ProjectionDefinition{
	ID: ContextProjectionID, Version: 1,
	Consumes: chatlogConsumes,
	Initial: func() (any, error) {
		return Context{Pending: map[InputID]Input{}, Superseded: map[ToolResultID]ToolResultID{}}, nil
	},
	Apply:      applyContext,
	StateCodec: extension.JSONStateCodec[Context]{},
}

// applyContext is copy-on-write like applySurface: Entries and Checkpoints
// grow by append, a shrunk slice is clipped so a later append cannot reach an
// element the previous state still holds, and a map is copied only by the
// event that writes it. Entry positions come from nextPos, not the wire.
func applyContext(state any, e extension.DecodedEvent) (any, error) {
	c := state.(Context)
	if c.nextPos == 0 {
		// Restored from a snapshot: continue from the largest entry position,
		// including entries archived in checkpoint bases.
		for _, en := range c.Entries {
			if en.Seq > c.nextPos {
				c.nextPos = en.Seq
			}
		}
		for _, ap := range c.Checkpoints {
			for _, en := range ap.Base {
				if en.Seq > c.nextPos {
					c.nextPos = en.Seq
				}
			}
		}
	}
	switch p := e.Value.(type) {
	case InputSubmittedPayload:
		d, err := DigestInput(p.InputID, p.Content)
		if err != nil {
			return nil, err
		}
		c.Pending = cow(c.Pending)
		c.Pending[p.InputID] = Input{ID: p.InputID, Content: p.Content, Digest: d}
	case InputDeliveredPayload:
		in, ok := c.Pending[p.InputID]
		if !ok {
			return nil, fmt.Errorf("input %s delivered before submission", p.InputID)
		}
		c.Pending = cow(c.Pending)
		delete(c.Pending, p.InputID)
		in.TurnID = p.TurnID
		c.nextPos++
		c.Entries = append(c.Entries, Entry{Kind: EntryInput, ID: string(in.ID), Digest: in.Digest, Seq: c.nextPos, Input: &in})
	case InputWithdrawnPayload:
		c.Pending = cow(c.Pending)
		delete(c.Pending, p.InputID)
	case InputRejectedPayload:
		c.Pending = cow(c.Pending)
		delete(c.Pending, p.InputID)
	case AssistantPayload:
		a := p.Assistant
		c.nextPos++
		c.Entries = append(c.Entries, Entry{Kind: EntryAssistant, ID: string(a.ID), Digest: a.Digest, Seq: c.nextPos, Assistant: &a})
	case ToolResultPayload:
		r := p.ToolResult
		c.nextPos++
		c.Entries = append(c.Entries, Entry{Kind: EntryToolResult, ID: string(r.ID), Digest: r.Digest, Seq: c.nextPos, ToolResult: &r})
	case ToolResultSupersededPayload:
		c.Superseded = cow(c.Superseded)
		c.Superseded[p.ToolResultID] = p.ReplacementToolResultID
		kept := c.Entries[:0:0]
		found := false
		for _, en := range c.Entries {
			if en.Kind == EntryToolResult && en.ID == string(p.ToolResultID) {
				found = true
				continue
			}
			kept = append(kept, en)
		}
		if !found {
			// A result outside the active context was either never created or
			// compacted; its Turn completed, so superseding it violates
			// CHT-ENT-2 rather than invalidating the checkpoint.
			return nil, fmt.Errorf("tool_result %s superseded outside the active context", p.ToolResultID)
		}
		c.Entries = kept
	case SummaryPayload:
		s := p.Summary
		c.nextPos++
		c.Entries = append(c.Entries, Entry{Kind: EntrySummary, ID: string(s.ID), Digest: s.Digest, Seq: c.nextPos, Summary: &s})
	case CheckpointCreatedPayload:
		c.nextPos++
		return applyCheckpoint(c, &p, c.nextPos)
	case CheckpointInvalidatedPayload:
		n := len(c.Checkpoints)
		if n == 0 || c.Checkpoints[n-1].ID != p.CheckpointID {
			return nil, fmt.Errorf("checkpoint %s is not the latest active checkpoint", p.CheckpointID)
		}
		top := c.Checkpoints[n-1]
		if len(c.Entries) < top.PrefixLen {
			return nil, fmt.Errorf("checkpoint %s prefix exceeds the context", p.CheckpointID)
		}
		c.Entries = append(append([]Entry(nil), top.Base...), c.Entries[top.PrefixLen:]...)
		c.Checkpoints = clip(c.Checkpoints[:n-1])
	default:
		return nil, fmt.Errorf("chatlog context: unexpected %T", e.Value)
	}
	return c, nil
}

// applyCheckpoint validates and applies one checkpoint_created (CHT-EVT-3).
// pos is the checkpoint's own position; CoveredThrough names an entry
// position, so it must precede the checkpoint event.
func applyCheckpoint(c Context, p *CheckpointCreatedPayload, pos uint64) (any, error) {
	if p.CoveredThrough >= pos {
		return nil, fmt.Errorf("checkpoint %s covers through %d at position %d", p.CheckpointID, p.CoveredThrough, pos)
	}
	for _, ap := range c.Checkpoints {
		if ap.ID == p.CheckpointID {
			return nil, fmt.Errorf("checkpoint %s created twice", p.CheckpointID)
		}
	}
	cut := len(c.Entries)
	for cut > 0 && c.Entries[cut-1].Seq > p.CoveredThrough {
		cut--
	}
	base, gap := c.Entries[:cut], c.Entries[cut:]
	if len(gap) != 1 || gap[0].Kind != EntrySummary || gap[0].ID != string(p.SummaryID) || gap[0].Digest != p.SummaryDigest {
		return nil, fmt.Errorf("checkpoint %s: the entries after coveredThrough must be exactly its summary", p.CheckpointID)
	}
	pairs := make([]EntryDigestPair, len(base))
	for i := range base {
		pairs[i] = base[i].Pair()
	}
	wantBase, err := DigestBaseContext(pairs)
	if err != nil {
		return nil, err
	}
	if wantBase != p.BaseContextDigest {
		return nil, fmt.Errorf("checkpoint %s: base context digest mismatch", p.CheckpointID)
	}
	retained, err := selectRetained(base, p.Retained)
	if err != nil {
		return nil, fmt.Errorf("checkpoint %s: %w", p.CheckpointID, err)
	}
	// Base is clipped: it shares the covered prefix's storage, and no later
	// state may append into it.
	c.Checkpoints = append(c.Checkpoints, AppliedCheckpoint{ID: p.CheckpointID, Base: clip(base), PrefixLen: 1 + len(retained)})
	c.Entries = append([]Entry{gap[0]}, retained...)
	return c, nil
}

// selectRetained resolves the retained pairs as an ordered subset of base.
func selectRetained(base []Entry, pairs []EntryDigestPair) ([]Entry, error) {
	out := make([]Entry, 0, len(pairs))
	i := 0
	for _, p := range pairs {
		for i < len(base) && base[i].Pair() != p {
			i++
		}
		if i == len(base) {
			return nil, fmt.Errorf("retained %s %s is not in the base context in order", p.Kind, p.ID)
		}
		out = append(out, base[i])
		i++
	}
	return out, nil
}

// ContextFold folds decoded chatlog events into entries (CHT-CTX-1).
func ContextFold(events []extension.DecodedEvent) ([]Entry, error) {
	state, _ := ContextProjection.Initial()
	for _, e := range events {
		if e.Module != extension.TwilightModule(ModuleID) || e.Unknown {
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
