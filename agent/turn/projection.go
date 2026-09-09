package turn

import (
	"fmt"

	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/chatlog"
	"github.com/felinics/twilight/agent/session/extension"
	runmod "github.com/felinics/twilight/agent/session/run"
)

const SurfaceProjectionID extension.ProjectionID = "twilight/turn/surface"

type TurnStatus string

const (
	TurnActive        TurnStatus = "active"
	TurnAttemptFailed TurnStatus = "attempt_failed"
	TurnCompleted     TurnStatus = "completed"
	TurnFailed        TurnStatus = "failed"
	TurnStopped       TurnStatus = "stopped"
	TurnSuperseded    TurnStatus = "superseded"
)

type AttemptView struct {
	RunID   run.RunID `json:"runId"`
	Attempt uint32    `json:"attempt"`
	// SchemaVersion is created.SchemaVersion: the Coordinator builds command
	// envelopes for this attempt from it without reading the machine projection.
	SchemaVersion uint16 `json:"schemaVersion"`
	// End is the terminal result from twilight/run/ended; nil while active.
	End *run.RunEnded `json:"end,omitempty"`
}

// Ended returns the RunEnd variant, or nil for a non-terminal attempt.
func (a *AttemptView) Ended() *run.RunEnd {
	if a == nil || a.End == nil {
		return nil
	}
	end := a.End.End
	return &end
}

type TurnView struct {
	TurnID            TurnID            `json:"turnId"`
	Status            TurnStatus        `json:"status"`
	InputIDs          []chatlog.InputID `json:"inputIds,omitempty"`
	Profile           ProfileRef        `json:"profile"`
	Companion         CompanionVersion  `json:"companion"`
	Attempts          []AttemptView     `json:"attempts,omitempty"`
	ActiveRun         run.RunID         `json:"activeRun,omitempty"`
	ReplacementTurnID TurnID            `json:"replacementTurnId,omitempty"`
}

// LastAttempt returns the most recent attempt, if any.
func (v *TurnView) LastAttempt() *AttemptView {
	if len(v.Attempts) == 0 {
		return nil
	}
	return &v.Attempts[len(v.Attempts)-1]
}

// ActiveAttempt returns the attempt behind ActiveRun.
func (v *TurnView) ActiveAttempt() *AttemptView {
	for i := range v.Attempts {
		if v.Attempts[i].RunID == v.ActiveRun && v.ActiveRun != "" {
			return &v.Attempts[i]
		}
	}
	return nil
}

type TurnSurface struct {
	Order []TurnID            `json:"order"`
	Turns map[TurnID]TurnView `json:"turns"`
	// runOwner maps a RunID to its Turn for run event routing.
	RunOwner map[run.RunID]TurnID `json:"runOwner"`
}

func (s TurnSurface) clone() TurnSurface {
	out := TurnSurface{Order: append([]TurnID(nil), s.Order...), Turns: make(map[TurnID]TurnView, len(s.Turns)), RunOwner: make(map[run.RunID]TurnID, len(s.RunOwner))}
	for k, v := range s.Turns {
		v.InputIDs = append([]chatlog.InputID(nil), v.InputIDs...)
		v.Attempts = append([]AttemptView(nil), v.Attempts...)
		out.Turns[k] = v
	}
	for k, v := range s.RunOwner {
		out.RunOwner[k] = v
	}
	return out
}

// Active returns the single active Turn of the Session, if any (TRN-SCP-2).
func (s *TurnSurface) Active() (TurnView, bool) {
	for _, id := range s.Order {
		if v := s.Turns[id]; v.Status == TurnActive {
			return v, true
		}
	}
	return TurnView{}, false
}

var SurfaceProjection = extension.ProjectionDefinition{
	ID: SurfaceProjectionID, Version: 1,
	Consumes: []session.EventType{TypeStarted, TypeCompleted, TypeFailed, TypeSuperseded,
		runmod.Prefix + "run_created", runmod.Prefix + "input_accepted", runmod.Prefix + "run_ended"},
	Initial: func() (any, error) {
		return TurnSurface{Turns: map[TurnID]TurnView{}, RunOwner: map[run.RunID]TurnID{}}, nil
	},
	Apply:      applySurface,
	StateCodec: extension.JSONStateCodec[TurnSurface]{},
}

func applySurface(state any, e extension.DecodedEvent) (any, error) {
	s := state.(TurnSurface).clone()
	switch p := e.Value.(type) {
	case StartedPayload:
		if _, dup := s.Turns[p.TurnID]; dup {
			return nil, fmt.Errorf("turn %s started twice", p.TurnID)
		}
		s.Order = append(s.Order, p.TurnID)
		s.Turns[p.TurnID] = TurnView{TurnID: p.TurnID, Status: TurnActive, InputIDs: append([]chatlog.InputID(nil), p.InputIDs...),
			Profile: p.Profile, Companion: p.Companion}
	case CompletedPayload:
		v, err := s.settling(p.TurnID)
		if err != nil {
			return nil, err
		}
		v.Status, v.ActiveRun = TurnCompleted, ""
		s.Turns[p.TurnID] = v
	case FailedPayload:
		v, err := s.settling(p.TurnID)
		if err != nil {
			return nil, err
		}
		v.ActiveRun = ""
		if p.Settlement == SettlementStopped {
			v.Status = TurnStopped
		} else {
			v.Status = TurnFailed
		}
		s.Turns[p.TurnID] = v
	case SupersededPayload:
		v, err := s.settling(p.TurnID)
		if err != nil {
			return nil, err
		}
		v.Status, v.ActiveRun, v.ReplacementTurnID = TurnSuperseded, "", p.ReplacementTurnID
		s.Turns[p.TurnID] = v
	case runmod.Event:
		return s.applyRun(p)
	default:
		return nil, fmt.Errorf("turn surface: unexpected %T", e.Value)
	}
	return s, nil
}

func (s *TurnSurface) settling(id TurnID) (TurnView, error) {
	v, ok := s.Turns[id]
	if !ok {
		return TurnView{}, fmt.Errorf("turn %s settled before started", id)
	}
	if v.Status != TurnActive && v.Status != TurnAttemptFailed {
		return TurnView{}, fmt.Errorf("turn %s settled twice", id)
	}
	return v, nil
}

func (s TurnSurface) applyRun(ev runmod.Event) (any, error) {
	switch f := ev.Fact.(type) {
	case run.RunCreated:
		turnID := TurnID(f.Owner)
		v, ok := s.Turns[turnID]
		if !ok {
			// A Run whose owner is not a Turn of this Session is not ours.
			return s, nil
		}
		if v.ActiveRun != "" {
			return nil, fmt.Errorf("turn %s already has active run %s", turnID, v.ActiveRun)
		}
		v.Attempts = append(v.Attempts, AttemptView{RunID: ev.RunID, Attempt: f.Attempt, SchemaVersion: f.SchemaVersion})
		v.ActiveRun = ev.RunID
		v.Status = TurnActive
		s.Turns[turnID] = v
		s.RunOwner[ev.RunID] = turnID
	case run.InputAccepted:
		turnID, ok := s.RunOwner[ev.RunID]
		if !ok {
			return s, nil
		}
		v := s.Turns[turnID]
		id := chatlog.InputID(f.Input.ID)
		for _, have := range v.InputIDs {
			if have == id {
				return s, nil
			}
		}
		v.InputIDs = append(v.InputIDs, id)
		s.Turns[turnID] = v
	case run.RunEnded:
		turnID, ok := s.RunOwner[ev.RunID]
		if !ok {
			return s, nil
		}
		v := s.Turns[turnID]
		for i := range v.Attempts {
			if v.Attempts[i].RunID == ev.RunID {
				v.Attempts[i].End = &run.RunEnded{End: f.End}
			}
		}
		v.ActiveRun = ""
		if v.Status == TurnActive {
			// completed runs are settled by the companion's turn/completed in
			// the same commit; anything else waits for Retry or Settle.
			v.Status = TurnAttemptFailed
		}
		s.Turns[turnID] = v
	}
	return s, nil
}
