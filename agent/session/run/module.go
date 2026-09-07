// Package runmod is the first-party Run Session Module (agent-run.md 5): the
// twilight/run/ EventDefinitions, the twilight/run/machine projection and the
// run.Runtime implementation over the Session Module Framework.
package runmod

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/memohai/twilight/agent/es"
	"github.com/memohai/twilight/agent/jsonstable"
	"github.com/memohai/twilight/agent/run"
	"github.com/memohai/twilight/agent/session"
	"github.com/memohai/twilight/agent/session/extension"
)

const (
	ModuleID extension.ModuleID = "run"
	// Prefix is the EventType namespace of every Run fact.
	Prefix session.EventType = "twilight/run/"
	// LeaseNamespace holds execution occupancy (RUN 5.1).
	LeaseNamespace session.ControlNamespace = "twilight/run/lease"
)

// factNames is the closed list of v1 fact discriminators.
var factNames = []string{
	"run_created", "model_step_prepared", "model_step_withdrawn", "model_step_started", "model_step_recovered",
	"model_step_rejected", "model_step_completed", "tool_step_opened", "tool_call_started", "tool_call_approved",
	"tool_call_completed", "tool_call_answered", "tool_call_failed", "input_accepted", "run_ended",
}

// EventType returns the EventType of a fact.
func EventType(f run.Fact) session.EventType { return Prefix + session.EventType(run.FactType(f)) }

// Event is the typed value of one twilight/run/ event: the fact plus the
// RunID that every payload carries at its first level (RUN-WIR-2).
type Event struct {
	RunID run.RunID
	Fact  run.Fact
}

// factCodec encodes one fact type for one SchemaVersion. The payload is the
// canonical fact object with "runId" added; `v` is the Registry's.
type factCodec struct {
	local string
	proto run.Protocol
}

func (c factCodec) Validate(value any) error {
	ev, ok := value.(Event)
	if !ok {
		return fmt.Errorf("value is %T, want runmod.Event", value)
	}
	if ev.RunID == "" || ev.Fact == nil {
		return errors.New("event requires runId and fact")
	}
	if run.FactType(ev.Fact) != c.local {
		return fmt.Errorf("fact is %s, codec is %s", run.FactType(ev.Fact), c.local)
	}
	return nil
}

func (c factCodec) Encode(value any) (jsonstable.Value, error) {
	if err := c.Validate(value); err != nil {
		return jsonstable.Value{}, err
	}
	ev := value.(Event)
	raw, err := es.MarshalCanonical(ev.Fact)
	if err != nil {
		return jsonstable.Value{}, err
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return jsonstable.Value{}, err
	}
	if m == nil {
		m = map[string]json.RawMessage{}
	}
	if existing, has := m["runId"]; has {
		// RunCreated already carries runId; it must agree.
		var id run.RunID
		if err := json.Unmarshal(existing, &id); err != nil || id != ev.RunID {
			return jsonstable.Value{}, errors.New("fact runId disagrees with event runId")
		}
	}
	m["runId"] = json.RawMessage(fmt.Sprintf("%q", string(ev.RunID)))
	return jsonstable.FromValue(m)
}

func (c factCodec) Decode(wire jsonstable.Value) (any, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(wire.Bytes(), &m); err != nil {
		return nil, err
	}
	rawID, ok := m["runId"]
	if !ok {
		return nil, errors.New("run event has no runId")
	}
	var id run.RunID
	if err := json.Unmarshal(rawID, &id); err != nil || id == "" {
		return nil, errors.New("run event runId is not a string")
	}
	if c.local != "run_created" {
		delete(m, "runId")
	}
	body, err := jsonstable.FromValue(m)
	if err != nil {
		return nil, err
	}
	fact, err := c.proto.DecodeFact(c.local, body.Bytes())
	if err != nil {
		return nil, err
	}
	if created, ok := fact.(run.RunCreated); ok && created.RunID != id {
		return nil, errors.New("run_created runId disagrees with payload runId")
	}
	return Event{RunID: id, Fact: fact}, nil
}

// Module is the run ModuleDescriptor (RUN-SCP-2: no Requires; Companion is a
// constructor parameter, not a module dependency).
var Module = buildModule()

func buildModule() extension.ModuleDescriptor {
	m := extension.ModuleDescriptor{ID: ModuleID, Projections: []extension.ProjectionDefinition{MachineProjection}}
	for _, name := range factNames {
		m.Events = append(m.Events, extension.EventDefinition{
			Type:    Prefix + session.EventType(name),
			Current: extension.PayloadVersion(run.SchemaVersion1),
			Codecs: map[extension.PayloadVersion]extension.PayloadCodec{
				extension.PayloadVersion(run.SchemaVersion1): factCodec{local: name, proto: run.ProtocolV1()},
			},
		})
	}
	return m
}

// AllTypes lists every registered twilight/run/ EventType.
func AllTypes() []session.EventType {
	out := make([]session.EventType, len(factNames))
	for i, name := range factNames {
		out[i] = Prefix + session.EventType(name)
	}
	return out
}
