package runmod

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
)

const MachineProjectionID extension.ProjectionID = "twilight/run/machine"

// Machine is the twilight/run/machine projection state (RUN-CMT-2): every
// non-terminal Run of the Session with its last event position and schema.
// Terminal Runs leave the projection; Record and the turn surface keep their
// results. Ended keeps only the RunIDs of terminated Runs so a second
// run_created for a used RunID is refused (RUN-NEW-1) without keeping state.
type Machine struct {
	Active    map[run.RunID]run.MachineState
	Positions map[run.RunID]run.RunPosition
	Schemas   map[run.RunID]uint16
	Ended     map[run.RunID]struct{}
}

func newMachine() Machine {
	return Machine{Active: map[run.RunID]run.MachineState{}, Positions: map[run.RunID]run.RunPosition{}, Schemas: map[run.RunID]uint16{}, Ended: map[run.RunID]struct{}{}}
}

func (m Machine) clone() Machine {
	out := newMachine()
	for k, v := range m.Active {
		out.Active[k] = v
	}
	for k, v := range m.Positions {
		out.Positions[k] = v
	}
	for k, v := range m.Schemas {
		out.Schemas[k] = v
	}
	for k := range m.Ended {
		out.Ended[k] = struct{}{}
	}
	return out
}

// Apply folds one decoded run event (RUN-MCH-3 via Protocol.Evolve).
func (m Machine) Apply(e extension.DecodedEvent) (Machine, error) {
	ev, ok := e.Value.(Event)
	if !ok {
		return m, fmt.Errorf("run machine: unexpected %T", e.Value)
	}
	out := m.clone()
	var proto run.Protocol
	var state run.MachineState
	if created, isCreated := ev.Fact.(run.RunCreated); isCreated {
		if _, dup := out.Active[ev.RunID]; dup {
			return m, fmt.Errorf("run machine: %s created twice", ev.RunID)
		}
		if _, ended := out.Ended[ev.RunID]; ended {
			return m, fmt.Errorf("run machine: %s created again after it ended", ev.RunID)
		}
		p, err := run.ProtocolFor(created.SchemaVersion)
		if err != nil {
			return m, err
		}
		proto = p
		out.Schemas[ev.RunID] = created.SchemaVersion
	} else {
		cur, active := out.Active[ev.RunID]
		if !active {
			return m, fmt.Errorf("run machine: fact %s for unknown or terminal run %s", run.FactType(ev.Fact), ev.RunID)
		}
		p, err := run.ProtocolFor(out.Schemas[ev.RunID])
		if err != nil {
			return m, err
		}
		proto, state = p, cur
	}
	next, err := proto.Evolve(state, ev.Fact)
	if err != nil {
		return m, err
	}
	if next.Status.Terminal() {
		delete(out.Active, ev.RunID)
		delete(out.Positions, ev.RunID)
		delete(out.Schemas, ev.RunID)
		out.Ended[ev.RunID] = struct{}{}
		return out, nil
	}
	out.Active[ev.RunID] = next
	out.Positions[ev.RunID] = e.Event.Seq
	return out, nil
}

type machineWire struct {
	Runs  map[run.RunID]machineRunWire `json:"runs"`
	Ended []run.RunID                  `json:"ended,omitempty"`
}

type machineRunWire struct {
	Schema   uint16           `json:"schema"`
	Position run.RunPosition  `json:"position"`
	State    jsonstable.Value `json:"state"`
}

type machineCodec struct{}

func (machineCodec) Validate(value any) error {
	if _, ok := value.(Machine); !ok {
		return fmt.Errorf("state is %T, want runmod.Machine", value)
	}
	return nil
}

func (c machineCodec) Encode(value any) (jsonstable.Value, error) {
	if err := c.Validate(value); err != nil {
		return jsonstable.Value{}, err
	}
	m := value.(Machine)
	wire := machineWire{Runs: make(map[run.RunID]machineRunWire, len(m.Active))}
	for id, state := range m.Active {
		proto, err := run.ProtocolFor(m.Schemas[id])
		if err != nil {
			return jsonstable.Value{}, err
		}
		raw, err := proto.EncodeMachineState(&state)
		if err != nil {
			return jsonstable.Value{}, err
		}
		encoded, err := jsonstable.Parse(raw)
		if err != nil {
			return jsonstable.Value{}, err
		}
		wire.Runs[id] = machineRunWire{Schema: m.Schemas[id], Position: m.Positions[id], State: encoded}
	}
	for id := range m.Ended {
		wire.Ended = append(wire.Ended, id)
	}
	sort.Slice(wire.Ended, func(i, j int) bool { return wire.Ended[i] < wire.Ended[j] })
	return jsonstable.FromValue(wire)
}

func (machineCodec) Decode(wire jsonstable.Value) (any, error) {
	if wire.IsZero() {
		return nil, errors.New("empty machine snapshot")
	}
	var w machineWire
	if err := json.Unmarshal(wire.Bytes(), &w); err != nil {
		return nil, err
	}
	m := newMachine()
	for id, r := range w.Runs {
		proto, err := run.ProtocolFor(r.Schema)
		if err != nil {
			return nil, err
		}
		state, err := proto.DecodeMachineState(r.State.Bytes())
		if err != nil {
			return nil, err
		}
		m.Active[id] = state
		m.Positions[id] = r.Position
		m.Schemas[id] = r.Schema
	}
	for _, id := range w.Ended {
		m.Ended[id] = struct{}{}
	}
	return m, nil
}

// MachineProjection consumes every twilight/run/ event.
var MachineProjection = extension.ProjectionDefinition{
	ID: MachineProjectionID, Version: 1,
	Consumes: AllTypes(),
	Initial:  func() (any, error) { return newMachine(), nil },
	Apply: func(state any, e extension.DecodedEvent) (any, error) {
		return state.(Machine).Apply(e)
	},
	StateCodec: machineCodec{},
}

var _ session.EventType = Prefix
