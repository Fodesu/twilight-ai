package runmod

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
)

const MachineProjectionID extension.ProjectionID = "twilight/run/machine"

// Machine is the twilight/run/machine projection state (RUN-CMT-2): every
// non-terminal Run of the Session with its last event position and schema.
// Terminal Runs leave the projection entirely; Record and the turn surface
// keep their results, and a second creation of a used RunID is refused at
// commit time from the ledger's stream index (CreateRun), so the projection
// is bounded by the Runs active now.
type Machine struct {
	Active    map[run.RunID]run.MachineState
	Positions map[run.RunID]run.RunPosition
	Schemas   map[run.RunID]uint16
}

func newMachine() Machine {
	return Machine{Active: map[run.RunID]run.MachineState{}, Positions: map[run.RunID]run.RunPosition{}, Schemas: map[run.RunID]uint16{}}
}

func (m Machine) clone() Machine {
	out := newMachine()
	for k := range m.Active {
		out.Active[k] = m.Active[k]
	}
	for k, v := range m.Positions {
		out.Positions[k] = v
	}
	for k, v := range m.Schemas {
		out.Schemas[k] = v
	}
	return out
}

// snapshot returns the RuntimeSnapshot of an active Run.
func (m Machine) snapshot(runID run.RunID) (run.RuntimeSnapshot, bool) {
	ms, ok := m.Active[runID]
	if !ok {
		return run.RuntimeSnapshot{}, false
	}
	return run.RuntimeSnapshot{State: ms, Position: m.Positions[runID], SchemaVersion: m.Schemas[runID]}, true
}

// Apply folds one decoded run event (RUN-MCH-3 via Protocol.Evolve).
func (m Machine) Apply(e extension.DecodedEvent) (Machine, error) { //nolint:gocritic // hugeParam: DecodedEvent is the extension Apply shape
	ev, ok := e.Value.(Event)
	if !ok {
		return m, fmt.Errorf("run machine: unexpected %T", e.Value)
	}
	if want := (session.StreamRef{Kind: session.StreamKindRun, ID: string(ev.RunID)}); e.Stream != want {
		return m, fmt.Errorf("run machine: fact for %s arrived via stream %s/%s", ev.RunID, e.Stream.Kind, e.Stream.ID)
	}
	out := m.clone()
	var schema run.Schema
	var state run.MachineState
	if created, isCreated := ev.Fact.(run.RunCreated); isCreated {
		if _, dup := out.Active[ev.RunID]; dup {
			return m, fmt.Errorf("run machine: %s created twice", ev.RunID)
		}
		p, err := run.SchemaFor(created.SchemaVersion)
		if err != nil {
			return m, err
		}
		schema = p
		out.Schemas[ev.RunID] = created.SchemaVersion
	} else {
		cur, active := out.Active[ev.RunID]
		if !active {
			return m, fmt.Errorf("run machine: fact %s for unknown or terminal run %s", run.FactType(ev.Fact), ev.RunID)
		}
		p, err := run.SchemaFor(out.Schemas[ev.RunID])
		if err != nil {
			return m, err
		}
		schema, state = p, cur
	}
	next, err := schema.Machine.Evolve(state, ev.Fact)
	if err != nil {
		return m, err
	}
	if next.Status.Terminal() {
		delete(out.Active, ev.RunID)
		delete(out.Positions, ev.RunID)
		delete(out.Schemas, ev.RunID)
		return out, nil
	}
	if _, tracked := out.Positions[ev.RunID]; tracked {
		out.Positions[ev.RunID]++
	} else {
		out.Positions[ev.RunID] = 0
	}
	out.Active[ev.RunID] = next
	return out, nil
}

type machineWire struct {
	Runs map[run.RunID]machineRunWire `json:"runs"`
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
	m, _ := value.(Machine) // Validate checked the type
	wire := machineWire{Runs: make(map[run.RunID]machineRunWire, len(m.Active))}
	for id := range m.Active {
		state := m.Active[id]
		schema, err := run.SchemaFor(m.Schemas[id])
		if err != nil {
			return jsonstable.Value{}, err
		}
		raw, err := schema.Snapshot.Encode(&state)
		if err != nil {
			return jsonstable.Value{}, err
		}
		encoded, err := jsonstable.Parse(raw)
		if err != nil {
			return jsonstable.Value{}, err
		}
		wire.Runs[id] = machineRunWire{Schema: m.Schemas[id], Position: m.Positions[id], State: encoded}
	}
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
		schema, err := run.SchemaFor(r.Schema)
		if err != nil {
			return nil, err
		}
		state, err := schema.Snapshot.Decode(r.State.Bytes())
		if err != nil {
			return nil, err
		}
		m.Active[id] = state
		m.Positions[id] = r.Position
		m.Schemas[id] = r.Schema
	}
	return m, nil
}

// MachineProjection consumes every twilight/run/ event.
var MachineProjection = extension.ProjectionDefinition{
	ID: MachineProjectionID, Version: 1,
	Consumes: AllTypes(), Authoritative: true,
	Initial: func() (any, error) { return newMachine(), nil },
	Apply: func(state any, e extension.DecodedEvent) (any, error) {
		m, ok := state.(Machine)
		if !ok {
			return nil, fmt.Errorf("run machine: state is %T", state)
		}
		return m.Apply(e)
	},
	StateCodec: machineCodec{},
}

var _ session.EventType = Prefix

// WriterCachePolicy is this module's use of the projection cache for the
// Writer: every projection at the deployment's interval, except the machine
// projection, whose entry the SessionRunStore refreshes itself through SnapshotPolicy
// and which must never be cached mid-step (RUN-CMT-2). every is the interval in
// commits; zero or less takes extension.DefaultCacheEvery.
func WriterCachePolicy(every session.CommitSeq) extension.CachePolicy {
	return extension.CacheEvery(every).Exclude(MachineProjectionID)
}
