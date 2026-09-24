// Package runmod is the first-party Run Session Module (agent-run.md 5): the
// twilight/run/ EventDefinitions, the twilight/run/machine projection and the
// Session adapter of the Run core (SessionRunStore) over the Session Module
// Framework.
package runmod

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/wire"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
)

const (
	ModuleID extension.ModuleID = "run"
	// Prefix is the EventType namespace of every Run fact.
	Prefix session.EventType = "twilight/run/"
	// StreamDomain is the stream domain of Run facts: one keyed stream per
	// Run, bound by the payload's runId.
	StreamDomain = "run"
)

// streamDefinition declares the run domain: keyed by RunID and of segment
// lineage, so a fork never reads an ancestor's Runs as its own.
var streamDefinition = extension.StreamDefinition{Domain: StreamDomain, Key: streamKey, Lineage: session.LineageSegment}

// streamKey binds a run event to its Run's stream (EXT-STR-1).
func streamKey(value any) (string, error) {
	ev, ok := value.(Event)
	if !ok {
		return "", fmt.Errorf("value is %T, want runmod.Event", value)
	}
	return string(ev.RunID), nil
}

// Stream is the logical stream of one Run's facts.
func Stream(runID run.RunID) session.StreamRef { return streamDefinition.Ref(string(runID)) }

// factNames is the closed list of fact discriminators, from the Run core's
// variant registry: a fact the core knows is a wire type this module
// registers, with no second list to keep in step.
var factNames = wire.FactTypes()

// EventType returns the EventType of a fact.
func EventType(f run.Fact) session.EventType {
	return Prefix + session.EventType(wire.Facts{}.FactType(f))
}

// Event is the typed value of one twilight/run/ event: the fact plus the
// RunID that every payload carries at its first level (RUN-WIR-2).
type Event struct {
	RunID run.RunID
	Fact  run.Fact
}

// factCodec encodes one fact type at one payload version. The payload is
// the canonical fact object with "runId" added; `v` is the Registry's.
type factCodec struct {
	local string
	wire  wire.Facts
}

func (c factCodec) Validate(value any) error {
	ev, ok := value.(Event)
	if !ok {
		return fmt.Errorf("value is %T, want runmod.Event", value)
	}
	if ev.RunID == "" || ev.Fact == nil {
		return errors.New("event requires runId and fact")
	}
	if got := c.wire.FactType(ev.Fact); got != c.local {
		return fmt.Errorf("fact is %s, codec is %s", got, c.local)
	}
	return nil
}

func (c factCodec) Encode(value any) (jsonstable.Value, error) {
	if err := c.Validate(value); err != nil {
		return jsonstable.Value{}, err
	}
	ev, _ := value.(Event) // Validate checked the type
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

func (c factCodec) Decode(w jsonstable.Value) (any, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(w.Bytes(), &m); err != nil {
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
	fact, err := c.wire.DecodeFact(c.local, body.Bytes())
	if err != nil {
		return nil, err
	}
	if created, ok := fact.(run.RunCreated); ok && created.RunID != id {
		return nil, errors.New("run_created runId disagrees with payload runId")
	}
	return Event{RunID: id, Fact: fact}, nil
}

// frozenBodyFacts are the fact types whose digest names a frozen body; each
// declares one artifact reference so the Writer admits the body's Binding
// and claims it for the commit (RUN-WIR-4, EXT-REF-2).
var frozenBodyFacts = map[string]bool{
	"model_step_prepared": true, "model_step_completed": true, "tool_call_completed": true, "tool_call_answered": true,
}

var frozenBinding = extension.BindingReferenceDefinition{
	Extractor:          extension.BindingExtractorFunc(frozenRefs),
	Cardinality:        extension.Cardinality{Min: 1, Max: &one},
	AllowedSchemes:     []artifact.Scheme{artifact.SchemeCAS},
	RequiredDurability: artifact.EventBound,
}

var one uint32 = 1

// Module is the run ModuleDescriptor (RUN-SCP-2: no Requires). Conversation
// and Turn projections consume its facts; nothing of theirs is written here.
var Module = buildModule()

// factVersions is the payload version each fact type is written with
// (SES-VER-1, EXT-REG-2). A type whose shape changes gets a new entry here
// and a codec for the new version; types not listed are at version 1. The
// version belongs to the event type: nothing ties the run facts to one
// number (RUN-CMT-8).
var factVersions = map[string]extension.PayloadVersion{}

// factVersion returns the payload version of one fact type.
func factVersion(name string) extension.PayloadVersion {
	if v, ok := factVersions[name]; ok {
		return v
	}
	return 1
}

func buildModule() extension.ModuleDescriptor {
	m := extension.ModuleDescriptor{Source: extension.SourceTwilight, ID: ModuleID,
		Streams: []extension.StreamDefinition{streamDefinition}, Projections: []extension.ProjectionDefinition{MachineProjection}}
	for _, name := range factNames {
		def := extension.EventDefinition{
			Type:   Prefix + session.EventType(name),
			Stream: StreamDomain,
			Codecs: map[extension.PayloadVersion]extension.PayloadCodec{
				factVersion(name): factCodec{local: name, wire: wire.Facts{}},
			},
		}
		if frozenBodyFacts[name] {
			def.Bindings = []extension.BindingReferenceDefinition{frozenBinding}
		}
		m.Events = append(m.Events, def)
	}
	return m
}

// Type returns the EventType of one fact discriminator.
func Type(name string) session.EventType { return Prefix + session.EventType(name) }

// AllTypes lists every registered twilight/run/ EventType.
func AllTypes() []session.EventType {
	out := make([]session.EventType, len(factNames))
	for i, name := range factNames {
		out[i] = Prefix + session.EventType(name)
	}
	return out
}
