// Package target is the first-party module that binds a Session to the
// opaque resource target its tool effects execute against (RUN-LOP-9,
// APP-TGT-1). The binding is a Session fact: one singleton stream of
// session lineage carrying bound events, a projection of the current
// target, the Bind command through the Session's Writer and a
// loop.TargetResolver that answers the Loop from the projection. Agent Core
// never interprets the target; a Workspace domain defines its meaning
// (agent-workspace.md).
package target

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
)

// ModuleID is the module's identity under extension.SourceTwilight.
const ModuleID extension.ModuleID = "target"

// StreamDomain is the singleton stream domain of the binding: one stream
// per Session, of session lineage, so a fork child starts with its parent's
// binding until it binds its own target.
const StreamDomain = "target"

var streamDefinition = extension.StreamDefinition{Domain: StreamDomain, Lineage: session.LineageSession}

// Stream is the binding's logical stream.
var Stream = streamDefinition.Ref("")

// TypeBound records the Session's current target; a later bound replaces
// the earlier one.
const TypeBound = session.EventType("twilight/target/bound")

// BoundPayload is the payload of TypeBound.
type BoundPayload struct {
	Target run.TargetRef `json:"target"`
}

func checkBound(p *BoundPayload) error {
	if p.Target.Kind == "" || p.Target.ID == "" {
		return errors.New("bound requires target kind and id")
	}
	return nil
}

// ProjectionID is the current-target projection.
const ProjectionID extension.ProjectionID = "twilight/target/current"

// Current is the state of ProjectionID: the target the Session is bound to,
// nil while unbound.
type Current struct {
	Target *run.TargetRef `json:"target,omitempty"`
}

// Projection folds the bound events into the current target. Bind plans
// against it on the Writer's View, so it is authoritative (EXT-PRJ-9).
var Projection = extension.ProjectionDefinition{
	ID: ProjectionID, Version: 1,
	Consumes:      []session.EventType{TypeBound},
	Initial:       func() (any, error) { return Current{}, nil },
	Apply:         applyCurrent,
	StateCodec:    extension.JSONStateCodec[Current]{},
	Authoritative: true,
}

func applyCurrent(state any, e extension.DecodedEvent) (any, error) { //nolint:gocritic // hugeParam: DecodedEvent is the extension API shape
	cur, err := current(state)
	if err != nil {
		return nil, err
	}
	p, ok := e.Value.(BoundPayload)
	if !ok {
		return nil, fmt.Errorf("target: %s payload is %T", e.Event.Type, e.Value)
	}
	bound := p.Target
	cur.Target = &bound
	return cur, nil
}

func current(state any) (Current, error) {
	cur, ok := state.(Current)
	if !ok {
		return Current{}, fmt.Errorf("target: current projection is %T", state)
	}
	return cur, nil
}

// Module is the target ModuleDescriptor: its own stream, event and
// projection, no Requires.
var Module = extension.ModuleDescriptor{
	Source:  extension.SourceTwilight,
	ID:      ModuleID,
	Streams: []extension.StreamDefinition{streamDefinition},
	Events: []extension.EventDefinition{{
		Type: TypeBound, Stream: StreamDomain,
		Codecs: map[extension.SchemaVersion]extension.PayloadCodec{1: extension.JSONCodec[BoundPayload]{Check: checkBound}},
	}},
	Projections: []extension.ProjectionDefinition{Projection},
}
