// Package extension is the Session Module Framework
// (docs/design/agent-session-extension.md): typed event codecs with
// payload versions, Binding admission, the in-process Writer that serializes
// every write and holds the idempotency index, and pure projections with an
// optional cache.
package extension

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/session"
)

type (
	SourceID          string
	ModuleID          string
	ProjectionID      string
	ProjectionVersion uint16
	PayloadVersion    uint16
)

const SourceTwilight SourceID = "twilight"

// PayloadCodec encodes and decodes one payload version. Encode never writes
// the `v` field: the Registry adds it (EXT-COD-2).
type PayloadCodec interface {
	Encode(value any) (jsonstable.Value, error)
	Decode(wire jsonstable.Value) (any, error)
	Validate(value any) error
}

type EventDefinition struct {
	Type     session.EventType
	Current  PayloadVersion
	Codecs   map[PayloadVersion]PayloadCodec
	Bindings []BindingReferenceDefinition
	// Ignorable marks purely informational events: rows are written with
	// session.SessionEvent.Ignorable so readers that do not know the type may
	// skip them (EXT-PRJ-2).
	Ignorable bool
}

// ModuleRequirement declares that a module consumes another module's events
// and which payload versions it can handle (EXT-REG-4).
type ModuleRequirement struct {
	Module ModuleID
	Events map[session.EventType][]PayloadVersion
}

type ModuleDescriptor struct {
	ID          ModuleID
	Requires    []ModuleRequirement
	Events      []EventDefinition
	Projections []ProjectionDefinition
}

type DecodedEvent struct {
	Event    session.SessionEvent
	ModuleID ModuleID
	Version  PayloadVersion
	Value    any
	Unknown  bool
}

// Registry is the immutable index built once at startup (EXT-REG-1).
type Registry struct {
	ProtocolVersion uint16

	modules     map[ModuleID]ModuleDescriptor
	events      map[session.EventType]eventEntry
	projections map[projectionKey]projectionEntry
}

type eventEntry struct {
	module ModuleID
	def    EventDefinition
}

type projectionKey struct {
	id      ProjectionID
	version ProjectionVersion
}

type projectionEntry struct {
	module ModuleID
	def    ProjectionDefinition
}

// BuildRegistry validates the module set and freezes the indexes.
func BuildRegistry(protocolVersion uint16, modules ...ModuleDescriptor) (*Registry, error) {
	if protocolVersion == 0 {
		return nil, errors.New("extension: registry: zero protocol version")
	}
	r := &Registry{ProtocolVersion: protocolVersion,
		modules: make(map[ModuleID]ModuleDescriptor), events: make(map[session.EventType]eventEntry), projections: make(map[projectionKey]projectionEntry)}
	for _, m := range modules {
		if m.ID == "" || strings.Contains(string(m.ID), "/") {
			return nil, &Error{Code: ErrInvalid, Detail: fmt.Sprintf("invalid module id %q", m.ID)}
		}
		if _, dup := r.modules[m.ID]; dup {
			return nil, &Error{Code: ErrInvalid, Detail: fmt.Sprintf("duplicate module %q", m.ID)}
		}
		r.modules[m.ID] = m
		prefix := ModulePrefix(m.ID)
		for _, def := range m.Events {
			if !strings.HasPrefix(string(def.Type), string(prefix)) || len(def.Type) == len(prefix) {
				return nil, &Error{Code: ErrInvalid, Type: def.Type, Detail: fmt.Sprintf("event type is not under module %q", m.ID)}
			}
			if _, dup := r.events[def.Type]; dup {
				return nil, &Error{Code: ErrInvalid, Type: def.Type, Detail: "duplicate event type"}
			}
			if def.Current == 0 || def.Codecs[def.Current] == nil {
				return nil, &Error{Code: ErrInvalid, Type: def.Type, Detail: "no codec for the current payload version"}
			}
			for _, b := range def.Bindings {
				if err := b.validate(); err != nil {
					return nil, &Error{Code: ErrInvalid, Type: def.Type, Detail: err.Error()}
				}
			}
			r.events[def.Type] = eventEntry{module: m.ID, def: def}
		}
		for _, p := range m.Projections {
			if p.ID == "" || p.Version == 0 || p.Initial == nil || p.Apply == nil || p.StateCodec == nil {
				return nil, &Error{Code: ErrInvalid, Detail: fmt.Sprintf("projection %q is incomplete", p.ID)}
			}
			k := projectionKey{p.ID, p.Version}
			if _, dup := r.projections[k]; dup {
				return nil, &Error{Code: ErrInvalid, Detail: fmt.Sprintf("duplicate projection %q v%d", p.ID, p.Version)}
			}
			r.projections[k] = projectionEntry{module: m.ID, def: p}
		}
	}
	if err := r.checkRequirements(); err != nil {
		return nil, err
	}
	return r, nil
}

// checkRequirements enforces EXT-REG-4: registered dependencies, no cycles,
// projection consumption within scope, and handled payload versions.
func (r *Registry) checkRequirements() error {
	state := make(map[ModuleID]int) // 0 unvisited, 1 visiting, 2 done
	var visit func(ModuleID) error
	visit = func(id ModuleID) error {
		switch state[id] {
		case 1:
			return &Error{Code: ErrInvalid, Detail: fmt.Sprintf("module requirement cycle through %q", id)}
		case 2:
			return nil
		}
		state[id] = 1
		for _, req := range r.modules[id].Requires {
			dep, ok := r.modules[req.Module]
			if !ok {
				return &Error{Code: ErrInvalid, Detail: fmt.Sprintf("module %q requires unregistered module %q", id, req.Module)}
			}
			for typ, versions := range req.Events {
				entry, ok := r.events[typ]
				if !ok || entry.module != dep.ID {
					return &Error{Code: ErrInvalid, Type: typ, Detail: fmt.Sprintf("module %q requires event not owned by %q", id, req.Module)}
				}
				if !containsVersion(versions, entry.def.Current) {
					return &Error{Code: ErrInvalid, Type: typ, Detail: fmt.Sprintf("module %q handles versions %v but %q currently writes v%d", id, versions, req.Module, entry.def.Current)}
				}
			}
			if err := visit(req.Module); err != nil {
				return err
			}
		}
		state[id] = 2
		return nil
	}
	for id := range r.modules {
		if err := visit(id); err != nil {
			return err
		}
	}
	for k, p := range r.projections {
		scope := r.scopeOf(p.module)
		for _, typ := range p.def.Consumes {
			entry, ok := r.events[typ]
			if !ok {
				return &Error{Code: ErrInvalid, Type: typ, Detail: fmt.Sprintf("projection %q consumes unregistered event", k.id)}
			}
			if _, inScope := scope[entry.module]; !inScope {
				return &Error{Code: ErrInvalid, Type: typ, Detail: fmt.Sprintf("projection %q consumes event of module %q outside its Requires", k.id, entry.module)}
			}
		}
	}
	return nil
}

// scopeOf is the module plus its Requires: the modules whose unknown events a
// projection must not silently skip (EXT-PRJ-2).
func (r *Registry) scopeOf(id ModuleID) map[ModuleID]struct{} {
	scope := map[ModuleID]struct{}{id: {}}
	for _, req := range r.modules[id].Requires {
		scope[req.Module] = struct{}{}
	}
	return scope
}

func containsVersion(vs []PayloadVersion, v PayloadVersion) bool {
	for _, x := range vs {
		if x == v {
			return true
		}
	}
	return false
}

func (r *Registry) LookupEvent(typ session.EventType) (ModuleID, EventDefinition, bool) {
	e, ok := r.events[typ]
	return e.module, e.def, ok
}

// ModuleOf names the module an EventType belongs to by its
// twilight/<module>/ prefix, registered or not; false when the prefix names no
// registered module.
func (r *Registry) ModuleOf(typ session.EventType) (ModuleID, bool) {
	parts := strings.SplitN(string(typ), "/", 3)
	if len(parts) == 3 && parts[0] == string(SourceTwilight) {
		if _, registered := r.modules[ModuleID(parts[1])]; registered {
			return ModuleID(parts[1]), true
		}
	}
	return "", false
}

func (r *Registry) LookupProjection(id ProjectionID, v ProjectionVersion) (ProjectionDefinition, ModuleID, bool) {
	e, ok := r.projections[projectionKey{id, v}]
	return e.def, e.module, ok
}

// Projections lists every registered projection with its owning module.
func (r *Registry) Projections() []ProjectionDefinition {
	out := make([]ProjectionDefinition, 0, len(r.projections))
	for _, e := range r.projections {
		out = append(out, e.def)
	}
	return out
}

// ModulePrefix is the EventType prefix of one module.
func ModulePrefix(id ModuleID) session.EventType {
	return session.EventType(fmt.Sprintf("%s/%s/", SourceTwilight, id))
}

// Encode validates value, encodes it with the current codec and adds `v`.
func (r *Registry) Encode(typ session.EventType, value any) (jsonstable.Value, PayloadVersion, error) {
	_, def, ok := r.LookupEvent(typ)
	if !ok {
		return jsonstable.Value{}, 0, &Error{Code: ErrUnknownEvent, Type: typ}
	}
	codec := def.Codecs[def.Current]
	if err := codec.Validate(value); err != nil {
		return jsonstable.Value{}, 0, &Error{Code: ErrCodec, Type: typ, Detail: err.Error()}
	}
	body, err := codec.Encode(value)
	if err != nil {
		return jsonstable.Value{}, 0, &Error{Code: ErrCodec, Type: typ, Detail: err.Error()}
	}
	wire, err := addVersion(body, def.Current)
	if err != nil {
		return jsonstable.Value{}, 0, &Error{Code: ErrCodec, Type: typ, Detail: err.Error()}
	}
	// The canonical Encode/Decode/Encode round trip is a module test
	// obligation (EXT-COD-1), not re-verified per Encode.
	return wire, def.Current, nil
}

// Decode selects the codec by (EventType, v). Unknown types or versions are
// returned as Unknown with the raw payload retained (EXT-REG-3).
func (r *Registry) Decode(e session.SessionEvent) (DecodedEvent, error) {
	out := DecodedEvent{Event: e}
	module, def, ok := r.LookupEvent(e.Type)
	if !ok {
		out.ModuleID, _ = r.ModuleOf(e.Type)
		out.Unknown = true
		return out, nil
	}
	out.ModuleID = module
	body, v, err := splitVersion(e.Payload)
	if err != nil {
		return out, &Error{Code: ErrCodec, Type: e.Type, Detail: err.Error()}
	}
	out.Version = v
	codec := def.Codecs[v]
	if codec == nil {
		out.Unknown = true
		return out, nil
	}
	value, err := codec.Decode(body)
	if err != nil {
		return out, &Error{Code: ErrCodec, Type: e.Type, Detail: err.Error()}
	}
	out.Value = value
	return out, nil
}

// addVersion inserts the integer `v` field into the first level of body.
func addVersion(body jsonstable.Value, v PayloadVersion) (jsonstable.Value, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body.Bytes(), &m); err != nil {
		return jsonstable.Value{}, fmt.Errorf("payload is not an object: %w", err)
	}
	if m == nil {
		m = map[string]json.RawMessage{}
	}
	if _, has := m["v"]; has {
		return jsonstable.Value{}, errors.New("payload must not define its own \"v\" field")
	}
	m["v"] = json.RawMessage(fmt.Sprintf("%d", v))
	return jsonstable.FromValue(m)
}

// splitVersion removes `v` and returns the codec-facing body.
func splitVersion(payload jsonstable.Value) (jsonstable.Value, PayloadVersion, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(payload.Bytes(), &m); err != nil {
		return jsonstable.Value{}, 0, fmt.Errorf("payload is not an object: %w", err)
	}
	raw, ok := m["v"]
	if !ok {
		return jsonstable.Value{}, 0, errors.New("payload has no \"v\" field")
	}
	var v uint16
	if err := json.Unmarshal(raw, &v); err != nil || v == 0 {
		return jsonstable.Value{}, 0, errors.New("payload \"v\" is not a positive integer")
	}
	delete(m, "v")
	body, err := jsonstable.FromValue(m)
	if err != nil {
		return jsonstable.Value{}, 0, err
	}
	return body, PayloadVersion(v), nil
}

// JSONCodec is a PayloadCodec for a plain Go struct type T with json tags.
// Decode is strict: unknown fields, duplicate keys and trailing data are
// rejected by the canonical parse and DisallowUnknownFields.
type JSONCodec[T any] struct {
	// Check validates a decoded/encoded value; nil accepts every T.
	Check func(*T) error
}

func (c JSONCodec[T]) Validate(value any) error {
	v, ok := value.(T)
	if !ok {
		p, isPtr := value.(*T)
		if !isPtr || p == nil {
			return fmt.Errorf("value is %T, want %T", value, v)
		}
		v = *p
	}
	if c.Check != nil {
		return c.Check(&v)
	}
	return nil
}

func (c JSONCodec[T]) Encode(value any) (jsonstable.Value, error) {
	if err := c.Validate(value); err != nil {
		return jsonstable.Value{}, err
	}
	if p, ok := value.(*T); ok {
		value = *p
	}
	return jsonstable.FromValue(value)
}

func (c JSONCodec[T]) Decode(wire jsonstable.Value) (any, error) {
	var v T
	if err := StrictDecode(wire, &v); err != nil {
		return nil, err
	}
	if c.Check != nil {
		if err := c.Check(&v); err != nil {
			return nil, err
		}
	}
	return v, nil
}

// StrictDecode decodes canonical JSON into dst rejecting unknown fields.
func StrictDecode(wire jsonstable.Value, dst any) error {
	dec := json.NewDecoder(strings.NewReader(wire.String()))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("trailing data after JSON value")
	}
	return nil
}

// --- errors -------------------------------------------------------------------

type ErrorCode string

const (
	ErrInvalid       ErrorCode = "invalid"
	ErrUnknownEvent  ErrorCode = "unknown_event"
	ErrCodec         ErrorCode = "codec"
	ErrBinding       ErrorCode = "binding"
	ErrConflict      ErrorCode = "conflict"
	ErrOwnershipLost ErrorCode = "ownership_lost"
)

type Error struct {
	Code   ErrorCode
	Type   session.EventType
	Detail string
}

func (e *Error) Error() string {
	s := "extension: " + string(e.Code)
	if e.Type != "" {
		s += " " + string(e.Type)
	}
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	return s
}

func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code
}
