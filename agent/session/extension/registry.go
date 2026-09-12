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
	"unicode/utf8"

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

// SourceTwilight is the source reserved for this repository's first-party
// modules; application modules register under their own SourceID (EXT-REG-1).
const SourceTwilight SourceID = "twilight"

// ModuleKey is the registry identity of one module: (Source, ID).
type ModuleKey struct {
	Source SourceID
	ID     ModuleID
}

// TwilightModule is the ModuleKey of a first-party module.
func TwilightModule(id ModuleID) ModuleKey { return ModuleKey{Source: SourceTwilight, ID: id} }

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
// and which payload versions it can handle (EXT-REG-4). Source is required:
// module identity is the (Source, ID) pair.
type ModuleRequirement struct {
	Source SourceID
	Module ModuleID
	Events map[session.EventType][]PayloadVersion
}

// Key is the identity the requirement points at.
func (r ModuleRequirement) Key() ModuleKey { return ModuleKey{Source: r.Source, ID: r.Module} }

type ModuleDescriptor struct {
	Source      SourceID
	ID          ModuleID
	Requires    []ModuleRequirement
	Events      []EventDefinition
	Projections []ProjectionDefinition
}

// Key is the module's registry identity.
func (m ModuleDescriptor) Key() ModuleKey { return ModuleKey{Source: m.Source, ID: m.ID} }

type DecodedEvent struct {
	Event   session.SessionEvent
	Module  ModuleKey
	Version PayloadVersion
	Value   any
	Unknown bool
}

// Registry is the immutable index built once at startup (EXT-REG-1).
type Registry struct {
	ProtocolVersion uint16

	modules     map[ModuleKey]ModuleDescriptor
	events      map[session.EventType]eventEntry
	projections map[projectionKey]projectionEntry
}

type eventEntry struct {
	module ModuleKey
	def    EventDefinition
}

type projectionKey struct {
	id      ProjectionID
	version ProjectionVersion
}

type projectionEntry struct {
	module ModuleKey
	def    ProjectionDefinition
}

// validSegment checks one identity segment of an EventType prefix.
func validSegment(kind, v string) error {
	if v == "" {
		return fmt.Errorf("empty %s", kind)
	}
	if strings.Contains(v, "/") {
		return fmt.Errorf("%s %q contains %q", kind, v, "/")
	}
	if !utf8.ValidString(v) {
		return fmt.Errorf("%s is not valid UTF-8", kind)
	}
	return nil
}

// BuildRegistry validates the module set and freezes the indexes.
func BuildRegistry(protocolVersion uint16, modules ...ModuleDescriptor) (*Registry, error) {
	if protocolVersion == 0 {
		return nil, errors.New("extension: registry: zero protocol version")
	}
	r := &Registry{ProtocolVersion: protocolVersion,
		modules: make(map[ModuleKey]ModuleDescriptor), events: make(map[session.EventType]eventEntry), projections: make(map[projectionKey]projectionEntry)}
	for _, m := range modules {
		if err := validSegment("source", string(m.Source)); err != nil {
			return nil, &Error{Code: ErrInvalid, Detail: fmt.Sprintf("module %q: %v", m.ID, err)}
		}
		if err := validSegment("module id", string(m.ID)); err != nil {
			return nil, &Error{Code: ErrInvalid, Detail: fmt.Sprintf("source %q: %v", m.Source, err)}
		}
		key := m.Key()
		if _, dup := r.modules[key]; dup {
			return nil, &Error{Code: ErrInvalid, Detail: fmt.Sprintf("duplicate module %s/%s", key.Source, key.ID)}
		}
		r.modules[key] = m
		prefix := ModulePrefix(m.Source, m.ID)
		for _, def := range m.Events {
			if !strings.HasPrefix(string(def.Type), string(prefix)) || len(def.Type) == len(prefix) {
				return nil, &Error{Code: ErrInvalid, Type: def.Type, Detail: fmt.Sprintf("event type is not under module %s/%s", key.Source, key.ID)}
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
			r.events[def.Type] = eventEntry{module: key, def: def}
		}
		for _, p := range m.Projections {
			if p.ID == "" || p.Version == 0 || p.Initial == nil || p.Apply == nil || p.StateCodec == nil {
				return nil, &Error{Code: ErrInvalid, Detail: fmt.Sprintf("projection %q is incomplete", p.ID)}
			}
			k := projectionKey{p.ID, p.Version}
			if _, dup := r.projections[k]; dup {
				return nil, &Error{Code: ErrInvalid, Detail: fmt.Sprintf("duplicate projection %q v%d", p.ID, p.Version)}
			}
			r.projections[k] = projectionEntry{module: key, def: p}
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
	state := make(map[ModuleKey]int) // 0 unvisited, 1 visiting, 2 done
	var visit func(ModuleKey) error
	visit = func(key ModuleKey) error {
		switch state[key] {
		case 1:
			return &Error{Code: ErrInvalid, Detail: fmt.Sprintf("module requirement cycle through %s/%s", key.Source, key.ID)}
		case 2:
			return nil
		}
		state[key] = 1
		for _, req := range r.modules[key].Requires {
			if req.Source == "" {
				return &Error{Code: ErrInvalid, Detail: fmt.Sprintf("module %s/%s: requirement on %q has no source", key.Source, key.ID, req.Module)}
			}
			depKey := req.Key()
			dep, ok := r.modules[depKey]
			if !ok {
				return &Error{Code: ErrInvalid, Detail: fmt.Sprintf("module %s/%s requires unregistered module %s/%s", key.Source, key.ID, depKey.Source, depKey.ID)}
			}
			for typ, versions := range req.Events {
				entry, ok := r.events[typ]
				if !ok || entry.module != dep.Key() {
					return &Error{Code: ErrInvalid, Type: typ, Detail: fmt.Sprintf("module %s/%s requires event not owned by %s/%s", key.Source, key.ID, depKey.Source, depKey.ID)}
				}
				if !containsVersion(versions, entry.def.Current) {
					return &Error{Code: ErrInvalid, Type: typ, Detail: fmt.Sprintf("module %s/%s handles versions %v but %s/%s currently writes v%d", key.Source, key.ID, versions, depKey.Source, depKey.ID, entry.def.Current)}
				}
			}
			if err := visit(depKey); err != nil {
				return err
			}
		}
		state[key] = 2
		return nil
	}
	for key := range r.modules {
		if err := visit(key); err != nil {
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
				return &Error{Code: ErrInvalid, Type: typ, Detail: fmt.Sprintf("projection %q consumes event of module %s/%s outside its Requires", k.id, entry.module.Source, entry.module.ID)}
			}
		}
	}
	return nil
}

// scopeOf is the module plus its Requires: the modules whose unknown events a
// projection must not silently skip (EXT-PRJ-2).
func (r *Registry) scopeOf(key ModuleKey) map[ModuleKey]struct{} {
	scope := map[ModuleKey]struct{}{key: {}}
	for _, req := range r.modules[key].Requires {
		scope[req.Key()] = struct{}{}
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

func (r *Registry) LookupEvent(typ session.EventType) (ModuleKey, EventDefinition, bool) {
	e, ok := r.events[typ]
	return e.module, e.def, ok
}

// ModuleOf names the module an EventType belongs to by its
// <source>/<module>/ prefix; false when the prefix names no registered module.
func (r *Registry) ModuleOf(typ session.EventType) (ModuleKey, bool) {
	parts := strings.SplitN(string(typ), "/", 3)
	if len(parts) == 3 {
		key := ModuleKey{Source: SourceID(parts[0]), ID: ModuleID(parts[1])}
		if _, registered := r.modules[key]; registered {
			return key, true
		}
	}
	return ModuleKey{}, false
}

func (r *Registry) LookupProjection(id ProjectionID, v ProjectionVersion) (ProjectionDefinition, ModuleKey, bool) {
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

// ModulePrefix is the EventType prefix of one module: <source>/<module>/.
func ModulePrefix(source SourceID, id ModuleID) session.EventType {
	return session.EventType(fmt.Sprintf("%s/%s/", source, id))
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
		out.Module, _ = r.ModuleOf(e.Type)
		out.Unknown = true
		return out, nil
	}
	out.Module = module
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
	// ErrUnknownOutcome: an Append failed in a way that leaves what reached
	// the log unknown (an IO error, or the kernel's ErrHandleFailed). The
	// Writer's head and projections may no longer match the log, so it fails
	// closed; the host reopens and replays (EXT-WRT-4).
	ErrUnknownOutcome ErrorCode = "unknown_outcome"
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
