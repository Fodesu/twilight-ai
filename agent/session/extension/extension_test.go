package extension

import (
	"errors"
	"github.com/felinics/twilight/agent/artifact"
	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/session"
	"strings"
	"testing"
)

type notePayload struct {
	Text string   `json:"text"`
	Refs []string `json:"refs,omitempty"`
}

type noteState struct {
	Notes []string `json:"notes"`
}

var refsExtractor = BindingExtractorFunc(func(value any) ([]artifact.BindingID, error) {
	var out []artifact.BindingID
	for _, r := range value.(notePayload).Refs {
		out = append(out, artifact.BindingID(r))
	}
	return out, nil
})

// tpfx is the first-party prefix of a test module.
func tpfx(id ModuleID) session.EventType { return ModulePrefix(SourceTwilight, id) }

func noteModule(id ModuleID, requires ...ModuleRequirement) ModuleDescriptor {
	typ := tpfx(id) + "note"
	return ModuleDescriptor{Source: SourceTwilight, ID: id, Requires: requires,
		Events: []EventDefinition{
			{Type: typ, Current: 1, Codecs: map[PayloadVersion]PayloadCodec{1: JSONCodec[notePayload]{}},
				Bindings: []BindingReferenceDefinition{{Extractor: refsExtractor, RequiredDurability: artifact.EventBound}}},
			{Type: tpfx(id) + "hint", Current: 1, Codecs: map[PayloadVersion]PayloadCodec{1: JSONCodec[notePayload]{}}, Ignorable: true},
		},
		Projections: []ProjectionDefinition{{
			ID: ProjectionID(string(typ) + "s"), Version: 1, Consumes: []session.EventType{typ},
			Initial: func() (any, error) { return noteState{}, nil },
			Apply: func(state any, e DecodedEvent) (any, error) {
				s := state.(noteState)
				text := e.Value.(notePayload).Text
				if text == "reject" {
					return nil, errors.New("rejected by projection")
				}
				s.Notes = append(append([]string(nil), s.Notes...), text)
				return s, nil
			},
			StateCodec: JSONStateCodec[noteState]{},
		}},
	}
}

func TestBuildRegistryValidatesRequires(t *testing.T) {
	cases := map[string][]ModuleDescriptor{
		"unregistered dependency": {noteModule("a", ModuleRequirement{Source: SourceTwilight, Module: "zzz"})},
		"cycle":                   {noteModule("a", ModuleRequirement{Source: SourceTwilight, Module: "b"}), noteModule("b", ModuleRequirement{Source: SourceTwilight, Module: "a"})},
		"unhandled version": {noteModule("a"), noteModule("b", ModuleRequirement{Source: SourceTwilight, Module: "a",
			Events: map[session.EventType][]PayloadVersion{tpfx("a") + "note": {2}}})},
		"event outside module": {{Source: SourceTwilight, ID: "a", Events: []EventDefinition{{Type: "twilight/b/x", Current: 1, Codecs: map[PayloadVersion]PayloadCodec{1: JSONCodec[notePayload]{}}}}}},
		"projection outside scope": {noteModule("a"), {Source: SourceTwilight, ID: "b", Projections: []ProjectionDefinition{{ID: "p", Version: 1, Consumes: []session.EventType{tpfx("a") + "note"},
			Initial: func() (any, error) { return nil, nil }, Apply: func(s any, _ DecodedEvent) (any, error) { return s, nil }, StateCodec: JSONStateCodec[noteState]{}}}}},
	}
	for name, modules := range cases {
		if _, err := BuildRegistry(session.ProtocolVersion1, modules...); err == nil {
			t.Errorf("%s: registry built", name)
		}
	}
	if _, err := BuildRegistry(session.ProtocolVersion1, noteModule("a"), noteModule("b", ModuleRequirement{Source: SourceTwilight, Module: "a",
		Events: map[session.EventType][]PayloadVersion{tpfx("a") + "note": {1}}})); err != nil {
		t.Fatalf("valid registry: %v", err)
	}
}

// srcModule is a minimal module under an arbitrary source.
func srcModule(source SourceID, id ModuleID) ModuleDescriptor {
	return ModuleDescriptor{Source: source, ID: id, Events: []EventDefinition{{
		Type: ModulePrefix(source, id) + "note", Current: 1,
		Codecs: map[PayloadVersion]PayloadCodec{1: JSONCodec[notePayload]{}},
	}}}
}

// EXT-REG-1: module identity is (Source, ID); sources are validated segments.
func TestBuildRegistryValidatesSource(t *testing.T) {
	rejects := map[string][]ModuleDescriptor{
		"empty source":           {srcModule("", "a")},
		"source with slash":      {srcModule("x/y", "a")},
		"source not utf8":        {srcModule(SourceID([]byte{0xff, 0xfe}), "a")},
		"duplicate (source, id)": {srcModule("app", "a"), srcModule("app", "a")},
		"app id colliding first-party under twilight": {noteModule("a"), srcModule(SourceTwilight, "a")},
		"requirement without source":                  {noteModule("a"), {Source: "app", ID: "b", Requires: []ModuleRequirement{{Module: "a"}}}},
	}
	for name, modules := range rejects {
		if _, err := BuildRegistry(session.ProtocolVersion1, modules...); err == nil {
			t.Errorf("%s: registry built", name)
		}
	}
	// The same ID under two sources coexists and both prefixes resolve.
	r, err := BuildRegistry(session.ProtocolVersion1, noteModule("a"), srcModule("app", "a"))
	if err != nil {
		t.Fatalf("two sources, one id: %v", err)
	}
	if key, ok := r.ModuleOf("app/a/note"); !ok || key != (ModuleKey{Source: "app", ID: "a"}) {
		t.Fatalf("ModuleOf app/a/note = %+v %v", key, ok)
	}
	if key, ok := r.ModuleOf(tpfx("a") + "note"); !ok || key != TwilightModule("a") {
		t.Fatalf("ModuleOf twilight/a/note = %+v %v", key, ok)
	}
	if _, ok := r.ModuleOf("ghost/a/note"); ok {
		t.Fatal("unregistered source resolved")
	}
}

// Encode adds v; Decode selects the codec by v and keeps unknown versions raw.
func TestRegistryPayloadVersion(t *testing.T) {
	r, err := BuildRegistry(session.ProtocolVersion1, noteModule("a"))
	if err != nil {
		t.Fatal(err)
	}
	typ := tpfx("a") + "note"
	wire, v, err := r.Encode(typ, notePayload{Text: "hi"})
	if err != nil || v != 1 || wire.String() != `{"text":"hi","v":1}` {
		t.Fatalf("encode = %s v%d %v", wire, v, err)
	}
	decoded, err := r.Decode(session.SessionEvent{Type: typ, Payload: wire})
	if err != nil || decoded.Unknown || decoded.Value.(notePayload).Text != "hi" {
		t.Fatalf("decode = %+v %v", decoded, err)
	}
	future, err := r.Decode(session.SessionEvent{Type: typ, Payload: jsonstable.MustParse(`{"text":"hi","v":2}`)})
	if err != nil || !future.Unknown || future.Version != 2 {
		t.Fatalf("future version = %+v %v", future, err)
	}
	if _, _, err := r.Encode("twilight/a/other", notePayload{}); err == nil {
		t.Fatal("unknown type encoded")
	}
}

// EXT-REG-1: advancing Current obliges the module to supply that version's
// codec. Encode selects def.Codecs[def.Current], so a missing entry must be
// refused when the registry is built rather than discovered on first write.
func TestBuildRegistryRequiresCodecForCurrent(t *testing.T) {
	for _, tc := range []struct {
		name    string
		current PayloadVersion
	}{
		{"current version without a codec", 2},
		{"zero current version", 0},
	} {
		_, err := BuildRegistry(session.ProtocolVersion1, ModuleDescriptor{Source: SourceTwilight, ID: "a",
			Events: []EventDefinition{{
				Type: tpfx("a") + "note", Current: tc.current,
				Codecs: map[PayloadVersion]PayloadCodec{1: JSONCodec[notePayload]{}},
			}}})
		if err == nil {
			t.Fatalf("%s: registry built", tc.name)
		}
		if !strings.Contains(err.Error(), "no codec for the current payload version") {
			t.Fatalf("%s: error = %v", tc.name, err)
		}
	}
	// Retaining the older codec alongside the current one is the supported
	// shape, so it must keep building.
	if _, err := BuildRegistry(session.ProtocolVersion1, ModuleDescriptor{Source: SourceTwilight, ID: "a",
		Events: []EventDefinition{{
			Type: tpfx("a") + "note", Current: 2,
			Codecs: map[PayloadVersion]PayloadCodec{1: legacyCodec{}, 2: JSONCodec[notePayload]{}},
		}}}); err != nil {
		t.Fatalf("coexisting versions: %v", err)
	}
}

// legacyCodec decodes the v1 wire of the note event, which predates the
// optional refs field. It deliberately produces a distinguishable value so a
// test can prove the version, not the current codec, selected it.
type legacyCodec struct{}

func (legacyCodec) Encode(v any) (jsonstable.Value, error) {
	return jsonstable.FromValue(struct {
		Text string `json:"text"`
	}{v.(notePayload).Text})
}
func (legacyCodec) Decode(w jsonstable.Value) (any, error) {
	var body struct {
		Text string `json:"text"`
	}
	if err := w.Decode(&body); err != nil {
		return nil, err
	}
	return notePayload{Text: "v1:" + body.Text}, nil
}
func (legacyCodec) Validate(v any) error {
	if v.(notePayload).Text == "" {
		return errors.New("text is required")
	}
	return nil
}

// EXT-REG-1..4, EXT-COD-1/2: payload versions coexist. Advancing Current must
// not orphan stored rows: an old version still selects its own codec, decodes
// to a value and folds, while a version no codec claims stays Unknown with its
// raw payload.
func TestRegistryMultiVersionCodecsCoexist(t *testing.T) {
	typ := tpfx("v") + "note"
	upgraded := ModuleDescriptor{Source: SourceTwilight, ID: "v", Events: []EventDefinition{{
		Type: typ, Current: 2,
		Codecs: map[PayloadVersion]PayloadCodec{1: legacyCodec{}, 2: JSONCodec[notePayload]{}},
	}}}
	r, err := BuildRegistry(session.ProtocolVersion1, upgraded)
	if err != nil {
		t.Fatal(err)
	}

	// Writing uses Current.
	wire, v, err := r.Encode(typ, notePayload{Text: "hi"})
	if err != nil || v != 2 {
		t.Fatalf("encode = %s v%d %v, want v2", wire, v, err)
	}
	if wire.String() != `{"text":"hi","v":2}` {
		t.Fatalf("current wire = %s", wire)
	}

	// A row written before the upgrade still decodes, through its own codec.
	old, err := r.Decode(session.SessionEvent{Type: typ, Payload: jsonstable.MustParse(`{"text":"old","v":1}`)})
	if err != nil {
		t.Fatalf("decode v1: %v", err)
	}
	if old.Unknown {
		t.Fatal("a retained older version decoded as Unknown")
	}
	if old.Version != 1 || old.Value.(notePayload).Text != "v1:old" {
		t.Fatalf("v1 row = version %d value %+v: the v1 codec did not run", old.Version, old.Value)
	}
	current, err := r.Decode(session.SessionEvent{Type: typ, Payload: wire})
	if err != nil || current.Unknown || current.Value.(notePayload).Text != "hi" {
		t.Fatalf("v2 row = %+v %v", current, err)
	}

	// A version no codec claims is preserved raw rather than reinterpreted.
	future, err := r.Decode(session.SessionEvent{Type: typ, Payload: jsonstable.MustParse(`{"text":"x","v":3}`)})
	if err != nil || !future.Unknown || future.Version != 3 {
		t.Fatalf("v3 row = %+v %v, want Unknown v3", future, err)
	}
	if future.Event.Payload.String() != `{"text":"x","v":3}` {
		t.Fatalf("unknown-version payload was not preserved: %s", future.Event.Payload)
	}
}
