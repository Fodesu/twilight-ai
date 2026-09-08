package chatlog

import (
	"testing"

	"github.com/memohai/twilight/agent/jsonstable"
	"github.com/memohai/twilight/agent/session"
	"github.com/memohai/twilight/agent/session/extension"
)

func registry(t *testing.T) *extension.Registry {
	t.Helper()
	r, err := extension.BuildRegistry(session.ProtocolVersion1, Module)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// Every part kind round-trips through the discriminated wire; foreign fields
// and unknown kinds are rejected (CHT-COD-1).
func TestPartsCodecRoundTripAndRejects(t *testing.T) {
	parts := Parts{
		ReasoningPart{Text: "think"},
		TextPart{Text: "hello"},
		ToolCallPart{CallID: "c1", ProviderCallID: "call_x", Name: "lookup", Input: jsonstable.MustParse(`{"q":1}`)},
		ReferencePart{BindingID: "b1", Name: "file.txt"},
	}
	raw, err := parts.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var back Parts
	if err := back.UnmarshalJSON(raw); err != nil {
		t.Fatalf("%v\n%s", err, raw)
	}
	again, _ := back.MarshalJSON()
	if string(again) != string(raw) {
		t.Fatalf("round trip differs:\n%s\n%s", raw, again)
	}
	for name, wire := range map[string]string{
		"unknown kind":            `[{"kind":"twilight/chatlog/video"}]`,
		"text with call id":       `[{"kind":"twilight/chatlog/text","text":"x","callId":"c"}]`,
		"tool call without input": `[{"kind":"twilight/chatlog/tool_call","callId":"c","name":"n"}]`,
		"reference without id":    `[{"kind":"twilight/chatlog/reference","name":"f"}]`,
		"unknown field":           `[{"kind":"twilight/chatlog/text","text":"x","extra":1}]`,
	} {
		var p Parts
		if err := p.UnmarshalJSON([]byte(wire)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// The assistant codec rejects a payload whose digest does not cover its parts;
// the Registry adds v and decodes back to the same value.
func TestAssistantDigestIsVerified(t *testing.T) {
	r := registry(t)
	a := Assistant{ID: "a1", TurnID: "t1", Parts: Parts{TextPart{Text: "hi"}}, SourceDigest: "sha256:src"}
	d, err := DigestAssistant(&a)
	if err != nil {
		t.Fatal(err)
	}
	a.Digest = d
	wire, _, err := r.Encode(TypeAssistant, AssistantPayload{Assistant: a})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := r.Decode(session.SessionEvent{Type: TypeAssistant, Payload: wire})
	if err != nil || decoded.Value.(AssistantPayload).Assistant.Digest != d {
		t.Fatalf("decode = %+v %v", decoded, err)
	}
	a.Parts = Parts{TextPart{Text: "changed"}}
	if _, _, err := r.Encode(TypeAssistant, AssistantPayload{Assistant: a}); err == nil {
		t.Fatal("digest mismatch accepted")
	}
	if ids, _ := PartsExtractor.BindingIDs(AssistantPayload{Assistant: Assistant{Parts: Parts{ReferencePart{BindingID: "b2"}, TextPart{}, ReferencePart{BindingID: "b1"}}}}); len(ids) != 2 || ids[0] != "b2" {
		t.Fatalf("extractor = %v", ids)
	}
}

// Surface and Context agree: only delivered inputs enter the context, in
// stream order with assistant and tool_result entries; a superseded tool
// result leaves the context.
func TestSurfaceAndContextFold(t *testing.T) {
	r := registry(t)
	content := jsonstable.MustParse(`{"text":"hi"}`)
	tr := ToolResult{ID: "r1", TurnID: "t1", CallID: "c1", Status: ToolUnknown, Parts: Parts{TextPart{Text: "lost"}}}
	tr.Digest, _ = DigestToolResult(&tr)
	tr2 := ToolResult{ID: "r2", TurnID: "t1", CallID: "c1", Status: ToolSuccess, Parts: Parts{TextPart{Text: "ok"}}}
	tr2.Digest, _ = DigestToolResult(&tr2)
	events := []struct {
		typ   session.EventType
		value any
	}{
		{TypeInputSubmitted, InputSubmittedPayload{InputID: "in-1", Content: content, SubmittedAtUnixMilli: 1}},
		{TypeInputSubmitted, InputSubmittedPayload{InputID: "in-2", Content: content, SubmittedAtUnixMilli: 2}},
		{TypeInputDelivered, InputDeliveredPayload{InputID: "in-1", TurnID: "t1"}},
		{TypeToolResult, ToolResultPayload{ToolResult: tr}},
		{TypeToolResult, ToolResultPayload{ToolResult: tr2}},
		{TypeToolResultSuperseded, ToolResultSupersededPayload{ToolResultID: "r1", ReplacementToolResultID: "r2"}},
	}
	var decoded []extension.DecodedEvent
	for i, e := range events {
		wire, _, err := r.Encode(e.typ, e.value)
		if err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
		d, err := r.Decode(session.SessionEvent{Type: e.typ, Payload: wire, Index: uint16(i)})
		if err != nil {
			t.Fatal(err)
		}
		decoded = append(decoded, d)
	}
	surfaceState, _ := SurfaceProjection.Initial()
	contextState, _ := ContextProjection.Initial()
	for _, d := range decoded {
		var err error
		if surfaceState, err = SurfaceProjection.Apply(surfaceState, d); err != nil {
			t.Fatal(err)
		}
		if contextState, err = ContextProjection.Apply(contextState, d); err != nil {
			t.Fatal(err)
		}
	}
	surface := surfaceState.(Surface)
	if surface.Inputs["in-1"].Status != InputDelivered || surface.Inputs["in-2"].Status != InputSubmitted {
		t.Fatalf("inputs = %+v", surface.Inputs)
	}
	if pending := surface.SubmittedInputs(); len(pending) != 1 || pending[0].ID != "in-2" {
		t.Fatalf("submitted = %+v", pending)
	}
	if len(surface.EntryOrder) != 3 || surface.Superseded["r1"] != "r2" {
		t.Fatalf("surface = %+v", surface)
	}
	entries := contextState.(Context).Entries
	if len(entries) != 2 || entries[0].Kind != EntryInput || entries[1].ID != "r2" {
		t.Fatalf("context = %+v", entries)
	}
	folded, err := ContextFold(decoded)
	if err != nil || len(folded) != 2 {
		t.Fatalf("ContextFold = %+v %v", folded, err)
	}
	// Delivering an input twice is a reducer error (CHT-EVT-2).
	if _, err := SurfaceProjection.Apply(surfaceState, decoded[2]); err == nil {
		t.Fatal("second delivery accepted")
	}
}
