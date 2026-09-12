package chatlog

import (
	"testing"

	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
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
	if replaced, _ := surface.Superseded.Get("r1"); len(surface.EntryOrder) != 3 || replaced != "r2" {
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

// EXT-COD-1: every registered event type's current codec is canonical
// round-trip stable — Encode, Decode, Encode reproduces the bytes.
func TestEventCodecCanonicalRoundTrip(t *testing.T) {
	assistant := Assistant{ID: "a1", TurnID: "t1", Parts: Parts{TextPart{Text: "hi"}, ReferencePart{BindingID: "b1"}}, SourceDigest: "sha256:src"}
	var err error
	if assistant.Digest, err = DigestAssistant(&assistant); err != nil {
		t.Fatal(err)
	}
	toolResult := ToolResult{ID: "tr1", TurnID: "t1", CallID: "c1", Status: ToolSuccess, Parts: Parts{TextPart{Text: "ok"}}, SourceDigest: "sha256:out"}
	if toolResult.Digest, err = DigestToolResult(&toolResult); err != nil {
		t.Fatal(err)
	}
	summary := Summary{ID: "sum1", Parts: Parts{TextPart{Text: "so far"}}}
	if summary.Digest, err = DigestSummary(&summary); err != nil {
		t.Fatal(err)
	}
	checkpoint := CheckpointCreatedPayload{CheckpointID: "ck1", CoveredThrough: 3, BaseContextDigest: "sha256:base",
		SummaryID: summary.ID, SummaryDigest: summary.Digest,
		Retained: []EntryDigestPair{{Kind: EntryAssistant, ID: "a1", Digest: assistant.Digest}}}
	if checkpoint.Digest, err = DigestCheckpoint(&checkpoint); err != nil {
		t.Fatal(err)
	}
	samples := map[session.EventType]any{
		TypeInputSubmitted:        InputSubmittedPayload{InputID: "in-1", Content: jsonstable.MustParse(`{"text":"hi"}`), SubmittedAtUnixMilli: 1},
		TypeInputDelivered:        InputDeliveredPayload{InputID: "in-1", TurnID: "t1"},
		TypeInputWithdrawn:        InputWithdrawnPayload{InputID: "in-1", Reason: "user"},
		TypeInputRejected:         InputRejectedPayload{InputID: "in-1"},
		TypeAssistant:             AssistantPayload{Assistant: assistant},
		TypeToolResult:            ToolResultPayload{ToolResult: toolResult},
		TypeToolResultSuperseded:  ToolResultSupersededPayload{ToolResultID: "tr1", ReplacementToolResultID: "tr2"},
		TypeSummary:               SummaryPayload{Summary: summary},
		TypeCheckpointCreated:     checkpoint,
		TypeCheckpointInvalidated: CheckpointInvalidatedPayload{CheckpointID: "ck1", Reason: "host"},
	}
	for _, def := range Module.Events {
		value, ok := samples[def.Type]
		if !ok {
			t.Fatalf("no sample for %s", def.Type)
		}
		codec := def.Codecs[def.Current]
		first, err := codec.Encode(value)
		if err != nil {
			t.Fatalf("%s: encode: %v", def.Type, err)
		}
		back, err := codec.Decode(first)
		if err != nil {
			t.Fatalf("%s: decode: %v", def.Type, err)
		}
		again, err := codec.Encode(back)
		if err != nil || !again.Equal(first) {
			t.Fatalf("%s: round trip changed bytes: %s vs %s (%v)", def.Type, first, again, err)
		}
	}
}

// --- checkpoint fold (CHT-EVT-3, CHT-CTX-2, CHT-SUR-1) --------------------------

type step struct {
	typ   session.EventType
	value any
}

// foldSteps encodes, decodes and folds steps through both projections,
// returning the states and the first fold error.
func foldSteps(t *testing.T, steps []step) (Context, Surface, error) {
	t.Helper()
	r := registry(t)
	surfaceState, _ := SurfaceProjection.Initial()
	contextState, _ := ContextProjection.Initial()
	for i, st := range steps {
		wire, _, err := r.Encode(st.typ, st.value)
		if err != nil {
			t.Fatalf("step %d encode: %v", i, err)
		}
		d, err := r.Decode(session.SessionEvent{Type: st.typ, Payload: wire, Seq: session.Seq(i)})
		if err != nil {
			t.Fatal(err)
		}
		nextSurface, err := SurfaceProjection.Apply(surfaceState, d)
		if err != nil {
			return contextState.(Context), surfaceState.(Surface), err
		}
		nextContext, err := ContextProjection.Apply(contextState, d)
		if err != nil {
			return contextState.(Context), nextSurface.(Surface), err
		}
		surfaceState, contextState = nextSurface, nextContext
	}
	return contextState.(Context), surfaceState.(Surface), nil
}

func mustSummary(t *testing.T, id SummaryID, text string) Summary {
	t.Helper()
	s := Summary{ID: id, Parts: Parts{TextPart{Text: text}}}
	var err error
	if s.Digest, err = DigestSummary(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

func mustAssistant(t *testing.T, id AssistantID, parts Parts) Assistant {
	t.Helper()
	a := Assistant{ID: id, TurnID: "t1", Parts: parts}
	var err error
	if a.Digest, err = DigestAssistant(&a); err != nil {
		t.Fatal(err)
	}
	return a
}

func mustCheckpoint(t *testing.T, id CheckpointID, covered session.Seq, base []EntryDigestPair, sum Summary, retained []EntryDigestPair) CheckpointCreatedPayload {
	t.Helper()
	baseDigest, err := DigestBaseContext(base)
	if err != nil {
		t.Fatal(err)
	}
	p := CheckpointCreatedPayload{CheckpointID: id, CoveredThrough: covered, BaseContextDigest: baseDigest,
		SummaryID: sum.ID, SummaryDigest: sum.Digest, Retained: retained}
	if p.Digest, err = DigestCheckpoint(&p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestCheckpointFold(t *testing.T) {
	content := jsonstable.MustParse(`{"text":"hi"}`)
	inDigest, err := DigestInput("in-1", content)
	if err != nil {
		t.Fatal(err)
	}
	a1 := mustAssistant(t, "a1", Parts{TextPart{Text: "one"}})
	sum := mustSummary(t, "sum1", "so far")
	base := []EntryDigestPair{{Kind: EntryInput, ID: "in-1", Digest: inDigest}, {Kind: EntryAssistant, ID: "a1", Digest: a1.Digest}}
	// Steps 0..4: delivered input (entry seq 1), assistant (seq 2), a queued
	// input that must survive compaction, the summary (seq 4).
	prefix := []step{
		{TypeInputSubmitted, InputSubmittedPayload{InputID: "in-1", Content: content, SubmittedAtUnixMilli: 1}},
		{TypeInputDelivered, InputDeliveredPayload{InputID: "in-1", TurnID: "t1"}},
		{TypeAssistant, AssistantPayload{Assistant: a1}},
		{TypeInputSubmitted, InputSubmittedPayload{InputID: "in-q", Content: content, SubmittedAtUnixMilli: 2}},
		{TypeSummary, SummaryPayload{Summary: sum}},
	}
	valid := mustCheckpoint(t, "ck1", 3, base, sum, base[1:])

	t.Run("valid checkpoint replaces the base and keeps the queue", func(t *testing.T) {
		a2 := mustAssistant(t, "a2", Parts{TextPart{Text: "after"}})
		ctxState, surf, err := foldSteps(t, append(prefix, step{TypeCheckpointCreated, valid}, step{TypeAssistant, AssistantPayload{Assistant: a2}}))
		if err != nil {
			t.Fatal(err)
		}
		entries := ctxState.Entries
		if len(entries) != 3 || entries[0].Kind != EntrySummary || entries[1].ID != "a1" || entries[2].ID != "a2" {
			t.Fatalf("entries = %+v", entries)
		}
		if _, pending := ctxState.Pending["in-q"]; !pending {
			t.Fatal("queued input compacted away")
		}
		if got := surf.SubmittedInputs(); len(got) != 1 || got[0].ID != "in-q" {
			t.Fatalf("surface queue = %+v", got)
		}
		if v, _ := surf.Checkpoints.Get("ck1"); v.Status != CheckpointActive {
			t.Fatalf("surface checkpoint = %+v", v)
		}
		if len(surf.EntryOrder) != 4 { // full history stays visible
			t.Fatalf("entry order = %+v", surf.EntryOrder)
		}
	})

	t.Run("invalidating the latest checkpoint restores base plus tail", func(t *testing.T) {
		a2 := mustAssistant(t, "a2", Parts{TextPart{Text: "after"}})
		ctxState, surf, err := foldSteps(t, append(prefix,
			step{TypeCheckpointCreated, valid},
			step{TypeAssistant, AssistantPayload{Assistant: a2}},
			step{TypeCheckpointInvalidated, CheckpointInvalidatedPayload{CheckpointID: "ck1", Reason: "host"}}))
		if err != nil {
			t.Fatal(err)
		}
		entries := ctxState.Entries
		if len(entries) != 3 || entries[0].ID != "in-1" || entries[1].ID != "a1" || entries[2].ID != "a2" {
			t.Fatalf("restored entries = %+v", entries)
		}
		if len(ctxState.Checkpoints) != 0 {
			t.Fatalf("checkpoint stack = %+v", ctxState.Checkpoints)
		}
		if v, _ := surf.Checkpoints.Get("ck1"); v.Status != CheckpointInvalidated || v.Reason != "host" {
			t.Fatalf("surface checkpoint = %+v", v)
		}
	})

	rejects := []struct {
		name  string
		steps []step
	}{
		{"covered through at or past the checkpoint row",
			append(prefix, step{TypeCheckpointCreated, mustCheckpoint(t, "ck2", 5, base, sum, nil)})},
		{"base context digest mismatch",
			append(prefix, step{TypeCheckpointCreated, mustCheckpoint(t, "ck3", 3, base[:1], sum, nil)})},
		{"retained outside the base",
			append(prefix, step{TypeCheckpointCreated, mustCheckpoint(t, "ck4", 3, base,
				sum, []EntryDigestPair{{Kind: EntryAssistant, ID: "a1", Digest: "sha256:wrong"}})})},
		{"gap holds more than the summary",
			append(append([]step{}, prefix...), step{TypeAssistant, AssistantPayload{Assistant: mustAssistant(t, "a9", Parts{TextPart{Text: "x"}})}},
				step{TypeCheckpointCreated, mustCheckpoint(t, "ck5", 3,
					append(base, EntryDigestPair{Kind: EntryAssistant, ID: "a9"}), sum, nil)})},
		{"invalidating an unknown checkpoint",
			append(prefix, step{TypeCheckpointInvalidated, CheckpointInvalidatedPayload{CheckpointID: "nope"}})},
	}
	for _, tc := range rejects {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := foldSteps(t, tc.steps); err == nil {
				t.Fatal("fold accepted")
			}
		})
	}

	t.Run("superseding a compacted result is rejected", func(t *testing.T) {
		call := Parts{ToolCallPart{CallID: "c1", Name: "lookup", Input: jsonstable.MustParse(`{}`)}}
		aCall := mustAssistant(t, "ac", call)
		r1 := ToolResult{ID: "r1", TurnID: "t1", CallID: "c1", Status: ToolSuccess, Parts: Parts{TextPart{Text: "ok"}}}
		if r1.Digest, err = DigestToolResult(&r1); err != nil {
			t.Fatal(err)
		}
		toolBase := []EntryDigestPair{{Kind: EntryAssistant, ID: "ac", Digest: aCall.Digest}, {Kind: EntryToolResult, ID: "r1", Digest: r1.Digest}}
		sum2 := mustSummary(t, "sum2", "tools done")
		steps := []step{
			{TypeAssistant, AssistantPayload{Assistant: aCall}},
			{TypeToolResult, ToolResultPayload{ToolResult: r1}},
			{TypeSummary, SummaryPayload{Summary: sum2}},
			{TypeCheckpointCreated, mustCheckpoint(t, "ck6", 1, toolBase, sum2, nil)},
			{TypeToolResultSuperseded, ToolResultSupersededPayload{ToolResultID: "r1", ReplacementToolResultID: "r2"}},
		}
		if _, _, err := foldSteps(t, steps); err == nil {
			t.Fatal("supersede of a compacted result accepted")
		}
	})
}
