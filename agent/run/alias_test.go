package run

import (
	"context"
	"testing"

	"github.com/memohai/twilight/sdk"
)

func TestMemoryRuntimeClonesInitialState(t *testing.T) {
	raw := []byte(`{"q":"hi"}`)
	payload, err := ParseCanonicalJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	rt := NewMemoryRuntime()
	newRun, err := BuildNewRun("run-1", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Create(context.Background(), newRun); err != nil {
		t.Fatal(err)
	}
	// Initial payload admission uses the actual AcceptInput transition, never a
	// synthetic non-Revision-0 initial state.
	acceptInput(t, rt, "run-1", AgentInput{ID: "seed", Payload: payload})

	copy(raw, []byte(`{"q":"no"}`))
	payload = cj(`{"q":"mutated"}`)

	snap, err := rt.Load(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if got := snap.State.PendingInputs[0].Payload.String(); got != `{"q":"hi"}` {
		t.Fatalf("runtime initial payload aliased caller state: %s", got)
	}
	diverged, err := rt.Rebuild("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if diverged {
		t.Fatal("rebuild diverged after caller mutated initial state")
	}
}

func TestCommitSnapshotsCommandPayloadBeforeFoldingState(t *testing.T) {
	rt := newTestRuntime(t)
	raw := []byte(`{"v":"one"}`)
	payload, err := ParseCanonicalJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	cmdID := DeriveInputCommandID("run-1", "in-1")
	snapshot, err := rt.Load(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	mustCommit(t, rt, cmdID, snapshot.Revision, "", AcceptInput{Input: AgentInput{ID: "in-1", Payload: payload}})

	copy(raw, []byte(`{"v":"two"}`))

	snap, err := rt.Load(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, in := range snap.State.PendingInputs {
		if in.ID == "in-1" {
			found = true
			if got := in.Payload.String(); got != `{"v":"one"}` {
				t.Fatalf("state payload aliased command buffer: %s", got)
			}
		}
	}
	if !found {
		t.Fatal("accepted input not found")
	}

	events := recordEvents(t, rt, "run-1")
	for _, e := range events {
		if f, ok := e.Fact.(InputAccepted); ok && f.Input.ID == "in-1" {
			if got := f.Input.Payload.String(); got != `{"v":"one"}` {
				t.Fatalf("event payload aliased command buffer: %s", got)
			}
		}
	}
	diverged, err := rt.Rebuild("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if diverged {
		t.Fatal("state diverged from log after command buffer mutation")
	}
}

func TestCommitCanonicalizesAgentOwnedJSONBeforePersisting(t *testing.T) {
	rt := newTestRuntime(t)
	snap, _ := rt.Load(context.Background(), "run-1")
	req := ModelRequest{
		Model: "m-1",
		ProviderOptions: map[string]CanonicalJSON{
			"p": cj(`{"b":2,"a":1}`),
		},
	}
	reqDigest, err := DigestRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	toolsDigest, err := DigestToolSpecs(nil)
	if err != nil {
		t.Fatal(err)
	}
	model := ModelRef(req.Model)
	binding, err := DigestModelStepBinding(model, reqDigest, toolsDigest)
	if err != nil {
		t.Fatal(err)
	}
	cmdID := DeriveModelRequestCommandID(snap.State.RunID, snap.Revision)
	ids := make([]InputID, len(snap.State.PendingInputs))
	for i, in := range snap.State.PendingInputs {
		ids[i] = in.ID
	}
	res := mustCommit(t, rt, cmdID, snap.Revision, "", PrepareModelRequest{
		StepID:        DeriveModelStepID(snap.State.RunID, cmdID, binding),
		Model:         model,
		Request:       req,
		RequestDigest: reqDigest,
		InputIDs:      ids,
		ToolsDigest:   toolsDigest,
	})

	ms := res.Snapshot.State.Current.(ModelStep)
	if got := ms.Request.ProviderOptions["p"].String(); got != `{"a":1,"b":2}` {
		t.Fatalf("snapshot stored non-canonical provider option: %s", got)
	}
	for _, e := range recordEvents(t, rt, "run-1") {
		if f, ok := e.Fact.(ModelStepPrepared); ok {
			if got := f.Request.ProviderOptions["p"].String(); got != `{"a":1,"b":2}` {
				t.Fatalf("event stored non-canonical provider option: %s", got)
			}
		}
	}
}

func TestLoadSnapshotDoesNotAliasFrozenRequest(t *testing.T) {
	rt := newTestRuntime(t)
	meta := map[string]any{"provider": map[string]any{"sig": "s1"}}
	req := sdk.Request{
		Model: "m-1",
		Messages: []sdk.Message{{
			Role: sdk.MessageRoleUser,
			Content: []sdk.MessagePart{sdk.TextPart{
				Text:             "hi",
				ProviderMetadata: meta,
			}},
		}},
	}
	snap, _ := rt.Load(context.Background(), "run-1")
	prep, cmdID := buildPrepareFromSnap(t, snap, req, nil)
	mustCommit(t, rt, cmdID, snap.Revision, "", prep)

	snap, err := rt.Load(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	ms := snap.State.Current.(ModelStep)
	part := ms.Request.Messages[0].Content[0]
	part.Text = "edited"
	part.ProviderMetadata["provider"] = cj(`{"sig":"bad"}`)
	part.ProviderMetadata["new"] = cj(`"bad"`)
	ms.Request.Messages[0].Content[0] = part

	snap, err = rt.Load(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	got := snap.State.Current.(ModelStep).Request.Messages[0].Content[0]
	if got.Text != "hi" {
		t.Fatalf("request content aliased Load snapshot: %q", got.Text)
	}
	if sig := got.ProviderMetadata["provider"].String(); sig != `{"sig":"s1"}` {
		t.Fatalf("request metadata aliased Load snapshot: %v", sig)
	}
	if _, ok := got.ProviderMetadata["new"]; ok {
		t.Fatal("request metadata accepted mutation from Load snapshot")
	}
}

func TestCommitResultEventsDoNotAliasStateOrLog(t *testing.T) {
	rt, stepID, grant := preparedRuntime(t, nil, nil)
	result := sdk.ModelResult{
		Text:         "ok",
		FinishReason: sdk.FinishReasonStop,
		ReasoningParts: []sdk.ReasoningPart{{
			ID:               "r1",
			Text:             "why",
			Format:           sdk.ReasoningFormatAnthropic,
			ProviderMetadata: map[string]any{"anthropic": map[string]any{"signature": "s1"}},
		}},
		TextProviderMetadata: map[string]any{"google": map[string]any{"thoughtSignature": "g1"}},
		Sources: []sdk.Source{{
			SourceType:       "url",
			ID:               "src-1",
			URL:              "https://example.test",
			ProviderMetadata: map[string]any{"p": "v"},
		}},
		Response: &sdk.ResponseMetadata{ID: "resp-1", Headers: map[string]string{"h": "v"}},
	}
	frozen, err := FreezeModelResult(result)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := rt.Load(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	res := mustCommit(t, rt, "done-1", snapshot.Revision, grant, SubmitModelResult{StepID: stepID, Result: frozen})

	fact := res.Events[0].Fact.(ModelStepCompleted)
	fact.Result.ReasoningParts[0].ProviderMetadata["anthropic"] = cj(`{"signature":"bad"}`)
	fact.Result.TextProviderMetadata["google"] = cj(`{"thoughtSignature":"bad"}`)
	fact.Result.Sources[0].ProviderMetadata["p"] = cj(`"bad"`)
	fact.Result.Response.Headers["h"] = "bad"
	res.Events[0].Fact = fact

	snap, err := rt.Load(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	last := snap.State.LastModelResult
	if last == nil {
		t.Fatal("missing LastModelResult")
	}
	if sig := last.ReasoningParts[0].ProviderMetadata["anthropic"].String(); sig != `{"signature":"s1"}` {
		t.Fatalf("state reasoning metadata aliased returned event: %v", sig)
	}
	if sig := last.TextProviderMetadata["google"].String(); sig != `{"thoughtSignature":"g1"}` {
		t.Fatalf("state text metadata aliased returned event: %v", sig)
	}
	if p := last.Sources[0].ProviderMetadata["p"].String(); p != `"v"` {
		t.Fatalf("state source metadata aliased returned event: %v", p)
	}
	if h := last.Response.Headers["h"]; h != "v" {
		t.Fatalf("state response headers aliased returned event: %v", h)
	}

	for _, e := range recordEvents(t, rt, "run-1") {
		if f, ok := e.Fact.(ModelStepCompleted); ok {
			if sig := f.Result.ReasoningParts[0].ProviderMetadata["anthropic"].String(); sig != `{"signature":"s1"}` {
				t.Fatalf("log reasoning metadata aliased returned event: %v", sig)
			}
			if h := f.Result.Response.Headers["h"]; h != "v" {
				t.Fatalf("log response headers aliased returned event: %v", h)
			}
		}
	}
	diverged, err := rt.Rebuild("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if diverged {
		t.Fatal("state diverged from log after returned event mutation")
	}
}
