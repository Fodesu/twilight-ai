package agent

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/memohai/twilight-ai/sdk"
)

func TestMemoryRuntimeClonesInitialState(t *testing.T) {
	payload := json.RawMessage(`{"q":"hi"}`)
	initial, err := Initialize("run-1", testConfig(), NextRun(AgentInput{ID: "seed", Payload: payload}))
	if err != nil {
		t.Fatal(err)
	}
	rt := NewMemoryRuntime(initial)

	copy(payload, []byte(`{"q":"no"}`))
	initial.PendingInputs[0].Payload[6] = 'x'

	snap, err := rt.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := string(snap.State.PendingInputs[0].Payload); got != `{"q":"hi"}` {
		t.Fatalf("runtime initial payload aliased caller state: %s", got)
	}
	diverged, err := rt.Rebuild()
	if err != nil {
		t.Fatal(err)
	}
	if diverged {
		t.Fatal("rebuild diverged after caller mutated initial state")
	}
}

func TestCommitSnapshotsCommandPayloadBeforeFoldingState(t *testing.T) {
	rt := newTestRuntime(t, RunConfig{Model: "m-1"})
	payload := json.RawMessage(`{"v":"one"}`)
	cmdID := DeriveInputCommandID("run-1", "in-1")
	mustCommit(t, rt, cmdID, 0, "", AcceptInput{Input: AgentInput{ID: "in-1", Payload: payload}})

	copy(payload, []byte(`{"v":"two"}`))

	snap, err := rt.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, in := range snap.State.PendingInputs {
		if in.ID == "in-1" {
			found = true
			if got := string(in.Payload); got != `{"v":"one"}` {
				t.Fatalf("state payload aliased command buffer: %s", got)
			}
		}
	}
	if !found {
		t.Fatal("accepted input not found")
	}

	events := rt.Events()
	for _, e := range events {
		if f, ok := e.Fact.(InputAccepted); ok && f.Input.ID == "in-1" {
			if got := string(f.Input.Payload); got != `{"v":"one"}` {
				t.Fatalf("event payload aliased command buffer: %s", got)
			}
		}
	}
	diverged, err := rt.Rebuild()
	if err != nil {
		t.Fatal(err)
	}
	if diverged {
		t.Fatal("state diverged from log after command buffer mutation")
	}
}

func TestCommitCanonicalizesAgentOwnedJSONBeforePersisting(t *testing.T) {
	rt := newTestRuntime(t, RunConfig{Model: "m-1"})
	snap, _ := rt.Load(context.Background())
	req := ModelRequest{
		Model: "m-1",
		ProviderOptions: map[string]json.RawMessage{
			"p": json.RawMessage(`{"b":2,"a":1}`),
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
	binding, err := DigestModelStepBinding(snap.State.Config.Model, reqDigest, toolsDigest)
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
		Model:         snap.State.Config.Model,
		Request:       req,
		RequestDigest: reqDigest,
		InputIDs:      ids,
		ToolsDigest:   toolsDigest,
	})

	ms := res.Snapshot.State.Current.(ModelStep)
	if got := string(ms.Request.ProviderOptions["p"]); got != `{"a":1,"b":2}` {
		t.Fatalf("snapshot stored non-canonical provider option: %s", got)
	}
	for _, e := range rt.Events() {
		if f, ok := e.Fact.(ModelStepPrepared); ok {
			if got := string(f.Request.ProviderOptions["p"]); got != `{"a":1,"b":2}` {
				t.Fatalf("event stored non-canonical provider option: %s", got)
			}
		}
	}
}

func TestLoadSnapshotDoesNotAliasFrozenRequest(t *testing.T) {
	rt := newTestRuntime(t, RunConfig{Model: "m-1"})
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
	snap, _ := rt.Load(context.Background())
	prep, cmdID := buildPrepareFromSnap(t, snap, req, nil)
	mustCommit(t, rt, cmdID, snap.Revision, "", prep)

	snap, err := rt.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ms := snap.State.Current.(ModelStep)
	part := ms.Request.Messages[0].Content[0]
	part.Text = "edited"
	part.ProviderMetadata["provider"] = json.RawMessage(`{"sig":"bad"}`)
	part.ProviderMetadata["new"] = json.RawMessage(`"bad"`)
	ms.Request.Messages[0].Content[0] = part

	snap, err = rt.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := snap.State.Current.(ModelStep).Request.Messages[0].Content[0]
	if got.Text != "hi" {
		t.Fatalf("request content aliased Load snapshot: %q", got.Text)
	}
	if sig := string(got.ProviderMetadata["provider"]); sig != `{"sig":"s1"}` {
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
	res := mustCommit(t, rt, "done-1", 2, grant, SubmitModelResult{StepID: stepID, Result: frozen})

	fact := res.Events[0].Fact.(ModelStepCompleted)
	fact.Result.ReasoningParts[0].ProviderMetadata["anthropic"] = json.RawMessage(`{"signature":"bad"}`)
	fact.Result.TextProviderMetadata["google"] = json.RawMessage(`{"thoughtSignature":"bad"}`)
	fact.Result.Sources[0].ProviderMetadata["p"] = json.RawMessage(`"bad"`)
	fact.Result.Response.Headers["h"] = "bad"
	res.Events[0].Fact = fact

	snap, err := rt.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	last := snap.State.LastModelResult
	if last == nil {
		t.Fatal("missing LastModelResult")
	}
	if sig := string(last.ReasoningParts[0].ProviderMetadata["anthropic"]); sig != `{"signature":"s1"}` {
		t.Fatalf("state reasoning metadata aliased returned event: %v", sig)
	}
	if sig := string(last.TextProviderMetadata["google"]); sig != `{"thoughtSignature":"g1"}` {
		t.Fatalf("state text metadata aliased returned event: %v", sig)
	}
	if p := string(last.Sources[0].ProviderMetadata["p"]); p != `"v"` {
		t.Fatalf("state source metadata aliased returned event: %v", p)
	}
	if h := last.Response.Headers["h"]; h != "v" {
		t.Fatalf("state response headers aliased returned event: %v", h)
	}

	for _, e := range rt.Events() {
		if f, ok := e.Fact.(ModelStepCompleted); ok {
			if sig := string(f.Result.ReasoningParts[0].ProviderMetadata["anthropic"]); sig != `{"signature":"s1"}` {
				t.Fatalf("log reasoning metadata aliased returned event: %v", sig)
			}
			if h := f.Result.Response.Headers["h"]; h != "v" {
				t.Fatalf("log response headers aliased returned event: %v", h)
			}
		}
	}
	diverged, err := rt.Rebuild()
	if err != nil {
		t.Fatal(err)
	}
	if diverged {
		t.Fatal("state diverged from log after returned event mutation")
	}
}
