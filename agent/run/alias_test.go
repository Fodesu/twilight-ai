package run

import (
	"context"
	"testing"

	"github.com/memohai/twilight/sdk"
)

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
	diverged, err := rebuildRun(t, rt, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if diverged {
		t.Fatal("state diverged from log after command buffer mutation")
	}
}

// The frozen request body is stored canonically in the FrozenValueStore and
// named by digest in the fact (RUN-WIR-4).
func TestCommitCanonicalizesFrozenRequestBeforePersisting(t *testing.T) {
	rt := newTestRuntime(t)
	snap, _ := rt.Load(context.Background(), "run-1")
	req := ModelRequest{
		Model: "m-1",
		ProviderOptions: map[string]CanonicalJSON{
			"p": cj(`{"b":2,"a":1}`),
		},
	}
	reqDigest, err := ProtocolV1().DigestRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	toolsDigest, err := ProtocolV1().DigestToolSpecs(nil)
	if err != nil {
		t.Fatal(err)
	}
	model := ModelRef(req.Model)
	binding, err := ProtocolV1().DigestModelStepBinding(model, reqDigest, toolsDigest)
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
	if ms.RequestDigest != reqDigest {
		t.Fatalf("snapshot RequestDigest = %s, want %s", ms.RequestDigest, reqDigest)
	}
	stored, err := rt.FrozenRequest(context.Background(), ms.RequestDigest)
	if err != nil {
		t.Fatal(err)
	}
	if got := stored.ProviderOptions["p"].String(); got != `{"a":1,"b":2}` {
		t.Fatalf("frozen store returned non-canonical provider option: %s", got)
	}
	for _, e := range recordEvents(t, rt, "run-1") {
		if f, ok := e.Fact.(ModelStepPrepared); ok && f.RequestDigest != reqDigest {
			t.Fatalf("event RequestDigest = %s, want %s", f.RequestDigest, reqDigest)
		}
	}
	if _, err := rt.FrozenRequest(context.Background(), "sha256:missing"); err == nil {
		t.Fatal("unknown digest returned a request")
	}
}

func TestFrozenRequestDoesNotAliasStoredBody(t *testing.T) {
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

	first, err := rt.FrozenRequest(context.Background(), prep.RequestDigest)
	if err != nil {
		t.Fatal(err)
	}
	part := first.Messages[0].Content[0]
	part.Text = "edited"
	part.ProviderMetadata["provider"] = cj(`{"sig":"bad"}`)
	part.ProviderMetadata["new"] = cj(`"bad"`)
	first.Messages[0].Content[0] = part

	second, err := rt.FrozenRequest(context.Background(), prep.RequestDigest)
	if err != nil {
		t.Fatal(err)
	}
	got := second.Messages[0].Content[0]
	if got.Text != "hi" {
		t.Fatalf("request content aliased a previous read: %q", got.Text)
	}
	if sig := got.ProviderMetadata["provider"].String(); sig != `{"sig":"s1"}` {
		t.Fatalf("request metadata aliased a previous read: %v", sig)
	}
	if _, ok := got.ProviderMetadata["new"]; ok {
		t.Fatal("request metadata accepted mutation from a previous read")
	}
}

// ModelStepCompleted carries the digest of the frozen result and its usage;
// the result body itself never enters state or log (RUN-WIR-4).
func TestModelStepCompletedCarriesResultDigestOnly(t *testing.T) {
	rt, stepID, grant := preparedRuntime(t, nil, nil)
	result := sdk.ModelResult{
		Text:         "ok",
		FinishReason: sdk.FinishReasonStop,
		Usage:        sdk.Usage{InputTokens: 2, OutputTokens: 3, TotalTokens: 5},
		ReasoningParts: []sdk.ReasoningPart{{
			ID:               "r1",
			Text:             "why",
			Format:           sdk.ReasoningFormatAnthropic,
			ProviderMetadata: map[string]any{"anthropic": map[string]any{"signature": "s1"}},
		}},
		Response: &sdk.ResponseMetadata{ID: "resp-1", Headers: map[string]string{"h": "v"}},
	}
	frozen, err := FreezeModelResult(result)
	if err != nil {
		t.Fatal(err)
	}
	wantDigest, err := ProtocolV1().DigestModelResult(frozen)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := rt.Load(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	res := mustCommit(t, rt, "done-1", snapshot.Revision, grant, SubmitModelResult{StepID: stepID, Result: frozen})

	fact := res.Events[0].Fact.(ModelStepCompleted)
	if fact.ResultDigest != wantDigest || fact.Usage.TotalTokens != 5 || fact.FinishReason != FinishReasonStop {
		t.Fatalf("completed = %+v", fact)
	}
	if res.Snapshot.State.Usage.TotalTokens != 5 {
		t.Fatalf("usage = %+v", res.Snapshot.State.Usage)
	}
	if res.Snapshot.State.Status != RunCompleted {
		t.Fatalf("status = %v, want completed", res.Snapshot.State.Status)
	}
	// Mutating the caller's frozen result after commit changes nothing the
	// authority holds: the digest was taken before the transition.
	frozen.Text = "changed"
	diverged, err := rebuildRun(t, rt, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if diverged {
		t.Fatal("state diverged from log")
	}
}
