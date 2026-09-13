package run

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestCommandEnvelopeJSONRoundTripRestoresVariants(t *testing.T) {
	commands := []AgentCommand{
		PrepareModelRequest{StepID: "s", Model: "m", Request: ModelRequest{Model: "m"}, RequestDigest: "sha256:req", ToolsDigest: "sha256:tools"},
		WithdrawPreparedStep{StepID: "s"},
		StartModelExecution{StepID: "s", Claim: "claim-s"},
		RecoverModelExecution{StepID: "s", Claim: "claim-s"},
		SubmitModelResult{StepID: "s", Result: ModelResult{Text: "ok"}},
		SubmitModelFailure{StepID: "s", Failure: StepFailure{Class: FailureProvider, Message: "down"}},
		RejectModelResult{StepID: "s", Usage: Usage{TotalTokens: 1}, Failure: StepFailure{Class: FailureMalformedModel}},
		StartToolCall{StepID: "ts", CallID: "c", Claim: "claim-c"},
		SubmitToolResult{StepID: "ts", CallID: "c", Result: ToolExecutionResult{Output: cj(`{"ok":true}`)}},
		SubmitToolFailure{StepID: "ts", CallID: "c", Failure: ToolFailure{Class: FailureExecution}, Outcome: ToolOutcomeKnown},
		ApproveToolCall{StepID: "ts", CallID: "c", ResponseID: "r", ResponseDigest: "sha256:resp"},
		RejectToolCall{StepID: "ts", CallID: "c", ResponseID: "r", ResponseDigest: "sha256:resp", Reason: "no"},
		SubmitToolResponse{StepID: "ts", CallID: "c", ResponseID: "r", ResponseDigest: "sha256:resp", Payload: cj(`{"answer":1}`)},
		CancelRun{},
		NextStep(AgentInput{ID: "in", Payload: cj(`{"q":"hi"}`)}),
	}
	for _, cmd := range commands {
		env, err := ProtocolV1().BuildEnvelope("s-1", "run-1", CommandID("cmd-"+commandType(cmd)), cmd)
		if err != nil {
			t.Fatalf("ProtocolV1().BuildEnvelope(%T): %v", cmd, err)
		}
		raw, err := json.Marshal(env)
		if err != nil {
			t.Fatalf("Marshal(%T): %v", cmd, err)
		}
		decoded, err := DecodeCommandEnvelope(raw)
		if err != nil {
			t.Fatalf("DecodeCommandEnvelope(%T): %v\n%s", cmd, err, raw)
		}
		if reflect.TypeOf(decoded.Command) != reflect.TypeOf(cmd) {
			t.Fatalf("decoded command type = %T, want %T", decoded.Command, cmd)
		}
		if decoded.Type != env.Type || decoded.ID != env.ID {
			t.Fatalf("decoded envelope = %+v, want %+v", decoded, env)
		}
	}
}

// Every fact variant round-trips through the v1 fact codec: canonical bytes
// decode back to the same variant and re-encode to the same bytes.
func TestFactCodecRoundTripRestoresVariants(t *testing.T) {
	facts := []Fact{
		RunCreated{SchemaVersion: SchemaVersion1, RunID: "run-1", Owner: "turn-1", Attempt: 1, CausationID: "cause"},
		ModelStepPrepared{StepID: "s", Model: "m", RequestDigest: "sha256:req", ToolsDigest: "sha256:tools", BindingDigest: "sha256:binding"},
		ModelStepWithdrawn{StepID: "s"},
		ModelStepStarted{StepID: "s", Claim: "claim-m"},
		ModelStepRecovered{StepID: "s"},
		ModelStepRejected{StepID: "s", Usage: Usage{TotalTokens: 1}, Failure: StepFailure{Class: FailureMalformedModel}},
		ModelStepCompleted{StepID: "s", Usage: Usage{TotalTokens: 1}, FinishReason: FinishReasonStop, ResultDigest: "sha256:result"},
		ToolStepOpened{StepID: "ts", Source: "s", BindingSetDigest: "sha256:set", Calls: []ToolCallBinding{{CallID: "c", ToolRef: "t", BindingDigest: "sha256:binding", Arguments: cj(`{}`), Policy: DirectExecution}}},
		ToolCallStarted{StepID: "ts", CallID: "c", Claim: "claim-t"},
		ToolCallApproved{StepID: "ts", CallID: "c", ResponseID: "r", ResponseDigest: "sha256:resp"},
		ToolCallCompleted{StepID: "ts", CallID: "c", OutputDigest: "sha256:output"},
		ToolCallAnswered{StepID: "ts", CallID: "c", ResponseID: "r", ResponseDigest: "sha256:resp"},
		ToolCallFailed{StepID: "ts", CallID: "c", Failure: ToolFailure{Class: FailureExecution}, Outcome: ToolOutcomeKnown},
		InputAccepted{Input: AgentInput{ID: "in", Payload: cj(`{"q":"hi"}`)}},
		RunEnded{End: RunCompletedEnd{}},
	}
	for _, fact := range facts {
		typ := factType(fact)
		raw, err := marshalCanonical(fact)
		if err != nil {
			t.Fatalf("marshal(%T): %v", fact, err)
		}
		decoded, err := ProtocolV1().DecodeFact(typ, raw)
		if err != nil {
			t.Fatalf("DecodeFact(%T): %v\n%s", fact, err, raw)
		}
		if reflect.TypeOf(decoded) != reflect.TypeOf(fact) {
			t.Fatalf("decoded fact type = %T, want %T", decoded, fact)
		}
		again, err := marshalCanonical(decoded)
		if err != nil || string(again) != string(raw) {
			t.Fatalf("re-encode of %T differs:\n%s\n%s", fact, raw, again)
		}
		if _, err := ProtocolV1().DecodeFact("unknown", raw); err == nil {
			t.Fatalf("unknown fact type decoded for %T", fact)
		}
	}
}

func TestWireCodecRejectsAmbiguousJSONBeforeVariantDecode(t *testing.T) {
	cmd := NextStep(AgentInput{ID: "in", Payload: cj(`1`)})
	env, err := ProtocolV1().BuildEnvelope("s-1", "run-1", DeriveInputCommandID("run-1", "in"), cmd)
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte(fmt.Sprintf(`{"schemaVersion":1,"type":"accept_input","runId":"run-1","id":%q,"command":{"inputs":[{"id":"in","payload":1}],"inputs":[{"id":"in","payload":1}]}}`, env.ID))
	if _, err := DecodeCommandEnvelope(raw); err == nil {
		t.Fatal("duplicate key command decoded")
	}

	canonical, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	wrongCase := strings.Replace(string(canonical), `"command":`, `"Command":`, 1)
	if _, err := DecodeCommandEnvelope([]byte(wrongCase)); err == nil {
		t.Fatal("case-insensitive command field decoded")
	}
}

func TestWireCodecRejectsUnknownType(t *testing.T) {
	env, err := ProtocolV1().BuildEnvelope("s-1", "run-1", "cmd-1", CancelRun{})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	badType := strings.Replace(string(raw), `"type":"cancel_run"`, `"type":"unknown"`, 1)
	if _, err := DecodeCommandEnvelope([]byte(badType)); err == nil {
		t.Fatal("unknown command type decoded")
	}
}

func TestRunEndedTaggedUnionRejectsInvalidValues(t *testing.T) {
	for name, fact := range map[string]RunEnded{
		"nil end":                {},
		"stopped without reason": {End: RunStoppedEnd{}},
		"failed without class":   {End: RunFailedEnd{Reason: ReasonProviderFailure}},
		"unknown end variant":    {End: fakeRunEnd{}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ProtocolV1().DigestFact("run_ended", fact); err == nil {
				t.Fatal("invalid tagged terminal value was accepted")
			}
		})
	}
}

type fakeRunEnd struct{}

func (fakeRunEnd) runEnd() {}

// RunEnded wire is a tagged union: exactly one variant key.
func TestRunEndedWireIsTaggedUnion(t *testing.T) {
	cases := map[string]RunEnded{
		"completed": {End: RunCompletedEnd{}},
		"stopped":   {End: RunStoppedEnd{Reason: ReasonCancelled, UncertainCalls: []CallID{"c1"}}},
		"failed":    {End: RunFailedEnd{Reason: ReasonProviderFailure, Failure: RunFailure{Class: FailureProvider}}},
	}
	for key, fact := range cases {
		raw, err := json.Marshal(fact)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]json.RawMessage
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		if len(m) != 1 || m[key] == nil {
			t.Fatalf("%s wire = %s, want single %q key", key, raw, key)
		}
		var back RunEnded
		if err := json.Unmarshal(raw, &back); err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		if fmt.Sprint(back.End) != fmt.Sprint(fact.End) {
			t.Fatalf("%s round trip = %+v, want %+v", key, back.End, fact.End)
		}
	}
	for name, raw := range map[string]string{
		"no variant":        `{}`,
		"two variants":      `{"completed":{},"stopped":{"reason":"cancelled"}}`,
		"legacy flat":       `{"status":1}`,
		"stopped no reason": `{"stopped":{}}`,
	} {
		var back RunEnded
		if err := json.Unmarshal([]byte(raw), &back); err == nil {
			t.Fatalf("%s accepted: %s", name, raw)
		}
	}
}
