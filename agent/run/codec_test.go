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
		AcceptInput{Input: AgentInput{ID: "in", Payload: cj(`{"q":"hi"}`)}},
	}
	for _, cmd := range commands {
		env, err := BuildEnvelope("run-1", CommandID("cmd-"+commandType(cmd)), cmd)
		if err != nil {
			t.Fatalf("BuildEnvelope(%T): %v", cmd, err)
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
		if decoded.Type != env.Type || decoded.Digest != env.Digest || decoded.ID != env.ID {
			t.Fatalf("decoded envelope = %+v, want %+v", decoded, env)
		}
	}
}

func TestAgentEventJSONRoundTripRestoresVariants(t *testing.T) {
	facts := []Fact{
		ModelStepPrepared{StepID: "s", Model: "m", Request: ModelRequest{Model: "m"}, RequestDigest: "sha256:req", ToolsDigest: "sha256:tools", BindingDigest: "sha256:binding"},
		ModelStepStarted{StepID: "s"},
		ModelStepRecovered{StepID: "s"},
		ModelStepRejected{StepID: "s", Usage: Usage{TotalTokens: 1}, Failure: StepFailure{Class: FailureMalformedModel}},
		ModelStepCompleted{StepID: "s", Result: ModelResult{Text: "ok"}},
		ToolStepOpened{StepID: "ts", Source: "s", BindingSetDigest: "sha256:set", Calls: []ToolCallBinding{{CallID: "c", ToolRef: "t", BindingDigest: "sha256:binding", Arguments: cj(`{}`), Policy: DirectExecution}}},
		ToolCallStarted{StepID: "ts", CallID: "c"},
		ToolCallApproved{StepID: "ts", CallID: "c", ResponseID: "r", ResponseDigest: "sha256:resp"},
		ToolCallCompleted{StepID: "ts", CallID: "c", Result: ToolExecutionResult{Output: cj(`{"ok":true}`)}},
		ToolCallAnswered{StepID: "ts", CallID: "c", ResponseID: "r", ResponseDigest: "sha256:resp", Payload: cj(`{"answer":1}`)},
		ToolCallFailed{StepID: "ts", CallID: "c", Failure: ToolFailure{Class: FailureExecution}, Outcome: ToolOutcomeKnown},
		InputAccepted{Input: AgentInput{ID: "in", Payload: cj(`{"q":"hi"}`)}},
		RunEnded{End: RunCompletedEnd{}},
	}
	for i, fact := range facts {
		typ := factType(fact)
		digest, err := DigestFact(currentSchemaVersion, typ, fact)
		if err != nil {
			t.Fatalf("DigestFact(%T): %v", fact, err)
		}
		event := AgentEvent{
			SchemaVersion: currentSchemaVersion,
			Type:          typ,
			RunID:         "run-1",
			Revision:      uint64(i + 1),
			Index:         0,
			CommandID:     CommandID("cmd"),
			CommandDigest: Digest("sha256:cmd"),
			Digest:        digest,
			Fact:          fact,
		}
		raw, err := json.Marshal(event)
		if err != nil {
			t.Fatalf("Marshal(%T): %v", fact, err)
		}
		decoded, err := DecodeAgentEvent(raw)
		if err != nil {
			t.Fatalf("DecodeAgentEvent(%T): %v\n%s", fact, err, raw)
		}
		if reflect.TypeOf(decoded.Fact) != reflect.TypeOf(fact) {
			t.Fatalf("decoded fact type = %T, want %T", decoded.Fact, fact)
		}
		if decoded.Type != event.Type || decoded.Digest != event.Digest || decoded.Revision != event.Revision {
			t.Fatalf("decoded event = %+v, want %+v", decoded, event)
		}
	}
}

func TestTransitionRecordJSONRoundTripRestoresVariants(t *testing.T) {
	facts := []Fact{
		ModelStepCompleted{StepID: "s", Result: ModelResult{Text: "ok"}},
		RunEnded{End: RunCompletedEnd{}},
	}
	events := make([]AgentEvent, len(facts))
	for i, fact := range facts {
		typ := factType(fact)
		digest, err := DigestFact(currentSchemaVersion, typ, fact)
		if err != nil {
			t.Fatal(err)
		}
		events[i] = AgentEvent{
			SchemaVersion: currentSchemaVersion,
			Type:          typ,
			RunID:         "run-1",
			Revision:      1,
			Index:         uint16(i),
			CommandID:     "cmd-1",
			CommandDigest: "sha256:cmd",
			Digest:        digest,
			Fact:          fact,
		}
	}
	record, err := BuildTransitionRecord(events)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeTransitionRecord(raw)
	if err != nil {
		t.Fatalf("DecodeTransitionRecord: %v\n%s", err, raw)
	}
	if decoded.TransitionDigest != record.TransitionDigest || len(decoded.Events) != len(record.Events) {
		t.Fatalf("decoded transition = %+v, want %+v", decoded, record)
	}
	for i := range decoded.Events {
		if reflect.TypeOf(decoded.Events[i].Fact) != reflect.TypeOf(record.Events[i].Fact) {
			t.Fatalf("decoded event %d fact type = %T, want %T", i, decoded.Events[i].Fact, record.Events[i].Fact)
		}
	}

	partial := cloneTransitionRecord(&record)
	partial.Events = partial.Events[:1]
	if err := ValidateTransitionRecord(&partial); err == nil {
		t.Fatal("partial transition validated")
	}
}

func TestWireCodecRejectsAmbiguousJSONBeforeVariantDecode(t *testing.T) {
	cmd := AcceptInput{Input: AgentInput{ID: "in", Payload: cj(`1`)}}
	env, err := BuildEnvelope("run-1", DeriveInputCommandID("run-1", "in"), cmd)
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte(fmt.Sprintf(`{"schemaVersion":1,"type":"accept_input","runId":"run-1","id":%q,"digest":%q,"command":{"input":{"id":"in","payload":1},"input":{"id":"in","payload":1}}}`, env.ID, env.Digest))
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

func TestWireCodecRejectsUnknownTypeAndDigestMismatch(t *testing.T) {
	env, err := BuildEnvelope("run-1", "cmd-1", CancelRun{})
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
	badDigest := strings.Replace(string(raw), string(env.Digest), "sha256:bad", 1)
	if _, err := DecodeCommandEnvelope([]byte(badDigest)); err == nil {
		t.Fatal("bad command digest decoded")
	}

	fact := RunEnded{End: RunCompletedEnd{}}
	digest, err := DigestFact(currentSchemaVersion, factType(fact), fact)
	if err != nil {
		t.Fatal(err)
	}
	event := AgentEvent{SchemaVersion: currentSchemaVersion, Type: factType(fact), RunID: "run-1", Revision: 1, CommandID: "cmd", CommandDigest: env.Digest, Digest: digest, Fact: fact}
	raw, err = json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	badEventDigest := strings.Replace(string(raw), string(digest), "sha256:bad", 1)
	if _, err := DecodeAgentEvent([]byte(badEventDigest)); err == nil {
		t.Fatal("bad fact digest decoded")
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
			if _, err := DigestFact(currentSchemaVersion, "run_ended", fact); err == nil {
				t.Fatal("invalid tagged terminal value was accepted")
			}
		})
	}
}

type fakeRunEnd struct{}

func (fakeRunEnd) runEnd() {}
