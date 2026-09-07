package run

import (
	"strings"
	"testing"
)

// The snapshot codec round-trips every Current variant and the terminal
// shape, and its bytes equal statesEquivalent's identity.
func TestSnapshotCodecRoundTrip(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, DirectExecution)
	check := func(name string, s MachineState) {
		t.Helper()
		raw, err := ProtocolV1().EncodeMachineState(&s)
		if err != nil {
			t.Fatalf("%s: encode: %v", name, err)
		}
		decoded, err := ProtocolV1().DecodeMachineState(raw)
		if err != nil {
			t.Fatalf("%s: decode: %v\n%s", name, err, raw)
		}
		if !statesEquivalent(&s, &decoded) {
			t.Fatalf("%s: round trip changed state\n%s", name, raw)
		}
		if (s.Current == nil) != (decoded.Current == nil) {
			t.Fatalf("%s: Current presence changed: %T -> %T", name, s.Current, decoded.Current)
		}
	}

	s := newRun(t)
	check("open", s)
	s, stepID := advanceToExecuting(t, s, testRequest(def), []ToolSpec{spec})
	check("model executing", s)
	b := makeBinding(t, stepID, 0, "c1", spec, `{}`)
	facts := mustDecide(t, s, SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []ToolCallBinding{b}})
	s = fold(t, s, facts)
	check("tool step pending", s)
	toolStep := facts[1].(ToolStepOpened).StepID
	s = fold(t, s, mustDecide(t, s, StartToolCall{StepID: toolStep, CallID: cid(stepID, 0), Claim: "claim"}))
	check("tool step executing", s)
	s = fold(t, s, mustDecide(t, s, SubmitToolResult{StepID: toolStep, CallID: cid(stepID, 0), Result: ToolExecutionResult{Output: cj(`"ok"`)}}))
	check("open with last tool step", s)
	s = fold(t, s, mustDecide(t, s, CancelRun{}))
	check("terminal", s)
}

func TestSnapshotCodecRejectsMalformedWire(t *testing.T) {
	initial, err := InitializeRun("run-1", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	good, err := ProtocolV1().EncodeMachineState(&initial)
	if err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string]string{
		"unknown field":          strings.Replace(string(good), `"runId"`, `"extra":1,"runId"`, 1),
		"unknown current":        strings.Replace(string(good), `"current":"open"`, `"current":"weird"`, 1),
		"open with step body":    strings.Replace(string(good), `"current":"open"`, `"current":"open","modelStep":{}`, 1),
		"active without current": strings.Replace(string(good), `"current":"open",`, ``, 1),
		"trailing data":          string(good) + `{}`,
	} {
		if _, err := ProtocolV1().DecodeMachineState([]byte(raw)); err == nil {
			t.Fatalf("%s: accepted\n%s", name, raw)
		}
	}
	if _, err := ProtocolV1().DecodeMachineState(good); err != nil {
		t.Fatalf("canonical wire rejected: %v", err)
	}
}
