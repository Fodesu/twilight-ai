package run

import (
	"context"
	"strings"
	"testing"

	"github.com/memohai/twilight/sdk"
)

// The snapshot codec round-trips every Current variant and the terminal
// shape, and its bytes equal statesEquivalent's identity.
func TestSnapshotCodecRoundTrip(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, DirectExecution)
	rt, stepID, grant := preparedRuntime(t, []sdk.ToolDefinition{def}, []ToolSpec{spec})
	ctx := context.Background()

	check := func(name string) {
		t.Helper()
		snap, err := rt.Load(ctx, "run-1")
		if err != nil {
			t.Fatal(err)
		}
		raw, err := ProtocolV1.EncodeMachineState(&snap.State)
		if err != nil {
			t.Fatalf("%s: encode: %v", name, err)
		}
		decoded, err := ProtocolV1.DecodeMachineState(raw)
		if err != nil {
			t.Fatalf("%s: decode: %v\n%s", name, err, raw)
		}
		if !statesEquivalent(&snap.State, &decoded) {
			t.Fatalf("%s: round trip changed state\n%s", name, raw)
		}
		if (snap.State.Current == nil) != (decoded.Current == nil) {
			t.Fatalf("%s: Current presence changed: %T -> %T", name, snap.State.Current, decoded.Current)
		}
	}

	check("model executing")
	b := makeBinding(t, "c1", spec, `{}`)
	snap, _ := rt.Load(ctx, "run-1")
	res := mustCommit(t, rt, "complete-1", snap.Revision, grant,
		SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []ToolCallBinding{b}})
	check("tool step pending")
	toolStep := res.Events[1].Fact.(ToolStepOpened).StepID
	sRes := mustCommit(t, rt, "start-c1", res.Snapshot.Revision, "", StartToolCall{StepID: toolStep, CallID: "c1"})
	check("tool step executing")
	mustCommit(t, rt, "done-c1", sRes.Snapshot.Revision, sRes.Grant,
		SubmitToolResult{StepID: toolStep, CallID: "c1", Result: ToolExecutionResult{Output: cj(`"ok"`)}})
	check("open with last tool step")
	snap, _ = rt.Load(ctx, "run-1")
	mustCommit(t, rt, "cancel", snap.Revision, "", CancelRun{})
	check("terminal")
}

func TestSnapshotCodecRejectsMalformedWire(t *testing.T) {
	initial, err := InitializeRun("run-1")
	if err != nil {
		t.Fatal(err)
	}
	good, err := ProtocolV1.EncodeMachineState(&initial)
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
		if _, err := ProtocolV1.DecodeMachineState([]byte(raw)); err == nil {
			t.Fatalf("%s: accepted\n%s", name, raw)
		}
	}
	if _, err := ProtocolV1.DecodeMachineState(good); err != nil {
		t.Fatalf("canonical wire rejected: %v", err)
	}
}
