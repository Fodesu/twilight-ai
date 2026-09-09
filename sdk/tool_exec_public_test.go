package sdk_test

import (
	"context"
	"sync"
	"testing"

	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

func objSchema() *jsonschema.Schema { return &jsonschema.Schema{Type: "object"} }

func echoTool(name string, executed *bool) sdk.Tool {
	return sdk.Tool{
		Name:       name,
		Parameters: objSchema(),
		Execute: func(ctx *sdk.ToolExecContext, input any) (any, error) {
			if executed != nil {
				*executed = true
			}
			return "output-" + name, nil
		},
	}
}

func TestExecuteTools_DeferralPartialResults(t *testing.T) {
	var executedA, executedB bool
	toolA := echoTool("tool-a", &executedA)
	toolA.RequireApproval = true
	toolB := echoTool("tool-b", &executedB)
	toolB.RequireApproval = true

	calls := []sdk.ToolCall{
		{ToolCallID: "c1", ToolName: "tool-a", Input: map[string]any{"n": 1}},
		{ToolCallID: "c2", ToolName: "tool-b", Input: map[string]any{"n": 2}},
	}

	outcome, err := sdk.ExecuteTools(context.Background(), calls, sdk.ToolExecOptions{
		Tools: []sdk.Tool{toolA, toolB},
		Approve: func(_ context.Context, tc sdk.ToolCall) (sdk.ToolApprovalResult, error) {
			switch tc.ToolCallID {
			case "c1":
				return sdk.ToolApprovalResult{Decision: sdk.ToolApprovalDecisionRejected, Reason: "nope"}, nil
			case "c2":
				return sdk.ToolApprovalResult{Decision: sdk.ToolApprovalDecisionDeferred, ApprovalID: "approval-2"}, nil
			}
			t.Fatalf("unexpected call %q", tc.ToolCallID)
			return sdk.ToolApprovalResult{}, nil
		},
	})
	if err != nil {
		t.Fatalf("deferral must not be an error, got: %v", err)
	}
	if outcome.Deferred == nil || outcome.Deferred.ApprovalID != "approval-2" {
		t.Fatalf("Deferred: got %#v", outcome.Deferred)
	}
	if outcome.DeferredIndex != 1 {
		t.Fatalf("DeferredIndex: got %d, want 1", outcome.DeferredIndex)
	}
	if len(outcome.Results) != 1 {
		t.Fatalf("Results: got %d entries, want 1: %#v", len(outcome.Results), outcome.Results)
	}
	r := outcome.Results[0]
	if r.ToolCallID != "c1" || !r.IsError {
		t.Fatalf("Results[0]: got %#v, want rejected IsError result for c1", r)
	}
	if executedA || executedB {
		t.Fatalf("no tool should execute (rejected + deferred): a=%v b=%v", executedA, executedB)
	}
}

func TestExecuteTools_DeferralKeepsApprovedResults(t *testing.T) {
	var executedA bool
	toolA := echoTool("tool-a", &executedA)
	toolB := echoTool("tool-b", nil)
	toolB.RequireApproval = true

	calls := []sdk.ToolCall{
		{ToolCallID: "c1", ToolName: "tool-a"},
		{ToolCallID: "c2", ToolName: "tool-b"},
	}

	outcome, err := sdk.ExecuteTools(context.Background(), calls, sdk.ToolExecOptions{
		Tools: []sdk.Tool{toolA, toolB},
		Approve: func(_ context.Context, tc sdk.ToolCall) (sdk.ToolApprovalResult, error) {
			return sdk.ToolApprovalResult{Decision: sdk.ToolApprovalDecisionDeferred, ApprovalID: "approval-b"}, nil
		},
	})
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !executedA {
		t.Fatal("tool-a was approved-free and before the deferral point; it must execute")
	}
	if outcome.DeferredIndex != 1 || outcome.Deferred == nil {
		t.Fatalf("deferral marker: index=%d deferred=%#v", outcome.DeferredIndex, outcome.Deferred)
	}
	if len(outcome.Results) != 1 || outcome.Results[0].Result != "output-tool-a" || outcome.Results[0].IsError {
		t.Fatalf("Results: got %#v, want tool-a output", outcome.Results)
	}
}

func TestExecuteTools_ParallelExecution(t *testing.T) {
	tools := []sdk.Tool{echoTool("t1", nil), echoTool("t2", nil), echoTool("t3", nil)}
	calls := []sdk.ToolCall{
		{ToolCallID: "c1", ToolName: "t1"},
		{ToolCallID: "c2", ToolName: "t2"},
		{ToolCallID: "c3", ToolName: "t3"},
	}

	outcome, err := sdk.ExecuteTools(context.Background(), calls, sdk.ToolExecOptions{Tools: tools})
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if outcome.Deferred != nil || outcome.DeferredIndex != -1 {
		t.Fatalf("unexpected deferral: %#v index=%d", outcome.Deferred, outcome.DeferredIndex)
	}
	if len(outcome.Results) != 3 {
		t.Fatalf("Results: got %d entries, want 3", len(outcome.Results))
	}
	for i, want := range []string{"output-t1", "output-t2", "output-t3"} {
		if outcome.Results[i].Result != want || outcome.Results[i].IsError {
			t.Fatalf("Results[%d]: got %#v, want %q", i, outcome.Results[i], want)
		}
	}
}

func TestExecuteTools_SingleExecution(t *testing.T) {
	var executed bool
	outcome, err := sdk.ExecuteTools(context.Background(),
		[]sdk.ToolCall{{ToolCallID: "c1", ToolName: "only"}},
		sdk.ToolExecOptions{Tools: []sdk.Tool{echoTool("only", &executed)}},
	)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !executed {
		t.Fatal("tool did not execute")
	}
	if len(outcome.Results) != 1 || outcome.Results[0].Result != "output-only" {
		t.Fatalf("Results: got %#v", outcome.Results)
	}
}

func TestExecuteTools_NilOnPart(t *testing.T) {
	denied := sdk.Tool{Name: "denied", Parameters: objSchema(), RequireApproval: true,
		Execute: func(ctx *sdk.ToolExecContext, input any) (any, error) { return "x", nil }}
	rejected := sdk.Tool{Name: "rejected", Parameters: objSchema(), RequireApproval: true,
		Execute: func(ctx *sdk.ToolExecContext, input any) (any, error) { return "y", nil }}

	// Approve nil + RequireApproval: denied, no panic with nil OnPart.
	outcome, err := sdk.ExecuteTools(context.Background(),
		[]sdk.ToolCall{{ToolCallID: "c1", ToolName: "denied"}},
		sdk.ToolExecOptions{Tools: []sdk.Tool{denied}},
	)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(outcome.Results) != 1 || !outcome.Results[0].IsError {
		t.Fatalf("Results: got %#v, want denied IsError", outcome.Results)
	}

	// Rejected decision, nil OnPart: no panic, IsError result.
	outcome, err = sdk.ExecuteTools(context.Background(),
		[]sdk.ToolCall{{ToolCallID: "c2", ToolName: "rejected"}},
		sdk.ToolExecOptions{
			Tools: []sdk.Tool{rejected},
			Approve: func(_ context.Context, _ sdk.ToolCall) (sdk.ToolApprovalResult, error) {
				return sdk.ToolApprovalResult{Decision: sdk.ToolApprovalDecisionRejected}, nil
			},
		},
	)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(outcome.Results) != 1 || !outcome.Results[0].IsError {
		t.Fatalf("Results: got %#v, want rejected IsError", outcome.Results)
	}
}

func TestExecuteTools_OnPartObservesEvents(t *testing.T) {
	rejected := sdk.Tool{Name: "rejected", Parameters: objSchema(), RequireApproval: true,
		Execute: func(ctx *sdk.ToolExecContext, input any) (any, error) { return "never", nil }}
	approved := echoTool("approved", nil)
	approved.RequireApproval = true

	var mu sync.Mutex
	var parts []sdk.StreamPart

	outcome, err := sdk.ExecuteTools(context.Background(),
		[]sdk.ToolCall{
			{ToolCallID: "c1", ToolName: "rejected", Input: map[string]any{"k": "v"}},
			{ToolCallID: "c2", ToolName: "approved"},
		},
		sdk.ToolExecOptions{
			Tools: []sdk.Tool{rejected, approved},
			Approve: func(_ context.Context, tc sdk.ToolCall) (sdk.ToolApprovalResult, error) {
				if tc.ToolCallID == "c1" {
					return sdk.ToolApprovalResult{Decision: sdk.ToolApprovalDecisionRejected, ApprovalID: "a1"}, nil
				}
				return sdk.ToolApprovalResult{Decision: sdk.ToolApprovalDecisionApproved}, nil
			},
			OnPart: func(p sdk.StreamPart) {
				mu.Lock()
				parts = append(parts, p)
				mu.Unlock()
			},
		},
	)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(outcome.Results) != 2 {
		t.Fatalf("Results: got %#v", outcome.Results)
	}

	var sawApprovalRequest, sawDenied, sawResult bool
	for _, p := range parts {
		switch part := p.(type) {
		case *sdk.ToolApprovalRequestPart:
			if part.ToolCallID == "c1" && part.ApprovalID == "a1" {
				sawApprovalRequest = true
			}
		case *sdk.ToolOutputDeniedPart:
			if part.ToolCallID == "c1" {
				sawDenied = true
			}
		case *sdk.StreamToolResultPart:
			if part.ToolCallID == "c2" && part.Output == "output-approved" {
				sawResult = true
			}
		}
	}
	if !sawApprovalRequest || !sawDenied || !sawResult {
		t.Fatalf("missing events: approvalRequest=%v denied=%v result=%v (parts=%#v)",
			sawApprovalRequest, sawDenied, sawResult, parts)
	}
}

func TestToolCallResults_FillsInput(t *testing.T) {
	calls := []sdk.ToolCall{
		{ToolCallID: "c1", ToolName: "t1", Input: map[string]any{"path": "/tmp/a"}},
		{ToolCallID: "c2", ToolName: "t2", Input: "raw-input"},
	}
	parts := []sdk.ToolResultPart{
		{ToolCallID: "c1", ToolName: "t1", Result: "r1"},
		{ToolCallID: "c2", ToolName: "t2", Result: "r2", IsError: true},
	}

	results := sdk.ToolCallResults(calls, parts)
	if len(results) != 2 {
		t.Fatalf("got %d results", len(results))
	}
	in, ok := results[0].Input.(map[string]any)
	if !ok || in["path"] != "/tmp/a" {
		t.Fatalf("Results[0].Input: got %#v", results[0].Input)
	}
	if results[0].Output != "r1" {
		t.Fatalf("Results[0].Output: got %#v", results[0].Output)
	}
	if results[1].Input != "raw-input" || results[1].Output != "r2" {
		t.Fatalf("Results[1]: got %#v", results[1])
	}
}

func TestBuildStepMessages_AssemblesAssistantAndToolMessages(t *testing.T) {
	usage := &sdk.Usage{InputTokens: 3, OutputTokens: 5}
	msgs := sdk.BuildStepMessages(
		"hello",
		map[string]any{"k": "v"},
		[]sdk.ReasoningPart{{Text: "thinking"}},
		[]sdk.ToolCall{{ToolCallID: "c1", ToolName: "t1", Input: "in"}},
		[]sdk.ToolResultPart{{ToolCallID: "c1", ToolName: "t1", Result: "out"}},
		usage,
	)
	if len(msgs) != 2 {
		t.Fatalf("got %d messages, want 2", len(msgs))
	}
	assistant := msgs[0]
	if assistant.Role != sdk.MessageRoleAssistant || assistant.Usage != usage {
		t.Fatalf("assistant message: %#v", assistant)
	}
	// Order: reasoning, text, tool call.
	if len(assistant.Content) != 3 {
		t.Fatalf("assistant content: %#v", assistant.Content)
	}
	if _, ok := assistant.Content[0].(sdk.ReasoningPart); !ok {
		t.Fatalf("content[0] is %T, want ReasoningPart", assistant.Content[0])
	}
	if tp, ok := assistant.Content[1].(sdk.TextPart); !ok || tp.Text != "hello" {
		t.Fatalf("content[1] is %#v, want TextPart hello", assistant.Content[1])
	}
	if tc, ok := assistant.Content[2].(sdk.ToolCallPart); !ok || tc.ToolCallID != "c1" {
		t.Fatalf("content[2] is %#v, want ToolCallPart c1", assistant.Content[2])
	}
	if msgs[1].Role != sdk.MessageRoleTool {
		t.Fatalf("second message role: %v", msgs[1].Role)
	}
}
