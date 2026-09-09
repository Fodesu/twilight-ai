package sdk

import (
	"context"
	"fmt"
	"sync"
)

// ToolExecOptions configures a single ExecuteTools invocation.
type ToolExecOptions struct {
	// Tools is the set of executable tool definitions. Calls are matched to
	// tools by name.
	Tools []Tool
	// Approve is consulted for every tool with RequireApproval set. When nil,
	// approval-required calls are denied with an IsError result. An approved
	// (or zero-value) decision continues to execution; rejected records an
	// IsError result; deferred stops the batch (see ToolExecOutcome.Deferred).
	Approve func(context.Context, ToolCall) (ToolApprovalResult, error)
	// OnPart, when non-nil, observes execution events: approval requests
	// (ToolApprovalRequestPart), denials (ToolOutputDeniedPart), progress
	// (ToolProgressPart), results (StreamToolResultPart), and errors
	// (StreamToolErrorPart). Parallel executions may invoke OnPart
	// concurrently; callers must synchronize if needed.
	OnPart func(StreamPart)
}

// ToolExecOutcome is the result of ExecuteTools over one batch of tool calls.
type ToolExecOutcome struct {
	// Results holds one ToolResultPart per resolved call, in call order.
	// When Deferred is nil it covers every call; when Deferred is non-nil it
	// covers exactly the calls before DeferredIndex (including rejected and
	// not-found IsError results, and the outputs of already-approved tools).
	Results []ToolResultPart
	// Deferred is non-nil when an approval handler returned
	// ToolApprovalDecisionDeferred: the batch parked at DeferredIndex.
	// Deferral is a normal outcome, not an error.
	Deferred *ToolApprovalResult
	// DeferredIndex is the index of the deferred call in calls when Deferred
	// is non-nil, and -1 otherwise.
	DeferredIndex int
}

// ExecuteTools resolves approvals for and executes one batch of tool calls.
//
// Approvals are resolved sequentially in call order; approved tools then
// execute (in parallel when more than one). When a deferred approval is
// encountered, tools already approved before the deferral point are still
// executed and their results are returned alongside the deferral marker, so no
// computed result is lost. A non-nil error is returned only for handler
// failures (approval handler error, unknown decision); in that case the
// outcome is empty.
func ExecuteTools(ctx context.Context, calls []ToolCall, opts ToolExecOptions) (ToolExecOutcome, error) {
	toolMap := buildToolMap(opts.Tools)
	results := make([]ToolResultPart, len(calls))
	pending := make([]pendingToolExec, 0, len(calls))

	finishPending := func() {
		runPendingTools(ctx, pending, results, opts.OnPart)
	}

	for i, tc := range calls {
		tool, ok := toolMap[tc.ToolName]
		if !ok || tool.Execute == nil {
			results[i] = ToolResultPart{
				ToolCallID: tc.ToolCallID,
				ToolName:   tc.ToolName,
				Result:     fmt.Sprintf("tool %q not found or has no execute handler", tc.ToolName),
				IsError:    true,
			}
			continue
		}

		if tool.RequireApproval {
			if opts.Approve == nil {
				if opts.OnPart != nil {
					opts.OnPart(&ToolOutputDeniedPart{
						ToolCallID: tc.ToolCallID,
						ToolName:   tc.ToolName,
					})
				}
				results[i] = ToolResultPart{
					ToolCallID: tc.ToolCallID,
					ToolName:   tc.ToolName,
					Result:     "tool execution denied: no approval handler",
					IsError:    true,
				}
				continue
			}

			approval, err := opts.Approve(ctx, tc)
			if err != nil {
				return ToolExecOutcome{}, fmt.Errorf("twilightai: approval handler for %q: %w", tc.ToolName, err)
			}
			switch approval.Decision {
			case "", ToolApprovalDecisionApproved:
				// Continue to execution below.
			case ToolApprovalDecisionRejected:
				if opts.OnPart != nil {
					opts.OnPart(&ToolApprovalRequestPart{
						ApprovalID: approval.ApprovalID,
						ToolCallID: tc.ToolCallID,
						ToolName:   tc.ToolName,
						Input:      tc.Input,
						Metadata:   approval.Metadata,
					})
					opts.OnPart(&ToolOutputDeniedPart{
						ToolCallID: tc.ToolCallID,
						ToolName:   tc.ToolName,
					})
				}
				results[i] = ToolResultPart{
					ToolCallID: tc.ToolCallID,
					ToolName:   tc.ToolName,
					Result:     rejectedToolResultText(approval),
					IsError:    true,
				}
				continue
			case ToolApprovalDecisionDeferred:
				if opts.OnPart != nil {
					opts.OnPart(&ToolApprovalRequestPart{
						ApprovalID: approval.ApprovalID,
						ToolCallID: tc.ToolCallID,
						ToolName:   tc.ToolName,
						Input:      tc.Input,
						Metadata:   approval.Metadata,
					})
				}
				finishPending()
				deferred := approval
				return ToolExecOutcome{
					Results:       results[:i],
					Deferred:      &deferred,
					DeferredIndex: i,
				}, nil
			default:
				return ToolExecOutcome{}, fmt.Errorf("twilightai: unknown approval decision %q for %q", approval.Decision, tc.ToolName)
			}
		}

		pending = append(pending, pendingToolExec{idx: i, tc: tc, tool: tool})
	}

	finishPending()
	return ToolExecOutcome{Results: results, DeferredIndex: -1}, nil
}

// runPendingTools executes approved tool calls, in parallel when more than one,
// writing each result to its call index in results.
func runPendingTools(ctx context.Context, pending []pendingToolExec, results []ToolResultPart, onPart func(StreamPart)) {
	switch {
	case len(pending) == 1:
		results[pending[0].idx] = runTool(ctx, pending[0].tc, pending[0].tool, onPart)
	case len(pending) > 1:
		var wg sync.WaitGroup
		wg.Add(len(pending))
		for _, p := range pending {
			go func(p pendingToolExec) {
				defer wg.Done()
				results[p.idx] = runTool(ctx, p.tc, p.tool, onPart)
			}(p)
		}
		wg.Wait()
	}
}

// ToolCallResults joins tool result parts with their originating calls,
// filling ToolResult.Input from the matching ToolCall by ToolCallID.
func ToolCallResults(calls []ToolCall, parts []ToolResultPart) []ToolResult {
	inputs := make(map[string]any, len(calls))
	for _, c := range calls {
		inputs[c.ToolCallID] = c.Input
	}
	out := make([]ToolResult, len(parts))
	for i, p := range parts {
		out[i] = ToolResult{
			ToolCallID: p.ToolCallID,
			ToolName:   p.ToolName,
			Input:      inputs[p.ToolCallID],
			Output:     p.Result,
		}
	}
	return out
}
