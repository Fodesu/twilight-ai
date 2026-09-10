package sdk

import (
	"context"
	"errors"
	"fmt"
)

// StreamText is the caller-facing high-level streaming text wrapper. When MaxSteps !=
// 0 and tools have Execute handlers, it runs the compatibility multi-step loop,
// forwarding all stream parts (including ToolProgressPart) through a single
// channel. New multi-step runtimes should use agent/run/loop.Loop instead of
// this SDK loop.
//
// Every step goes through the same path: the legacy options are projected into
// the Request boundary, the provider yields parts, and the SDK assembles those
// same parts into the step result. There is no second, faster path that skips
// assembly, because that is how a streamed step and a streamed result come to
// disagree.
//
// StreamResult.Steps and StreamResult.Messages are populated during stream
// consumption and safe to read after the stream is fully consumed.
func (c *Client) StreamText(ctx context.Context, options ...GenerateOption) (*StreamResult, error) {
	cfg, _, err := buildConfig(options)
	if err != nil {
		return nil, err
	}
	model := cfg.Params.Model

	autoExecuteTools := cfg.MaxSteps != 0
	maxSteps := cfg.MaxSteps
	if maxSteps == 0 {
		maxSteps = 1
	}

	toolMap := buildToolMap(cfg.Params.Tools)
	messages := make([]Message, len(cfg.Params.Messages))
	copy(messages, cfg.Params.Messages)

	ch := make(chan StreamPart, 64)
	sr := &StreamResult{Stream: ch}

	go func() {
		send := func(part StreamPart) bool {
			select {
			case ch <- part:
				return true
			case <-ctx.Done():
				return false
			}
		}

		var totalUsage Usage
		var lastFinishReason FinishReason
		var lastRawFinishReason string
		var allSteps []StepResult
		var allMessages []Message
		defer func() {
			sr.Steps = allSteps
			sr.Messages = allMessages
			for i := range allSteps {
				if allSteps[i].DeferredToolApproval != nil {
					sr.DeferredToolApproval = allSteps[i].DeferredToolApproval
					break
				}
			}
			close(ch)
		}()

		for step := 0; shouldContinueLoop(maxSteps, step); step++ {
			if step > 0 {
				messages = applyPrepareStep(cfg, messages)
			}

			params := cfg.Params
			params.Messages = messages

			req, err := requestFromGenerateParams(params)
			if err != nil {
				send(&ErrorPart{Error: fmt.Errorf("twilightai: stream step %d: %w", step, err)})
				return
			}
			stream, err := model.Stream(ctx, req)
			if err != nil {
				send(&ErrorPart{Error: fmt.Errorf("twilightai: stream step %d: %w", step, err)})
				return
			}

			// Forward the parts as they arrive. The SDK assembler folds the very
			// same parts into the step result, so what the consumer saw and what
			// gets committed cannot disagree.
			for part := range stream.Parts {
				if !send(part) {
					return
				}
			}
			mr, err := stream.Result()
			if err != nil {
				// A provider failure already reached the consumer as an
				// ErrorPart, and a cancelled step is not this loop's to commit:
				// either way the step is poisoned, and committing what remains
				// would persist a step the provider itself reported as broken.
				return
			}
			if mr.FinishReason == "" && ctx.Err() == nil {
				send(&ErrorPart{Error: fmt.Errorf("twilightai: stream step %d ended before finish-step", step)})
				return
			}

			lastFinishReason = mr.FinishReason
			lastRawFinishReason = mr.RawFinishReason
			totalUsage = addUsage(&totalUsage, &mr.Usage)

			// No tool calls or not a tool-calls finish → done
			if !autoExecuteTools || mr.FinishReason != FinishReasonToolCalls || len(mr.ToolCalls) == 0 || !hasExecutableTools(mr.ToolCalls, toolMap) {
				stepMsgs := buildStepMessages(mr.Text, mr.TextProviderMetadata, mr.ReasoningParts, mr.ToolCalls, nil, &mr.Usage)
				stepR := stepResultFromModelResult(*mr, stepMsgs, nil, nil)
				if err := applyOnStepCommitted(ctx, cfg, step, &stepR); err != nil {
					send(&ErrorPart{Error: err})
					return
				}
				allSteps = append(allSteps, stepR)
				allMessages = append(allMessages, stepMsgs...)
				applyOnStep(cfg, &stepR)
				break
			}

			// Execute tools
			sendProgress := func(part StreamPart) { send(part) }
			toolResults, err := executeTools(ctx, mr.ToolCalls, toolMap, cfg.ApprovalHandler, sendProgress)
			if err != nil {
				var deferred *ToolApprovalDeferredError
				if errors.As(err, &deferred) {
					stepMsgs := buildStepMessages(mr.Text, mr.TextProviderMetadata, mr.ReasoningParts, mr.ToolCalls, nil, &mr.Usage)
					stepR := stepResultFromModelResult(*mr, stepMsgs, nil, &deferred.Approval)
					if err := applyOnStepCommitted(ctx, cfg, step, &stepR); err != nil {
						send(&ErrorPart{Error: err})
						return
					}
					allSteps = append(allSteps, stepR)
					allMessages = append(allMessages, stepMsgs...)
					applyOnStep(cfg, &stepR)
					break
				}
				send(&ErrorPart{Error: err})
				return
			}

			stepMsgs := buildStepMessages(mr.Text, mr.TextProviderMetadata, mr.ReasoningParts, mr.ToolCalls, toolResults, &mr.Usage)
			stepR := stepResultFromModelResult(*mr, stepMsgs, toolCallResultsFromParts(toolResults), nil)
			if err := applyOnStepCommitted(ctx, cfg, step, &stepR); err != nil {
				send(&ErrorPart{Error: err})
				return
			}
			allSteps = append(allSteps, stepR)
			allMessages = append(allMessages, stepMsgs...)
			applyOnStep(cfg, &stepR)

			messages = append(messages, stepMsgs...)
		}

		if !send(&FinishPart{
			FinishReason:    lastFinishReason,
			RawFinishReason: lastRawFinishReason,
			TotalUsage:      totalUsage,
		}) {
			return
		}

		if cfg.OnFinish != nil {
			var deferredToolApproval *ToolApprovalResult
			for i := range allSteps {
				if allSteps[i].DeferredToolApproval != nil {
					deferredToolApproval = allSteps[i].DeferredToolApproval
					break
				}
			}
			cfg.OnFinish(&GenerateResult{
				FinishReason:         lastFinishReason,
				RawFinishReason:      lastRawFinishReason,
				Usage:                totalUsage,
				Steps:                allSteps,
				Messages:             allMessages,
				DeferredToolApproval: deferredToolApproval,
			})
		}
	}()

	return sr, nil
}
