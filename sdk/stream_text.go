package sdk

import (
	"context"
	"errors"
	"fmt"
)

// StreamText is the legacy high-level streaming text wrapper. When MaxSteps !=
// 0 and tools have Execute handlers, it runs the compatibility multi-step loop,
// forwarding all stream parts (including ToolProgressPart) through a single
// channel. New multi-step runtimes should use agent.Loop instead of this SDK
// loop.
//
// StreamResult.Steps and StreamResult.Messages are populated during stream
// consumption and safe to read after Stream is fully consumed.
func (c *Client) StreamText(ctx context.Context, options ...GenerateOption) (*StreamResult, error) {
	cfg, prov, err := buildConfig(options)
	if err != nil {
		return nil, err
	}

	// Preserve the direct provider fast path unless the caller requested a
	// commit barrier, which requires the SDK to assemble and validate the step.
	if cfg.MaxSteps == 0 && cfg.OnStepCommitted == nil {
		return prov.DoStream(ctx, cfg.Params)
	}
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

			provSR, err := prov.DoStream(ctx, params)
			if err != nil {
				send(&ErrorPart{Error: fmt.Errorf("twilightai: stream step %d: %w", step, err)})
				return
			}

			var (
				stepText            string
				stepTextMeta        map[string]any
				stepReasoning       reasoningAccumulator
				stepToolCalls       []ToolCall
				stepErrored         bool
				stepUsage           Usage
				stepResponse        ResponseMetadata
				stepFinishReason    FinishReason
				stepRawFinishReason string
				sawFinishStep       bool
			)

			for part := range provSR.Stream {
				switch p := part.(type) {
				case *TextDeltaPart:
					stepText += p.Text
				case *TextEndPart:
					if p.ProviderMetadata != nil {
						stepTextMeta = p.ProviderMetadata
					}
				case *ReasoningStartPart:
					stepReasoning.openBlock(p.ID, p.Format, p.Model, p.ProviderMetadata)
				case *ReasoningDeltaPart:
					stepReasoning.appendDelta(p.ID, p.Text, p.Format, p.Model, p.ProviderMetadata)
				case *ReasoningEndPart:
					stepReasoning.closeBlock(p.ID, p.Format, p.Model, p.ProviderMetadata)
				case *StreamToolCallPart:
					stepToolCalls = append(stepToolCalls, ToolCall{
						ToolCallID:       p.ToolCallID,
						ToolName:         p.ToolName,
						Input:            p.Input,
						ProviderMetadata: p.ProviderMetadata,
					})
				case *FinishStepPart:
					sawFinishStep = true
					stepUsage = p.Usage
					stepResponse = p.Response
					stepFinishReason = p.FinishReason
					stepRawFinishReason = p.RawFinishReason
				case *FinishPart:
					stepFinishReason = p.FinishReason
					stepRawFinishReason = p.RawFinishReason
					continue
				case *ErrorPart:
					stepErrored = true
				}

				if !send(part) {
					return
				}
			}
			// A provider error poisons the step: the consumer already saw the
			// ErrorPart, and committing what remains would persist a step the
			// provider itself reported as broken. ToResult treats the same
			// part as fatal; the streaming loop has to agree with it.
			if stepErrored {
				return
			}
			if !sawFinishStep {
				if ctx.Err() == nil {
					send(&ErrorPart{Error: fmt.Errorf("twilightai: stream step %d ended before finish-step", step)})
				}
				return
			}

			lastFinishReason = stepFinishReason
			lastRawFinishReason = stepRawFinishReason
			totalUsage = addUsage(&totalUsage, &stepUsage)

			// No tool calls or not a tool-calls finish → done
			if !autoExecuteTools || stepFinishReason != FinishReasonToolCalls || len(stepToolCalls) == 0 || !hasExecutableTools(stepToolCalls, toolMap) {
				stepMsgs := buildStepMessages(stepText, stepTextMeta, stepReasoning.result(), stepToolCalls, nil, &stepUsage)
				stepR := StepResult{
					Text:            stepText,
					Reasoning:       ReasoningText(stepReasoning.result()),
					ReasoningParts:  stepReasoning.result(),
					FinishReason:    stepFinishReason,
					RawFinishReason: stepRawFinishReason,
					Usage:           stepUsage,
					ToolCalls:       stepToolCalls,
					Response:        stepResponse,
					Messages:        stepMsgs,
				}
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
			toolResults, err := executeTools(ctx, stepToolCalls, toolMap, cfg.ApprovalHandler, sendProgress)
			if err != nil {
				var deferred *ToolApprovalDeferredError
				if errors.As(err, &deferred) {
					stepMsgs := buildStepMessages(stepText, stepTextMeta, stepReasoning.result(), stepToolCalls, nil, &stepUsage)
					stepR := StepResult{
						Text:                 stepText,
						Reasoning:            ReasoningText(stepReasoning.result()),
						ReasoningParts:       stepReasoning.result(),
						FinishReason:         stepFinishReason,
						RawFinishReason:      stepRawFinishReason,
						Usage:                stepUsage,
						ToolCalls:            stepToolCalls,
						Response:             stepResponse,
						DeferredToolApproval: &deferred.Approval,
						Messages:             stepMsgs,
					}
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

			stepMsgs := buildStepMessages(stepText, stepTextMeta, stepReasoning.result(), stepToolCalls, toolResults, &stepUsage)
			stepR := StepResult{
				Text:            stepText,
				Reasoning:       ReasoningText(stepReasoning.result()),
				ReasoningParts:  stepReasoning.result(),
				FinishReason:    stepFinishReason,
				RawFinishReason: stepRawFinishReason,
				Usage:           stepUsage,
				ToolCalls:       stepToolCalls,
				ToolResults:     toolCallResultsFromParts(toolResults),
				Response:        stepResponse,
				Messages:        stepMsgs,
			}
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
