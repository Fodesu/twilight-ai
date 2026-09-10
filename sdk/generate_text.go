package sdk

import (
	"context"
	"errors"
)

// GenerateText runs the SDK text-generation loop and returns the final text.
//
// Deprecated: this wrapper runs the SDK's own multi-step tool loop, which duplicates
// the orchestration a runtime has to own. Build an sdk.Request and call
// Client.Generate instead.
func (c *Client) GenerateText(ctx context.Context, options ...GenerateOption) (string, error) {
	result, err := c.GenerateTextResult(ctx, options...)
	if err != nil {
		return "", err
	}
	return result.Text, nil
}

// GenerateTextResult is the legacy high-level text wrapper. MaxSteps == 0
// performs one model call; MaxSteps != 0 runs the compatibility tool loop.
// A caller that owns its own step loop should drive
// Model.Generate directly instead of this SDK loop.
//
// The options are a client-side convenience: each step is projected into the
// provider-neutral Request boundary by requestFromGenerateParams, so the
// provider only ever sees the single-call shape.
//
// Deprecated: this wrapper runs the SDK's own multi-step tool loop, which duplicates
// the orchestration a runtime has to own. Build an sdk.Request and call
// Client.Generate instead.
func (c *Client) GenerateTextResult(ctx context.Context, options ...GenerateOption) (*GenerateResult, error) {
	cfg, _, err := buildConfig(options)
	if err != nil {
		return nil, err
	}
	model := cfg.Params.Model

	// MaxSteps == 0: single call, no tool auto-execution.
	if cfg.MaxSteps == 0 {
		result, mr, err := generateOnce(ctx, cfg, model, cfg.Params)
		if err != nil {
			return nil, err
		}
		stepMsgs := buildStepMessages(mr.Text, mr.TextProviderMetadata, mr.ReasoningParts, mr.ToolCalls, nil, &mr.Usage)
		step := stepResultFromModelResult(mr, stepMsgs, nil, nil)
		if err := applyOnStepCommitted(ctx, cfg, 0, &step); err != nil {
			return nil, err
		}
		result.Steps = []StepResult{step}
		result.Messages = stepMsgs
		applyOnStep(cfg, &step)
		if cfg.OnFinish != nil {
			cfg.OnFinish(result)
		}
		return result, nil
	}

	toolMap := buildToolMap(cfg.Params.Tools)
	messages := make([]Message, len(cfg.Params.Messages))
	copy(messages, cfg.Params.Messages)

	var (
		totalUsage  Usage
		lastResult  *GenerateResult
		allSteps    []StepResult
		allMessages []Message
	)

	for step := 0; shouldContinueLoop(cfg.MaxSteps, step); step++ {
		if step > 0 {
			messages = applyPrepareStep(cfg, messages)
		}

		params := cfg.Params
		params.Messages = messages

		result, mr, err := generateOnce(ctx, cfg, model, params)
		if err != nil {
			return nil, err
		}
		lastResult = result
		totalUsage = addUsage(&totalUsage, &mr.Usage)

		// No tool calls or not a tool-calls finish → final step
		if mr.FinishReason != FinishReasonToolCalls || len(mr.ToolCalls) == 0 || !hasExecutableTools(mr.ToolCalls, toolMap) {
			stepMsgs := buildStepMessages(mr.Text, mr.TextProviderMetadata, mr.ReasoningParts, mr.ToolCalls, nil, &mr.Usage)
			sr := stepResultFromModelResult(mr, stepMsgs, nil, nil)
			if err := applyOnStepCommitted(ctx, cfg, step, &sr); err != nil {
				return nil, err
			}
			allSteps = append(allSteps, sr)
			allMessages = append(allMessages, stepMsgs...)
			applyOnStep(cfg, &sr)
			break
		}

		// Execute tools
		toolResults, err := executeTools(ctx, mr.ToolCalls, toolMap, cfg.ApprovalHandler, nil)
		if err != nil {
			var deferred *ToolApprovalDeferredError
			if errors.As(err, &deferred) {
				stepMsgs := buildStepMessages(mr.Text, mr.TextProviderMetadata, mr.ReasoningParts, mr.ToolCalls, nil, &mr.Usage)
				sr := stepResultFromModelResult(mr, stepMsgs, nil, &deferred.Approval)
				if err := applyOnStepCommitted(ctx, cfg, step, &sr); err != nil {
					return nil, err
				}
				allSteps = append(allSteps, sr)
				allMessages = append(allMessages, stepMsgs...)
				applyOnStep(cfg, &sr)
				result.DeferredToolApproval = &deferred.Approval
				break
			}
			return nil, err
		}

		stepMsgs := buildStepMessages(mr.Text, mr.TextProviderMetadata, mr.ReasoningParts, mr.ToolCalls, toolResults, &mr.Usage)
		sr := stepResultFromModelResult(mr, stepMsgs, toolCallResultsFromParts(toolResults), nil)
		if err := applyOnStepCommitted(ctx, cfg, step, &sr); err != nil {
			return nil, err
		}
		allSteps = append(allSteps, sr)
		allMessages = append(allMessages, stepMsgs...)
		applyOnStep(cfg, &sr)

		messages = append(messages, stepMsgs...)
	}

	if lastResult != nil {
		lastResult.Usage = totalUsage
		lastResult.Steps = allSteps
		lastResult.Messages = allMessages
		if lastResult.DeferredToolApproval == nil {
			for i := range allSteps {
				if allSteps[i].DeferredToolApproval != nil {
					lastResult.DeferredToolApproval = allSteps[i].DeferredToolApproval
					break
				}
			}
		}
	}

	if cfg.OnFinish != nil && lastResult != nil {
		cfg.OnFinish(lastResult)
	}

	return lastResult, nil
}

// generateOnce projects one step of the legacy options into the Request
// boundary, makes exactly one model call, and adapts the single-call result
// back to the legacy result shape.
func generateOnce(ctx context.Context, cfg *generateConfig, model *Model, params GenerateParams) (*GenerateResult, ModelResult, error) {
	req, err := requestFromGenerateParams(params)
	if err != nil {
		return nil, ModelResult{}, err
	}
	mr, err := model.Generate(ctx, req)
	if err != nil {
		return nil, ModelResult{}, err
	}
	return GenerateResultFromModelResult(mr), mr, nil
}

// stepResultFromModelResult builds a legacy step from the single-call result.
// StepResult carries response metadata by value while the boundary type carries
// it by pointer, so an absent metadata becomes the zero value.
func stepResultFromModelResult(mr ModelResult, messages []Message, toolResults []ToolResult, deferred *ToolApprovalResult) StepResult {
	var response ResponseMetadata
	if mr.Response != nil {
		response = *mr.Response
	}
	return StepResult{
		Text:                 mr.Text,
		Reasoning:            mr.Reasoning,
		ReasoningParts:       mr.ReasoningParts,
		FinishReason:         mr.FinishReason,
		RawFinishReason:      mr.RawFinishReason,
		Usage:                mr.Usage,
		ToolCalls:            mr.ToolCalls,
		ToolResults:          toolResults,
		Response:             response,
		DeferredToolApproval: deferred,
		Messages:             messages,
	}
}
