package sdk

import "context"

// generateConfig holds both the provider-level params and client-level
// orchestration settings (callbacks, max steps, approval handler).
type generateConfig struct {
	Params GenerateParams

	// MaxSteps controls the tool auto-execution loop.
	//   0  = single LLM call, no auto-execution (default, backward-compatible)
	//  >0  = at most N LLM calls
	//  -1  = unlimited loop until LLM stops producing tool calls
	MaxSteps        int
	OnFinish        func(*GenerateResult)
	OnStep          func(*StepResult) *GenerateParams
	OnStepCommitted func(ctx context.Context, stepIndex int, step *StepResult) error
	PrepareStep     func(*GenerateParams) *GenerateParams
	ApprovalHandler func(ctx context.Context, call ToolCall) (ToolApprovalResult, error)
}

// GenerateOption configures a text generation request.
//
// Deprecated: this option configures the SDK's own text-generation loop. Build an
// sdk.Request and call Client.Generate or Client.Stream instead.
type GenerateOption func(*generateConfig)

// --- Provider-level options ---

// WithModel is part of the SDK's client-side text-generation option set.
//
// Deprecated: this option configures the SDK's own text-generation loop. Build an
// sdk.Request and call Client.Generate or Client.Stream instead.
func WithModel(model *Model) GenerateOption {
	return func(c *generateConfig) { c.Params.Model = model }
}

// WithMessages is part of the SDK's client-side text-generation option set.
//
// Deprecated: this option configures the SDK's own text-generation loop. Build an
// sdk.Request and call Client.Generate or Client.Stream instead.
func WithMessages(messages []Message) GenerateOption {
	return func(c *generateConfig) { c.Params.Messages = messages }
}

// WithSystem sets the stable root instruction placed before the conversation.
// Use SystemMessage inside WithMessages for an instruction at a specific point
// in the message timeline.
//
// Deprecated: this option configures the SDK's own text-generation loop. Build an
// sdk.Request and call Client.Generate or Client.Stream instead.
func WithSystem(text string) GenerateOption {
	return func(c *generateConfig) { c.Params.System = text }
}

// WithTools is part of the SDK's client-side text-generation option set.
//
// Deprecated: this option configures the SDK's own text-generation loop. Build an
// sdk.Request and call Client.Generate or Client.Stream instead.
func WithTools(tools []Tool) GenerateOption {
	return func(c *generateConfig) { c.Params.Tools = tools }
}

// WithToolChoice is part of the SDK's client-side text-generation option set.
//
// Deprecated: this option configures the SDK's own text-generation loop. Build an
// sdk.Request and call Client.Generate or Client.Stream instead.
func WithToolChoice(choice any) GenerateOption {
	return func(c *generateConfig) { c.Params.ToolChoice = choice }
}

// WithResponseFormat is part of the SDK's client-side text-generation option set.
//
// Deprecated: this option configures the SDK's own text-generation loop. Build an
// sdk.Request and call Client.Generate or Client.Stream instead.
func WithResponseFormat(rf ResponseFormat) GenerateOption {
	return func(c *generateConfig) { c.Params.ResponseFormat = &rf }
}

// WithTemperature is part of the SDK's client-side text-generation option set.
//
// Deprecated: this option configures the SDK's own text-generation loop. Build an
// sdk.Request and call Client.Generate or Client.Stream instead.
func WithTemperature(t float64) GenerateOption {
	return func(c *generateConfig) { c.Params.Temperature = &t }
}

// WithTopP is part of the SDK's client-side text-generation option set.
//
// Deprecated: this option configures the SDK's own text-generation loop. Build an
// sdk.Request and call Client.Generate or Client.Stream instead.
func WithTopP(topP float64) GenerateOption {
	return func(c *generateConfig) { c.Params.TopP = &topP }
}

// WithMaxTokens is part of the SDK's client-side text-generation option set.
//
// Deprecated: this option configures the SDK's own text-generation loop. Build an
// sdk.Request and call Client.Generate or Client.Stream instead.
func WithMaxTokens(n int) GenerateOption {
	return func(c *generateConfig) { c.Params.MaxTokens = &n }
}

// WithStopSequences is part of the SDK's client-side text-generation option set.
//
// Deprecated: this option configures the SDK's own text-generation loop. Build an
// sdk.Request and call Client.Generate or Client.Stream instead.
func WithStopSequences(s []string) GenerateOption {
	return func(c *generateConfig) { c.Params.StopSequences = s }
}

// WithFrequencyPenalty is part of the SDK's client-side text-generation option set.
//
// Deprecated: this option configures the SDK's own text-generation loop. Build an
// sdk.Request and call Client.Generate or Client.Stream instead.
func WithFrequencyPenalty(penalty float64) GenerateOption {
	return func(c *generateConfig) { c.Params.FrequencyPenalty = &penalty }
}

// WithPresencePenalty is part of the SDK's client-side text-generation option set.
//
// Deprecated: this option configures the SDK's own text-generation loop. Build an
// sdk.Request and call Client.Generate or Client.Stream instead.
func WithPresencePenalty(penalty float64) GenerateOption {
	return func(c *generateConfig) { c.Params.PresencePenalty = &penalty }
}

// WithSeed is part of the SDK's client-side text-generation option set.
//
// Deprecated: this option configures the SDK's own text-generation loop. Build an
// sdk.Request and call Client.Generate or Client.Stream instead.
func WithSeed(s int) GenerateOption {
	return func(c *generateConfig) { c.Params.Seed = &s }
}

// WithReasoningEffort is part of the SDK's client-side text-generation option set.
//
// Deprecated: this option configures the SDK's own text-generation loop. Build an
// sdk.Request and call Client.Generate or Client.Stream instead.
func WithReasoningEffort(effort string) GenerateOption {
	return func(c *generateConfig) { c.Params.ReasoningEffort = &effort }
}

// WithReasoningSummary explicitly requests a provider-supported,
// human-readable reasoning summary. OpenAI Responses supports "auto",
// "concise", and "detailed".
//
// Deprecated: this option configures the SDK's own text-generation loop. Build an
// sdk.Request and call Client.Generate or Client.Stream instead.
func WithReasoningSummary(summary string) GenerateOption {
	return func(c *generateConfig) { c.Params.ReasoningSummary = &summary }
}

// WithPromptCacheKey is part of the SDK's client-side text-generation option set.
//
// Deprecated: this option configures the SDK's own text-generation loop. Build an
// sdk.Request and call Client.Generate or Client.Stream instead.
func WithPromptCacheKey(key string) GenerateOption {
	return func(c *generateConfig) { c.Params.PromptCacheKey = &key }
}

// --- Client-level orchestration options ---

// WithMaxSteps sets the maximum number of LLM calls in the tool-execution loop.
//
//	0  (default) = single call, no auto tool execution
//	N  (N > 0)   = at most N calls
//	-1           = unlimited, loops until LLM stops requesting tools
//
// Deprecated: this option configures the SDK's own text-generation loop. Build an
// sdk.Request and call Client.Generate or Client.Stream instead.
func WithMaxSteps(n int) GenerateOption {
	return func(c *generateConfig) { c.MaxSteps = n }
}

// WithOnFinish registers a callback invoked once when all steps complete.
//
// Deprecated: this option configures the SDK's own text-generation loop. Build an
// sdk.Request and call Client.Generate or Client.Stream instead.
func WithOnFinish(fn func(*GenerateResult)) GenerateOption {
	return func(c *generateConfig) { c.OnFinish = fn }
}

// WithOnStep registers a callback invoked after each step (LLM call + tool round).
// If the callback returns a non-nil *GenerateParams, it overrides the params
// for the next step.
//
// Deprecated: this option configures the SDK's own text-generation loop. Build an
// sdk.Request and call Client.Generate or Client.Stream instead.
func WithOnStep(fn func(*StepResult) *GenerateParams) GenerateOption {
	return func(c *generateConfig) { c.OnStep = fn }
}

// WithOnStepCommitted registers a synchronous commit barrier invoked after a
// complete step has been assembled, including tool results or a deferred
// approval marker, and before the SDK accepts the step or starts the next model
// call. stepIndex is zero-based. Returning an error stops generation and leaves
// the step uncommitted.
//
// Deprecated: this option configures the SDK's own text-generation loop. Build an
// sdk.Request and call Client.Generate or Client.Stream instead.
func WithOnStepCommitted(fn func(ctx context.Context, stepIndex int, step *StepResult) error) GenerateOption {
	return func(c *generateConfig) { c.OnStepCommitted = fn }
}

// WithPrepareStep registers a callback invoked before each step (starting from
// the second step). It receives the current params and may return new params to
// override them. Returning nil keeps the (possibly mutated) original params.
//
// Deprecated: this option configures the SDK's own text-generation loop. Build an
// sdk.Request and call Client.Generate or Client.Stream instead.
func WithPrepareStep(fn func(*GenerateParams) *GenerateParams) GenerateOption {
	return func(c *generateConfig) { c.PrepareStep = fn }
}

// WithApprovalHandler registers a function that decides how to handle a tool
// call marked with RequireApproval.
//
// Deprecated: this option configures the SDK's own text-generation loop. Build an
// sdk.Request and call Client.Generate or Client.Stream instead.
func WithApprovalHandler(fn func(ctx context.Context, call ToolCall) (ToolApprovalResult, error)) GenerateOption {
	return func(c *generateConfig) { c.ApprovalHandler = fn }
}

// WithApprovalHandlerBool adapts the original bool-based approval callback.
//
// Deprecated: this option configures the SDK's own text-generation loop. Build an
// sdk.Request and call Client.Generate or Client.Stream instead.
func WithApprovalHandlerBool(fn func(ctx context.Context, call ToolCall) (bool, error)) GenerateOption {
	return func(c *generateConfig) {
		c.ApprovalHandler = func(ctx context.Context, call ToolCall) (ToolApprovalResult, error) {
			approved, err := fn(ctx, call)
			if err != nil {
				return ToolApprovalResult{}, err
			}
			if approved {
				return ToolApprovalResult{Decision: ToolApprovalDecisionApproved}, nil
			}
			return ToolApprovalResult{Decision: ToolApprovalDecisionRejected}, nil
		}
	}
}
