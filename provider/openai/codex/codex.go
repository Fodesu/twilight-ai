package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/memohai/twilight-ai/internal/messagecompat"
	"github.com/memohai/twilight-ai/internal/utils"
	openaiutil "github.com/memohai/twilight-ai/provider/openai"
	"github.com/memohai/twilight-ai/sdk"
)

const (
	outputTypeMessage      = "message"
	outputTypeReasoning    = "reasoning"
	outputTypeFunctionCall = "function_call"
)

type Provider struct {
	accessToken string
	accountID   string
	originator  string
	baseURL     string
	httpClient  *http.Client
}

type Option func(*Provider)

func WithAccessToken(token string) Option {
	return func(p *Provider) { p.accessToken = token }
}

// WithAPIKey is an alias for WithAccessToken to make migration from other
// OpenAI-style providers less disruptive at the call site.
func WithAPIKey(token string) Option {
	return WithAccessToken(token)
}

func WithAccountID(accountID string) Option {
	return func(p *Provider) { p.accountID = accountID }
}

func WithOriginator(originator string) Option {
	return func(p *Provider) { p.originator = originator }
}

func WithBaseURL(baseURL string) Option {
	return func(p *Provider) { p.baseURL = baseURL }
}

func WithHTTPClient(client *http.Client) Option {
	return func(p *Provider) { p.httpClient = client }
}

func New(options ...Option) *Provider {
	p := &Provider{
		baseURL:    defaultBaseURL,
		originator: defaultOriginator,
		httpClient: &http.Client{},
	}
	for _, o := range options {
		o(p)
	}
	return p
}

func (p *Provider) Name() string { return "openai-codex" }

func (p *Provider) ListModels(context.Context) ([]sdk.Model, error) {
	models := Catalog()
	out := make([]sdk.Model, 0, len(models))
	for _, m := range models {
		out = append(out, sdk.Model{
			ID:          m.ID,
			DisplayName: m.DisplayName,
			Provider:    p,
			Type:        sdk.ModelTypeChat,
		})
	}
	return out, nil
}

func (p *Provider) Test(ctx context.Context) *sdk.ProviderTestResult {
	_, err := p.TestModel(ctx, Catalog()[0].ID)
	if err != nil {
		return classifyError(err)
	}
	return &sdk.ProviderTestResult{Status: sdk.ProviderStatusOK, Message: "ok"}
}

func (p *Provider) TestModel(ctx context.Context, modelID string) (*sdk.ModelTestResult, error) {
	req, err := p.buildRequest(&sdk.GenerateParams{
		Model:    p.ChatModel(modelID),
		System:   "You are a helpful AI assistant.",
		Messages: []sdk.Message{sdk.UserMessage("ping")},
	})
	if err != nil {
		return nil, fmt.Errorf("openai-codex: build probe request: %w", err)
	}
	req.Stream = false

	status, err := utils.ProbeStatus(ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodPost,
		BaseURL: p.baseURL,
		Path:    "/codex/responses",
		Headers: p.authHeaders(),
		Body:    req,
	})
	if err != nil {
		return nil, fmt.Errorf("openai-codex: probe model request failed: %w", err)
	}
	return sdk.ClassifyProbeStatus(status)
}

func (p *Provider) ChatModel(id string) *sdk.Model {
	return &sdk.Model{
		ID:       id,
		Provider: p,
		Type:     sdk.ModelTypeChat,
	}
}

func (p *Provider) DoGenerate(ctx context.Context, params sdk.GenerateParams) (*sdk.GenerateResult, error) { //nolint:gocritic // interface method
	sr, err := p.DoStream(ctx, params)
	if err != nil {
		return nil, err
	}
	result, err := sr.ToResult()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (p *Provider) DoStream(ctx context.Context, params sdk.GenerateParams) (*sdk.StreamResult, error) { //nolint:gocritic,gocyclo // provider streaming
	if params.Model == nil {
		return nil, fmt.Errorf("openai-codex: model is required")
	}

	req, err := p.buildRequest(&params)
	if err != nil {
		return nil, fmt.Errorf("openai-codex: build request: %w", err)
	}
	req.Stream = true

	ch := make(chan sdk.StreamPart, 64)
	go func() {
		defer close(ch)

		var (
			responseID       string
			responseModel    string
			responseCreated  int64
			usage            sdk.Usage
			incompleteReason string
			hasFunctionCall  bool

			textStartSent     bool
			activeReasoningID string
			pendingToolCalls  = map[int]*streamingToolCall{}
		)

		send := func(part sdk.StreamPart) bool {
			select {
			case ch <- part:
				return true
			case <-ctx.Done():
				return false
			}
		}

		// endReasoning closes the active reasoning block. meta carries the
		// block's final metadata: encrypted_content is populated only on
		// response.output_item.done, so the closing part is the only chance to
		// deliver it. Every other close path passes nil.
		endReasoning := func(meta map[string]any) {
			if activeReasoningID == "" {
				return
			}
			send(&sdk.ReasoningEndPart{
				ID:               activeReasoningID,
				Format:           sdk.ReasoningFormatOpenAIResponses,
				ProviderMetadata: meta,
			})
			activeReasoningID = ""
		}

		flush := func() {
			endReasoning(nil)
			if textStartSent {
				send(&sdk.TextEndPart{ID: responseID})
				textStartSent = false
			}
		}

		if !send(&sdk.StartPart{}) || !send(&sdk.StartStepPart{}) {
			return
		}

		err := utils.FetchSSE(ctx, p.httpClient, &utils.RequestOptions{
			Method:  http.MethodPost,
			BaseURL: p.baseURL,
			Path:    "/codex/responses",
			Headers: p.authHeaders(),
			Body:    req,
		}, func(ev *utils.SSEEvent) error {
			switch ev.Event {
			case "response.created":
				var chunk codexCreatedChunk
				if json.Unmarshal([]byte(ev.Data), &chunk) == nil {
					responseID = chunk.Response.ID
					responseModel = chunk.Response.Model
					responseCreated = chunk.Response.CreatedAt
				}

			case "response.output_item.added":
				var chunk codexOutputItemAddedChunk
				if json.Unmarshal([]byte(ev.Data), &chunk) != nil {
					return nil
				}
				switch chunk.Item.Type {
				case outputTypeMessage:
					if !textStartSent {
						send(&sdk.TextStartPart{ID: chunk.Item.ID})
						textStartSent = true
					}
				case outputTypeReasoning:
					if activeReasoningID != chunk.Item.ID {
						endReasoning(nil)
						send(&sdk.ReasoningStartPart{
							ID:               chunk.Item.ID,
							Format:           sdk.ReasoningFormatOpenAIResponses,
							Model:            responseModel,
							ProviderMetadata: openaiutil.ReasoningItemMetadata(chunk.Item.ID, chunk.Item.EncryptedContent),
						})
						activeReasoningID = chunk.Item.ID
					}
				case outputTypeFunctionCall:
					flush()
					callID := chunk.Item.CallID
					if callID == "" {
						callID = generateID()
					}
					pendingToolCalls[chunk.OutputIndex] = &streamingToolCall{id: callID, name: chunk.Item.Name}
					send(&sdk.ToolInputStartPart{ID: callID, ToolName: chunk.Item.Name})
				}

			case "response.output_text.delta":
				var chunk codexTextDeltaChunk
				if json.Unmarshal([]byte(ev.Data), &chunk) != nil {
					return nil
				}
				endReasoning(nil)
				if !textStartSent {
					send(&sdk.TextStartPart{ID: chunk.ItemID})
					textStartSent = true
				}
				send(&sdk.TextDeltaPart{ID: chunk.ItemID, Text: chunk.Delta})

			case "response.reasoning_summary_text.delta":
				var chunk codexReasoningSummaryDeltaChunk
				if json.Unmarshal([]byte(ev.Data), &chunk) != nil {
					return nil
				}
				if activeReasoningID != chunk.ItemID {
					endReasoning(nil)
					send(&sdk.ReasoningStartPart{ID: chunk.ItemID, Format: sdk.ReasoningFormatOpenAIResponses, Model: responseModel})
					activeReasoningID = chunk.ItemID
				}
				send(&sdk.ReasoningDeltaPart{ID: chunk.ItemID, Text: chunk.Delta, Format: sdk.ReasoningFormatOpenAIResponses, Model: responseModel})

			case "response.function_call_arguments.delta":
				var chunk codexFuncArgsDeltaChunk
				if json.Unmarshal([]byte(ev.Data), &chunk) != nil {
					return nil
				}
				stc := pendingToolCalls[chunk.OutputIndex]
				if stc == nil {
					return nil
				}
				stc.args.WriteString(chunk.Delta)
				send(&sdk.ToolInputDeltaPart{ID: stc.id, Delta: chunk.Delta})

			case "response.output_item.done":
				var chunk codexOutputItemDoneChunk
				if json.Unmarshal([]byte(ev.Data), &chunk) != nil {
					return nil
				}
				switch chunk.Item.Type {
				case outputTypeMessage:
					if textStartSent {
						send(&sdk.TextEndPart{ID: chunk.Item.ID})
						textStartSent = false
					}
				case outputTypeReasoning:
					// The done event is where encrypted_content arrives; the
					// added event fires before it is populated.
					meta := openaiutil.ReasoningItemMetadata(chunk.Item.ID, chunk.Item.EncryptedContent)
					switch {
					case activeReasoningID == chunk.Item.ID:
						endReasoning(meta)
					case chunk.Item.EncryptedContent != "":
						// The block was already closed by an interleaved event.
						// Send another end part for it: the accumulator merges
						// metadata by block ID, so the payload still lands on
						// the right block instead of being lost.
						send(&sdk.ReasoningEndPart{
							ID:               chunk.Item.ID,
							Format:           sdk.ReasoningFormatOpenAIResponses,
							ProviderMetadata: meta,
						})
					}
				case outputTypeFunctionCall:
					hasFunctionCall = true
					stc := pendingToolCalls[chunk.OutputIndex]
					if stc != nil && !stc.finished {
						send(&sdk.ToolInputEndPart{ID: stc.id})
						args := chunk.Item.Arguments
						if args == "" {
							args = stc.args.String()
						}
						var input any
						// A call whose arguments cannot be parsed must not
						// become a call: nil input would hand the tool empty
						// arguments and run it anyway.
						if args != "" {
							if err := json.Unmarshal([]byte(args), &input); err != nil {
								send(&sdk.ErrorPart{Error: fmt.Errorf("openai-codex: unmarshal tool call arguments for %q: %w", stc.name, err)})
								stc.finished = true
								break
							}
						}
						send(&sdk.StreamToolCallPart{ToolCallID: stc.id, ToolName: stc.name, Input: input})
						stc.finished = true
					}
				}

			case "response.completed", "response.incomplete":
				var chunk codexCompletedChunk
				if json.Unmarshal([]byte(ev.Data), &chunk) != nil {
					return nil
				}
				if chunk.Response.IncompleteDetails != nil {
					incompleteReason = chunk.Response.IncompleteDetails.Reason
				}
				if chunk.Response.Usage != nil {
					usage = convertCodexUsage(chunk.Response.Usage)
				}
				flush()
				send(&sdk.FinishStepPart{
					FinishReason:    mapCodexFinishReason(incompleteReason, hasFunctionCall),
					RawFinishReason: incompleteReason,
					Usage:           usage,
					Response: sdk.ResponseMetadata{
						ID:        responseID,
						ModelID:   responseModel,
						Timestamp: time.Unix(responseCreated, 0),
					},
				})
				return utils.ErrStreamDone

			case "error":
				var chunk codexErrorChunk
				if json.Unmarshal([]byte(ev.Data), &chunk) != nil {
					return nil
				}
				send(&sdk.ErrorPart{Error: fmt.Errorf("openai-codex: %s: %s", chunk.Error.Code, chunk.Error.Message)})
				return utils.ErrStreamDone
			}

			return nil
		})

		if err != nil {
			var apiErr *utils.APIError
			if errors.As(err, &apiErr) {
				send(&sdk.ErrorPart{Error: fmt.Errorf("openai-codex: stream failed: %s", apiErr.Detail())})
			} else {
				send(&sdk.ErrorPart{Error: fmt.Errorf("openai-codex: stream failed: %w", err)})
			}
		}

		flush()
		send(&sdk.FinishPart{
			FinishReason:    mapCodexFinishReason(incompleteReason, hasFunctionCall),
			RawFinishReason: incompleteReason,
			TotalUsage:      usage,
		})
	}()

	return &sdk.StreamResult{Stream: ch}, nil
}

func (p *Provider) buildRequest(params *sdk.GenerateParams) (*codexRequest, error) {
	messages, err := messagecompat.Normalize(params.Messages, sdk.MessageRoleCapabilities{})
	if err != nil {
		return nil, err
	}
	instructions, input := convertToCodexInput(params.System, messages)
	req := &codexRequest{
		Model:        params.Model.ID,
		Instructions: instructions,
		Input:        input,
		Include:      []string{"reasoning.encrypted_content"},
		Store:        false,
	}

	if len(params.Tools) > 0 {
		req.Tools = convertCodexTools(params.Tools)
		req.ToolChoice = params.ToolChoice
	}

	if params.ResponseFormat != nil {
		tf := &codexTextFmt{}
		switch params.ResponseFormat.Type {
		case sdk.ResponseFormatJSONObject:
			tf.Format = &codexTextFormat{Type: "json_object"}
		case sdk.ResponseFormatJSONSchema:
			tf.Format = &codexTextFormat{Type: "json_schema", Name: "response", Schema: params.ResponseFormat.JSONSchema}
		}
		if tf.Format != nil {
			req.Text = tf
		}
	}

	if params.ReasoningEffort != nil && *params.ReasoningEffort != "" {
		// The Codex endpoint accepts max even though generic OpenAI endpoints do not.
		req.Reasoning = &codexReasoning{Effort: *params.ReasoningEffort}
	}
	return req, nil
}

func convertCodexTools(tools []sdk.Tool) []codexTool {
	out := make([]codexTool, 0, len(tools))
	for _, t := range tools {
		out = append(out, codexTool{
			Type:        "function",
			Name:        t.Name,
			Description: t.Description,
			Parameters:  t.Parameters,
		})
	}
	return out
}

func convertToCodexInput(systemPrompt string, messages []sdk.Message) (string, []json.RawMessage) {
	var (
		instructions []string
		items        []json.RawMessage
	)

	if systemPrompt != "" {
		instructions = append(instructions, systemPrompt)
	}
	for _, msg := range messages {
		if msg.Role == sdk.MessageRoleSystem {
			instructions = append(instructions, messagecompat.InstructionText(msg))
			continue
		}
		items = append(items, convertCodexMessage(msg)...)
	}
	joined := "You are a helpful AI assistant."
	if len(instructions) > 0 {
		joined = joinNonEmpty(instructions...)
	}
	return joined, items
}

func convertCodexMessage(msg sdk.Message) []json.RawMessage {
	switch msg.Role {
	case sdk.MessageRoleUser:
		return convertCodexUserMessage(msg)
	case sdk.MessageRoleAssistant:
		return convertCodexAssistantMessage(msg)
	case sdk.MessageRoleTool:
		return convertCodexToolResults(msg)
	default:
		return nil
	}
}

func convertCodexUserMessage(msg sdk.Message) []json.RawMessage {
	var parts []codexUserContentPart
	for _, part := range msg.Content {
		switch p := part.(type) {
		case sdk.TextPart:
			parts = append(parts, codexUserContentPart{Type: "input_text", Text: p.Text})
		case sdk.ImagePart:
			parts = append(parts, codexUserContentPart{Type: "input_image", ImageURL: p.Image})
		case sdk.FilePart:
			// Codex has no confirmed native file input; emit an explicit
			// marker instead of the raw payload. Swap for a native part once
			// upstream support is verified.
			parts = append(parts, codexUserContentPart{Type: "input_text", Text: utils.OmittedFileNotice(p.Filename, p.MediaType)})
		}
	}
	return []json.RawMessage{marshalRaw(codexUserMessage{Role: "user", Content: parts})}
}

func convertCodexAssistantMessage(msg sdk.Message) []json.RawMessage {
	var items []json.RawMessage
	var textParts []codexOutputTextPart
	var reasoningItems []codexReasoningItem
	reasoningIndexByID := map[string]int{}

	for _, part := range msg.Content {
		switch p := part.(type) {
		case sdk.TextPart:
			textParts = append(textParts, codexOutputTextPart{Type: "output_text", Text: p.Text})
		case sdk.ReasoningPart:
			// Same dialect as the public Responses API; anything else cannot be
			// verified here and is dropped rather than re-sent as text.
			if p.Format != sdk.ReasoningFormatOpenAIResponses {
				continue
			}
			id := openaiutil.ReasoningItemID(p.ProviderMetadata)
			if id == "" {
				id = p.ID
			}
			idx, ok := reasoningIndexByID[id]
			if !ok || id == "" {
				reasoningItems = append(reasoningItems, codexReasoningItem{
					Type:             "reasoning",
					ID:               id,
					EncryptedContent: openaiutil.ReasoningEncryptedContent(p.ProviderMetadata),
				})
				idx = len(reasoningItems) - 1
				if id != "" {
					reasoningIndexByID[id] = idx
				}
			}
			if p.Text != "" {
				reasoningItems[idx].Summary = append(reasoningItems[idx].Summary,
					codexReasoningSummaryText{Type: "summary_text", Text: p.Text})
			}
		case sdk.ToolCallPart:
			args, err := json.Marshal(p.Input)
			if err != nil {
				continue
			}
			id := p.ToolCallID
			if id == "" {
				id = generateID()
			}
			items = appendRaw(items, codexFunctionCall{
				Type:      "function_call",
				CallID:    id,
				Name:      p.ToolName,
				Arguments: string(args),
			})
		}
	}

	var prefix []json.RawMessage
	for i := range reasoningItems {
		// Summary is schema-required, so an item that carried only encrypted
		// content still needs the key present.
		if reasoningItems[i].Summary == nil {
			reasoningItems[i].Summary = []codexReasoningSummaryText{}
		}
		prefix = append(prefix, marshalRaw(reasoningItems[i]))
	}
	if len(textParts) > 0 {
		prefix = append(prefix, marshalRaw(codexAssistantMessage{Role: "assistant", Content: textParts}))
	}
	items = append(prefix, items...)
	return items
}

func convertCodexToolResults(msg sdk.Message) []json.RawMessage {
	var items []json.RawMessage
	for _, part := range msg.Content {
		if trp, ok := part.(sdk.ToolResultPart); ok {
			output, _ := json.Marshal(trp.Result)
			items = appendRaw(items, codexFunctionCallOutput{
				Type:   "function_call_output",
				CallID: trp.ToolCallID,
				Output: string(output),
			})
		}
	}
	return items
}

func (p *Provider) authHeaders() map[string]string {
	accountID := p.accountID
	if accountID == "" {
		accountID, _ = accountIDFromToken(p.accessToken)
	}
	headers := map[string]string{
		"Authorization":        "Bearer " + p.accessToken,
		openAIBetaHeader:       openAIBetaValue,
		openAIOriginatorHeader: p.originator,
		"Accept":               "text/event-stream",
	}
	if accountID != "" {
		headers[openAIAccountHeader] = accountID
	}
	return headers
}

func mapCodexFinishReason(incompleteReason string, hasFunctionCall bool) sdk.FinishReason {
	switch incompleteReason {
	case "max_output_tokens":
		return sdk.FinishReasonLength
	case "content_filter":
		return sdk.FinishReasonContentFilter
	case "":
		if hasFunctionCall {
			return sdk.FinishReasonToolCalls
		}
		return sdk.FinishReasonStop
	default:
		if hasFunctionCall {
			return sdk.FinishReasonToolCalls
		}
		return sdk.FinishReasonOther
	}
}

func convertCodexUsage(u *codexUsage) sdk.Usage {
	inputTokens := u.InputTokens
	outputTokens := u.OutputTokens
	cachedTokens := 0
	reasoningTokens := 0
	if u.InputTokensDetails != nil {
		cachedTokens = u.InputTokensDetails.CachedTokens
	}
	if u.OutputTokensDetails != nil {
		reasoningTokens = u.OutputTokensDetails.ReasoningTokens
	}
	return sdk.Usage{
		InputTokens:       inputTokens,
		OutputTokens:      outputTokens,
		TotalTokens:       inputTokens + outputTokens,
		CachedInputTokens: cachedTokens,
		ReasoningTokens:   reasoningTokens,
		InputTokenDetails: sdk.InputTokenDetail{
			CacheReadTokens: cachedTokens,
			NoCacheTokens:   inputTokens - cachedTokens,
		},
		OutputTokenDetails: sdk.OutputTokenDetail{
			ReasoningTokens: reasoningTokens,
		},
	}
}

func marshalRaw(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func appendRaw(dst []json.RawMessage, v any) []json.RawMessage {
	return append(dst, marshalRaw(v))
}

func joinNonEmpty(values ...string) string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value != "" {
			out = append(out, value)
		}
	}
	if len(out) == 0 {
		return "You are a helpful AI assistant."
	}
	return joinWithDoubleNewline(out)
}

func joinWithDoubleNewline(values []string) string {
	if len(values) == 0 {
		return ""
	}
	result := values[0]
	for _, value := range values[1:] {
		result += "\n\n" + value
	}
	return result
}
