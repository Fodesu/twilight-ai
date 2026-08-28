package sdk

import (
	"context"
	"fmt"
)

// ModelInvoker is the Request/ModelResult boundary for one model invocation.
// Existing Provider implementations do not need to implement it; Model.Generate
// falls back to Provider.DoGenerate via adapters.
type ModelInvoker interface {
	Generate(context.Context, Request) (ModelResult, error)
}

// StreamingModelInvoker is the streaming counterpart of ModelInvoker. Existing
// providers can continue implementing DoStream.
type StreamingModelInvoker interface {
	Stream(context.Context, Request) (ModelStream, error)
}

// Generate performs exactly one provider model call using the provider-neutral
// Request boundary type and returns the single-call ModelResult. It does not
// execute tools or run the legacy multi-step loop.
func Generate(ctx context.Context, model *Model, req Request) (ModelResult, error) {
	return defaultClient.Generate(ctx, model, req)
}

// Stream performs exactly one provider streaming model call using the
// provider-neutral Request boundary type. The returned ModelStream assembles
// exactly one ModelResult after Parts is consumed.
func Stream(ctx context.Context, model *Model, req Request) (ModelStream, error) {
	return defaultClient.Stream(ctx, model, req)
}

// Generate performs exactly one provider model call using the provider-neutral
// Request boundary type and returns the single-call ModelResult. The supplied
// model provides the provider binding; req.Model must be empty or match
// model.ID.
func (c *Client) Generate(ctx context.Context, model *Model, req Request) (ModelResult, error) {
	if model == nil {
		return ModelResult{}, fmt.Errorf("twilightai: model is required")
	}
	return model.Generate(ctx, req)
}

// Stream performs exactly one provider streaming model call using the
// provider-neutral Request boundary type. The supplied model provides the
// provider binding; req.Model must be empty or match model.ID.
func (c *Client) Stream(ctx context.Context, model *Model, req Request) (ModelStream, error) {
	if model == nil {
		return ModelStream{}, fmt.Errorf("twilightai: model is required")
	}
	return model.Stream(ctx, req)
}

// Generate performs exactly one provider model call using the provider-neutral
// Request boundary type and returns the single-call ModelResult. It is the
// non-legacy text-generation boundary: tool execution and approval orchestration
// live outside this call.
func (m *Model) Generate(ctx context.Context, req Request) (ModelResult, error) {
	if m == nil {
		return ModelResult{}, fmt.Errorf("twilightai: model is required")
	}
	if m.Provider == nil {
		return ModelResult{}, fmt.Errorf("twilightai: model %q has no provider", m.ID)
	}
	req, err := bindRequestModel(m, req)
	if err != nil {
		return ModelResult{}, err
	}
	if provider, ok := m.Provider.(ModelInvoker); ok {
		return provider.Generate(ctx, req)
	}
	params, err := GenerateParamsFromRequest(m, req)
	if err != nil {
		return ModelResult{}, err
	}
	result, err := m.Provider.DoGenerate(ctx, params)
	if err != nil {
		return ModelResult{}, err
	}
	return ModelResultFromGenerateResult(result), nil
}

// Stream performs exactly one provider streaming model call. Result must be
// called only after the Parts channel is fully consumed.
func (m *Model) Stream(ctx context.Context, req Request) (ModelStream, error) {
	if m == nil {
		return ModelStream{}, fmt.Errorf("twilightai: model is required")
	}
	if m.Provider == nil {
		return ModelStream{}, fmt.Errorf("twilightai: model %q has no provider", m.ID)
	}
	req, err := bindRequestModel(m, req)
	if err != nil {
		return ModelStream{}, err
	}
	if provider, ok := m.Provider.(StreamingModelInvoker); ok {
		return provider.Stream(ctx, req)
	}
	params, err := GenerateParamsFromRequest(m, req)
	if err != nil {
		return ModelStream{}, err
	}
	stream, err := m.Provider.DoStream(ctx, params)
	if err != nil {
		return ModelStream{}, err
	}
	return ModelStreamFromStreamResult(stream), nil
}

func bindRequestModel(model *Model, req Request) (Request, error) {
	if req.Model == "" {
		req.Model = model.ID
	}
	if model.ID != "" && req.Model != model.ID {
		return Request{}, fmt.Errorf("twilightai: request model %q does not match provider model %q", req.Model, model.ID)
	}
	return req, nil
}
