package sdk

import (
	"context"
	"fmt"
)

// Generate performs exactly one provider model call using the provider-neutral
// Request boundary type and returns the single-call ModelResult. It does not
// execute tools or run the legacy multi-step loop.
//
//nolint:gocritic // hugeParam: public single-call API keeps Request as a value DTO for compatibility and copy semantics.
func Generate(ctx context.Context, model *Model, req Request) (ModelResult, error) {
	return defaultClient.Generate(ctx, model, req)
}

// Stream performs exactly one provider streaming model call using the
// provider-neutral Request boundary type. The returned ModelStream assembles
// exactly one ModelResult after Parts is consumed.
//
//nolint:gocritic // hugeParam: public single-call API keeps Request as a value DTO for compatibility and copy semantics.
func Stream(ctx context.Context, model *Model, req Request) (ModelStream, error) {
	return defaultClient.Stream(ctx, model, req)
}

// Generate performs exactly one provider model call using the provider-neutral
// Request boundary type and returns the single-call ModelResult. The supplied
// model provides the provider binding; req.Model must be empty or match
// model.ID.
//
//nolint:gocritic // hugeParam: public single-call API keeps Request as a value DTO for compatibility and copy semantics.
func (c *Client) Generate(ctx context.Context, model *Model, req Request) (ModelResult, error) {
	if model == nil {
		return ModelResult{}, fmt.Errorf("twilightai: model is required")
	}
	return model.Generate(ctx, req)
}

// Stream performs exactly one provider streaming model call using the
// provider-neutral Request boundary type. The supplied model provides the
// provider binding; req.Model must be empty or match model.ID.
//
//nolint:gocritic // hugeParam: public single-call API keeps Request as a value DTO for compatibility and copy semantics.
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
//
//nolint:gocritic // hugeParam: public single-call API keeps Request as a value DTO for compatibility and copy semantics.
func (m *Model) Generate(ctx context.Context, req Request) (ModelResult, error) {
	if m == nil {
		return ModelResult{}, fmt.Errorf("twilightai: model is required")
	}
	if m.Provider == nil {
		return ModelResult{}, fmt.Errorf("twilightai: model %q has no provider", m.ID)
	}
	req, err := bindRequestModel(m, &req)
	if err != nil {
		return ModelResult{}, err
	}
	result, err := m.Provider.DoGenerate(ctx, req)
	if err != nil {
		return ModelResult{}, err
	}
	// A streamed call returns through the same hardening, so both paths answer
	// with the same representation.
	return hardenResult(result), nil
}

// Stream performs exactly one provider streaming model call. Result must be
// called only after the Parts channel is fully consumed.
//
//nolint:gocritic // hugeParam: public single-call API keeps Request as a value DTO for compatibility and copy semantics.
func (m *Model) Stream(ctx context.Context, req Request) (ModelStream, error) {
	if m == nil {
		return ModelStream{}, fmt.Errorf("twilightai: model is required")
	}
	if m.Provider == nil {
		return ModelStream{}, fmt.Errorf("twilightai: model %q has no provider", m.ID)
	}
	req, err := bindRequestModel(m, &req)
	if err != nil {
		return ModelStream{}, err
	}
	parts, err := m.Provider.DoStream(ctx, req)
	if err != nil {
		return ModelStream{}, err
	}
	return assembleStream(ctx, parts), nil
}

func bindRequestModel(model *Model, req *Request) (Request, error) {
	out := *req
	if out.Model == "" {
		out.Model = model.ID
	}
	if model.ID != "" && out.Model != model.ID {
		return Request{}, fmt.Errorf("twilightai: request model %q does not match provider model %q", out.Model, model.ID)
	}
	return out, nil
}
