package sdk

import (
	"context"
	"fmt"
)

// Generate performs exactly one provider model call: the Request is the
// complete input, the ModelResult the complete output. Tool execution and
// approval live outside this call (ExecuteTools, BuildStepMessages).
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
	return hardenResult(&result), nil
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
