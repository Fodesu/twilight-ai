package models_test

import (
	"context"
	"testing"

	"github.com/felinics/twilight/agent/models"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/sdk"
)

// fakeProvider records the model id each request reaches it with.
type fakeProvider struct {
	name string
	seen []string
}

func (p *fakeProvider) Name() string                                    { return p.name }
func (p *fakeProvider) ListModels(context.Context) ([]sdk.Model, error) { return nil, nil }
func (p *fakeProvider) Test(context.Context) *sdk.ProviderTestResult    { return nil }
func (p *fakeProvider) TestModel(context.Context, string) (*sdk.ModelTestResult, error) {
	return nil, nil
}
func (p *fakeProvider) DoGenerate(_ context.Context, req sdk.Request) (sdk.ModelResult, error) {
	p.seen = append(p.seen, req.Model)
	return sdk.ModelResult{Text: "from " + req.Model, FinishReason: sdk.FinishReasonStop}, nil
}
func (p *fakeProvider) DoStream(_ context.Context, req sdk.Request) (<-chan sdk.StreamPart, error) {
	p.seen = append(p.seen, req.Model)
	ch := make(chan sdk.StreamPart, 3)
	ch <- &sdk.TextDeltaPart{Text: "from "}
	ch <- &sdk.TextDeltaPart{Text: req.Model}
	ch <- &sdk.FinishPart{FinishReason: sdk.FinishReasonStop}
	close(ch)
	return ch, nil
}

// The frozen request names the logical ref; the provider receives the
// physical id, on Generate and on Stream alike, and an unknown ref is
// refused at resolution.
func TestCatalogBindsLogicalRefToPhysicalModel(t *testing.T) {
	ctx := context.Background()
	p := &fakeProvider{name: "fake"}
	cat, err := models.FromModels(map[run.ModelRef]*sdk.Model{"fast": {ID: "vendor-small-2", Provider: p}})
	if err != nil {
		t.Fatal(err)
	}
	inv, err := cat.ResolveModel("fast")
	if err != nil {
		t.Fatal(err)
	}
	frozen := sdk.Request{Model: "fast", Messages: []sdk.Message{sdk.UserMessage("hi")}}
	result, err := inv.Generate(ctx, frozen)
	if err != nil || result.Text != "from vendor-small-2" {
		t.Fatalf("generate = %+v, %v", result, err)
	}
	stream, err := inv.(loop.StreamingModelInvoker).Stream(ctx, frozen)
	if err != nil {
		t.Fatal(err)
	}
	for range stream.Parts {
	}
	streamed, err := stream.Result()
	if err != nil || streamed.Text != "from vendor-small-2" {
		t.Fatalf("stream = %+v, %v", streamed, err)
	}
	if len(p.seen) != 2 || p.seen[0] != "vendor-small-2" || p.seen[1] != "vendor-small-2" {
		t.Fatalf("provider saw models %v, want the physical id twice", p.seen)
	}
	if _, err := cat.ResolveModel("slow"); err == nil {
		t.Fatal("unknown ref resolved")
	}
	if refs := cat.Refs(); len(refs) != 1 || refs[0] != "fast" {
		t.Fatalf("refs = %v", refs)
	}
}

// Build reads credentials from the provider's environment variables, shares
// one provider between entries that name the same endpoint, and refuses an
// entry it cannot serve: no credential, unknown kind, duplicate ref.
func TestBuildFromEntries(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "k")
	t.Setenv("OPENAI_BASE_URL", "https://gateway.example/v1")
	t.Setenv("ANTHROPIC_API_KEY", "")
	t.Setenv("ANTHROPIC_AUTH_TOKEN", "")
	cat, err := models.Build([]models.Entry{
		{Ref: "fast", Kind: models.KindOpenAI, Model: "vendor-small"},
		{Ref: "smart", Kind: models.KindOpenAI, Model: "vendor-large"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if refs := cat.Refs(); len(refs) != 2 || refs[0] != "fast" || refs[1] != "smart" {
		t.Fatalf("refs = %v", refs)
	}
	if inv := cat.Invokers(); len(inv) != 2 || inv["fast"] == nil {
		t.Fatalf("invokers = %v", inv)
	}
	for _, tc := range []struct {
		name    string
		entries []models.Entry
	}{
		{"no credential", []models.Entry{{Ref: "a", Kind: models.KindAnthropic, Model: "m"}}},
		{"unknown kind", []models.Entry{{Ref: "a", Kind: "mystery", Model: "m", APIKey: "k"}}},
		{"duplicate ref", []models.Entry{{Ref: "a", Kind: models.KindOpenAI, Model: "m"}, {Ref: "a", Kind: models.KindOpenAI, Model: "n"}}},
		{"missing model", []models.Entry{{Ref: "a", Kind: models.KindOpenAI}}},
	} {
		if _, err := models.Build(tc.entries); err == nil {
			t.Fatalf("%s: build succeeded", tc.name)
		}
	}
}

// A spec is ref=kind:model with an optional @base; credentials never
// appear in it.
func TestParseSpec(t *testing.T) {
	for _, tc := range []struct {
		spec string
		want models.Entry
		bad  bool
	}{
		{"fast=openai:vendor-small", models.Entry{Ref: "fast", Kind: models.KindOpenAI, Model: "vendor-small"}, false},
		{"smart=anthropic:vendor-large@https://gw.example", models.Entry{Ref: "smart", Kind: models.KindAnthropic, Model: "vendor-large", BaseURL: "https://gw.example"}, false},
		{"fast=openai", models.Entry{}, true},
		{"=openai:m", models.Entry{}, true},
		{"fast=mystery:m", models.Entry{}, true},
	} {
		got, err := models.ParseSpec(tc.spec)
		if (err != nil) != tc.bad || got != tc.want {
			t.Fatalf("ParseSpec(%q) = %+v, %v; want %+v bad=%v", tc.spec, got, err, tc.want, tc.bad)
		}
	}
	entries, err := models.ParseSpecs([]string{"fast=openai:a", "", "smart=google:b"})
	if err != nil || len(entries) != 2 {
		t.Fatalf("ParseSpecs = %+v, %v", entries, err)
	}
}
