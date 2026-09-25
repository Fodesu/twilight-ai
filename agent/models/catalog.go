// Package models is the application's model catalog: the table that maps a
// logical run.ModelRef, the name a preset freezes, to a physical model at a
// provider. It is deliberately a lookup and nothing more. Routing between
// providers, fallback, quotas and key rotation are a gateway's job; a
// deployment that wants them points an Entry's BaseURL at one.
//
// The catalog lives in agent/, not agentcore/: Agent Core knows the
// loop.ModelCatalog seam and the frozen ModelRef, and nothing about
// providers or credentials.
package models

import (
	"context"
	"fmt"
	"os"
	"sort"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	anthropic "github.com/felinics/twilight/provider/anthropic/messages"
	copilot "github.com/felinics/twilight/provider/github/copilot"
	google "github.com/felinics/twilight/provider/google/generativeai"
	completions "github.com/felinics/twilight/provider/openai/completions"
	responses "github.com/felinics/twilight/provider/openai/responses"
	"github.com/felinics/twilight/sdk"
)

// Kind names a provider implementation in the tree.
type Kind string

const (
	KindAnthropic       Kind = "anthropic"
	KindOpenAI          Kind = "openai" // chat completions
	KindOpenAIResponses Kind = "openai-responses"
	KindGoogle          Kind = "google"
	KindCopilot         Kind = "copilot"
)

// Entry maps one logical ModelRef to a physical model. BaseURL and APIKey
// are optional: empty values take the provider's conventional environment
// variables (Credentials).
type Entry struct {
	Ref     run.ModelRef
	Kind    Kind
	Model   string
	BaseURL string
	APIKey  string
}

// Credentials are the environment variables an Entry falls back to, by
// Kind; they follow the repository's .env.example.
var Credentials = map[Kind]struct{ Key, Base string }{
	KindAnthropic:       {"ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL"},
	KindOpenAI:          {"OPENAI_API_KEY", "OPENAI_BASE_URL"},
	KindOpenAIResponses: {"OPENAI_API_KEY", "OPENAI_BASE_URL"},
	KindGoogle:          {"GOOGLE_GENERATIVE_AI_API_KEY", "GOOGLE_GENERATIVE_AI_BASE_URL"},
	KindCopilot:         {"GITHUB_COPILOT_TOKEN", "GITHUB_COPILOT_BASE_URL"},
}

// Catalog is loop.ModelCatalog over the built entries.
type Catalog struct {
	invokers map[run.ModelRef]loop.ModelInvoker
}

// Build constructs one provider per distinct (Kind, BaseURL, APIKey) and a
// model per Entry. An Entry without a key, after the environment fallback,
// is an error: a catalog that resolves a model the provider will refuse
// is worse than one that refuses at Build.
func Build(entries []Entry) (*Catalog, error) {
	providers := map[providerKey]sdk.Provider{}
	c := &Catalog{invokers: make(map[run.ModelRef]loop.ModelInvoker, len(entries))}
	for _, e := range entries {
		if e.Ref == "" || e.Model == "" {
			return nil, fmt.Errorf("models: entry %q needs a ref and a model", e.Ref)
		}
		if _, dup := c.invokers[e.Ref]; dup {
			return nil, fmt.Errorf("models: duplicate ref %q", e.Ref)
		}
		e = e.withEnv()
		k := providerKey{e.Kind, e.BaseURL, e.APIKey}
		p, ok := providers[k]
		if !ok {
			var err error
			if p, err = newProvider(e); err != nil {
				return nil, fmt.Errorf("models: %s: %w", e.Ref, err)
			}
			providers[k] = p
		}
		c.invokers[e.Ref] = &invoker{model: &sdk.Model{ID: e.Model, Provider: p, Type: sdk.ModelTypeChat}}
	}
	return c, nil
}

// FromModels builds a catalog over ready sdk.Models, for hosts that
// construct providers themselves and for tests.
func FromModels(models map[run.ModelRef]*sdk.Model) (*Catalog, error) {
	c := &Catalog{invokers: make(map[run.ModelRef]loop.ModelInvoker, len(models))}
	for ref, m := range models {
		if ref == "" || m == nil || m.Provider == nil {
			return nil, fmt.Errorf("models: ref %q needs a model with a provider", ref)
		}
		c.invokers[ref] = &invoker{model: m}
	}
	return c, nil
}

// ResolveModel is loop.ModelCatalog.
func (c *Catalog) ResolveModel(ref run.ModelRef) (loop.ModelInvoker, error) {
	inv, ok := c.invokers[ref]
	if !ok {
		return nil, fmt.Errorf("models: unknown model ref %q", ref)
	}
	return inv, nil
}

// Invokers returns the catalog as the map app.ExecutorConfig.Models takes.
func (c *Catalog) Invokers() map[run.ModelRef]loop.ModelInvoker {
	out := make(map[run.ModelRef]loop.ModelInvoker, len(c.invokers))
	for ref, inv := range c.invokers {
		out[ref] = inv
	}
	return out
}

// Refs lists the logical names the catalog serves, sorted.
func (c *Catalog) Refs() []run.ModelRef {
	refs := make([]run.ModelRef, 0, len(c.invokers))
	for ref := range c.invokers {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i] < refs[j] })
	return refs
}

type providerKey struct {
	kind    Kind
	baseURL string
	apiKey  string
}

func (e Entry) withEnv() Entry {
	env, ok := Credentials[e.Kind]
	if !ok {
		return e
	}
	if e.APIKey == "" {
		e.APIKey = os.Getenv(env.Key)
	}
	if e.BaseURL == "" {
		e.BaseURL = os.Getenv(env.Base)
	}
	return e
}

func newProvider(e Entry) (sdk.Provider, error) {
	bearer := e.Kind == KindAnthropic && os.Getenv("ANTHROPIC_AUTH_TOKEN") != ""
	if e.APIKey == "" && !bearer {
		return nil, fmt.Errorf("no credential for %s (set %s)", e.Kind, Credentials[e.Kind].Key)
	}
	switch e.Kind {
	case KindAnthropic:
		opts := []anthropic.Option{}
		if e.APIKey != "" {
			opts = append(opts, anthropic.WithAPIKey(e.APIKey))
		} else {
			opts = append(opts, anthropic.WithAuthToken(os.Getenv("ANTHROPIC_AUTH_TOKEN")))
		}
		if e.BaseURL != "" {
			opts = append(opts, anthropic.WithBaseURL(e.BaseURL))
		}
		return anthropic.New(opts...), nil
	case KindOpenAI:
		opts := []completions.Option{completions.WithAPIKey(e.APIKey)}
		if e.BaseURL != "" {
			opts = append(opts, completions.WithBaseURL(e.BaseURL))
		}
		return completions.New(opts...), nil
	case KindOpenAIResponses:
		opts := []responses.Option{responses.WithAPIKey(e.APIKey)}
		if e.BaseURL != "" {
			opts = append(opts, responses.WithBaseURL(e.BaseURL))
		}
		return responses.New(opts...), nil
	case KindGoogle:
		opts := []google.Option{google.WithAPIKey(e.APIKey)}
		if e.BaseURL != "" {
			opts = append(opts, google.WithBaseURL(e.BaseURL))
		}
		return google.New(opts...), nil
	case KindCopilot:
		opts := []copilot.Option{copilot.WithGitHubToken(e.APIKey)}
		if e.BaseURL != "" {
			opts = append(opts, copilot.WithBaseURL(e.BaseURL))
		}
		return copilot.New(opts...), nil
	default:
		return nil, fmt.Errorf("unsupported provider kind %q", e.Kind)
	}
}

// invoker is the one adaptation between a frozen request and an sdk.Model:
// the request names the logical ModelRef in its Model field, and sdk.Model
// binds requests to its own physical ID, so the field is cleared and the
// model fills it in. Everything else, Generate and Stream alike, is the
// sdk.Model's.
type invoker struct {
	model *sdk.Model
}

func (i *invoker) Generate(ctx context.Context, req sdk.Request) (sdk.ModelResult, error) { //nolint:gocritic // hugeParam: interface method
	req.Model = ""
	return i.model.Generate(ctx, req)
}

func (i *invoker) Stream(ctx context.Context, req sdk.Request) (sdk.ModelStream, error) { //nolint:gocritic // hugeParam: interface method
	req.Model = ""
	return i.model.Stream(ctx, req)
}

var (
	_ loop.ModelCatalog          = (*Catalog)(nil)
	_ loop.ModelInvoker          = (*invoker)(nil)
	_ loop.StreamingModelInvoker = (*invoker)(nil)
)
