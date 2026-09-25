package models

import (
	"fmt"
	"strings"

	"github.com/felinics/twilight/agentcore/run"
)

// ParseSpec reads one Entry from the form a flag or an environment variable
// carries:
//
//	ref=kind:model
//	ref=kind:model@https://gateway.example/v1
//
// ref is the logical name presets freeze; kind is a Kind; model is the
// provider's model id; the optional @base overrides the provider's base
// URL. Credentials never appear in a spec: they come from the Entry's
// APIKey when a host sets it, or from the environment (Credentials).
func ParseSpec(spec string) (Entry, error) {
	ref, physical, ok := strings.Cut(strings.TrimSpace(spec), "=")
	if !ok || ref == "" || physical == "" {
		return Entry{}, fmt.Errorf("models: spec %q is not ref=kind:model", spec)
	}
	physical, base, _ := strings.Cut(physical, "@")
	kind, model, ok := strings.Cut(physical, ":")
	if !ok || kind == "" || model == "" {
		return Entry{}, fmt.Errorf("models: spec %q is not ref=kind:model", spec)
	}
	if _, known := Credentials[Kind(kind)]; !known {
		return Entry{}, fmt.Errorf("models: spec %q names unsupported kind %q", spec, kind)
	}
	return Entry{Ref: run.ModelRef(ref), Kind: Kind(kind), Model: model, BaseURL: base}, nil
}

// ParseSpecs reads several specs; one bad spec fails the whole list.
func ParseSpecs(specs []string) ([]Entry, error) {
	out := make([]Entry, 0, len(specs))
	for _, s := range specs {
		if strings.TrimSpace(s) == "" {
			continue
		}
		e, err := ParseSpec(s)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}
