package extension

import (
	"errors"
	"strings"

	"github.com/memohai/twilight/agent/artifact"
	"github.com/memohai/twilight/agent/jsonstable"
)

type Cardinality struct {
	Min uint32
	Max *uint32
}

// BindingExtractor returns every Artifact reference inside a decoded typed
// value, in appearance order (EXT-REF-1).
type BindingExtractor interface {
	BindingIDs(value any) ([]artifact.BindingID, error)
}

// BindingExtractorFunc adapts a function to BindingExtractor.
type BindingExtractorFunc func(value any) ([]artifact.BindingID, error)

func (f BindingExtractorFunc) BindingIDs(value any) ([]artifact.BindingID, error) { return f(value) }

// BindingReferenceDefinition declares where an event may reference Artifacts
// and what admission requires of them.
type BindingReferenceDefinition struct {
	JSONPointer        string
	Extractor          BindingExtractor
	Cardinality        Cardinality
	AllowedSchemes     []artifact.Scheme
	RequiredDurability artifact.Durability
}

func (d *BindingReferenceDefinition) validate() error {
	if (d.JSONPointer == "") == (d.Extractor == nil) {
		return errors.New("binding declaration needs exactly one of JSONPointer or Extractor")
	}
	if d.JSONPointer != "" && !strings.HasPrefix(d.JSONPointer, "/") {
		return errors.New("JSONPointer must start with /")
	}
	if d.Cardinality.Max != nil && *d.Cardinality.Max < d.Cardinality.Min {
		return errors.New("cardinality max below min")
	}
	if d.RequiredDurability.Rank() < artifact.EventBound.Rank() {
		return errors.New("required durability must be at least event_bound")
	}
	return nil
}

// extract returns the references one declaration finds in value/payload.
func (d *BindingReferenceDefinition) extract(value any, payload jsonstable.Value) ([]artifact.BindingID, error) {
	if d.Extractor != nil {
		return d.Extractor.BindingIDs(value)
	}
	node, err := payload.Any()
	if err != nil {
		return nil, err
	}
	for _, seg := range strings.Split(strings.TrimPrefix(d.JSONPointer, "/"), "/") {
		seg = strings.ReplaceAll(strings.ReplaceAll(seg, "~1", "/"), "~0", "~")
		obj, ok := node.(map[string]any)
		if !ok {
			return nil, nil
		}
		node, ok = obj[seg]
		if !ok {
			return nil, nil
		}
	}
	switch v := node.(type) {
	case string:
		return []artifact.BindingID{artifact.BindingID(v)}, nil
	case []any:
		out := make([]artifact.BindingID, 0, len(v))
		for _, item := range v {
			s, ok := item.(string)
			if !ok {
				return nil, errors.New("binding pointer array holds a non-string")
			}
			out = append(out, artifact.BindingID(s))
		}
		return out, nil
	default:
		return nil, errors.New("binding pointer does not address a string or string array")
	}
}
