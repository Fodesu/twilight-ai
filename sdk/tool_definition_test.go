package sdk

import (
	"encoding/json"
	"testing"
)

func TestNewToolDefinition(t *testing.T) {
	type weatherParams struct {
		City  string `json:"city" jsonschema:"City name"`
		Units string `json:"units,omitempty"`
	}
	def, err := NewToolDefinition[weatherParams]("get_weather", "Current weather")
	if err != nil {
		t.Fatal(err)
	}
	if def.Name != "get_weather" || def.Description != "Current weather" || def.Parameters == nil {
		t.Fatalf("definition = %+v", def)
	}
	raw, err := json.Marshal(def.Parameters)
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Type       string                     `json:"type"`
		Properties map[string]json.RawMessage `json:"properties"`
		Required   []string                   `json:"required"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	if schema.Type != "object" || len(schema.Properties) != 2 || len(schema.Required) != 1 || schema.Required[0] != "city" {
		t.Fatalf("inferred schema = %s", raw)
	}
	if _, err := NewToolDefinition[weatherParams]("", "no name"); err == nil {
		t.Fatal("an unnamed tool was accepted")
	}
}
