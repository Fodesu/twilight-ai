package sdk

import (
	"reflect"
	"testing"
)

func TestProviderMetadata(t *testing.T) {
	if got := NewProviderMetadata("anthropic", map[string]string{"signature": "", "other": ""}); got != nil {
		t.Fatalf("all-empty values = %v, want nil", got)
	}
	m := NewProviderMetadata("anthropic", map[string]string{"signature": "SIG", "empty": ""})
	if !reflect.DeepEqual(m, ProviderMetadata{"anthropic": {"signature": "SIG"}}) {
		t.Fatalf("NewProviderMetadata = %v", m)
	}
	if m.Get("anthropic", "signature") != "SIG" || m.Get("anthropic", "missing") != "" || m.Get("openai", "x") != "" || ProviderMetadata(nil).Get("a", "b") != "" {
		t.Fatal("Get does not tolerate missing namespaces and keys")
	}

	merged := m.Merge(ProviderMetadata{"anthropic": {"signature": "NEW"}, "openai": {"itemId": "rs_1"}})
	want := ProviderMetadata{"anthropic": {"signature": "NEW"}, "openai": {"itemId": "rs_1"}}
	if !reflect.DeepEqual(merged, want) {
		t.Fatalf("Merge = %v, want %v", merged, want)
	}
	if m.Get("anthropic", "signature") != "SIG" {
		t.Fatal("Merge modified its receiver")
	}
	if ProviderMetadata(nil).Merge(nil) != nil || !reflect.DeepEqual(ProviderMetadata(nil).Merge(m), m) {
		t.Fatal("Merge over nil is not the other operand")
	}

	clone := m.Clone()
	clone["anthropic"]["signature"] = "MUTATED"
	if m.Get("anthropic", "signature") != "SIG" {
		t.Fatal("Clone shares the inner map")
	}
	if ProviderMetadata(nil).Clone() != nil {
		t.Fatal("Clone of nil is not nil")
	}

	values := StringValues(map[string]any{"s": "text", "n": 3, "b": true, "obj": map[string]any{"k": "v"}, "nil": nil})
	wantValues := map[string]string{"s": "text", "n": "3", "b": "true", "obj": `{"k":"v"}`}
	if !reflect.DeepEqual(values, wantValues) {
		t.Fatalf("StringValues = %v, want %v", values, wantValues)
	}
}
