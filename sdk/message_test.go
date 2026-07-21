package sdk_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/memohai/twilight-ai/sdk"
)

func TestDeveloperMessageJSONRoundTrip(t *testing.T) {
	message := sdk.DeveloperMessage("Follow the application policy.")
	if message.Role != sdk.MessageRoleDeveloper {
		t.Fatalf("role: got %q", message.Role)
	}

	data, err := json.Marshal(message)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(data) != `{"role":"developer","content":"Follow the application policy."}` {
		t.Fatalf("json: got %s", data)
	}

	var decoded sdk.Message
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.Role != sdk.MessageRoleDeveloper {
		t.Fatalf("decoded role: got %q", decoded.Role)
	}
}

func TestMessageJSONRejectsUnknownRole(t *testing.T) {
	_, err := json.Marshal(sdk.Message{
		Role:    sdk.MessageRole("operator"),
		Content: []sdk.MessagePart{sdk.TextPart{Text: "policy"}},
	})
	if !errors.Is(err, sdk.ErrInvalidMessageRole) {
		t.Fatalf("Marshal error: got %v, want ErrInvalidMessageRole", err)
	}

	var message sdk.Message
	err = json.Unmarshal([]byte(`{"role":"operator","content":"policy"}`), &message)
	if !errors.Is(err, sdk.ErrInvalidMessageRole) {
		t.Fatalf("Unmarshal error: got %v, want ErrInvalidMessageRole", err)
	}
}

func TestMessageRoleValid(t *testing.T) {
	valid := []sdk.MessageRole{
		sdk.MessageRoleUser,
		sdk.MessageRoleAssistant,
		sdk.MessageRoleSystem,
		sdk.MessageRoleTool,
		sdk.MessageRoleDeveloper,
	}
	for _, role := range valid {
		if !role.Valid() {
			t.Errorf("role %q should be valid", role)
		}
	}
	if sdk.MessageRole("operator").Valid() {
		t.Fatal("unknown role should be invalid")
	}
}
