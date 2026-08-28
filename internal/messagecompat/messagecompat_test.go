package messagecompat

import (
	"errors"
	"testing"

	"github.com/memohai/twilight/sdk"
)

func TestNormalizeFallbacksAndEscapesSystemXML(t *testing.T) {
	original := []sdk.Message{
		sdk.SystemMessage("root"),
		sdk.UserMessage("question"),
		sdk.SystemMessage("use <safe> & do not close </system>"),
		sdk.DeveloperMessage("developer instruction"),
	}

	got, err := Normalize(original, sdk.MessageRoleCapabilities{})
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if got[0].Role != sdk.MessageRoleSystem {
		t.Fatalf("leading system role: got %q", got[0].Role)
	}
	if got[2].Role != sdk.MessageRoleUser {
		t.Fatalf("mid-conversation system role: got %q", got[2].Role)
	}
	wantXML := "<system>\nuse &lt;safe&gt; &amp; do not close &lt;/system&gt;\n</system>"
	if text := got[2].Content[0].(sdk.TextPart).Text; text != wantXML {
		t.Fatalf("tagged system content:\n got: %q\nwant: %q", text, wantXML)
	}
	if got[3].Role != sdk.MessageRoleUser {
		t.Fatalf("developer fallback role: got %q", got[3].Role)
	}
	if text := got[3].Content[0].(sdk.TextPart).Text; text != "developer instruction" {
		t.Fatalf("developer fallback content: got %q", text)
	}

	if original[2].Role != sdk.MessageRoleSystem || original[2].Content[0].(sdk.TextPart).Text != "use <safe> & do not close </system>" {
		t.Fatal("Normalize mutated the caller's message")
	}
}

func TestNormalizePreservesSupportedInstructionRoles(t *testing.T) {
	messages := []sdk.Message{
		sdk.UserMessage("question"),
		sdk.SystemMessage("runtime changed"),
		sdk.DeveloperMessage("policy"),
	}
	got, err := Normalize(messages, sdk.MessageRoleCapabilities{
		Developer:             true,
		MidConversationSystem: true,
	})
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if got[1].Role != sdk.MessageRoleSystem || got[2].Role != sdk.MessageRoleDeveloper {
		t.Fatalf("roles: got %q, %q", got[1].Role, got[2].Role)
	}
}

func TestNormalizeRejectsInstructionInsideToolExchange(t *testing.T) {
	messages := []sdk.Message{
		{
			Role: sdk.MessageRoleAssistant,
			Content: []sdk.MessagePart{sdk.ToolCallPart{
				ToolCallID: "call-1",
				ToolName:   "lookup",
				Input:      map[string]any{"q": "value"},
			}},
		},
		sdk.SystemMessage("runtime changed"),
		sdk.ToolMessage(sdk.ToolResultPart{ToolCallID: "call-1", ToolName: "lookup", Result: "ok"}),
	}

	_, err := Normalize(messages, sdk.MessageRoleCapabilities{})
	if !errors.Is(err, sdk.ErrInstructionInsideToolExchange) {
		t.Fatalf("error: got %v, want ErrInstructionInsideToolExchange", err)
	}
}

func TestNormalizeRejectsNonTextInstructionContent(t *testing.T) {
	messages := []sdk.Message{{
		Role:    sdk.MessageRoleSystem,
		Content: []sdk.MessagePart{sdk.ImagePart{Image: "https://example.com/image.png"}},
	}}

	_, err := Normalize(messages, sdk.MessageRoleCapabilities{})
	if !errors.Is(err, sdk.ErrInvalidInstructionContent) {
		t.Fatalf("error: got %v, want ErrInvalidInstructionContent", err)
	}
}
