// Package messagecompat normalizes provider-agnostic SDK messages for the
// instruction-role capabilities of a concrete provider protocol.
package messagecompat

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"strings"

	"github.com/memohai/twilight-ai/sdk"
)

// Normalize clones messages and applies provider-specific instruction-role
// fallbacks without mutating the caller's message slice.
func Normalize(messages []sdk.Message, capabilities sdk.MessageRoleCapabilities) ([]sdk.Message, error) {
	out := make([]sdk.Message, 0, len(messages))
	conversationStarted := false
	pendingToolCalls := make(map[string]int)
	pendingAnonymousToolCalls := 0

	for i, message := range messages {
		if !message.Role.Valid() {
			return nil, fmt.Errorf("message %d: %w: %q", i, sdk.ErrInvalidMessageRole, message.Role)
		}

		if isInstruction(message.Role) {
			if err := validateInstructionContent(message); err != nil {
				return nil, fmt.Errorf("message %d: %w", i, err)
			}
			if pendingAnonymousToolCalls > 0 || len(pendingToolCalls) > 0 {
				return nil, fmt.Errorf("message %d: %w", i, sdk.ErrInstructionInsideToolExchange)
			}
		}

		normalized := cloneMessage(message)
		switch message.Role {
		case sdk.MessageRoleDeveloper:
			if !capabilities.Developer {
				normalized.Role = sdk.MessageRoleUser
			}
		case sdk.MessageRoleSystem:
			if conversationStarted && !capabilities.MidConversationSystem {
				normalized = taggedSystemUser(message)
			}
		}

		out = append(out, normalized)
		if normalized.Role == sdk.MessageRoleUser || normalized.Role == sdk.MessageRoleAssistant || normalized.Role == sdk.MessageRoleTool {
			conversationStarted = true
		}

		switch message.Role {
		case sdk.MessageRoleAssistant:
			for _, part := range message.Content {
				call, ok := part.(sdk.ToolCallPart)
				if !ok {
					continue
				}
				if call.ToolCallID == "" {
					pendingAnonymousToolCalls++
					continue
				}
				pendingToolCalls[call.ToolCallID]++
			}
		case sdk.MessageRoleTool:
			for _, part := range message.Content {
				result, ok := part.(sdk.ToolResultPart)
				if !ok {
					continue
				}
				if count := pendingToolCalls[result.ToolCallID]; count > 1 {
					pendingToolCalls[result.ToolCallID] = count - 1
				} else if count == 1 {
					delete(pendingToolCalls, result.ToolCallID)
				} else if result.ToolCallID == "" && pendingAnonymousToolCalls > 0 {
					pendingAnonymousToolCalls--
				}
			}
		}
	}

	return out, nil
}

// InstructionText joins the text parts of a validated instruction message.
func InstructionText(message sdk.Message) string {
	texts := make([]string, 0, len(message.Content))
	for _, part := range message.Content {
		if text, ok := part.(sdk.TextPart); ok {
			texts = append(texts, text.Text)
		}
	}
	return strings.Join(texts, "\n")
}

func isInstruction(role sdk.MessageRole) bool {
	return role == sdk.MessageRoleSystem || role == sdk.MessageRoleDeveloper
}

func validateInstructionContent(message sdk.Message) error {
	for _, part := range message.Content {
		if _, ok := part.(sdk.TextPart); !ok {
			return fmt.Errorf("%w: role %q contains %q", sdk.ErrInvalidInstructionContent, message.Role, part.PartType())
		}
	}
	return nil
}

func taggedSystemUser(message sdk.Message) sdk.Message {
	var escaped bytes.Buffer
	_ = xml.EscapeText(&escaped, []byte(InstructionText(message)))
	return sdk.Message{
		Role: sdk.MessageRoleUser,
		Content: []sdk.MessagePart{sdk.TextPart{
			Text: "<system>\n" + escaped.String() + "\n</system>",
		}},
		Usage: message.Usage,
	}
}

func cloneMessage(message sdk.Message) sdk.Message {
	cloned := message
	cloned.Content = append([]sdk.MessagePart(nil), message.Content...)
	return cloned
}
