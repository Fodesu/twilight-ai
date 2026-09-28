package codex

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	defaultBaseURL         = "https://chatgpt.com/backend-api"
	defaultOriginator      = "codex_cli_rs"
	openAIBetaHeader       = "OpenAI-Beta"
	openAIBetaValue        = "responses=experimental"
	openAIAccountHeader    = "chatgpt-account-id"
	openAIOriginatorHeader = "originator"
	openAIAuthClaimPath    = "https://api.openai.com/auth"
)

// streamingToolCall accumulates one function call's argument deltas. args
// uses strings.Builder because it grows by one small delta per SSE event;
// instances are always held by pointer (pendingToolCalls map).
type streamingToolCall struct {
	id       string
	name     string
	args     strings.Builder
	finished bool
}

func generateID() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		panic("openai-codex: generateID entropy failure: " + err.Error())
	}
	return fmt.Sprintf("call_%x", b)
}

func accountIDFromToken(token string) (string, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("invalid codex access token")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", fmt.Errorf("decode codex token payload: %w", err)
	}
	var claims struct {
		OpenAIAuth struct {
			ChatGPTAccountID string `json:"chatgpt_account_id"`
		} `json:"https://api.openai.com/auth"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return "", fmt.Errorf("parse codex token payload: %w", err)
	}
	accountID := strings.TrimSpace(claims.OpenAIAuth.ChatGPTAccountID)
	if accountID == "" {
		return "", fmt.Errorf("codex access token missing %s.chatgpt_account_id", openAIAuthClaimPath)
	}
	return accountID, nil
}
