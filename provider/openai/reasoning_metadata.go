package openai

import sdk "github.com/memohai/twilight/sdk"

// The Responses reasoning dialect, shared by the public Responses API and the
// Codex backend: the same API behind a different base URL, producing
// byte-identical reasoning items.
const (
	metadataNamespace           = "openai"
	metadataKeyItemID           = "itemId"
	metadataKeyEncryptedContent = "reasoningEncryptedContent"

	// IncludeReasoningEncryptedContent asks the API to return the encrypted
	// reasoning payload. It is required to replay reasoning when the
	// conversation is not stored server-side.
	IncludeReasoningEncryptedContent = "reasoning.encrypted_content"
)

// ReasoningItemMetadata records what a reasoning item needs on replay: the item
// id, which the API requires, and the encrypted content that carries the
// reasoning state when the conversation is not stored server-side.
//
// The encrypted payload is scoped to the account and endpoint that issued it.
// Replaying it elsewhere fails verification even though the wire shape matches,
// so this metadata travels with the part rather than being reconstructed.
func ReasoningItemMetadata(itemID, encryptedContent string) map[string]any {
	return sdk.ReasoningMetadata(metadataNamespace, map[string]string{
		metadataKeyItemID:           itemID,
		metadataKeyEncryptedContent: encryptedContent,
	})
}

// ReasoningItemID returns the reasoning item id a part must be replayed under.
func ReasoningItemID(meta map[string]any) string {
	return sdk.ReasoningMetadataString(meta, metadataNamespace, metadataKeyItemID)
}

// ReasoningEncryptedContent returns the encrypted reasoning payload.
func ReasoningEncryptedContent(meta map[string]any) string {
	return sdk.ReasoningMetadataString(meta, metadataNamespace, metadataKeyEncryptedContent)
}
