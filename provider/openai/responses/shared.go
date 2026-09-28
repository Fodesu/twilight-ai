package responses

import (
	"crypto/rand"
	"fmt"
	"strings"
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
		panic("openai-responses: generateID entropy failure: " + err.Error())
	}
	return fmt.Sprintf("call_%x", b)
}
