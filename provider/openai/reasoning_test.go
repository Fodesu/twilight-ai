package openai_test

import (
	"testing"

	openai "github.com/memohai/twilight-ai/provider/openai"
)

// TestNormalizeReasoningEffort_IsIdentity pins the shell behaviour: the function
// is retained only because it is exported, and must not transform anything. In
// particular "max" is no longer rewritten to "xhigh".
func TestNormalizeReasoningEffort_IsIdentity(t *testing.T) {
	for _, effort := range []string{
		"max", "MAX", "  max  ", "xhigh", "high", "medium", "low", "minimal", "none", "",
		"some-future-tier",
	} {
		if got := openai.NormalizeReasoningEffort(effort); got != effort {
			t.Errorf("NormalizeReasoningEffort(%q): got %q, want it unchanged", effort, got)
		}
	}
}
