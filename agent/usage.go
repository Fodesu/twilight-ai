package agent

import (
	"github.com/memohai/twilight-ai/sdk"
)

// addUsage accumulates sdk.Usage field by field (spec §3.7.2, ModelStepRejected
// and ModelStepCompleted rows).
func addUsage(a, b sdk.Usage) sdk.Usage {
	return sdk.Usage{
		InputTokens:       a.InputTokens + b.InputTokens,
		OutputTokens:      a.OutputTokens + b.OutputTokens,
		TotalTokens:       a.TotalTokens + b.TotalTokens,
		ReasoningTokens:   a.ReasoningTokens + b.ReasoningTokens,
		CachedInputTokens: a.CachedInputTokens + b.CachedInputTokens,
		InputTokenDetails: sdk.InputTokenDetail{
			NoCacheTokens:      a.InputTokenDetails.NoCacheTokens + b.InputTokenDetails.NoCacheTokens,
			CacheReadTokens:    a.InputTokenDetails.CacheReadTokens + b.InputTokenDetails.CacheReadTokens,
			CacheWriteTokens:   a.InputTokenDetails.CacheWriteTokens + b.InputTokenDetails.CacheWriteTokens,
			CacheWrite5mTokens: a.InputTokenDetails.CacheWrite5mTokens + b.InputTokenDetails.CacheWrite5mTokens,
			CacheWrite1hTokens: a.InputTokenDetails.CacheWrite1hTokens + b.InputTokenDetails.CacheWrite1hTokens,
		},
		OutputTokenDetails: sdk.OutputTokenDetail{
			TextTokens:      a.OutputTokenDetails.TextTokens + b.OutputTokenDetails.TextTokens,
			ReasoningTokens: a.OutputTokenDetails.ReasoningTokens + b.OutputTokenDetails.ReasoningTokens,
		},
	}
}
