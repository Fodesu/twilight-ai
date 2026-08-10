package openai

// NormalizeReasoningEffort returns effort unchanged.
//
// Deprecated: this function no longer has any effect and will be removed in the
// next release. Callers should pass the effort straight through.
//
// It previously rewrote "max" into "xhigh" for OpenAI-format endpoints. That
// rewrite encoded a guess about what OpenAI would accept rather than something a
// caller had declared: on the day "max" becomes supported, it would silently
// downgrade every such request, with no diagnostic and no obvious culprit. Effort
// is an open enum precisely so new tiers work without an SDK release, and no
// official provider SDK maintains a tier allow-list. An effort the endpoint does
// not accept is the provider's 400 to return, not the SDK's to predict.
//
// The empty shell remains for one release because the symbol is exported.
func NormalizeReasoningEffort(effort string) string {
	return effort
}
