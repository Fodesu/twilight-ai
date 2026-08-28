package speech

import (
	"github.com/memohai/twilight/internal/utils"
)

// audioConfig holds OpenRouter audio-speech options extracted from SpeechParams.Config.
//
// OpenRouter routes TTS requests through its chat/completions endpoint using
// the audio modality; there is no dedicated /audio/speech path.
//
// Supported keys:
//   - "model"   (string):  OpenRouter model id, default "openai/gpt-audio-mini"
//   - "voice"   (string):  voice name compatible with the chosen model, default "coral"
//   - "speed"   (float64): speaking rate sent as an extra body field (model-dependent)
type audioConfig struct {
	Model string
	Voice string
	Speed float64
}

func parseConfig(cfg map[string]any) audioConfig {
	ac := audioConfig{
		Model: defaultModel,
		Voice: defaultVoice,
	}
	if cfg == nil {
		return ac
	}
	if v, ok := cfg["model"].(string); ok && v != "" {
		ac.Model = v
	}
	if v, ok := cfg["voice"].(string); ok && v != "" {
		ac.Voice = v
	}
	if v, ok := utils.ToFloat64(cfg["speed"]); ok {
		ac.Speed = v
	}
	return ac
}
