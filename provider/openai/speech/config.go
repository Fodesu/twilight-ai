package speech

import (
	"strings"

	"github.com/memohai/twilight/internal/utils"
)

// audioConfig holds the OpenAI TTS-specific options extracted from SpeechParams.Config.
//
// Supported keys:
//   - "api_key"          (string): API key; can also be set via WithAPIKey option
//   - "voice"            (string): voice ID, default "coral"
//   - "response_format"  (string): "mp3" | "opus" | "pcm" | "wav", default "mp3"
//   - "speed"            (float64): speech rate; sent when non-zero
//   - "instructions"     (string): style instructions, only sent for gpt-4o-mini-tts models
type audioConfig struct {
	Voice          string
	ResponseFormat string
	Speed          float64
	Instructions   string
}

func parseConfig(cfg map[string]any) audioConfig {
	ac := audioConfig{
		Voice:          defaultVoice,
		ResponseFormat: defaultFormat,
	}
	if cfg == nil {
		return ac
	}
	if v, ok := cfg["voice"].(string); ok && v != "" {
		ac.Voice = v
	}
	if v, ok := cfg["response_format"].(string); ok && v != "" {
		ac.ResponseFormat = v
	}
	if v, ok := utils.ToFloat64(cfg["speed"]); ok {
		ac.Speed = v
	}
	if v, ok := cfg["instructions"].(string); ok {
		ac.Instructions = v
	}
	return ac
}

func contentTypeForFormat(format string) string {
	switch strings.ToLower(format) {
	case "mp3":
		return "audio/mpeg"
	case "opus":
		return "audio/opus"
	case "wav":
		return "audio/wav"
	case "pcm":
		return "audio/pcm"
	default:
		return contentTypeAudio
	}
}
