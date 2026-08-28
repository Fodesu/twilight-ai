package speech

import (
	"strings"

	"github.com/memohai/twilight/internal/utils"
)

// audioConfig holds MiniMax TTS-specific options extracted from SpeechParams.Config.
//
// Supported keys:
//   - "api_key"       (string):  API key; can also be set via WithAPIKey option
//   - "voice_id"      (string):  voice ID, default "English_expressive_narrator"
//   - "model"         (string):  model, default "speech-2.8-hd"
//   - "speed"         (float64): speech rate [0.5, 2.0], default 1.0 (always sent)
//   - "vol"           (float64): volume (0, 10], default 1.0 (always sent)
//   - "pitch"         (int):     pitch adjustment [-12, 12], default 0 (always sent)
//   - "output_format" (string):  output format, default "mp3"
//   - "sample_rate"   (int):     audio sample rate, default 32000
type audioConfig struct {
	VoiceID      string
	Model        string
	Speed        float64
	Vol          float64
	Pitch        int
	OutputFormat string
	SampleRate   int
}

func parseConfig(cfg map[string]any) audioConfig {
	ac := audioConfig{
		VoiceID:      defaultVoiceID,
		Model:        defaultModel,
		Speed:        1.0,
		Vol:          1.0,
		Pitch:        0,
		OutputFormat: defaultFormat,
		SampleRate:   32000,
	}
	if cfg == nil {
		return ac
	}
	if v, ok := cfg["voice_id"].(string); ok && v != "" {
		ac.VoiceID = v
	}
	if v, ok := cfg["model"].(string); ok && v != "" {
		ac.Model = v
	}
	if v, ok := utils.ToFloat64(cfg["speed"]); ok {
		ac.Speed = v
	}
	if v, ok := utils.ToFloat64(cfg["vol"]); ok {
		ac.Vol = v
	}
	if v, ok := utils.ToInt(cfg["pitch"]); ok {
		ac.Pitch = v
	}
	if v, ok := cfg["output_format"].(string); ok && v != "" {
		ac.OutputFormat = v
	}
	if v, ok := utils.ToInt(cfg["sample_rate"]); ok {
		ac.SampleRate = v
	}
	return ac
}

func contentTypeForFormat(format string) string {
	switch strings.ToLower(format) {
	case "mp3":
		return "audio/mpeg"
	case "pcm":
		return "audio/pcm"
	case "flac":
		return "audio/flac"
	case "wav":
		return "audio/wav"
	default:
		return contentTypeAudio
	}
}
