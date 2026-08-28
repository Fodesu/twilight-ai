package speech

import (
	"strings"

	"github.com/memohai/twilight/internal/utils"
)

// audioConfig holds ElevenLabs-specific TTS options extracted from SpeechParams.Config.
//
// Supported keys:
//   - "api_key"                  (string):  API key; can also be set via WithAPIKey option
//   - "voice_id"                 (string):  voice ID, required
//   - "model_id"                 (string):  model ID, default "eleven_multilingual_v2"
//   - "stability"                (float64): voice stability 0–1, default 0.5
//   - "similarity_boost"         (float64): voice similarity boost 0–1, default 0.75
//   - "style"                    (float64): speaking style intensity 0–1, default 0.0
//   - "use_speaker_boost"        (bool):    speaker boost toggle, default true
//   - "speed"                    (float64): speech rate 0.5–2.0, default 1.0
//   - "output_format"            (string):  output format, default "mp3_44100_128"
//   - "seed"                     (int):     deterministic seed for reproducible output
//   - "apply_text_normalization" (string):  "auto" | "on" | "off"
//   - "language_code"            (string):  BCP-47 language code (e.g. "en-US")
type audioConfig struct {
	VoiceID                string
	ModelID                string
	Stability              float64
	SimilarityBoost        float64
	Style                  float64
	UseSpeakerBoost        bool
	Speed                  float64
	OutputFormat           string
	Seed                   *int
	ApplyTextNormalization string
	LanguageCode           string
}

func parseConfig(cfg map[string]any) audioConfig {
	ac := audioConfig{
		ModelID:         defaultModelLLM,
		Stability:       0.5,
		SimilarityBoost: 0.75,
		Style:           0.0,
		UseSpeakerBoost: true,
		Speed:           1.0,
		OutputFormat:    defaultFormat,
	}
	if cfg == nil {
		return ac
	}
	if v, ok := cfg["voice_id"].(string); ok && v != "" {
		ac.VoiceID = v
	}
	if v, ok := cfg["model_id"].(string); ok && v != "" {
		ac.ModelID = v
	}
	if v, ok := utils.ToFloat64(cfg["stability"]); ok {
		ac.Stability = v
	}
	if v, ok := utils.ToFloat64(cfg["similarity_boost"]); ok {
		ac.SimilarityBoost = v
	}
	if v, ok := utils.ToFloat64(cfg["style"]); ok {
		ac.Style = v
	}
	if v, ok := cfg["use_speaker_boost"].(bool); ok {
		ac.UseSpeakerBoost = v
	}
	if v, ok := utils.ToFloat64(cfg["speed"]); ok {
		ac.Speed = v
	}
	if v, ok := cfg["output_format"].(string); ok && v != "" {
		ac.OutputFormat = v
	}
	if v, ok := utils.ToInt(cfg["seed"]); ok {
		ac.Seed = &v
	}
	if v, ok := cfg["apply_text_normalization"].(string); ok && v != "" {
		ac.ApplyTextNormalization = v
	}
	if v, ok := cfg["language_code"].(string); ok && v != "" {
		ac.LanguageCode = v
	}
	return ac
}

func contentTypeForFormat(format string) string {
	lower := strings.ToLower(format)
	switch {
	case strings.HasPrefix(lower, "mp3"):
		return "audio/mpeg"
	case strings.HasPrefix(lower, "pcm"):
		return "audio/pcm"
	case strings.HasPrefix(lower, "ulaw"):
		return "audio/basic"
	case strings.HasPrefix(lower, "opus"):
		return "audio/opus"
	default:
		return contentTypeAudio
	}
}
