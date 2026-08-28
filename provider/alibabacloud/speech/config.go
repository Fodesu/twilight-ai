package speech

import "github.com/memohai/twilight/internal/utils"

// audioConfig holds DashScope CosyVoice TTS-specific options extracted from SpeechParams.Config.
//
// Supported keys:
//   - "api_key"     (string): API key; can also be set via WithAPIKey option
//   - "model"       (string): CosyVoice model ID, default "cosyvoice-v1"
//   - "voice"       (string): voice ID (system voice or custom clone ID), required
//   - "format"      (string): output audio format, default "mp3"
//   - "sample_rate" (int):    audio sample rate, default 22050
//   - "volume"      (int):    volume 0–100
//   - "rate"        (float64): speech rate multiplier 0.5–2.0
//   - "pitch"       (float64): pitch multiplier 0.5–2.0
type audioConfig struct {
	Model      string
	Voice      string
	Format     string
	SampleRate int
	Volume     int
	Rate       float64
	Pitch      float64
}

func parseConfig(cfg map[string]any) audioConfig {
	ac := audioConfig{
		Model:      defaultModel,
		Format:     defaultFormat,
		SampleRate: defaultSampleRate,
	}
	if cfg == nil {
		return ac
	}
	if v, ok := cfg["model"].(string); ok && v != "" {
		ac.Model = v
	}
	if v, ok := cfg["voice"].(string); ok {
		ac.Voice = v
	}
	if v, ok := cfg["format"].(string); ok && v != "" {
		ac.Format = v
	}
	if v, ok := utils.ToInt(cfg["sample_rate"]); ok {
		ac.SampleRate = v
	}
	if v, ok := utils.ToInt(cfg["volume"]); ok {
		ac.Volume = v
	}
	if v, ok := utils.ToFloat64(cfg["rate"]); ok {
		ac.Rate = v
	}
	if v, ok := utils.ToFloat64(cfg["pitch"]); ok {
		ac.Pitch = v
	}
	return ac
}
