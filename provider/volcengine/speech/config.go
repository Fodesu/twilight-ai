package speech

import "github.com/memohai/twilight/internal/utils"

// audioConfig holds Volcengine SAMI TTS-specific options extracted from SpeechParams.Config.
//
// Supported keys:
//   - "access_key"  (string): Volcengine AccessKeyID, required
//   - "secret_key"  (string): Volcengine SecretAccessKey, required
//   - "app_key"     (string): SAMI Application AppKey, required
//   - "speaker"     (string): voice speaker ID, required
//   - "encoding"    (string): output audio format (mp3/wav/aac), default "mp3"
//   - "sample_rate" (int):    audio sample rate, default 24000
//   - "speech_rate" (int):    speech rate [-50,100], default 0
//   - "pitch_rate"  (int):    pitch adjustment [-12,12], default 0
type audioConfig struct {
	Speaker    string
	Encoding   string
	SampleRate int
	SpeechRate int
	PitchRate  int
}

func parseConfig(cfg map[string]any) audioConfig {
	ac := audioConfig{
		Encoding:   defaultEncoding,
		SampleRate: defaultSampleRate,
	}
	if cfg == nil {
		return ac
	}
	if v, ok := cfg["speaker"].(string); ok {
		ac.Speaker = v
	}
	if v, ok := cfg["encoding"].(string); ok && v != "" {
		ac.Encoding = v
	}
	if v, ok := utils.ToInt(cfg["sample_rate"]); ok {
		ac.SampleRate = v
	}
	if v, ok := utils.ToInt(cfg["speech_rate"]); ok {
		ac.SpeechRate = v
	}
	if v, ok := utils.ToInt(cfg["pitch_rate"]); ok {
		ac.PitchRate = v
	}
	return ac
}
