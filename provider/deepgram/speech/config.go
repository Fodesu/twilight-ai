package speech

import "github.com/memohai/twilight/internal/utils"

// audioConfig holds Deepgram TTS-specific options extracted from SpeechParams.Config.
//
// Supported keys:
//   - "api_key"     (string): API key; can also be set via WithAPIKey option
//   - "model"       (string): voice model, default "aura-2-asteria-en"
//   - "encoding"    (string): audio encoding (linear16/mulaw/alaw)
//   - "sample_rate" (int):    audio sample rate in Hz
//   - "container"   (string): container format (wav/none)
type audioConfig struct {
	Model      string
	Encoding   string
	SampleRate int
	Container  string
}

func parseConfig(cfg map[string]any) audioConfig {
	ac := audioConfig{
		Model: defaultVoiceModel,
	}
	if cfg == nil {
		return ac
	}
	if v, ok := cfg["model"].(string); ok && v != "" {
		ac.Model = v
	}
	if v, ok := cfg["encoding"].(string); ok {
		ac.Encoding = v
	}
	if v, ok := utils.ToInt(cfg["sample_rate"]); ok {
		ac.SampleRate = v
	}
	if v, ok := cfg["container"].(string); ok {
		ac.Container = v
	}
	return ac
}

func contentTypeForEncoding(encoding, container string) string {
	if container == "wav" {
		return "audio/wav"
	}
	switch encoding {
	case "linear16":
		return "audio/l16"
	case "mulaw":
		return "audio/basic"
	case "alaw":
		return "audio/alaw"
	default:
		return contentTypeAudio
	}
}
