package images

// --- Generation request ---

type generationRequest struct {
	Model             string `json:"model"`
	Prompt            string `json:"prompt"`
	N                 *int   `json:"n,omitempty"`
	Size              string `json:"size,omitempty"`
	Quality           string `json:"quality,omitempty"`
	Style             string `json:"style,omitempty"`              // dall-e-3: "vivid", "natural"
	ResponseFormat    string `json:"response_format,omitempty"`    // dall-e-2/3: "url", "b64_json"
	Background        string `json:"background,omitempty"`         // gpt-image: "transparent", "opaque", "auto"
	OutputFormat      string `json:"output_format,omitempty"`      // gpt-image: "png", "jpeg", "webp"
	OutputCompression *int   `json:"output_compression,omitempty"` // gpt-image, jpeg/webp: 0-100
	Moderation        string `json:"moderation,omitempty"`         // gpt-image: "low", "auto"
	User              string `json:"user,omitempty"`
}

// --- Edit request (JSON mode for GPT Image models) ---

type editRequest struct {
	Model             string     `json:"model"`
	Prompt            string     `json:"prompt"`
	Images            []imageRef `json:"images,omitempty"`
	Mask              *imageRef  `json:"mask,omitempty"`
	N                 *int       `json:"n,omitempty"`
	Size              string     `json:"size,omitempty"`
	Quality           string     `json:"quality,omitempty"`
	Background        string     `json:"background,omitempty"`
	OutputFormat      string     `json:"output_format,omitempty"`
	OutputCompression *int       `json:"output_compression,omitempty"`
	InputFidelity     string     `json:"input_fidelity,omitempty"`
	Moderation        string     `json:"moderation,omitempty"`
	ResponseFormat    string     `json:"response_format,omitempty"`
	User              string     `json:"user,omitempty"`
}

type imageRef struct {
	URL    string `json:"image_url,omitempty"`
	FileID string `json:"file_id,omitempty"`
}

// --- Shared response types ---

type imagesResponse struct {
	Created int64       `json:"created"`
	Data    []imageData `json:"data"`
	Usage   *imageUsage `json:"usage,omitempty"`
}

type imageData struct {
	B64JSON       string `json:"b64_json,omitempty"`
	URL           string `json:"url,omitempty"`
	RevisedPrompt string `json:"revised_prompt,omitempty"`
}

type imageUsage struct {
	TotalTokens       int                     `json:"total_tokens"`
	InputTokens       int                     `json:"input_tokens"`
	OutputTokens      int                     `json:"output_tokens"`
	InputTokenDetails *imageInputTokenDetails `json:"input_tokens_details,omitempty"`
}

type imageInputTokenDetails struct {
	TextTokens  int `json:"text_tokens"`
	ImageTokens int `json:"image_tokens"`
}
