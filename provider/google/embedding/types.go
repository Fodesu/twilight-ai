package embedding

// --- Request types ---

type embedContentRequest struct {
	Model                string  `json:"model"`
	Content              content `json:"content"`
	OutputDimensionality *int    `json:"outputDimensionality,omitempty"`
	TaskType             string  `json:"taskType,omitempty"`
}

type batchEmbedContentsRequest struct {
	Requests []embedContentRequest `json:"requests"`
}

type content struct {
	Role  string        `json:"role,omitempty"`
	Parts []contentPart `json:"parts"`
}

type contentPart struct {
	Text string `json:"text"`
}

// --- Response types ---

type embedContentResponse struct {
	Embedding embeddingValues `json:"embedding"`
}

type batchEmbedContentsResponse struct {
	Embeddings []embeddingValues `json:"embeddings"`
}

type embeddingValues struct {
	Values []float64 `json:"values"`
}
