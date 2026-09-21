package sdk

import "github.com/google/jsonschema-go/jsonschema"

type FinishReason string

const (
	FinishReasonStop          FinishReason = "stop"
	FinishReasonLength        FinishReason = "length"
	FinishReasonContentFilter FinishReason = "content-filter"
	FinishReasonToolCalls     FinishReason = "tool-calls"
	FinishReasonError         FinishReason = "error"
	FinishReasonOther         FinishReason = "other"
	FinishReasonUnknown       FinishReason = "unknown"
)

type ResponseFormatType string

const (
	ResponseFormatText       ResponseFormatType = "text"
	ResponseFormatJSONObject ResponseFormatType = "json_object"
	ResponseFormatJSONSchema ResponseFormatType = "json_schema"
)

type ResponseFormat struct {
	Type       ResponseFormatType `json:"type"`
	JSONSchema *jsonschema.Schema `json:"jsonSchema,omitempty"`
}
