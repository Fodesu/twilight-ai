package sdk

import "time"

// ResponseMetadata is what the provider said about the reply itself: its id,
// the model that answered, the server's timestamp and any response headers
// a provider chooses to keep. It is always present on a ModelResult; a field
// the wire did not carry stays zero and is omitted from JSON. Nothing here is
// ever constructed from a missing wire field.
type ResponseMetadata struct {
	ID        string            `json:"id,omitempty"`
	ModelID   string            `json:"modelId,omitempty"`
	Timestamp time.Time         `json:"timestamp,omitzero"`
	Headers   map[string]string `json:"headers,omitempty"`
}

// IsZero reports metadata with no field set. encoding/json's omitzero calls
// it, so a reply without metadata omits the whole object.
func (m ResponseMetadata) IsZero() bool {
	return m.ID == "" && m.ModelID == "" && m.Timestamp.IsZero() && len(m.Headers) == 0
}

// clone returns an independent copy in the SDK's one representation: the
// timestamp in UTC, the headers map owned by the copy.
func (m ResponseMetadata) clone() ResponseMetadata {
	if !m.Timestamp.IsZero() {
		m.Timestamp = m.Timestamp.UTC()
	}
	if m.Headers != nil {
		headers := make(map[string]string, len(m.Headers))
		for k, v := range m.Headers {
			headers[k] = v
		}
		m.Headers = headers
	}
	return m
}

// TimestampFromUnix converts a wire timestamp in Unix seconds. Zero means the
// wire carried none and yields the zero time, so a missing field is never
// mistaken for 1970-01-01.
func TimestampFromUnix(seconds int64) time.Time {
	if seconds == 0 {
		return time.Time{}
	}
	return time.Unix(seconds, 0).UTC()
}
