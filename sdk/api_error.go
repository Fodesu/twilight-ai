package sdk

import "fmt"

// APIError is the error twilight's HTTP provider clients return when the
// upstream answers with a non-2xx status. It is a public type so callers
// above the provider can classify a failure (rate limit, authentication
// error, server outage) with errors.As and read StatusCode, instead of
// parsing the message built by Error.
type APIError struct {
	StatusCode int    `json:"status_code"`
	Status     string `json:"status"`
	Message    string `json:"message"`
	RawBody    []byte `json:"-"`
}

func (e *APIError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("api error %d: %s", e.StatusCode, e.Message)
	}
	return fmt.Sprintf("api error %d: %s", e.StatusCode, e.Status)
}

// Detail returns the error message with the raw response body appended
// when available, useful for diagnosing opaque upstream errors like
// "Provider returned error".
func (e *APIError) Detail() string {
	base := e.Error()
	if len(e.RawBody) == 0 {
		return base
	}
	const maxBody = 1024
	body := string(e.RawBody)
	if len(body) > maxBody {
		body = body[:maxBody] + "...(truncated)"
	}
	return fmt.Sprintf("%s [body: %s]", base, body)
}
