package utils

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/felinics/twilight/sdk"
)

// TestFetchJSONNon2xxExposesAPIError pins the classification contract at the
// layer that defines it: a non-2xx answer from FetchJSON is an *sdk.APIError
// reachable with errors.As, carrying StatusCode and the extracted upstream
// message.
func TestFetchJSONNon2xxExposesAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"message":"rate limited"}}`))
	}))
	defer srv.Close()

	_, err := FetchJSON[map[string]any](context.Background(), srv.Client(), &RequestOptions{
		BaseURL: srv.URL,
		Path:    "/models",
	})
	if err == nil {
		t.Fatal("expected error for 429")
	}
	var apiErr *sdk.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("errors.As(*sdk.APIError) = false, err = %v", err)
	}
	if apiErr.StatusCode != http.StatusTooManyRequests {
		t.Errorf("StatusCode = %d, want %d", apiErr.StatusCode, http.StatusTooManyRequests)
	}
	if apiErr.Message != "rate limited" {
		t.Errorf("Message = %q, want %q", apiErr.Message, "rate limited")
	}
}
