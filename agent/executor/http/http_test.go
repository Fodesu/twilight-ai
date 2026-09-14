package http_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	executorhttp "github.com/felinics/twilight/agent/executor/http"
	"github.com/felinics/twilight/agent/run/effect"
)

type failingTransport struct{}

func (failingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("connection reset")
}

func TestDispatchTransportFailureIsUnknown(t *testing.T) {
	client := &executorhttp.Client{
		BaseURL: "http://executor.invalid",
		HTTP:    &http.Client{Transport: failingTransport{}},
	}
	err := client.Dispatch(context.Background(), effect.Assignment{})
	if !errors.Is(err, effect.ErrDispatchUnknown) {
		t.Fatalf("dispatch error = %v, want ErrDispatchUnknown", err)
	}
}
