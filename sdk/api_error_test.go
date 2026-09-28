package sdk_test

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/felinics/twilight/sdk"
)

func TestAPIErrorError(t *testing.T) {
	tests := []struct {
		name string
		err  *sdk.APIError
		want string
	}{
		{
			name: "message wins",
			err:  &sdk.APIError{StatusCode: 429, Status: "429 Too Many Requests", Message: "rate limited"},
			want: "api error 429: rate limited",
		},
		{
			name: "status text fallback",
			err:  &sdk.APIError{StatusCode: 502, Status: "502 Bad Gateway"},
			want: "api error 502: Bad Gateway",
		},
		{
			name: "status line fallback for unknown code",
			err:  &sdk.APIError{StatusCode: 599, Status: "599 Custom"},
			want: "api error 599: 599 Custom",
		},
		{
			name: "body appended",
			err:  &sdk.APIError{StatusCode: 400, Message: "bad request", RawBody: []byte(`{"error":"x"}`)},
			want: `api error 400: bad request [body: {"error":"x"}]`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.want {
				t.Errorf("Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAPIErrorErrorBodyTruncation(t *testing.T) {
	err := &sdk.APIError{StatusCode: 502, RawBody: []byte(strings.Repeat("a", 2000))}
	got := err.Error()
	const prefix = "api error 502: Bad Gateway [body: "
	if !strings.HasPrefix(got, prefix) {
		t.Fatalf("Error() = %q, want prefix %q", got, prefix)
	}
	if !strings.Contains(got, "...(truncated)") {
		t.Errorf("Error() = %q, want truncation marker", got)
	}
	excerpt := strings.TrimSuffix(strings.TrimPrefix(got, prefix), "...(truncated)]")
	if len(excerpt) != 1024 {
		t.Errorf("excerpt length = %d, want 1024", len(excerpt))
	}
}

func TestAPIErrorErrorBodyTruncationUTF8(t *testing.T) {
	t.Run("rune straddling cutoff is dropped", func(t *testing.T) {
		// "中" is 3 bytes: a byte cut at 1024 would land inside it.
		body := append([]byte(strings.Repeat("a", 1022)), []byte("中文")...)
		got := (&sdk.APIError{StatusCode: 500, RawBody: body}).Error()
		if !utf8.ValidString(got) {
			t.Errorf("Error() contains invalid UTF-8: %q", got)
		}
	})
	t.Run("pre-existing invalid byte is kept", func(t *testing.T) {
		// A latin-1/GBK error page must not trigger over-truncation: the
		// excerpt keeps its full byte budget even with a bad byte in it.
		body := append([]byte{0xff}, []byte(strings.Repeat("a", 2000))...)
		got := (&sdk.APIError{StatusCode: 500, RawBody: body}).Error()
		const marker = " [body: "
		i := strings.Index(got, marker)
		if i < 0 {
			t.Fatalf("Error() = %q, want body excerpt", got)
		}
		excerpt := strings.TrimSuffix(got[i+len(marker):], "...(truncated)]")
		if len(excerpt) != 1024 {
			t.Errorf("excerpt length = %d, want 1024", len(excerpt))
		}
	})
}

func TestNewAPIError(t *testing.T) {
	tests := []struct {
		name    string
		body    []byte
		wantMsg string
	}{
		{name: "error-wrapped message", body: []byte(`{"error":{"message":"bad"}}`), wantMsg: "bad"},
		{name: "plain message", body: []byte(`{"message":"bad"}`), wantMsg: "bad"},
		{name: "non-json body", body: []byte("<html>proxy error</html>"), wantMsg: ""},
		{name: "trailing junk after json", body: []byte(`{"message":"bad"}<html>`), wantMsg: "bad"},
		{name: "empty body", body: nil, wantMsg: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := sdk.NewAPIError(400, "400 Bad Request", tt.body)
			if e.StatusCode != 400 || e.Status != "400 Bad Request" {
				t.Errorf("got (StatusCode=%d, Status=%q)", e.StatusCode, e.Status)
			}
			if e.Message != tt.wantMsg {
				t.Errorf("Message = %q, want %q", e.Message, tt.wantMsg)
			}
		})
	}
}

func TestNewAPIErrorBodyCap(t *testing.T) {
	e := sdk.NewAPIError(500, "500 Internal Server Error", make([]byte, 70<<10))
	if len(e.RawBody) != 64<<10 {
		t.Errorf("RawBody length = %d, want %d", len(e.RawBody), 64<<10)
	}
}
