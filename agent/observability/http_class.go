package observability

import (
	"net/http"
	"net/url"
	"strings"
)

// requestClass is a bounded endpoint vocabulary, never a path/ID label. The
// handler's final route is not known on entry, so classify the registered stream
// endpoints by method and path. A rejected stream handshake still belongs to
// stream, not rpc. /metrics is excluded by the caller.
func requestClass(r *http.Request) string {
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
			return "probe"
		}
		// Split before unescaping: an escaped slash inside a Session ID is
		// one ServeMux wildcard, not an extra path component.
		parts := strings.Split(r.URL.EscapedPath(), "/")
		if len(parts) == 4 && parts[2] != "" {
			prefix, _ := url.PathUnescape(parts[1])
			suffix, _ := url.PathUnescape(parts[3])
			if prefix == "sessions" && suffix == "events" {
				return "stream"
			}
		}
	}
	if r.Method == http.MethodPost {
		switch r.URL.Path {
		case "/progress", "/settlements", "/notices":
			return "stream"
		}
	}
	return "rpc"
}
