package observability

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRequestClass(t *testing.T) {
	for _, tc := range []struct{ method, path, want string }{
		{"GET", "/healthz", "probe"}, {"HEAD", "/readyz", "probe"},
		{"POST", "/healthz", "rpc"}, {"GET", "/sessions/s1/events?from=0", "stream"},
		{"HEAD", "/sessions/s1/events", "stream"}, {"GET", "/sessions/a%2Fb/events", "stream"},
		{"GET", "/%73essions/s1/%65vents", "stream"}, {"GET", "/sessions//events", "rpc"},
		{"GET", "/sessions/a/b/events", "rpc"}, {"POST", "/sessions/s1/events", "rpc"},
		{"POST", "/progress", "stream"}, {"POST", "/settlements", "stream"},
		{"POST", "/notices", "stream"}, {"GET", "/notices", "rpc"},
		{"POST", "/dispatch", "rpc"}, {"POST", "/outcome", "rpc"}, {"GET", "/random-path", "rpc"},
	} {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		if got := requestClass(r); got != tc.want {
			t.Errorf("%s %s: %s want %s", tc.method, tc.path, got, tc.want)
		}
	}
}

func TestClassGaugeIsBoundedAndReturnsToZero(t *testing.T) {
	o := quietObserver()
	for _, tc := range []struct{ method, path, class string }{
		{"GET", "/healthz", "probe"}, {"POST", "/settlements", "stream"}, {"POST", "/dispatch", "rpc"},
	} {
		o.HTTP(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if !strings.Contains(scrape(t, o), `twilight_http_requests_in_flight_by_class{class="`+tc.class+`"} 1`) {
				t.Errorf("%s not counted", tc.class)
			}
			w.WriteHeader(204)
		})).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(tc.method, tc.path, nil))
	}
	metrics := scrape(t, o)
	for _, class := range []string{"probe", "rpc", "stream"} {
		if !strings.Contains(metrics, `twilight_http_requests_in_flight_by_class{class="`+class+`"} 0`) {
			t.Errorf("%s gauge leaked", class)
		}
	}
}
