package effect_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/felinics/twilight/agent/run/effect"
)

type statusErr int

func (e statusErr) Error() string   { return "status" }
func (e statusErr) HTTPStatus() int { return int(e) }

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

var _ net.Error = timeoutErr{}

// RUN-EXE-11: provider errors classify by HTTP status, transport errors as
// connection failures, everything else as executor_error; only the rate
// limit, provider outage and connection classes are transient.
func TestClassifyModelError(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		want      effect.FailureCode
		transient bool
	}{
		{"429", statusErr(429), effect.FailureRateLimited, true},
		{"503 wrapped", errors.Join(errors.New("openai"), statusErr(503)), effect.FailureProviderUnavailable, true},
		{"401", statusErr(401), effect.FailureAuthentication, false},
		{"402", statusErr(402), effect.FailureBilling, false},
		{"400", statusErr(400), effect.FailureBadRequest, false},
		{"connection reset", timeoutErr{}, effect.FailureConnection, true},
		{"unexpected eof", io.ErrUnexpectedEOF, effect.FailureConnection, true},
		{"plain error", errors.New("boom"), effect.FailureExecutor, false},
		{"deadline as net error", &net.OpError{Err: context.DeadlineExceeded}, effect.FailureDeadline, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := effect.ClassifyModelError(tc.err)
			if got != tc.want || got.Transient() != tc.transient {
				t.Fatalf("classify = %s (transient %v), want %s (%v)", got, got.Transient(), tc.want, tc.transient)
			}
		})
	}
}
