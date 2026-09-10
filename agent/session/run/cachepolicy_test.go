package runmod

import (
	"testing"

	"github.com/felinics/twilight/agent/session"
	"github.com/felinics/twilight/agent/session/extension"
)

// WriterCachePolicy is the module's answer to "who may cache this projection":
// the Writer may cache every projection except the machine projection, whose
// checkpoints the Runtime owns (RUN-CMT-2), and the interval is the
// deployment's, not a constant of this module.
func TestWriterCachePolicyExcludesTheMachineProjection(t *testing.T) {
	other := extension.ProjectionID("twilight/chatlog/surface")
	for _, every := range []session.Seq{1, extension.DefaultCacheEvery, 4096} {
		policy := WriterCachePolicy(every)
		// The interval has elapsed for any of these, so only the exclusion can
		// decline.
		head := session.Head{Next: every + 1}
		if policy(MachineProjectionID, MachineProjection.Version, head, session.Head{}, false) {
			t.Errorf("every=%d: the Writer would cache the machine projection", every)
		}
		if policy(MachineProjectionID, MachineProjection.Version, head, session.Head{}, true) {
			t.Errorf("every=%d: the Writer would cache the machine projection at Close", every)
		}
		if !policy(other, 1, head, session.Head{}, false) {
			t.Errorf("every=%d: another projection is not cached once the interval elapsed", every)
		}
	}
}

// The interval is honoured, including the zero value standing in for the
// default, so a deployment that passes no interval still gets bounded work.
func TestWriterCachePolicyHonoursTheInterval(t *testing.T) {
	other := extension.ProjectionID("twilight/chatlog/surface")
	cases := []struct {
		every   session.Seq
		head    session.Seq
		covered session.Seq
		want    bool
	}{
		{every: 8, head: 7, covered: 0, want: false},
		{every: 8, head: 8, covered: 0, want: true},
		{every: 8, head: 12, covered: 5, want: false},
		{every: 8, head: 13, covered: 5, want: true},
		{every: 0, head: extension.DefaultCacheEvery - 1, covered: 0, want: false},
		{every: 0, head: extension.DefaultCacheEvery, covered: 0, want: true},
	}
	for _, tc := range cases {
		got := WriterCachePolicy(tc.every)(other, 1, session.Head{Next: tc.head}, session.Head{Next: tc.covered}, false)
		if got != tc.want {
			t.Errorf("every=%d head=%d covered=%d: got %v, want %v", tc.every, tc.head, tc.covered, got, tc.want)
		}
	}
}
