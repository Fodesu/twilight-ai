package chatlog

import (
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/session"
)

// Compaction identifiers cover the complete plan, not only its base digest.
func TestCompactionIDsAreDerived(t *testing.T) {
	base := jsonstable.DigestBytes([]byte("base"))
	plan := CompactionPlan{BaseContextDigest: base, CoveredThrough: ledger.Position{Commit: 4}}
	ckpt, sum, err := compactionIDs("s", plan, "summary")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(ckpt), "ckpt-") || !strings.HasPrefix(string(sum), "sum-") ||
		strings.TrimPrefix(string(ckpt), "ckpt-") != strings.TrimPrefix(string(sum), "sum-") {
		t.Fatalf("ids = %s, %s; want one derived suffix behind both prefixes", ckpt, sum)
	}
	cases := []struct {
		name    string
		sid     session.SessionID
		plan    CompactionPlan
		summary string
		same    bool
	}{
		{"same inputs", "s", plan, "summary", true},
		{"other session", "s2", plan, "summary", false},
		{"other base", "s", CompactionPlan{BaseContextDigest: jsonstable.DigestBytes([]byte("other")), CoveredThrough: plan.CoveredThrough}, "summary", false},
		{"other boundary", "s", CompactionPlan{BaseContextDigest: base, CoveredThrough: ledger.Position{Commit: 5}}, "summary", false},
		{"other retained", "s", CompactionPlan{BaseContextDigest: base, CoveredThrough: plan.CoveredThrough, Retained: []EntryDigestPair{{Kind: EntryInput, ID: "i", Digest: "sha256:i"}}}, "summary", false},
		{"empty retained", "s", CompactionPlan{BaseContextDigest: base, CoveredThrough: plan.CoveredThrough, Retained: []EntryDigestPair{}}, "summary", true},
		{"other summary", "s", plan, "summary 2", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _, err := compactionIDs(tc.sid, tc.plan, tc.summary)
			if err != nil {
				t.Fatal(err)
			}
			if (got == ckpt) != tc.same {
				t.Fatalf("compactionIDs = %s; equal to %s is %v, want %v", got, ckpt, got == ckpt, tc.same)
			}
		})
	}
}
