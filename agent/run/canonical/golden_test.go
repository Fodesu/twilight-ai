package canonical

import (
	"testing"

	"github.com/felinics/twilight/agent/run"
)

// TestDerivedIdentityGolden freezes every derived identity of RUN-WIR-4 for
// one fixed input set. A drift with a non-empty want means the derivation
// preimage changed: intentional protocol changes must update this fixture and
// agent-run.md; anything else is accidental drift.
func TestDerivedIdentityGolden(t *testing.T) {
	const (
		runID run.RunID          = "run-1"
		step  run.StepID         = "step-1"
		call  run.CallID         = "call-1"
		claim run.ExecutionClaim = "claim-1"
	)
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"model request command", string((IdentityV1{}).DeriveModelRequestCommandID(runID, 7)), "7e42f75006e3271ce7420647345f238ee59e9603d596e7907cb2ccadc281763d"},
		{"takeover claim", string(DeriveTakeoverClaim("sess-1", 2)), "80bec5cf46e2222988b69a7a66c447bf94e39aa0217dd5eb751a75ccb02f7291"},
		{"model step", string((IdentityV1{}).DeriveModelStepID(runID, "cmd-1", "sha256:aa")), "e93e6d7910d106102f93720d5053fb021e5a11889b4f900e648605c2e2e27b60"},
		{"tool call", string((IdentityV1{}).DeriveCallID(step, 0)), "cca7fd2e5a02a565155747fd8c126ac497a425d2f627eb2c77605dc0551aea11"},
		{"tool step", string((IdentityV1{}).DeriveToolStepID(step, "sha256:bb")), "e3d3b8b789d6342ada0a91108759dbc050c9a2b44094e8e7dba6db7e4bdc2e02"},
		{"response id", string((IdentityV1{}).DeriveResponseID(runID, step, call, run.ResponseApproval)), "819924b49cc24df66e97f5821ac80bf286b983e600f20e25f045142c4bc0e77e"},
		{"response command", string((IdentityV1{}).DeriveResponseCommandID(runID, step, call, "resp-1")), "aacf7d247a13e4a696bff6f881ec517a6ad95ae262ec862d7dcf84b80c6bb32b"},
		{"input command", string((IdentityV1{}).DeriveInputCommandID(runID, "in-1")), "ce57abe29d2da34e02cf8fbfb7e4a25086b352096977094e9bed7eb66dd16aad"},
		{"withdraw command", string((IdentityV1{}).DeriveWithdrawCommandID(runID, step)), "582ffed8e0d762e83d68e0eef5c90f123e03787aad8462766667133d15961a8c"},
		{"start command", string((IdentityV1{}).DeriveStartCommandID(runID, step, call, claim)), "ee132f922d31d1f0d17a11bb39f7453246e1c3eac7c9f890173ef16a27684fb2"},
		{"settlement command", string((IdentityV1{}).DeriveSettlementCommandID(runID, step, call, claim)), "312290a27b13d1a49c28f0f8d5dd8cc7c31d5d42a60193bf11dcb20cdd1ca78f"},
		{"model recovery command", string((IdentityV1{}).DeriveModelRecoveryCommandID(runID, step, claim)), "473d80790e1e9e932261d5d96f089bd43df914fec2c8445f86ae2137291f575e"},
		{"tool recovery command", string((IdentityV1{}).DeriveToolRecoveryCommandID(runID, step, call, claim)), "0c2d31c1d90976861be36e69dbad7ea140e2d9ebc32fa21083df2140bcbfe11e"},
	}
	for _, c := range cases {
		if c.got == c.want {
			continue
		}
		if c.want == "" {
			t.Errorf("UNSET %s = %s", c.name, c.got)
			continue
		}
		t.Errorf("golden %s drifted — an intentional derivation change must update this fixture and agent-run.md:\n got: %s\nwant: %s", c.name, c.got, c.want)
	}
}
