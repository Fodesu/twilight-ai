package turn_test

import (
	"testing"

	"github.com/felinics/twilight/agent/turn"
)

// TestProfileDigestGolden freezes the profile digest and its field boundary
// (TRN-PRF-1): the system prompt is tunable and stays outside the digest;
// planner, policy and workspace refs are decision inputs and change it.
func TestProfileDigestGolden(t *testing.T) {
	base := turn.Profile{SchemaVersion: 1, Model: "m-1", Planner: "twilight/decision/planner/context-v1", Policy: "twilight/decision/policy/default-v1"}
	d, err := turn.DigestProfile(&base)
	if err != nil {
		t.Fatal(err)
	}
	const want = "sha256:f1c3002b434f1954be77444f0b82a691db6208a875de3bedacad393998305a1f"
	if want == "" {
		t.Errorf("UNSET profile digest = %s", d)
	} else if string(d) != want {
		t.Errorf("golden profile digest drifted — an intentional wire change must update this fixture and agent-turn.md TRN-PRF-1:\n got: %s\nwant: %s", d, want)
	}

	prompted := base
	prompted.SystemPrompt = "be brief"
	if pd, _ := turn.DigestProfile(&prompted); pd != d {
		t.Fatal("system prompt leaked into the profile digest")
	}
	cases := []struct {
		name   string
		mutate func(*turn.Profile)
	}{
		{"streaming", func(p *turn.Profile) { p.Streaming = true }},
		{"planner", func(p *turn.Profile) { p.Planner = "other/planner" }},
		{"policy", func(p *turn.Profile) { p.Policy = "other/policy" }},
		{"workspace", func(p *turn.Profile) { p.Workspace = "ws-1" }},
	}
	for _, tc := range cases {
		p := base
		tc.mutate(&p)
		if cd, _ := turn.DigestProfile(&p); cd == d {
			t.Fatalf("%s does not affect the profile digest", tc.name)
		}
	}
}

func TestValidateProfile(t *testing.T) {
	ok := turn.Profile{SchemaVersion: 1, Model: "m", Planner: "p", Policy: "q"}
	if err := turn.ValidateProfile(&ok); err != nil {
		t.Fatalf("valid profile rejected: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*turn.Profile)
	}{
		{"schema", func(p *turn.Profile) { p.SchemaVersion = 0 }},
		{"model", func(p *turn.Profile) { p.Model = "" }},
		{"planner", func(p *turn.Profile) { p.Planner = "" }},
		{"policy", func(p *turn.Profile) { p.Policy = "" }},
	}
	for _, tc := range cases {
		p := ok
		tc.mutate(&p)
		if err := turn.ValidateProfile(&p); err == nil {
			t.Fatalf("%s: missing field accepted", tc.name)
		}
	}
}
