package ref_test

import (
	"testing"

	"github.com/felinics/twilight/agent/ref"
)

// TestProfileDigestGolden freezes the profile digest and its field boundary:
// the system prompt is tunable and stays outside the digest (REF-BND-1).
func TestProfileDigestGolden(t *testing.T) {
	base := ref.Profile{SchemaVersion: 1, Model: "m-1"}
	d, err := ref.DigestProfile(&base)
	if err != nil {
		t.Fatal(err)
	}
	const want = "sha256:2f39ad1ba37be6d6ad66f6b2de213f14907a96d8061c1ac2c17e1f2b599a04a1"
	if want == "" {
		t.Errorf("UNSET profile digest = %s", d)
	} else if string(d) != want {
		t.Errorf("golden profile digest drifted — an intentional wire change must update this fixture and agent-reference-assembly.md:\n got: %s\nwant: %s", d, want)
	}

	prompted := base
	prompted.SystemPrompt = "be brief"
	pd, err := ref.DigestProfile(&prompted)
	if err != nil {
		t.Fatal(err)
	}
	if pd != d {
		t.Fatal("system prompt leaked into the profile digest")
	}

	streaming := base
	streaming.Streaming = true
	sd, err := ref.DigestProfile(&streaming)
	if err != nil {
		t.Fatal(err)
	}
	if sd == d {
		t.Fatal("streaming does not affect the profile digest")
	}
}
