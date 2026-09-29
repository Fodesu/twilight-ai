package preset

import (
	"testing"

	"github.com/felinics/twilight/agentcore/run"
)

// TestPresetDigestGolden freezes the preset digest and its field boundary:
// every frozen decision input changes the digest.
func TestPresetDigestGolden(t *testing.T) {
	base := AgentPreset{Model: "m-1", Prompt: "twilight/decision/prompt/context-v1"}
	d, err := DigestPreset(&base)
	if err != nil {
		t.Fatal(err)
	}
	const want = "sha256:bbc2900dd3cbdec56088512aeab717f31fc7604e5495ac05ec82c0a68f204eef"
	if want == "" {
		t.Errorf("UNSET preset digest = %s", d)
	} else if string(d) != want {
		t.Errorf("golden preset digest drifted — an intentional wire change must update this fixture:\n got: %s\nwant: %s", d, want)
	}

	cases := []struct {
		name   string
		mutate func(*AgentPreset)
	}{
		{"system prompt", func(p *AgentPreset) { p.SystemPrompt = "be brief" }},
		{"builder", func(p *AgentPreset) { p.Prompt = "other/builder" }},
		{"scheduling mode", func(p *AgentPreset) { p.Scheduling.Mode = run.ToolScheduleSequential }},
		{"scheduling bound", func(p *AgentPreset) { p.Scheduling.MaxParallel = 2 }},
		{"malformed retries", func(p *AgentPreset) { p.MalformedRetries = 1 }},
	}
	for _, tc := range cases {
		p := base
		tc.mutate(&p)
		if cd, _ := DigestPreset(&p); cd == d {
			t.Fatalf("%s does not affect the preset digest", tc.name)
		}
	}
}

func TestValidatePreset(t *testing.T) {
	ok := AgentPreset{Model: "m", Prompt: "p"}
	if err := ValidatePreset(&ok); err != nil {
		t.Fatalf("valid preset rejected: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*AgentPreset)
	}{
		{"model", func(p *AgentPreset) { p.Model = "" }},
		{"builder", func(p *AgentPreset) { p.Prompt = "" }},
		{"scheduling mode", func(p *AgentPreset) { p.Scheduling.Mode = "round-robin" }},
		{"scheduling bound", func(p *AgentPreset) { p.Scheduling.MaxParallel = -1 }},
	}
	for _, tc := range cases {
		p := ok
		tc.mutate(&p)
		if err := ValidatePreset(&p); err == nil {
			t.Fatalf("%s: invalid preset accepted", tc.name)
		}
	}
}
