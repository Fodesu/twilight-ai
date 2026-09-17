package compaction_test

import (
	"testing"

	"github.com/felinics/twilight/agent/context/compaction"
	"github.com/felinics/twilight/agent/session/chatlog"
)

func pairEntries() []chatlog.Entry {
	in := chatlog.Input{ID: "in1", Digest: "sha256:in1"}
	a1 := chatlog.Assistant{ID: "a1", TurnID: "t1", StepID: "a1", ResultDigest: "sha256:a1r", CallIDs: []chatlog.CallID{"c1"}, Digest: "sha256:a1"}
	r1 := chatlog.ToolResult{ID: "r1", TurnID: "t1", CallID: "c1", Status: chatlog.ToolSuccess, Source: chatlog.SourceToolOutput, OutputDigest: "sha256:r1o", Digest: "sha256:r1"}
	a2 := chatlog.Assistant{ID: "a2", TurnID: "t1", StepID: "a2", ResultDigest: "sha256:a2r", Digest: "sha256:a2"}
	return []chatlog.Entry{
		{Kind: chatlog.EntryInput, ID: "in1", Digest: in.Digest, Seq: 1, Input: &in},
		{Kind: chatlog.EntryAssistant, ID: "a1", Digest: a1.Digest, Seq: 2, Assistant: &a1},
		{Kind: chatlog.EntryToolResult, ID: "r1", Digest: r1.Digest, Seq: 3, ToolResult: &r1},
		{Kind: chatlog.EntryAssistant, ID: "a2", Digest: a2.Digest, Seq: 4, Assistant: &a2},
	}
}

// RetainLast expands a window that cuts a tool pair back to the issuing
// assistant, so the retained suffix stays valid provider input (APP-CKP-2).
func TestRetainLastPairClosure(t *testing.T) {
	entries := pairEntries()
	cases := []struct {
		name string
		n    int
		want []string
	}{
		{"zero keeps nothing", 0, nil},
		{"suffix without pairs stays as asked", 1, []string{"a2"}},
		{"orphan result pulls in its assistant", 2, []string{"a1", "r1", "a2"}},
		{"window past the start keeps everything", 10, []string{"in1", "a1", "r1", "a2"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := compaction.RetainLast(entries, tc.n)
			if len(got) != len(tc.want) {
				t.Fatalf("retain = %+v, want ids %v", got, tc.want)
			}
			for i := range got {
				if got[i].ID != tc.want[i] {
					t.Fatalf("retain[%d] = %s, want %s", i, got[i].ID, tc.want[i])
				}
			}
		})
	}
}

