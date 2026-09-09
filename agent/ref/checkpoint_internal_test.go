package ref

import (
	"testing"

	"github.com/felinics/twilight/agent/jsonstable"
	"github.com/felinics/twilight/agent/session/chatlog"
)

func pairEntries() []chatlog.Entry {
	in := chatlog.Input{ID: "in1", Digest: "sha256:in1"}
	a1 := chatlog.Assistant{ID: "a1", TurnID: "t1",
		Parts: chatlog.Parts{chatlog.ToolCallPart{CallID: "c1", Name: "lookup", Input: jsonstable.MustParse(`{}`)}}, Digest: "sha256:a1"}
	r1 := chatlog.ToolResult{ID: "r1", TurnID: "t1", CallID: "c1", Status: chatlog.ToolSuccess, Digest: "sha256:r1"}
	a2 := chatlog.Assistant{ID: "a2", TurnID: "t1", Parts: chatlog.Parts{chatlog.TextPart{Text: "done"}}, Digest: "sha256:a2"}
	return []chatlog.Entry{
		{Kind: chatlog.EntryInput, ID: "in1", Digest: in.Digest, Seq: 1, Input: &in},
		{Kind: chatlog.EntryAssistant, ID: "a1", Digest: a1.Digest, Seq: 2, Assistant: &a1},
		{Kind: chatlog.EntryToolResult, ID: "r1", Digest: r1.Digest, Seq: 3, ToolResult: &r1},
		{Kind: chatlog.EntryAssistant, ID: "a2", Digest: a2.Digest, Seq: 4, Assistant: &a2},
	}
}

// RetainLast expands a window that cuts a tool pair back to the issuing
// assistant, so the retained suffix stays valid provider input (REF-CKP-2).
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
			got := RetainLast(entries, tc.n)
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

// checkRetainClosure rejects a retained set that splits a tool pair in either
// direction and accepts closed sets (REF-CKP-2).
func TestCheckRetainClosure(t *testing.T) {
	entries := pairEntries()
	pair := func(i int) chatlog.EntryDigestPair { return entries[i].Pair() }
	cases := []struct {
		name    string
		retain  []chatlog.EntryDigestPair
		wantErr bool
	}{
		{"closed pair", []chatlog.EntryDigestPair{pair(1), pair(2)}, false},
		{"plain suffix", []chatlog.EntryDigestPair{pair(3)}, false},
		{"result without assistant", []chatlog.EntryDigestPair{pair(2)}, true},
		{"assistant without result", []chatlog.EntryDigestPair{pair(1)}, true},
		{"empty", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkRetainClosure(entries, tc.retain)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
