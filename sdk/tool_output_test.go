package sdk

import (
	"encoding/json"
	"testing"
)

func TestToolOutput(t *testing.T) {
	structured, err := JSONOutput(map[string]any{"temp": 22})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name       string
		out        ToolOutput
		wantJSON   bool
		wantString string
	}{
		{"zero value is empty text", ToolOutput{}, false, ""},
		{"text", TextOutput("sunny"), false, "sunny"},
		{"encoded value", structured, true, `{"temp":22}`},
		{"raw document", RawJSONOutput(json.RawMessage(`{"a":1}`)), true, `{"a":1}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.out.IsJSON() != tc.wantJSON || tc.out.String() != tc.wantString {
				t.Fatalf("IsJSON()=%v String()=%q, want %v %q", tc.out.IsJSON(), tc.out.String(), tc.wantJSON, tc.wantString)
			}
		})
	}
	if _, err := JSONOutput(make(chan int)); err == nil {
		t.Fatal("an unencodable value was accepted")
	}
	raw := json.RawMessage(`{"a":1}`)
	out := RawJSONOutput(raw)
	raw[2] = 'b'
	if out.String() != `{"a":1}` {
		t.Fatal("RawJSONOutput aliased the caller's buffer")
	}
}
