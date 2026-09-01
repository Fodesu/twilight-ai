package run

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestFreezeToolCallInputPreservesMalformedJSONText(t *testing.T) {
	for _, input := range []any{`{"x":`, json.RawMessage(`{"x":`)} {
		got, err := FreezeToolCallInput(input)
		if err != nil {
			t.Fatalf("FreezeToolCallInput(%T): %v", input, err)
		}
		if got.String() != `"{\"x\":"` {
			t.Fatalf("FreezeToolCallInput(%T) = %s", input, got.String())
		}
	}

	for _, input := range []any{string([]byte{0xff}), json.RawMessage{0xff}} {
		if _, err := FreezeToolCallInput(input); err == nil {
			t.Fatalf("FreezeToolCallInput(%T) accepted invalid UTF-8", input)
		}
	}
}

// RFC 8785 appendix test vectors plus structural cases.
func TestCanonicalJSON(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"key sort ascii", `{"b":1,"a":2}`, `{"a":2,"b":1}`},
		{"nested objects", `{"z":{"b":1,"a":[true,null]},"a":"x"}`, `{"a":"x","z":{"a":[true,null],"b":1}}`},
		{"whitespace stripped", "{\n  \"a\" : 1 ,\t\"b\": [ 1 , 2 ]\n}", `{"a":1,"b":[1,2]}`},
		// RFC 8785 §3.2.3: sort by UTF-16 code units — surrogate pairs (𝄞)
		// sort after BMP chars like € and 替.
		{"utf16 order", `{"𝄞":1,"€":2,"replace":3}`, `{"replace":3,"€":2,"𝄞":1}`},
		{"number integer", `{"a":1.0}`, `{"a":1}`},
		{"number negative zero", `{"a":-0}`, `{"a":0}`},
		{"number e-notation collapse", `{"a":1e+3}`, `{"a":1000}`},
		{"number small", `{"a":0.000001}`, `{"a":0.000001}`},
		{"number tiny goes exponential", `{"a":0.0000001}`, `{"a":1e-7}`},
		{"number large stays plain to 1e21", `{"a":100000000000000000000}`, `{"a":100000000000000000000}`},
		{"number 1e21 exponential", `{"a":1e21}`, `{"a":1e+21}`},
		{"number JSONB expanded 1e21", `{"a":1000000000000000000000}`, `{"a":1e+21}`},
		{"number shortest roundtrip", `{"a":0.1}`, `{"a":0.1}`},
		{"string escapes minimal", `{"a":"A\nB\u0041"}`, "{\"a\":\"A\\nBA\"}"},
		{"string control chars", `{"a":"\u0001"}`, "{\"a\":\"\\u0001\"}"},
		{"string unicode passthrough", `{"a":"\u00e9"}`, `{"a":"é"}`},
		{"string surrogate pair", `{"a":"\ud834\udd1e"}`, `{"a":"𝄞"}`},
		{"array order preserved", `[3,1,2]`, `[3,1,2]`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := canonicalJSON([]byte(c.in))
			if err != nil {
				t.Fatalf("canonicalJSON(%q): %v", c.in, err)
			}
			if string(got) != c.want {
				t.Fatalf("canonicalJSON(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

func TestCanonicalJSONRejects(t *testing.T) {
	for _, in := range []string{
		``, `{"a":1}garbage`, `{bad}`,
		`"\ud800"`, `"\udbff"`, `"\udc00"`, `"\ud800x"`, `"\ud800\u0041"`,
	} {
		if _, err := canonicalJSON([]byte(in)); err == nil {
			t.Fatalf("canonicalJSON(%q): expected error", in)
		}
	}
}

func TestCanonicalDeterminism(t *testing.T) {
	// Map iteration order must not leak into canonical bytes.
	v := map[string]any{"z": 1, "a": map[string]any{"y": []any{1, "s"}, "b": true}, "m": nil}
	first, err := marshalCanonical(v)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		got, err := marshalCanonical(v)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(first) {
			t.Fatalf("non-deterministic canonical output: %q vs %q", got, first)
		}
	}
}

func TestDigestCommandIdentity(t *testing.T) {
	cmd := StartToolCall{StepID: "s1", CallID: "c1", Claim: "claim-1"}
	d1, err := DigestCommand("start_tool_call", cmd)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(d1), "sha256:") || len(d1) != len("sha256:")+64 {
		t.Fatalf("bad digest wire form: %s", d1)
	}
	// Same content, same digest.
	d2, _ := DigestCommand("start_tool_call", StartToolCall{StepID: "s1", CallID: "c1", Claim: "claim-1"})
	if d1 != d2 {
		t.Fatal("same command produced different digests")
	}
	// Different content differs.
	d3, _ := DigestCommand("start_tool_call", StartToolCall{StepID: "s1", CallID: "c2", Claim: "claim-1"})
	if d1 == d3 {
		t.Fatal("different commands produced the same digest")
	}
	// Schema version participates in the digest preimage.
	body1, err := encodeEnvelopeBody(SchemaVersion1, "start_tool_call", cmd)
	if err != nil {
		t.Fatal(err)
	}
	body2, err := encodeEnvelopeBody(2, "start_tool_call", cmd)
	if err != nil {
		t.Fatal(err)
	}
	if string(body1) == string(body2) {
		t.Fatal("schema version did not affect digest preimage")
	}
	// Type mismatch is rejected.
	if _, err := DigestCommand("cancel_run", cmd); err == nil {
		t.Fatal("expected type/variant mismatch error")
	}
}

func TestDeriveStability(t *testing.T) {
	// Fixed inputs must produce fixed outputs across processes; freeze a few.
	id1 := DeriveModelRequestCommandID("run-1", 7)
	id2 := DeriveModelRequestCommandID("run-1", 7)
	if id1 != id2 {
		t.Fatal("derive is not deterministic")
	}
	if id1 == DeriveModelRequestCommandID("run-1", 8) {
		t.Fatal("revision does not separate command IDs")
	}
	if id1 == DeriveModelRequestCommandID("run-2", 7) {
		t.Fatal("run does not separate command IDs")
	}
	// Namespaces must not collide even with aligned parts.
	a := namespacedHash("twilight/model-step", "x", "y")
	b := namespacedHash("twilight/tool-step", "x", "y")
	if a == b {
		t.Fatal("namespace does not separate hashes")
	}
	// Length prefixing prevents concatenation collisions.
	c := namespacedHash("n", "ab", "c")
	d := namespacedHash("n", "a", "bc")
	if c == d {
		t.Fatal("part boundaries do not separate hashes")
	}
}

func TestDeriveResponseIDPerKind(t *testing.T) {
	a := DeriveResponseID("r", "s", "c", ResponseApproval)
	b := DeriveResponseID("r", "s", "c", ResponseExternal)
	if a == b {
		t.Fatal("response kind does not separate response IDs")
	}
}

func TestDigestBindingCanonicalizesArguments(t *testing.T) {
	d1, err := digestToolCallBinding("c1", "sha256:x", DirectExecution, cj(`{"b":1,"a":2}`))
	if err != nil {
		t.Fatal(err)
	}
	d2, err := digestToolCallBinding("c1", "sha256:x", DirectExecution, cj(`{ "a" : 2, "b" : 1 }`))
	if err != nil {
		t.Fatal(err)
	}
	if d1 != d2 {
		t.Fatal("argument formatting leaked into binding digest")
	}
	d3, _ := digestToolCallBinding("c1", "sha256:x", ApprovalRequired, cj(`{"a":2,"b":1}`))
	if d1 == d3 {
		t.Fatal("policy does not affect binding digest")
	}
}

// Golden vectors for the current pre-release SchemaVersion 1. They guard the
// current canonical encoding; update them deliberately when the pre-release
// protocol changes. Once v1 is published, these become permanent fixtures.
func TestSchemaVersion1Golden(t *testing.T) {
	cmd := CancelRun{Reason: ReasonCancelled}
	body, err := encodeEnvelopeBody(SchemaVersion1, "cancel_run", cmd)
	if err != nil {
		t.Fatal(err)
	}
	wantBody := `v1:10:cancel_run:{"reason":"cancelled"}`
	if string(body) != wantBody {
		t.Fatalf("golden body changed:\n got %q\nwant %q", body, wantBody)
	}
	d, err := DigestCommand("cancel_run", cmd)
	if err != nil {
		t.Fatal(err)
	}
	// Current pre-release fixture. After publication, a mismatch is a protocol
	// break that invalidates persisted digests and must not update this value.
	const wantDigest = "sha256:a7770a5443f180ec1935bfa4498af75375b8d5f182f239f587917b28b78ee80c"
	if string(d) != wantDigest {
		t.Fatalf("golden digest changed:\n got %s\nwant %s", d, wantDigest)
	}

	fact := InputAccepted{Input: AgentInput{ID: "in-1", Payload: cj(`{"text":"hi"}`)}}
	fbody, err := EncodeFact("input_accepted", fact)
	if err != nil {
		t.Fatal(err)
	}
	wantFact := `v1:14:input_accepted:{"input":{"id":"in-1","payload":{"text":"hi"}}}`
	if string(fbody) != wantFact {
		t.Fatalf("golden fact body changed:\n got %q\nwant %q", fbody, wantFact)
	}
}
