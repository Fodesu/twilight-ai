package run

import (
	"encoding/json"
	"github.com/felinics/twilight/agent/es"
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
			got, err := es.Canonicalize([]byte(c.in))
			if err != nil {
				t.Fatalf("es.Canonicalize(%q): %v", c.in, err)
			}
			if string(got) != c.want {
				t.Fatalf("es.Canonicalize(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

func TestCanonicalJSONRejects(t *testing.T) {
	for _, in := range []string{
		``, `{"a":1}garbage`, `{bad}`,
		`"\ud800"`, `"\udbff"`, `"\udc00"`, `"\ud800x"`, `"\ud800\u0041"`,
		`{"a":1,"a":2}`, `{"dry_run":true,"dry_run":false}`,
		`{"x":1}]`, `{"a":1}}}`, `[1,2]]`,
		"{\"a\":\"\xff\"}",
	} {
		if _, err := es.Canonicalize([]byte(in)); err == nil {
			t.Fatalf("es.Canonicalize(%q): expected error", in)
		}
	}
}

func TestCanonicalDeterminism(t *testing.T) {
	// Map iteration order must not leak into canonical bytes.
	v := map[string]any{"z": 1, "a": map[string]any{"y": []any{1, "s"}, "b": true}, "m": nil}
	first, err := es.MarshalCanonical(v)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		got, err := es.MarshalCanonical(v)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(first) {
			t.Fatalf("non-deterministic canonical output: %q vs %q", got, first)
		}
	}
}

func TestDigestPreimageCoversSchemaVersion(t *testing.T) {
	cmd := StartToolCall{StepID: "s1", CallID: "c1", Claim: "claim-1"}
	body1, err := es.EncodeTypedPayload(SchemaVersion1, "start_tool_call", cmd)
	if err != nil {
		t.Fatal(err)
	}
	body2, err := es.EncodeTypedPayload(2, "start_tool_call", cmd)
	if err != nil {
		t.Fatal(err)
	}
	if string(body1) == string(body2) {
		t.Fatal("schema version did not affect digest preimage")
	}
}

func TestDeriveStability(t *testing.T) {
	// Fixed inputs must produce fixed outputs across processes; freeze a few.
	id1 := (identityV1{}).DeriveModelRequestCommandID("run-1", 7)
	id2 := (identityV1{}).DeriveModelRequestCommandID("run-1", 7)
	if id1 != id2 {
		t.Fatal("derive is not deterministic")
	}
	if id1 == (identityV1{}).DeriveModelRequestCommandID("run-1", 8) {
		t.Fatal("revision does not separate command IDs")
	}
	if id1 == (identityV1{}).DeriveModelRequestCommandID("run-1", 70) {
		t.Fatal("index does not separate command IDs")
	}
	if id1 == (identityV1{}).DeriveModelRequestCommandID("run-2", 7) {
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
	a := (identityV1{}).DeriveResponseID("r", "s", "c", ResponseApproval)
	b := (identityV1{}).DeriveResponseID("r", "s", "c", ResponseExternal)
	if a == b {
		t.Fatal("response kind does not separate response IDs")
	}
}

func TestDigestBindingCanonicalizesArguments(t *testing.T) {
	d1, err := (canonicalV1{}).DigestToolCallBinding("c1", "sha256:x", DirectExecution, cj(`{"b":1,"a":2}`))
	if err != nil {
		t.Fatal(err)
	}
	d2, err := (canonicalV1{}).DigestToolCallBinding("c1", "sha256:x", DirectExecution, cj(`{ "a" : 2, "b" : 1 }`))
	if err != nil {
		t.Fatal(err)
	}
	if d1 != d2 {
		t.Fatal("argument formatting leaked into binding digest")
	}
	d3, _ := (canonicalV1{}).DigestToolCallBinding("c1", "sha256:x", ApprovalRequired, cj(`{"a":2,"b":1}`))
	if d1 == d3 {
		t.Fatal("policy does not affect binding digest")
	}
	id1, err := (canonicalV1{}).DigestToolCallBinding("c", "", DirectExecution, cj(`{"channel_id":"9007199254740993"}`))
	if err != nil {
		t.Fatal(err)
	}
	id2, err := (canonicalV1{}).DigestToolCallBinding("c", "", DirectExecution, cj(`{"channel_id":"9007199254740992"}`))
	if err != nil {
		t.Fatal(err)
	}
	if id1 == id2 {
		t.Fatal("distinct string identifiers collided in a binding digest")
	}
}

// Golden vectors for the current pre-release SchemaVersion 1. They guard the
// current canonical encoding; update them deliberately when the pre-release
// protocol changes. Once v1 is published, these become permanent fixtures.
func TestSchemaVersion1Golden(t *testing.T) {
	cmd := CancelRun{Reason: ReasonCancelled}
	body, err := es.EncodeTypedPayload(SchemaVersion1, "cancel_run", cmd)
	if err != nil {
		t.Fatal(err)
	}
	wantBody := `v1:10:cancel_run:{"reason":"cancelled"}`
	if string(body) != wantBody {
		t.Fatalf("golden body changed:\n got %q\nwant %q", body, wantBody)
	}

	fact := InputAccepted{Input: AgentInput{ID: "in-1", Digest: "sha256:e7b995efa755c5ff3b84d2188b58cb4ae916a59470eb3761df8a814f11763500"}}
	fbody, err := (wireV1{}).EncodeFact("input_accepted", fact)
	if err != nil {
		t.Fatal(err)
	}
	// The input body is not in the fact (RUN-WIR-4): the Run records the
	// input's identity and content digest, the chatlog holds the body.
	wantFact := `v1:14:input_accepted:{"input":{"digest":"sha256:e7b995efa755c5ff3b84d2188b58cb4ae916a59470eb3761df8a814f11763500","id":"in-1"}}`
	if string(fbody) != wantFact {
		t.Fatalf("golden fact body changed:\n got %q\nwant %q", fbody, wantFact)
	}
}
