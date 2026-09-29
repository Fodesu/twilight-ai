package ledger

import (
	"testing"
)

// TestKernelExtSlots covers SES-WIR-5: header extension slots are keyed by
// module and hold JSON the kernel does not interpret; a malformed key or
// value is ErrInvalid.
func TestKernelExtSlots(t *testing.T) {
	run := ModuleKey{Source: "twilight", ID: "run"}
	cases := []struct {
		name    string
		ext     Extensions
		invalid bool
	}{
		{name: "absent"},
		{name: "object", ext: Extensions{run: RawValue(`{"k":1}`)}},
		{name: "scalar", ext: Extensions{run: RawValue(`1`)}},
		{name: "two modules", ext: Extensions{run: RawValue(`{}`), {Source: "acme", ID: "audit"}: RawValue(`[1]`)}},
		{name: "empty value", ext: Extensions{run: nil}, invalid: true},
		{name: "not JSON", ext: Extensions{run: RawValue(`{`)}, invalid: true},
		{name: "source with separator", ext: Extensions{{Source: "a/b", ID: "x"}: RawValue(`1`)}, invalid: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			herr := (SegmentHeader{ID: "seg", Ext: tc.ext}).Validate()
			if tc.invalid {
				if herr == nil {
					t.Fatal("invalid ext accepted")
				}
				return
			}
			if herr != nil {
				t.Fatalf("valid ext rejected: %v", herr)
			}
		})
	}
}
