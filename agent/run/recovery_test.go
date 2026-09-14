package run

import "testing"

func TestRecoveryDispositionSemantics(t *testing.T) {
	for _, d := range []RecoveryDisposition{RecoveryMissing, RecoveryActive, RecoveryDeferred, RecoveryTerminal} {
		if !d.Valid() {
			t.Errorf("%q is not valid", d)
		}
	}
	if RecoveryMissing.PreservesExecution() {
		t.Fatal("missing execution must permit automatic disposition")
	}
	for _, d := range []RecoveryDisposition{RecoveryActive, RecoveryDeferred, RecoveryTerminal} {
		if !d.PreservesExecution() {
			t.Errorf("%q does not preserve execution", d)
		}
	}
	if d := RecoveryDisposition("unknown"); d.Valid() || d.PreservesExecution() {
		t.Fatalf("unknown disposition accepted: %q", d)
	}
}
