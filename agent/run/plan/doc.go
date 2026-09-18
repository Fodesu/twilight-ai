// Package plan derives the at-most-one pending effect of a Run from its
// MachineState (RUN-MCH-4): Next, and the read-only queries WaitingCalls,
// ExecutingCalls and NeedsRecovery the application inspects when Next has no
// executable effect. Effects are never persisted; the Loop re-derives them
// after every Load.
package plan
