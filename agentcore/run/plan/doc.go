// Package plan derives what a Run's MachineState asks for next and persists
// none of it. Next is the at-most-one pending action of a Run:
// the Loop re-derives the action after every Load. RecoveryTargets,
// RecoveryCommand and RecoveryCommands derive the takeover dispositions of
// the Executing targets: the recovery command of each target with
// its derived identity. agent/run/reconcile decides which dispositions a new
// owner actually issues. Read-only queries over the state live in the run
// package itself; the prompt boundary's input type is prompt.Input.
package plan
