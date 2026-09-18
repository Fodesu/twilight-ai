// Package recovery derives the takeover dispositions of a Run (RUN-CMT-7):
// the Executing targets of a MachineState and, for each, the recovery command
// with its derived identity. agent/run/reconcile decides which dispositions a
// new owner actually issues.
package recovery
