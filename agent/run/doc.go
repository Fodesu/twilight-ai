// Package run defines a Run: one execution attempt with a unique identity that
// is recoverable and never rewritten. It holds the Run's identifiers and value
// types, the MachineState, the sealed command and fact variants, and the legal
// state transitions (Decide, Evolve, CreateGroup) of each schema version.
//
// The Machine computes no digest and derives no identity itself: it is bound
// to the Canonical and Identity rules of the schema version a Run was created
// under, so a Run replays under its own rules whatever the current version is.
// Everything around a Run lives in the subpackages: agent/run/model holds the
// durable model data facts name by digest, agent/run/canonical the digest and
// identity rules, agent/run/wire the persisted fact and command protocol,
// agent/run/frozen the frozen-body protocol, agent/run/schema the version
// binding, agent/run/plan the effect derivation, agent/run/recovery the
// takeover dispositions and agent/run/runtime the RunStore port and commit
// evaluation.
//
// See docs/design/agent-run.md for the governing specification. The in-process
// execution interpreter and its model/tool ports are in agent/run/loop.
package run
