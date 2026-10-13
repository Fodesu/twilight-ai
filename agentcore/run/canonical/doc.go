// Package canonical is the canonical identity of a Run: the digest rules
// (Digests) for every body a fact names or a derived identity covers, and the
// derivation (Identity) of every RunID-scoped identity -- step, call,
// response, effect and command ids. Both are persisted
// semantics: their preimages never change. The state machine of
// agentcore/run is composed with them in agentcore/run/schema. The package
// also declares the envelope types under which agentcore/run/frozen stores
// the bodies these digests name.
package canonical
