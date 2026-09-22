// Package canonical is the canonical identity of a Run under one schema
// version: the digest rules (V1) for every body a fact names or a derived
// identity covers, and the derivation (IdentityV1) of every RunID-scoped
// identity -- step, call, response and command ids (RUN-WIR-3, RUN-WIR-4).
//
// The Machine of agent/run is bound to these rules by agent/run/schema; a
// later version that changes any preimage adds its own implementation and
// leaves V1 in place for the replay of v1 Runs. The package also declares the
// envelope types under which agent/run/frozen stores the bodies these digests
// name.
package canonical
