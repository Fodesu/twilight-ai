// Package schema is the one place the Run protocol's contracts are bound
// together: the state machine, the fact and command codec, the digest rules,
// the snapshot codec, the identity derivation and the frozen-body codec. They
// are unversioned singletons (RUN-CMT-8): identity derivation and digests are
// persisted semantics and never change under a new number, while the shape
// of persisted facts evolves through each event type's payload version and
// its codec (SES-VER-1, EXT-REG-2).
package schema
