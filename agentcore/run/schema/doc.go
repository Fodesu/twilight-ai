// Package schema binds one version number to the contracts frozen at a Run's
// creation (RUN-CMT-8): the Machine, the wire codec, the digest rules, the
// snapshot codec, the identity derivation and the frozen-body codec. A caller
// selects a version once, with V1 or For(version), and the bound Schema
// threads the number through everything else.
package schema
