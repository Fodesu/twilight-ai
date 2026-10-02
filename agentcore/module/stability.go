package module

import (
	"fmt"

	"github.com/felinics/twilight/agentcore/ledger"
)

// Prerelease says whether the wire shapes the registry writes are committed.
// While it is true nothing has shipped: a module changes a shape in place at
// version 1 and no event type carries a codec history, so NoHistory holds
// for every first-party module. Turning it false is the release: from then
// on a shape that changes keeps the codec of the shape it replaces and
// moves up one version.
const Prerelease = true

// NoHistory reports the first event of m that has a codec history: more than
// one codec, or a write version other than the first. A zero Version means
// what it means to the registry, the highest codec key. nil means every
// event is at its first and only shape.
func NoHistory(m *ModuleDescriptor) error {
	for i := range m.Events {
		def := &m.Events[i]
		if len(def.Codecs) == 1 && def.Codecs[1] != nil && (def.Version == 0 || def.Version == 1) {
			continue
		}
		return &ledger.Error{Code: ledger.CodeInvalid, Type: def.Type,
			Detail: fmt.Sprintf("version %d with %d codecs; a prerelease module writes version 1 with one codec", def.Version, len(def.Codecs))}
	}
	return nil
}
