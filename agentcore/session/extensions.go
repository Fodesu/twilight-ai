package session

import (
	"github.com/felinics/twilight/agentcore/ledger"
)

// The module identity and extension-slot vocabulary is the kernel's
// (agentcore/ledger): every commit-carrying record keys its module slots by
// ModuleKey and the ledger stores every entry opaquely. session aliases it.
type (
	SourceID   = ledger.SourceID
	ModuleID   = ledger.ModuleID
	ModuleKey  = ledger.ModuleKey
	RawValue   = ledger.RawValue
	Extensions = ledger.Extensions
)

// ValidateExtensions checks the shape of an extension map: every key is a
// well-formed ModuleKey and every value is valid, non-empty JSON.
func ValidateExtensions(ext Extensions) error { return ledger.ValidateExtensions(ext) }
