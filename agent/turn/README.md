# agent/turn (legacy)

This package implements the pre-single-Session-ES Turn coordinator (per-Run
store, FactMapper, MemoryLog). It no longer matches docs/design/agent-turn.md
and is excluded from the default build with the `legacy_turn` build tag until
the new Turn implementation replaces it. Build or test it with
`go test -tags legacy_turn ./agent/turn/...`.
