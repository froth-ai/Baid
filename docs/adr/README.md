# Architecture decision records

Each ADR names the trade-off it accepts, so a later contributor can tell whether the reason
still holds before reversing it. New ADRs copy [000-template.md](000-template.md) and take the
next number; a changed decision gets a new ADR that supersedes the old one.

| ADR | Decision | Status |
| --- | --- | --- |
| [001](001-layer-not-db.md) | GraphCache is a layer, not a database | Accepted |
| [002](002-rust-with-tokio.md) | Rust with tokio | Accepted |
| [003](003-statistical-predictor-first.md) | Statistical predictor first; no LLM on the request path | Accepted |
| [004](004-write-through-invalidation.md) | Write-through invalidation with entity versions, TTL as backstop | Accepted |
| [005](005-foyer-cache-tiers.md) | foyer for the RAM and NVMe tiers | Proposed |
| [006](006-mcp-main-surface.md) | MCP server as the main adoption surface | Accepted |
| [007](007-one-adapter-trait.md) | One adapter trait with capability flags | Accepted |
| [008](008-tenant-from-api-key.md) | Tenant comes from the API key, never the request body | Accepted |
| [009](009-fail-open.md) | Fail open on any internal error | Accepted |
| [010](010-apache-2-core.md) | Apache-2.0 core; paid managed service later | Proposed |
