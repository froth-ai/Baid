# GraphCache

GraphCache sits between AI agents and whatever memory backend they already use. It prefetches
memory while the LLM is thinking and trims what comes back to a token budget, so agent tasks
run faster and cheaper with no database change. It is not a database and never holds
source-of-truth data: everything it caches can be dropped and rebuilt.

**Status: pre-alpha, not usable yet.** The crates in this repository are stubs; nothing runs.

- Specs: [docs/specs](docs/specs)
- Architecture decisions: [docs/adr](docs/adr)
- How we work: [CONTRIBUTING.md](CONTRIBUTING.md)
- Reporting security issues: [SECURITY.md](SECURITY.md)

## Licence

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
