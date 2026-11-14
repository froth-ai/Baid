# GraphCache: rules for Claude Code
Spec files: docs/specs/*.md. 'X tab section N' in a prompt means docs/specs/X.md.

GraphCache is a Rust memory accelerator for AI agents. Specs live in the team doc:
SRS, Architecture and design, Code architecture, Research and reading.

## Git: hard rules
1. NEVER push. Do not run `git push` in any form, `gh pr create`, `gh pr merge`,
   `gh repo sync`, or anything else that sends commits to a remote. The human pushes.
2. NEVER force anything: no `--force`, no `reset --hard`, no `clean -fd`, no branch deletion,
   unless the human asks in this session and names the branch.
3. At the start of every session, run the sync check ("Session start" below) before editing.
4. Work only on a branch named `a/<topic>` or `b/<topic>`. Never commit on `main`.
5. Rebase onto `upstream/main`; never merge `main` into a branch.
6. If a rebase conflicts, stop, run `git rebase --abort`, and report the conflicting files.
   Do not resolve conflicts in files owned by the other person (see CODEOWNERS).
7. End every session with the report in "Session end". The human reads it, then pushes.

## Session start
- `git status` must be clean. If not, stop and report what is uncommitted.
- `git fetch upstream --prune` and `git fetch origin --prune`.
- Report how far `main` and the current branch are behind `upstream/main`, with the new
  commits' one-line summaries and which crates they touch.
- Fast-forward local `main` to `upstream/main` (`git merge --ff-only`). If that fails, stop and report.
- Rebase the working branch onto `upstream/main`, then run `just check`.

## Ownership
- Person A: gc-cache, gc-session, gc-predict, gc-exec, gc-invalidate, gc-server.
- Person B: memharness, gc-adapters, gc-pack, gc-telemetry, gc-mcp, python/, deploy/.
- Shared: gc-core, .github/. Change gc-core only in a PR that does nothing else.
- Do not edit the other person's crates. If you need a change there, write it up in the report.

## Code rules
- Follow the Code architecture tab: layer rules, traits, error types, config.
- `cargo fmt`, `cargo clippy --all-targets -- -D warnings`, `cargo test` must pass before a commit.
- No `unwrap()` or `expect()` outside tests and `main`. No lock held across `.await`.
- Every spawned task takes a CancellationToken.
- Every new behaviour gets a test; every new metric is listed in the report.
- Commits: Conventional Commits, small and focused, e.g. `feat(gc-cache): add RAM tier`.

## Commands
- `just check`  fmt, clippy, tests
- `just up`     start Neo4j, MinIO, Prometheus, Grafana
- `just bench-small`  memharness small run
