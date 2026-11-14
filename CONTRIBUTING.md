# Contributing to GraphCache

Two people build GraphCache: Person A owns the engine and Person B owns the edges and the
evidence (see [.github/CODEOWNERS](.github/CODEOWNERS)). One shared repository is the source
of truth. Each person works in a fork, on short branches, and merges only through reviewed pull
requests. The full plan is in `docs/specs/team-plan.md` section 3.

## Remotes

| Remote | Points to | Who writes to it |
| --- | --- | --- |
| `upstream` | The shared repo | Nobody directly: changes land only by merged pull requests |
| `origin` | Your own fork | You, by `git push` after reading Claude's report |

One-time setup in each clone:

```bash
git clone git@github.com:YOUR_USER/graphcache.git
cd graphcache
git remote add upstream git@github.com:ORG/graphcache.git
git fetch upstream
git branch --set-upstream-to=upstream/main main
```

If you skip forks, `origin` is the shared repo; read `upstream` as `origin` everywhere below.

## Branches

- Branch names start with the owner: `a/<topic>` for Person A, `b/<topic>` for Person B,
  for example `a/cache-ram-tier` or `b/bolt-adapter`.
- Never commit on `main`. It is protected: pull request required, one approval from the
  other person, CI green, linear history.
- A branch lives at most 3 days.

## Rebase, never merge

Bring your branch up to date with `git fetch upstream` and `git rebase upstream/main`. Never
merge `main` into a branch. After a rebase, push with `git push --force-with-lease`, never
`--force`.

## Pull requests

- Keep them small: under about 400 changed lines. Split bigger work.
- Run `just check` (fmt, clippy with `-D warnings`, tests) before opening one.
- The other person reviews every pull request.
- We squash-merge. The PR title becomes the commit message, in Conventional Commits form:
  `feat(gc-cache): add RAM tier`, `fix(gc-exec): ...`, `docs: ...`.
- Fill in the pull request template, including the SRS ids covered.

## Shared code

- `gc-core` and `.github/` belong to both people.
- A change to `gc-core` goes in its own small pull request: `gc-core` plus the compile fixes
  in its callers, nothing else. It needs both approvals. Never mix one into a feature PR.
- Do not edit the other person's crates. If you need a change there, open an issue with the
  **Blocked** template, label it `blocked`, and tag them.

## Claude Code

Claude Code prepares commits locally; the human pushes. It never pushes, never opens or merges
pull requests, and never forces anything. [CLAUDE.md](CLAUDE.md) states the rules and
[.claude/settings.json](.claude/settings.json) blocks those commands at the tool level. Read
Claude's session report and the diff before you push.

## Architecture decisions

A change to a recorded decision needs a new ADR in [docs/adr](docs/adr), copied from
`000-template.md`.
