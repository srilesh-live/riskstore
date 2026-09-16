# Riskstore — working notes for Claude

## Plan files

Write implementation plans to **`riskstore-perfbench/docs/`** with a descriptive filename, e.g. `perfbench-spec-and-build-plan.md`.

Claude Code's plan mode writes its own working copy to a harness-assigned path under `~/.claude/plans/` with an auto-generated slug name (`i-am-the-technical-giggly-cocoa.md` and similar). That path is not configurable and is outside the repo, so it is scratch. **The copy in `riskstore-perfbench/docs/` is canonical** — sync it there before finishing, or the work is invisible to everyone else and is not version controlled.

## This repo is public

`srilesh-live/riskstore` is a **public** GitHub repo, published deliberately. Everything committed is world-readable and indexed immediately; deleting later does not undo publication.

- The ClickHouse endpoints in `riskstore-perfbench/harness.yaml` (`ch-replica-1.example.internal` and similar) are **placeholders standing in for real internal hosts**. Never restore real hostnames, internal domains, IPs or credentials.
- A placeholder that looks wrong is intentional, not a bug to fix.
- Scan for internal identifiers before committing.
- Keep personal and unrelated documents out of the repo entirely.

## Layout

- `riskstore-perfbench/` — the ClickHouse performance and stress test harness (Java 21 + Muserver). See its own `README.md`.
- `docs/reference/` — extracted implementation specs from the source repos, produced by the `extract-feature` skill.
