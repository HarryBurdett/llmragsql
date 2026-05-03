# KB Update Policy + Pre-Commit Guard — Design Spec

**Status:** Draft for review
**Date:** 2026-05-03
**Author:** Claude (per Charlie Burdett's mandate to fix accumulated issues)

## Goal

Make the project's knowledge-base update rule **mechanically enforceable**, not a thing humans (or Claude) have to remember. Every Opera-related code change must update both the local and central knowledge bases; commits that touch Opera-related files without corresponding KB updates **fail at commit time** with a clear message explaining what to do.

## Why

Today's session exposed a real gap. The CLAUDE.md rule reads:

> When any Opera-related knowledge is learned, corrected, or changed... TWO knowledge bases must be updated: 1. LOCAL: `apps/core/docs/opera_knowledge_base.md`, 2. CENTRAL: `~/opera-knowledge-ref/packages/opera-knowledge/`

But:
- I sometimes updated only one (had to be reminded).
- The directory structure inside central (`schema/` vs `business-rules/` vs `query-patterns/`) is not consistently applied — the right destination is non-obvious.
- The "commit and push" step on central is a separate manual action; can be skipped.
- There's no enforcement, only the rule itself.

Result: rule compliance depends on memory and discipline, exactly the things humans (and AI assistants) fail at under pressure. The fix is to make the rule a **commit gate**, with clear guidance on which directory takes which content.

## Constraints (must hold)

- **Pre-commit hook blocks commits** that change Opera-related Python files without updating either KB.
- **Clear error message:** the hook tells the developer exactly what file in which KB to update.
- **Bypassable for genuine non-KB changes** (typo fixes, formatting) via an explicit `kb-not-required` annotation in the commit message.
- **Directory taxonomy documented** so devs know "this goes in `schema/`, that in `business-rules/`".
- **Central commit + push is part of the rule:** local-only KB update is insufficient. Hook checks both.

## Architecture

```
┌────────────────────────────────────────────────────────┐
│ Developer commits a change to apps/, sql_rag/, api/    │
└──────────────────┬─────────────────────────────────────┘
                   │
                   ▼
┌────────────────────────────────────────────────────────┐
│ pre-commit hook: scripts/kb_update_check.py            │
│   1. Check if staged files include any "Opera-related" │
│      files (configurable allowlist):                   │
│       - apps/bank_reconcile/api/routes.py              │
│       - sql_rag/opera_sql_import.py                    │
│       - sql_rag/opera3_foxpro_import.py                │
│       - sql_rag/bank_import*.py                        │
│       - sql_rag/statement_reconcile*.py                │
│       - etc. (file glob list in config)                │
│   2. If yes, check if the same commit also stages:     │
│       - apps/core/docs/opera_knowledge_base.md OR      │
│       - ~/opera-knowledge-ref/.../*.md                 │
│   3. If neither → check commit message for             │
│       "kb-not-required: <reason>" annotation           │
│   4. If still no → fail with clear message             │
└──────────────────┬─────────────────────────────────────┘
                   │
                   ▼
┌────────────────────────────────────────────────────────┐
│ Hook output (pass case):                               │
│   ✓ Opera-related change detected                      │
│   ✓ KB updated: apps/core/docs/opera_knowledge_base.md │
│   ✓ Central KB updated: ~/.../business-rules/X.md      │
│   ⚠ Reminder: push central repo separately             │
│                                                        │
│ Hook output (fail case):                               │
│   ✗ Opera-related change in: sql_rag/opera_sql_import. │
│     py (matched glob: sql_rag/opera_sql_import.py)     │
│   ✗ No KB updates staged.                              │
│                                                        │
│   You must either:                                     │
│     1. Update apps/core/docs/opera_knowledge_base.md   │
│        AND a file under                                │
│        ~/opera-knowledge-ref/.../packages/opera-       │
│        knowledge/ (per directory taxonomy below).      │
│     2. Or add "kb-not-required: <reason>" to the       │
│        commit message body explaining why this change  │
│        doesn't represent new Opera knowledge.          │
│                                                        │
│   Directory taxonomy:                                  │
│     - schema/        → table/column documentation      │
│     - business-rules/→ posting rules, conventions      │
│     - query-patterns/→ reusable query examples         │
│     - transaction-library/ → snapshot JSON (auto)      │
│                                                        │
│   See: docs/kb-update-guide.md                         │
└────────────────────────────────────────────────────────┘
```

## Components

### 1. `scripts/kb_update_check.py` (new)

Core logic:
- Read git index for staged files (`git diff --cached --name-only`).
- Match against configurable Opera-file allowlist (`scripts/kb_update_allowlist.yaml`).
- If any matched, check whether KB files are also staged.
- If not, parse `git COMMIT_EDITMSG` for `kb-not-required:` annotation.
- Exit 0 (pass), 1 (fail with message), or 2 (config error).

Configuration in `scripts/kb_update_allowlist.yaml`:
```yaml
opera_files:
  # Files where changes typically represent Opera knowledge updates
  - "apps/bank_reconcile/api/routes.py"
  - "sql_rag/opera_sql_import.py"
  - "sql_rag/opera3_foxpro_import.py"
  - "sql_rag/bank_import*.py"
  - "sql_rag/statement_reconcile*.py"
  - "sql_rag/duplicate_check*.py"     # post-spec-3
  - "sql_rag/period_reconciliation*.py"  # post-spec-4

local_kb:
  - "apps/core/docs/opera_knowledge_base.md"
  - "apps/core/docs/opera_transaction_field_reference.md"

# Central KB lives outside this repo; the hook checks the central
# repo's git status for unpushed commits as a secondary signal.
central_kb_repo: "~/opera-knowledge-ref"
central_kb_globs:
  - "packages/opera-knowledge/schema/*.md"
  - "packages/opera-knowledge/business-rules/*.md"
  - "packages/opera-knowledge/query-patterns/*.md"
```

### 2. `.pre-commit-config.yaml` integration

Add the hook:
```yaml
repos:
  - repo: local
    hooks:
      - id: kb-update-check
        name: KB update required for Opera changes
        entry: python scripts/kb_update_check.py
        language: python
        pass_filenames: false
        stages: [commit]
```

### 3. `docs/kb-update-guide.md` (new)

A short, decisive guide to:
- **What counts as Opera knowledge?** (Field semantics, posting patterns, table relationships, business rules. NOT typo fixes, formatting, refactors with no behaviour change.)
- **Local vs central:**
  - Local (`apps/core/docs/opera_knowledge_base.md`) is for project-specific application of Opera knowledge.
  - Central (`~/opera-knowledge-ref/packages/opera-knowledge/`) is the shared canonical knowledge.
  - Both must be updated when knowledge is learned/corrected.
- **Central directory taxonomy** (one paragraph per dir, with examples):
  - `schema/` — pure column listings, types, descriptions.
  - `business-rules/` — posting rules, calculation conventions, sign conventions, locking protocol, sequence-number behaviour.
  - `query-patterns/` — reusable patterns for common operations.
  - `transaction-library/` — auto-generated snapshot JSON (don't edit by hand).
- **The `kb-not-required:` annotation** — when and how to use it. Examples of legitimate uses (typo fix, formatter run, dependency bump in unrelated code that happens to live in the file).
- **The push step** — central must be `git push`-ed; the hook reminds but cannot push for you (network call out of scope for a commit hook).

### 4. CI complement

A workflow at `.github/workflows/kb-update-check.yml`:
- Runs the same `scripts/kb_update_check.py` on every PR commit.
- Catches the case where someone bypassed the local hook (e.g. used `git commit --no-verify`).
- Failure blocks merge.

### 5. Test suite

`tests/test_kb_update_check.py`:
- Stage an Opera file change without KB update → exit 1.
- Stage an Opera file change WITH local KB update → exit 0.
- Stage an Opera file change with `kb-not-required: typo fix` in commit msg → exit 0.
- Stage a non-Opera file change → exit 0 (KB not required).
- Glob matching: stage a new sql_rag/opera_*.py file → caught by glob.

## Data flow

```
1. Dev runs `git commit` after editing sql_rag/opera_sql_import.py
2. pre-commit hook fires
3. kb_update_check.py reads staged files
4. Detects Opera-related change
5. Checks staged files for KB updates → none
6. Reads .git/COMMIT_EDITMSG → no "kb-not-required:"
7. Exits 1, prints clear guidance
8. Dev either updates KB or adds the annotation
9. Re-commits; passes
```

## Error handling

- Allowlist file malformed → exit 2 with line number.
- Git command failure (not a repo, etc.) → propagate with clear message.
- Central repo not found → warning, not failure (dev may not have it cloned; hook is best-effort there). Documented limitation.

## Testing strategy

- Unit tests: simulate staged files via fixtures, assert exit codes and messages.
- Integration test: real git repo with real staged changes.
- Manual verification: try to commit an Opera-touching change without KB update; confirm it's blocked with helpful message.

## Migration plan

1. Write `scripts/kb_update_check.py` + tests (TDD).
2. Write `docs/kb-update-guide.md`.
3. Add to `.pre-commit-config.yaml` (or create one if not present).
4. Run on a test branch — make a deliberate Opera-change commit without KB update; confirm it's blocked.
5. Wire CI workflow.
6. Communicate to all developers: rule is now mechanically enforced; here's the guide.

## Done criteria

- [ ] Hook script exists and passes tests.
- [ ] Pre-commit hook blocks Opera-touching commits without KB updates.
- [ ] CI workflow does the same on PRs.
- [ ] `docs/kb-update-guide.md` exists with directory taxonomy and examples.
- [ ] `kb-not-required:` annotation works for legitimate exceptions.
- [ ] Documentation in central KB README explains the rule.

## Out of scope

- **Auto-generating KB content** from code changes. The hook just enforces "you updated something"; what you wrote is still on you.
- **Pushing the central repo** — out of commit-hook scope (network calls). Reminder text only.
- **Validating KB content quality** (e.g. spell-check, link-check). Different concern.

## Risks / failure modes

- **Annotation abuse:** developers using `kb-not-required:` for changes that DO need KB updates. **Mitigation:** PR review attention to commit messages with that annotation; over time, code-review culture catches abuse.
- **Allowlist drift:** new Opera-related files added to the codebase without being added to the allowlist won't trigger the hook. **Mitigation:** the allowlist itself is in git; PRs adding new Opera-touching files should add to the allowlist (a rule that itself benefits from CI).
- **False positives** for refactors that don't represent new knowledge. **Mitigation:** the `kb-not-required:` escape hatch with a documented reason in the commit body.
