# KB Update Guide

Every Opera-related code change must be accompanied by an update to the knowledge base. This is enforced by `scripts/kb_update_check.py` running as a pre-commit hook AND a GitHub Actions PR gate.

## What counts as Opera knowledge?

Opera knowledge is anything that explains:

- **How Opera structures data:** table names, column meanings, types, conventions (e.g. `aentry.ae_value` is in pence; `stran.st_trtype='R'` is a receipt).
- **Posting rules:** which tables get touched in what order for a given transaction type, sign conventions, sequence-number rules, period status, locking protocol.
- **Workflow:** what fields user-facing flows write to, how partial reconciles work, what the matcher uses for candidate selection.
- **Recoveries:** how to clear orphan tmpstats, how to undo a misposting, etc.

NOT Opera knowledge:

- Pure refactors (a function rename without behaviour change).
- Typo fixes in comments.
- Dependency bumps in unrelated code.
- Style-only changes.

When you're not sure — err on the side of updating the KB. Over-documentation is cheap; under-documentation is the bug class we're trying to eliminate.

## Local vs central knowledge bases

| | |
|---|---|
| **Local** (`apps/core/docs/opera_knowledge_base.md`) | Project-specific application of Opera knowledge — how this codebase uses it, references to specific functions/files, examples particular to this app. |
| **Central** (`~/opera-knowledge-ref/packages/opera-knowledge/`) | Canonical shared knowledge across all Opera consumers. Used by other developers and tools. |

Both must be updated. The pre-commit hook can only see the local file (the central repo is outside this checkout); for central, **commit AND push to the shared repo as part of the same change cycle**. Reviewers spot-check that you did.

## Central KB directory taxonomy

The central knowledge repo organises content by category:

| Directory | What goes here |
|---|---|
| `schema/` | Table-level documentation: column names, types, descriptions. One file per table family. |
| `business-rules/` | Posting rules, calculation conventions, sign conventions, locking protocol, sequence numbers, type maps. |
| `query-patterns/` | Reusable patterns for common operations (e.g. how to find an unallocated credit note, how to match a refund). |
| `transaction-library/` | **Auto-managed by the snapshot feature — don't edit by hand.** JSON snapshot files plus the deterministically-rendered `COMPLETE_FIELD_REFERENCE.md`. |

If unsure which directory, default to `business-rules/` for behaviour and `schema/` for structure.

## The `kb-not-required:` annotation

For genuine non-knowledge changes that touch an Opera-allowlisted file, add a line to your commit message body:

```
fix: rename a helper function

The Opera write path in opera_sql_import.py was using a private helper
named _do_thing(); rename to _post_to_aentry() for clarity. No new
Opera knowledge — purely a name change.

kb-not-required: pure rename, no behavioural change to Opera writes.
```

The hook checks that the line starts with `kb-not-required:` (case-insensitive) AND has a non-empty reason after the colon. Empty annotations fail the gate.

**Reviewers: scrutinise PRs that use the annotation.** If the change does represent new knowledge in disguise, request the KB update.

## Pushing the central KB

The hook can't push for you (network calls outside commit-hook scope). After committing locally:

```bash
cd ~/opera-knowledge-ref
git pull --rebase origin main   # always rebase, never lose someone else's work
# Make / verify your KB changes here
git add packages/opera-knowledge/<dir>/<file>.md
git commit -m "Document <topic>"
git push origin main
```

The CI gate runs against the main repo and won't catch a missing central push, so this step is on you.

## Examples

**Good — schema field added:**
- Local: append a "## Sequence Number Behaviour" section.
- Central: edit `business-rules/sequence-numbers.md`.
- Commit message: standard.

**Good — non-knowledge change with annotation:**
- Local: untouched.
- Central: untouched.
- Commit message ends with `kb-not-required: rename a helper function with no behavioural change.`

**Bad — Opera change without KB:**
- Local: untouched.
- Central: untouched.
- Commit message: standard, no annotation.
- → Hook blocks the commit.

## Failure modes and what they mean

```
✗ KB update required for Opera-related changes:
    sql_rag/opera_sql_import.py
```

You touched an Opera-allowlisted file. Choose one:

1. Update `apps/core/docs/opera_knowledge_base.md` with a new section, mirror to central.
2. If genuinely no new knowledge: add `kb-not-required: <specific reason>` to the commit body.
3. (If your change shouldn't actually be in the allowlist) edit `scripts/kb_update_allowlist.yaml` and explain in the commit message — that's also a meta-knowledge change worth a KB note.
